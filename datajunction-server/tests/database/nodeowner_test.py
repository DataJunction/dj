import pytest
import pytest_asyncio
from sqlalchemy import select

from datajunction_server.database.node import Node, NodeType, NodeRevision, Column
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.database.nodeowner import NodeOwner
from sqlalchemy.ext.asyncio import AsyncSession

import datajunction_server.sql.parsing.types as ct


@pytest_asyncio.fixture
async def user(session: AsyncSession) -> User:
    """
    A user fixture.
    """
    user = User(
        username="testuser",
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(user)
    await session.commit()
    return user


@pytest_asyncio.fixture
async def another_user(session: AsyncSession) -> User:
    """
    Another user fixture.
    """
    user = User(
        username="anotheruser",
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(user)
    await session.commit()
    return user


@pytest_asyncio.fixture
async def transform_node(session: AsyncSession, user: User) -> Node:
    """
    A transform node fixture.
    """
    node = Node(
        name="basic.users",
        type=NodeType.TRANSFORM,
        current_version="v1",
        display_name="Users Transform",
        created_by_id=user.id,
    )
    node_revision = NodeRevision(
        node=node,
        name=node.name,
        type=node.type,
        version="v1",
        query="SELECT user_id, username FROM users",
        columns=[
            Column(name="user_id", display_name="ID", type=ct.IntegerType()),
            Column(name="username", type=ct.StringType()),
        ],
        created_by_id=user.id,
    )
    session.add_all([node, node_revision])
    await session.commit()
    return node


@pytest.mark.asyncio
async def test_create_node_owner(
    session: AsyncSession,
    user: User,
    transform_node: Node,
):
    # This node initially has no owners
    await session.refresh(transform_node, ["owners"])
    assert transform_node.owners == []

    # Assign ownership to the user
    owner_association = NodeOwner(
        user_id=user.id,
        node_id=transform_node.id,
        ownership_type="domain",
    )
    session.add(owner_association)
    await session.commit()
    await session.refresh(owner_association)
    await session.refresh(user)
    await session.refresh(transform_node)

    # Check that the ownership relationship was created correctly
    assert owner_association.user_id == user.id
    assert owner_association.node_id == transform_node.id
    assert owner_association.ownership_type == "domain"

    # Test relationships
    assert owner_association.user.username == user.username
    assert owner_association.node.name == transform_node.name

    await session.refresh(transform_node, ["owners", "owner_associations"])
    assert transform_node.owners == [user]
    assert transform_node.owner_associations == [owner_association]

    await session.refresh(user, ["owned_nodes", "owned_associations"])
    assert user.owned_nodes == [transform_node]
    assert user.owned_associations == [owner_association]


@pytest.mark.asyncio
async def test_remove_node_owner(
    session: AsyncSession,
    user: User,
    transform_node: Node,
):
    # Add the ownership association
    owner_association = NodeOwner(
        user_id=user.id,
        node_id=transform_node.id,
        ownership_type="domain",
    )
    session.add(owner_association)
    await session.commit()

    # Confirm ownership exists
    await session.refresh(transform_node, ["owners", "owner_associations"])
    assert transform_node.owners == [user]
    assert transform_node.owner_associations == [owner_association]

    await session.refresh(user, ["owned_nodes", "owned_associations"])
    assert user.owned_nodes == [transform_node]
    assert user.owned_associations == [owner_association]

    # Delete the association
    await session.delete(owner_association)
    await session.commit()

    # Refresh and confirm it's removed
    await session.refresh(transform_node)
    await session.refresh(user)

    await session.refresh(transform_node, ["owners", "owner_associations"])
    assert transform_node.owners == []
    assert transform_node.owner_associations == []

    await session.refresh(user, ["owned_nodes", "owned_associations"])
    assert user.owned_nodes == []
    assert user.owned_associations == []

    # Optional: double-check DB-level deletion
    result = await session.execute(
        select(NodeOwner).where(
            NodeOwner.user_id == user.id,
            NodeOwner.node_id == transform_node.id,
        ),
    )
    assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_add_multiple_node_owners(
    session: AsyncSession,
    user: User,
    another_user: User,
    transform_node: Node,
):
    # Initially, no owners
    await session.refresh(transform_node, ["owners", "owner_associations"])
    assert transform_node.owners == []

    # Create multiple associations
    owner_1 = NodeOwner(
        user_id=user.id,
        node_id=transform_node.id,
        ownership_type="domain",
    )
    owner_2 = NodeOwner(
        user_id=another_user.id,
        node_id=transform_node.id,
        ownership_type="technical",
    )

    session.add_all([owner_1, owner_2])
    await session.commit()

    # Refresh node and users
    await session.refresh(transform_node, ["owners", "owner_associations"])
    await session.refresh(user, ["owned_nodes", "owned_associations"])
    await session.refresh(another_user, ["owned_nodes", "owned_associations"])

    # Confirm relationships on node
    owner_usernames = {u.username for u in transform_node.owners}
    assert owner_usernames == {user.username, another_user.username}
    assert len(transform_node.owner_associations) == 2

    # Confirm relationships on users
    assert transform_node in user.owned_nodes
    assert transform_node in another_user.owned_nodes
    assert len(user.owned_associations) == 1
    assert len(another_user.owned_associations) == 1

    # Confirm ownership types
    types = {assoc.ownership_type for assoc in transform_node.owner_associations}
    assert types == {"domain", "technical"}
