import pytest
import pytest_asyncio
from sqlalchemy import select

from datajunction_server.database.node import Node, NodeType, NodeRevision, Column
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.database.history import History
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.api.helpers import get_save_history
from datajunction_server.internal.nodes import update_owners
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
async def test_update_node_owners(
    session: AsyncSession,
    user: User,
    another_user: User,
    transform_node: Node,
):
    await session.refresh(transform_node, ["owners"])
    transform_node.owners = [user]
    session.add(transform_node)
    await session.commit()

    # Change the owner to another_user
    await update_owners(
        session=session,
        node=transform_node,
        new_owner_usernames=[another_user.username],
        current_user=user,
        save_history=(await get_save_history(notify=lambda event: None)),
    )

    # Refresh node and check updated owners
    await session.refresh(transform_node, ["owners"])
    owner_usernames = {owner.username for owner in transform_node.owners}
    assert owner_usernames == {another_user.username}

    # Check history was recorded
    history_entries = await session.execute(
        select(History).where(History.node == transform_node.name),
    )
    history = history_entries.scalars().all()
    assert any(
        "old_owners" in h.details and h.details["old_owners"] == [user.username]
        for h in history
    )
    assert any(
        "new_owners" in h.details and another_user.username in h.details["new_owners"]
        for h in history
    )


@pytest.mark.asyncio
async def test_update_node_owners_multiple_owners(
    session: AsyncSession,
    user: User,
    another_user: User,
    transform_node: Node,
):
    await session.refresh(transform_node, ["owners"])
    transform_node.owners = [user]
    await session.commit()

    # Add multiple owners
    await update_owners(
        session=session,
        node=transform_node,
        new_owner_usernames=[user.username, another_user.username],
        current_user=user,
        save_history=(await get_save_history(notify=lambda event: None)),
    )
    await session.refresh(transform_node, ["owners"])
    usernames = {owner.username for owner in transform_node.owners}
    assert usernames == {user.username, another_user.username}


@pytest.mark.asyncio
async def test_update_node_owners_clear_owners(
    session: AsyncSession,
    user: User,
    transform_node: Node,
):
    await session.refresh(transform_node, ["owners"])
    transform_node.owners = [user]
    await session.commit()

    # Remove all owners
    await update_owners(
        session=session,
        node=transform_node,
        new_owner_usernames=[],
        current_user=user,
        save_history=(await get_save_history(notify=lambda event: None)),
    )
    await session.refresh(transform_node, ["owners"])
    assert transform_node.owners == []


@pytest.mark.asyncio
async def test_update_node_owners_noop(
    session: AsyncSession,
    user: User,
    transform_node: Node,
):
    await session.refresh(transform_node, ["owners"])
    transform_node.owners = [user]
    await session.commit()

    # Set same owner
    await update_owners(
        session=session,
        node=transform_node,
        new_owner_usernames=[user.username],
        current_user=user,
        save_history=(await get_save_history(notify=lambda event: None)),
    )
    await session.refresh(transform_node, ["owners"])
    assert transform_node.owners == [user]

    # Confirm only one history record was created
    history_entries = await session.execute(
        select(History).where(History.node == transform_node.name),
    )
    history = history_entries.scalars().all()
    assert len(history) == 1
    assert history[0].details["old_owners"] == [user.username]
    assert history[0].details["new_owners"] == [user.username]


@pytest.mark.asyncio
async def test_update_node_owners_invalid_user(
    session: AsyncSession,
    user: User,
    transform_node: Node,
):
    await session.refresh(transform_node, ["owners"])
    transform_node.owners = [user]
    await session.commit()

    # Try to assign a non-existent user
    with pytest.raises(DJDoesNotExistException):
        await update_owners(
            session=session,
            node=transform_node,
            new_owner_usernames=["non_existent_user"],
            current_user=user,
            save_history=(await get_save_history(notify=lambda event: None)),
        )
