import pytest
import pytest_asyncio
from sqlalchemy import select, update

from datajunction_server.database.node import Node, NodeType, NodeRevision, Column
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.database.history import History
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.api.helpers import get_save_history
from datajunction_server.internal.nodes import (
    create_new_revision_for_dimension_link_update,
    update_owners,
)
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


@pytest.mark.asyncio
async def test_create_new_revision_stale_version_is_refreshed(
    session: AsyncSession,
    user: User,
):
    """
    Regression test for a UniqueViolation on uq_noderevision_version caused by concurrent
    dimension-link mutations. Two concurrent requests could both read the same current_version
    and each try to INSERT a NodeRevision with the same (version, node_id).

    The fix adds a SELECT FOR UPDATE on the node row followed by session.refresh inside
    create_new_revision_for_dimension_link_update. This test simulates the stale-cache scenario:
    - The session has node.current_version = "v1.0" cached in memory.
    - A separate transaction already bumped node.current_version to "v1.1" in the DB and
      inserted the corresponding NodeRevision (simulating a concurrent request winning the race).
    - Without the refresh, our call would try to INSERT version "v1.1" again → UniqueViolation.
    - With the refresh, it reads the updated DB value "v1.1" and correctly writes "v1.2".
    """
    node = Node(
        name="basic.race_condition_test",
        type=NodeType.TRANSFORM,
        current_version="v1.0",
        display_name="Race Condition Test Node",
        created_by_id=user.id,
    )
    node_revision = NodeRevision(
        node=node,
        name=node.name,
        type=node.type,
        version="v1.0",
        query="SELECT user_id FROM users",
        columns=[Column(name="user_id", type=ct.IntegerType())],
        created_by_id=user.id,
    )
    session.add_all([node, node_revision])
    await session.commit()
    await session.refresh(node)
    assert node.current_version == "v1.0"

    # Simulate the concurrent request winning: insert NodeRevision v1.1 and update
    # node.current_version in the DB, bypassing the ORM cache so `node` still reads
    # current_version as "v1.0" until the FOR UPDATE + refresh in our fix.
    concurrent_revision = NodeRevision(
        node_id=node.id,
        name=node.name,
        type=node.type,
        version="v1.1",
        query="SELECT user_id FROM users",
        columns=[],
        created_by_id=user.id,
    )
    session.add(concurrent_revision)
    await session.flush()
    # Use synchronize_session=False so SQLAlchemy does NOT update the in-memory ORM object,
    # simulating the stale-cache state a session would have after a concurrent request in
    # another session committed the same bump.
    await session.execute(
        update(Node)
        .where(Node.id == node.id)
        .values(current_version="v1.1")
        .execution_options(synchronize_session=False),
    )
    await session.flush()

    # The ORM-cached value is still stale — the node object shows "v1.0"
    assert node.current_version == "v1.0"

    # create_new_revision_for_dimension_link_update should read the DB value via
    # SELECT FOR UPDATE + refresh, compute v1.2, and succeed without UniqueViolation.
    new_revision = await create_new_revision_for_dimension_link_update(
        session,
        node,
        user,
    )

    assert new_revision.version == "v1.2"
    assert node.current_version == "v1.2"
