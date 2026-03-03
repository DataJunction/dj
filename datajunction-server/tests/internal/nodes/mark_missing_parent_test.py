"""
Tests for mark_node_as_missing_parent, deactivate_node, and activate_node functionality.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision, NodeType
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.internal.nodes import (
    deactivate_node,
    activate_node,
    hard_delete_node,
    mark_node_as_missing_parent,
)
from datajunction_server.models.node import NodeStatus


async def mock_save_history(event, session):
    """Mock save_history function for tests"""
    return


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
async def test_namespace(session: AsyncSession):
    """
    Create a test namespace.
    """
    from datajunction_server.database.namespace import NodeNamespace

    namespace = NodeNamespace(namespace="test")
    session.add(namespace)
    await session.commit()
    return namespace


@pytest_asyncio.fixture
async def parent_node(session: AsyncSession, user: User) -> Node:
    """
    A parent transform node.
    """
    node = Node(
        name="test.parent",
        type=NodeType.TRANSFORM,
        created_by_id=user.id,
        current_version="v1.0",
    )
    node_revision = NodeRevision(
        name="test.parent",
        display_name="Test Parent",
        type=NodeType.TRANSFORM,
        query="SELECT 1 as id",
        status=NodeStatus.VALID,
        version="v1.0",
        node=node,
        created_by_id=user.id,
    )
    session.add(node)
    session.add(node_revision)
    await session.commit()
    await session.refresh(node, ["current"])
    return node


@pytest_asyncio.fixture
async def child_node(session: AsyncSession, user: User, parent_node: Node) -> Node:
    """
    A child node that depends on parent_node.
    """
    node = Node(
        name="test.child",
        type=NodeType.TRANSFORM,
        created_by_id=user.id,
        current_version="v1.0",
    )
    node_revision = NodeRevision(
        name="test.child",
        display_name="Test Child",
        type=NodeType.TRANSFORM,
        query="SELECT * FROM test.parent",
        status=NodeStatus.VALID,
        version="v1.0",
        node=node,
        created_by_id=user.id,
    )
    session.add(node)
    session.add(node_revision)
    await session.flush()  # Flush to get IDs

    # Set up parent relationship directly (avoid lazy load)
    from sqlalchemy import insert
    from datajunction_server.database.node import NodeRelationship

    relationship_stmt = insert(NodeRelationship).values(
        parent_id=parent_node.id,
        child_id=node_revision.id,
    )
    await session.execute(relationship_stmt)
    await session.commit()

    await session.refresh(node, ["current"])
    await session.refresh(node.current, ["parents", "missing_parents"])
    return node


@pytest.mark.asyncio
async def test_mark_node_as_missing_parent_already_exists(
    session: AsyncSession,
    user: User,
    parent_node: Node,
    child_node: Node,
):
    """
    Test that marking the same node as missing parent twice uses existing MissingParent entry.
    This covers line 2741 in nodes.py.
    """
    save_history = mock_save_history

    # First call creates MissingParent
    missing_parent1, downstreams1 = await mark_node_as_missing_parent(
        session=session,
        node_name=parent_node.name,
        node=parent_node,
        invalidate_downstreams=True,
        current_user=user,
        save_history=save_history,
    )
    await session.commit()

    # Second call should reuse existing MissingParent
    missing_parent2, downstreams2 = await mark_node_as_missing_parent(
        session=session,
        node_name=parent_node.name,
        node=parent_node,
        invalidate_downstreams=True,
        current_user=user,
        save_history=save_history,
    )

    # Should be the same MissingParent object
    assert missing_parent1.id == missing_parent2.id
    assert missing_parent1.name == parent_node.name


@pytest.mark.asyncio
async def test_mark_node_as_missing_parent_requires_user_and_history(
    session: AsyncSession,
    parent_node: Node,
    child_node: Node,
):
    """
    Test that invalidate_downstreams=True requires current_user and save_history.
    This covers line 2753 in nodes.py.
    """
    with pytest.raises(ValueError, match="current_user and save_history are required"):
        await mark_node_as_missing_parent(
            session=session,
            node_name=parent_node.name,
            node=parent_node,
            invalidate_downstreams=True,
            current_user=None,  # Missing!
            save_history=None,  # Missing!
        )


@pytest.mark.asyncio
async def test_mark_node_as_missing_parent_already_in_downstreams(
    session: AsyncSession,
    user: User,
    parent_node: Node,
    child_node: Node,
):
    """
    Test that downstream already having MissingParent doesn't add duplicate.
    This covers lines 2772-2773 in nodes.py (the if branch).
    """
    save_history = mock_save_history

    # First call adds MissingParent to downstream
    missing_parent, _ = await mark_node_as_missing_parent(
        session=session,
        node_name=parent_node.name,
        node=parent_node,
        invalidate_downstreams=True,
        current_user=user,
        save_history=save_history,
    )
    await session.commit()
    await session.refresh(child_node.current, ["missing_parents"])

    initial_count = len(child_node.current.missing_parents)
    assert initial_count == 1

    # Second call should not add duplicate
    await mark_node_as_missing_parent(
        session=session,
        node_name=parent_node.name,
        node=parent_node,
        invalidate_downstreams=True,
        current_user=user,
        save_history=save_history,
    )
    await session.commit()
    await session.refresh(child_node.current, ["missing_parents"])

    # Should still only have one entry
    assert len(child_node.current.missing_parents) == initial_count


@pytest.mark.asyncio
async def test_deactivate_node_keeps_parent_relationships(
    session: AsyncSession,
    user: User,
    parent_node: Node,
    child_node: Node,
    query_service_client,
    background_tasks,
):
    """
    Test that deactivation keeps parent relationships intact (for upstream queries).
    This covers lines 2776 in nodes.py (remove_parent_relationships=False path).
    """
    save_history = mock_save_history

    # Verify parent relationship exists initially
    await session.refresh(child_node.current, ["parents"])
    parent_names = [p.name for p in child_node.current.parents]
    assert parent_node.name in parent_names

    # Deactivate parent (uses remove_parent_relationships=False)
    await deactivate_node(
        session=session,
        name=parent_node.name,
        current_user=user,
        save_history=save_history,
        query_service_client=query_service_client,
        background_tasks=background_tasks,
    )

    # Refresh child and check parents
    await session.refresh(child_node, ["current"])
    await session.refresh(child_node.current, ["parents", "missing_parents"])

    # Parent relationship should still exist!
    parent_names = [p.name for p in child_node.current.parents]
    assert parent_node.name in parent_names

    # But should also be in missing_parents
    missing_parent_names = [mp.name for mp in child_node.current.missing_parents]
    assert parent_node.name in missing_parent_names


@pytest.mark.asyncio
async def test_hard_delete_removes_parent_relationships(
    session: AsyncSession,
    user: User,
    parent_node: Node,
    child_node: Node,
    query_service_client,
    background_tasks,
):
    """
    Test that hard delete removes parent relationships.
    This covers lines 2776 in nodes.py (remove_parent_relationships=True path).
    """
    save_history = mock_save_history

    # Verify parent relationship exists initially
    await session.refresh(child_node.current, ["parents"])
    initial_parent_count = len(child_node.current.parents)
    parent_names = [p.name for p in child_node.current.parents]
    assert parent_node.name in parent_names

    # Hard delete parent (uses remove_parent_relationships=True)
    await hard_delete_node(
        name=parent_node.name,
        session=session,
        current_user=user,
        save_history=save_history,
    )

    # Refresh child and check parents
    await session.commit()  # Commit the hard delete transaction
    await session.refresh(child_node, ["current"])
    await session.refresh(child_node.current, ["parents", "missing_parents"])

    # Parent relationship should be removed (covers line 2776 remove_parent_relationships=True path)
    parent_names = [p.name for p in child_node.current.parents]
    assert parent_node.name not in parent_names
    assert len(child_node.current.parents) < initial_parent_count


@pytest.mark.asyncio
async def test_activate_node_when_parent_already_in_parents(
    session: AsyncSession,
    user: User,
    parent_node: Node,
    child_node: Node,
    query_service_client,
    background_tasks,
):
    """
    Test restoring node when downstream already has it in parents (edge case).
    This covers lines 2912-2913 in nodes.py (the if branch).
    """
    save_history = mock_save_history

    # Deactivate then restore parent
    await deactivate_node(
        session=session,
        name=parent_node.name,
        current_user=user,
        save_history=save_history,
        query_service_client=query_service_client,
        background_tasks=background_tasks,
    )

    # Parent should still be in child's parents (due to our recent fix)
    await session.refresh(child_node.current, ["parents"])
    parent_names = [p.name for p in child_node.current.parents]
    assert parent_node.name in parent_names

    # Restore the parent
    await activate_node(
        session=session,
        name=parent_node.name,
        current_user=user,
        save_history=save_history,
    )

    # Refresh and check
    await session.refresh(child_node, ["current"])
    await session.refresh(child_node.current, ["parents"])

    # Should only have one entry (no duplicate)
    parent_count = sum(
        1 for p in child_node.current.parents if p.name == parent_node.name
    )
    assert parent_count == 1


# Note: Line 2923 in nodes.py (if element.node_revision) is defensive code
# that cannot be realistically tested because the database has a NOT NULL
# constraint on node_revision_id in the column table. The check is there for
# safety but shouldn't be reachable in practice.
