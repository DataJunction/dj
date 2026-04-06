"""
Unit tests for datajunction_server.internal.impact.propagate_impact
"""

import pytest

from datajunction_server.database.node import Node, NodeRevision, NodeRelationship
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.user import User
from datajunction_server.internal.impact import propagate_impact
from datajunction_server.models.impact import ImpactType
from datajunction_server.models.node import NodeStatus, NodeType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    name: str,
    node_type: NodeType,
    status: NodeStatus,
    user_id: int,
    version: str = "v1.0",
) -> tuple[Node, NodeRevision]:
    """Create an unsaved (Node, NodeRevision) pair."""
    node = Node(
        name=name,
        type=node_type,
        current_version=version,
        created_by_id=user_id,
        namespace=name.rsplit(".", 1)[0] if "." in name else name,
    )
    rev = NodeRevision(
        name=name,
        type=node_type,
        node=node,
        version=version,
        status=status,
        query="SELECT 1",
        created_by_id=user_id,
    )
    return node, rev


async def _persist(session, *objects):
    for obj in objects:
        session.add(obj)
    await session.flush()


def _link(parent: Node, child_rev: NodeRevision) -> NodeRelationship:
    """Create a NodeRelationship row wiring parent → child_rev.

    Use this instead of child_rev.parents.append() to avoid triggering a
    synchronous lazy-load in async context.
    """
    return NodeRelationship(parent_id=parent.id, child_id=child_rev.id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_propagate_impact_empty(session):
    """No changed or deleted nodes → empty result."""
    result = await propagate_impact(session, "ns", set(), frozenset())
    assert result == []


@pytest.mark.asyncio
async def test_propagate_impact_no_downstream(session, current_user: User):
    """A changed VALID node with no children → empty result."""
    session.add(NodeNamespace(namespace="ns"))
    node, rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    await _persist(session, node, rev)

    result = await propagate_impact(session, "ns", {"ns.source"})
    assert result == []


@pytest.mark.asyncio
async def test_propagate_impact_valid_parent_may_affect(session, current_user: User):
    """Changed VALID parent → downstream gets MAY_AFFECT (not written INVALID)."""
    session.add(NodeNamespace(namespace="ns"))

    parent, parent_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
    )
    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    assert len(result) == 1
    impact = result[0]
    assert impact.name == "ns.transform"
    assert impact.impact_type == ImpactType.MAY_AFFECT
    assert impact.depth == 1
    assert "ns.source" in impact.caused_by
    # Status must NOT have been mutated
    assert child_rev.status == NodeStatus.VALID


@pytest.mark.asyncio
async def test_propagate_impact_invalid_parent_will_invalidate(
    session,
    current_user: User,
):
    """Changed INVALID parent → downstream gets WILL_INVALIDATE and is marked INVALID."""
    session.add(NodeNamespace(namespace="ns"))

    parent, parent_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.INVALID,
        current_user.id,
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
    )
    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    assert len(result) == 1
    impact = result[0]
    assert impact.impact_type == ImpactType.WILL_INVALIDATE
    assert impact.current_status == NodeStatus.VALID
    assert impact.predicted_status == NodeStatus.INVALID
    # The ORM object should be mutated inside the session
    assert child_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_propagate_impact_deleted_node_will_invalidate(
    session,
    current_user: User,
):
    """Deleted node → all its children get WILL_INVALIDATE."""
    session.add(NodeNamespace(namespace="ns"))

    parent, parent_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
    )
    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    # Pass parent as deleted (still in DB, simulating pre-deletion call)
    result = await propagate_impact(
        session,
        "ns",
        set(),
        deleted_node_names=frozenset(["ns.source"]),
    )

    assert len(result) == 1
    assert result[0].impact_type == ImpactType.WILL_INVALIDATE
    assert child_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_propagate_impact_transitive_invalidation(session, current_user: User):
    """INVALID root → child1 → child2: both get WILL_INVALIDATE (transitive)."""
    session.add(NodeNamespace(namespace="ns"))

    root, root_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.INVALID,
        current_user.id,
    )
    child1, child1_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
    )
    child2, child2_rev = _make_node(
        "ns.metric",
        NodeType.METRIC,
        NodeStatus.VALID,
        current_user.id,
    )
    await _persist(session, root, root_rev, child1, child1_rev, child2, child2_rev)
    await _persist(session, _link(root, child1_rev), _link(child1, child2_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    assert len(result) == 2
    by_name = {r.name: r for r in result}
    assert by_name["ns.transform"].impact_type == ImpactType.WILL_INVALIDATE
    assert by_name["ns.transform"].depth == 1
    assert by_name["ns.metric"].impact_type == ImpactType.WILL_INVALIDATE
    assert by_name["ns.metric"].depth == 2
    assert child1_rev.status == NodeStatus.INVALID
    assert child2_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_propagate_impact_is_external(session, current_user: User):
    """Nodes outside the deployment namespace have is_external=True."""
    session.add(NodeNamespace(namespace="ns"))
    session.add(NodeNamespace(namespace="other"))

    parent, parent_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    child, child_rev = _make_node(
        "other.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
    )
    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    assert len(result) == 1
    assert result[0].is_external is True


@pytest.mark.asyncio
async def test_propagate_impact_already_invalid_not_duplicated(
    session,
    current_user: User,
):
    """Downstream node already INVALID → impact recorded but not double-written."""
    session.add(NodeNamespace(namespace="ns"))

    parent, parent_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.INVALID,
        current_user.id,
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
    )
    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    assert len(result) == 1
    impact = result[0]
    # current_status == INVALID (was already invalid before propagation)
    assert impact.current_status == NodeStatus.INVALID
    assert impact.predicted_status == NodeStatus.INVALID
    assert impact.impact_type == ImpactType.WILL_INVALIDATE


@pytest.mark.asyncio
async def test_propagate_impact_deactivated_children_skipped(
    session,
    current_user: User,
):
    """Deactivated downstream nodes are excluded from impact results.

    Covers the while-loop exit path where next_frontier becomes empty because
    all discovered child nodes have been deactivated (filtered by deactivated_at IS NULL).
    """
    from datetime import datetime, timezone

    session.add(NodeNamespace(namespace="ns"))

    parent, parent_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
    )
    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    # Deactivate the child node so it is filtered out in BFS
    child.deactivated_at = datetime.now(timezone.utc)
    session.add(child)
    await session.flush()

    result = await propagate_impact(session, "ns", {"ns.source"})

    # The deactivated child should not appear in results
    assert result == []


@pytest.mark.asyncio
async def test_propagate_impact_cause_names_in_result(session, current_user: User):
    """caused_by traces back to the original changed root, not intermediate nodes."""
    session.add(NodeNamespace(namespace="ns"))

    root, root_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
    )
    grandchild, grandchild_rev = _make_node(
        "ns.metric",
        NodeType.METRIC,
        NodeStatus.VALID,
        current_user.id,
    )
    await _persist(
        session,
        root,
        root_rev,
        child,
        child_rev,
        grandchild,
        grandchild_rev,
    )
    await _persist(session, _link(root, child_rev), _link(child, grandchild_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    # Both should trace back to ns.source
    assert by_name["ns.transform"].caused_by == ["ns.source"]
    assert by_name["ns.metric"].caused_by == ["ns.source"]
