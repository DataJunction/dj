"""
Unit tests for datajunction_server.internal.impact.propagate_impact
"""

from unittest import mock

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


# ---------------------------------------------------------------------------
# Validity Recovery Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_propagate_impact_validity_recovery_basic(session, current_user: User):
    """INVALID child with VALID parent → child recovers to VALID."""
    session.add(NodeNamespace(namespace="ns"))

    # Parent is now VALID (simulating a fix in deployment)
    parent, parent_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    # Child was INVALID (presumably because parent was previously INVALID)
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
    assert impact.name == "ns.transform"
    assert impact.impact_type == ImpactType.WILL_RECOVER
    assert impact.current_status == NodeStatus.INVALID
    assert impact.predicted_status == NodeStatus.VALID
    # The ORM object should be mutated to VALID
    assert child_rev.status == NodeStatus.VALID


@pytest.mark.asyncio
async def test_propagate_impact_validity_recovery_cascading(
    session,
    current_user: User,
):
    """INVALID chain recovers when root becomes VALID: A→B→C all recover."""
    session.add(NodeNamespace(namespace="ns"))

    # Root is now VALID
    root, root_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    # Children were INVALID
    child1, child1_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
    )
    child2, child2_rev = _make_node(
        "ns.metric",
        NodeType.METRIC,
        NodeStatus.INVALID,
        current_user.id,
    )
    await _persist(session, root, root_rev, child1, child1_rev, child2, child2_rev)
    await _persist(session, _link(root, child1_rev), _link(child1, child2_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    assert len(result) == 2
    by_name = {r.name: r for r in result}

    # Both should recover
    assert by_name["ns.transform"].impact_type == ImpactType.WILL_RECOVER
    assert by_name["ns.transform"].predicted_status == NodeStatus.VALID
    assert by_name["ns.metric"].impact_type == ImpactType.WILL_RECOVER
    assert by_name["ns.metric"].predicted_status == NodeStatus.VALID

    # ORM objects mutated
    assert child1_rev.status == NodeStatus.VALID
    assert child2_rev.status == NodeStatus.VALID


@pytest.mark.asyncio
async def test_propagate_impact_no_recovery_if_other_parent_invalid(
    session,
    current_user: User,
):
    """INVALID child with one VALID parent but another INVALID parent → no recovery."""
    session.add(NodeNamespace(namespace="ns"))

    # parent1 is VALID (changed)
    parent1, parent1_rev = _make_node(
        "ns.source1",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    # parent2 is INVALID (not changed, external)
    parent2, parent2_rev = _make_node(
        "ns.source2",
        NodeType.SOURCE,
        NodeStatus.INVALID,
        current_user.id,
    )
    # Child is INVALID
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
    )
    await _persist(
        session,
        parent1,
        parent1_rev,
        parent2,
        parent2_rev,
        child,
        child_rev,
    )
    await _persist(session, _link(parent1, child_rev), _link(parent2, child_rev))

    # Only parent1 is in the changed set
    result = await propagate_impact(session, "ns", {"ns.source1"})

    assert len(result) == 1
    impact = result[0]
    assert impact.name == "ns.transform"
    # Should NOT recover because parent2 is still INVALID
    assert impact.impact_type == ImpactType.MAY_AFFECT
    assert impact.current_status == NodeStatus.INVALID
    assert impact.predicted_status == NodeStatus.INVALID
    # ORM object should NOT be changed
    assert child_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_propagate_impact_no_recovery_if_parent_deleted(
    session,
    current_user: User,
):
    """INVALID child whose parent is being deleted → invalidates, doesn't recover."""
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
        NodeStatus.INVALID,
        current_user.id,
    )
    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    # Parent is deleted, not changed
    result = await propagate_impact(
        session,
        "ns",
        set(),
        deleted_node_names=frozenset(["ns.source"]),
    )

    assert len(result) == 1
    impact = result[0]
    # Deletion invalidates, doesn't recover
    assert impact.impact_type == ImpactType.WILL_INVALIDATE
    assert impact.predicted_status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_propagate_impact_mixed_invalidation_and_recovery(
    session,
    current_user: User,
):
    """Multiple changes: one fixes a node (recovery), another breaks a node (invalidation)."""
    session.add(NodeNamespace(namespace="ns"))

    # source1 is now VALID (was fixed)
    source1, source1_rev = _make_node(
        "ns.source1",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    # source2 is now INVALID (was broken)
    source2, source2_rev = _make_node(
        "ns.source2",
        NodeType.SOURCE,
        NodeStatus.INVALID,
        current_user.id,
    )
    # child1 depends on source1, was INVALID → should recover
    child1, child1_rev = _make_node(
        "ns.child1",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
    )
    # child2 depends on source2, was VALID → should invalidate
    child2, child2_rev = _make_node(
        "ns.child2",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
    )
    await _persist(
        session,
        source1,
        source1_rev,
        source2,
        source2_rev,
        child1,
        child1_rev,
        child2,
        child2_rev,
    )
    await _persist(session, _link(source1, child1_rev), _link(source2, child2_rev))

    result = await propagate_impact(session, "ns", {"ns.source1", "ns.source2"})

    assert len(result) == 2
    by_name = {r.name: r for r in result}

    # child1 should recover
    assert by_name["ns.child1"].impact_type == ImpactType.WILL_RECOVER
    assert by_name["ns.child1"].predicted_status == NodeStatus.VALID
    assert child1_rev.status == NodeStatus.VALID

    # child2 should invalidate
    assert by_name["ns.child2"].impact_type == ImpactType.WILL_INVALIDATE
    assert by_name["ns.child2"].predicted_status == NodeStatus.INVALID
    assert child2_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_propagate_impact_valid_child_stays_valid(session, current_user: User):
    """VALID child with VALID parent → stays VALID (MAY_AFFECT, not recovery)."""
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
    # Not a recovery because it was already VALID
    assert impact.impact_type == ImpactType.MAY_AFFECT
    assert impact.current_status == NodeStatus.VALID
    assert impact.predicted_status == NodeStatus.VALID
    assert child_rev.status == NodeStatus.VALID


@pytest.mark.asyncio
async def test_propagate_impact_no_recovery_if_node_has_sql_error(
    session,
    current_user: User,
):
    """INVALID child with VALID parent but bad SQL → no recovery (validated)."""
    session.add(NodeNamespace(namespace="ns"))

    # Parent is VALID
    parent, parent_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )

    # Child is INVALID and has broken SQL (syntax error)
    child = Node(
        name="ns.transform",
        type=NodeType.TRANSFORM,
        current_version="v1.0",
        created_by_id=current_user.id,
        namespace="ns",
    )
    child_rev = NodeRevision(
        name="ns.transform",
        type=NodeType.TRANSFORM,
        node=child,
        version="v1.0",
        status=NodeStatus.INVALID,
        query="SELECT * FORM broken_syntax",  # Intentional typo: FORM instead of FROM
        created_by_id=current_user.id,
    )

    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    assert len(result) == 1
    impact = result[0]
    # Should NOT recover because validation fails on the SQL syntax error
    assert impact.impact_type == ImpactType.MAY_AFFECT
    assert impact.current_status == NodeStatus.INVALID
    assert impact.predicted_status == NodeStatus.INVALID
    # Status should remain INVALID
    assert child_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_propagate_impact_source_node_recovery(session, current_user: User):
    """INVALID source node with VALID parent → auto-recovers (no SQL to validate)."""
    session.add(NodeNamespace(namespace="ns"))

    # Parent source is VALID
    parent, parent_rev = _make_node(
        "ns.source1",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )

    # Child is a source node that was INVALID (unusual but possible)
    child = Node(
        name="ns.source2",
        type=NodeType.SOURCE,
        current_version="v1.0",
        created_by_id=current_user.id,
        namespace="ns",
    )
    child_rev = NodeRevision(
        name="ns.source2",
        type=NodeType.SOURCE,
        node=child,
        version="v1.0",
        status=NodeStatus.INVALID,
        query=None,  # Source nodes don't have queries
        created_by_id=current_user.id,
    )

    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    result = await propagate_impact(session, "ns", {"ns.source1"})

    assert len(result) == 1
    impact = result[0]
    # Source nodes auto-recover without validation
    assert impact.impact_type == ImpactType.WILL_RECOVER
    assert impact.current_status == NodeStatus.INVALID
    assert impact.predicted_status == NodeStatus.VALID


@pytest.mark.asyncio
async def test_propagate_impact_cube_node_recovery(session, current_user: User):
    """INVALID cube with all VALID metric parents → auto-recovers (no SQL parsing needed)."""
    session.add(NodeNamespace(namespace="ns"))

    # Parent metric is VALID
    parent, parent_rev = _make_node(
        "ns.metric1",
        NodeType.METRIC,
        NodeStatus.VALID,
        current_user.id,
    )

    # Child is a cube node that was INVALID (e.g., because its metric parent was invalid)
    child = Node(
        name="ns.cube1",
        type=NodeType.CUBE,
        current_version="v1.0",
        created_by_id=current_user.id,
        namespace="ns",
    )
    child_rev = NodeRevision(
        name="ns.cube1",
        type=NodeType.CUBE,
        node=child,
        version="v1.0",
        status=NodeStatus.INVALID,
        query=None,  # Cube nodes don't have queries
        created_by_id=current_user.id,
    )

    await _persist(session, parent, parent_rev, child, child_rev)
    await _persist(session, _link(parent, child_rev))

    result = await propagate_impact(session, "ns", {"ns.metric1"})

    assert len(result) == 1
    impact = result[0]
    # Cubes auto-recover when all their metric parents are VALID (no SQL parsing needed)
    assert impact.impact_type == ImpactType.WILL_RECOVER
    assert impact.current_status == NodeStatus.INVALID
    assert impact.predicted_status == NodeStatus.VALID
    assert child_rev.status == NodeStatus.VALID


@pytest.mark.asyncio
async def test_process_validity_recovery_skips_candidate_with_no_parents(
    session,
    current_user: User,
):
    """Directly test _process_validity_recovery: candidate with no parent edges is skipped."""
    from datajunction_server.internal.impact import _process_validity_recovery
    from datajunction_server.models.impact import DownstreamImpact

    session.add(NodeNamespace(namespace="ns"))

    # Create a node that will be passed as a recovery candidate
    orphan = Node(
        name="ns.orphan",
        type=NodeType.SOURCE,
        current_version="v1.0",
        created_by_id=current_user.id,
        namespace="ns",
    )
    orphan_rev = NodeRevision(
        name="ns.orphan",
        type=NodeType.SOURCE,
        node=orphan,
        version="v1.0",
        status=NodeStatus.INVALID,
        query=None,
        created_by_id=current_user.id,
    )
    await _persist(session, orphan, orphan_rev)

    # Create a placeholder result that _process_validity_recovery can update
    results = [
        DownstreamImpact(
            name="ns.orphan",
            node_type=NodeType.SOURCE,
            current_status=NodeStatus.INVALID,
            predicted_status=NodeStatus.INVALID,
            impact_type=ImpactType.MAY_AFFECT,
            impact_reason="test",
            depth=1,
        ),
    ]

    # Pass orphan as a recovery candidate — it has NO NodeRelationship rows
    candidates = [(orphan.id, orphan, 1, 0)]
    recovered = await _process_validity_recovery(
        session,
        candidates,
        visited_nodes_by_id={orphan.id: orphan},
        results=results,
    )

    # Should not recover (no parents found → skipped)
    assert recovered == 0
    assert orphan_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_propagate_impact_recovery_skips_candidate_with_missing_parent_node(
    session,
    current_user: User,
):
    """Recovery candidate whose parent Node row can't be loaded is skipped."""
    from datajunction_server.internal.impact import _process_validity_recovery
    from datajunction_server.models.impact import DownstreamImpact

    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    # Create a second parent that is linked to child
    other_parent, other_parent_rev = _make_node(
        "ns.other_parent",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
    )

    await _persist(
        session,
        source,
        source_rev,
        other_parent,
        other_parent_rev,
        child,
        child_rev,
    )
    await _persist(session, _link(source, child_rev))
    await _persist(session, _link(other_parent, child_rev))

    results = [
        DownstreamImpact(
            name="ns.transform",
            node_type=NodeType.TRANSFORM,
            current_status=NodeStatus.INVALID,
            predicted_status=NodeStatus.INVALID,
            impact_type=ImpactType.MAY_AFFECT,
            impact_reason="test",
            depth=1,
        ),
    ]

    # Call _process_validity_recovery directly.
    # visited_nodes_by_id includes source but NOT other_parent.
    # The function will load missing parents (other_parent) from DB, so it
    # would normally find it. We mock the load query to return empty so
    # other_parent stays missing from visited_nodes_by_id.
    original_execute = session.execute
    call_count = 0

    async def _intercept_execute(stmt, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        # The 2nd query is the missing-parent load (select Node where id in ...)
        # Return an empty result to simulate a missing parent
        if call_count == 2:
            empty_result = mock.MagicMock()
            empty_result.unique.return_value = empty_result
            empty_result.scalars.return_value = empty_result
            empty_result.all.return_value = []
            return empty_result
        return await original_execute(stmt, *args, **kwargs)

    # Pre-load source with its .current relationship to avoid lazy loads
    from sqlalchemy import select
    from sqlalchemy.orm import joinedload as jl

    source_loaded = (
        (
            await session.execute(
                select(Node).where(Node.id == source.id).options(jl(Node.current)),
            )
        )
        .unique()
        .scalar_one()
    )

    candidates = [(child.id, child, 1, 0)]
    with mock.patch.object(session, "execute", side_effect=_intercept_execute):
        recovered = await _process_validity_recovery(
            session,
            candidates,
            visited_nodes_by_id={child.id: child, source.id: source_loaded},
            results=results,
        )

    # Should not recover because other_parent can't be found
    assert recovered == 0
    assert child_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_propagate_impact_recovery_handles_validation_exception(
    session,
    current_user: User,
):
    """If validate_node_data raises, the candidate is skipped (not recovered)."""
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
    )

    await _persist(session, source, source_rev, child, child_rev)
    await _persist(session, _link(source, child_rev))

    with mock.patch(
        "datajunction_server.internal.impact.validate_node_data",
        side_effect=RuntimeError("boom"),
    ):
        result = await propagate_impact(session, "ns", {"ns.source"})

    # child should NOT recover because validation raised an exception
    child_impacts = [r for r in result if r.name == "ns.transform"]
    assert len(child_impacts) == 1
    assert child_impacts[0].impact_type != ImpactType.WILL_RECOVER
    # Status should remain INVALID
    assert child_rev.status == NodeStatus.INVALID
