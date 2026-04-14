"""
Unit tests for datajunction_server.internal.impact.propagate_impact
"""

from unittest.mock import patch

import pytest

from datajunction_server.database.column import Column as DBColumn
from datajunction_server.database.node import Node, NodeRevision, NodeRelationship
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.user import User
from datajunction_server.internal.impact import _merge_impacts, propagate_impact
from datajunction_server.models.impact import DownstreamImpact, ImpactType
from datajunction_server.models.node import NodeStatus, NodeType
from datajunction_server.sql.parsing.types import (
    BigIntType,
    DoubleType,
    IntegerType,
    StringType,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    name: str,
    node_type: NodeType,
    status: NodeStatus,
    user_id: int,
    version: str = "v1.0",
    query: str = "SELECT 1",
    columns: list[tuple[str, object]] | None = None,
) -> tuple[Node, NodeRevision]:
    """Create an unsaved (Node, NodeRevision) pair with optional columns."""
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
        query=query,
        created_by_id=user_id,
    )
    if columns:
        rev.columns = [
            DBColumn(name=col_name, type=col_type) for col_name, col_type in columns
        ]
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
        columns=[("id", IntegerType())],
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT id FROM ns.source",
        columns=[("id", IntegerType())],
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
        columns=[("id", IntegerType())],
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT id FROM ns.source",
        columns=[("id", IntegerType())],
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
        columns=[("id", IntegerType())],
    )
    child1, child1_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT id FROM ns.source",
        columns=[("id", IntegerType())],
    )
    child2, child2_rev = _make_node(
        "ns.metric",
        NodeType.METRIC,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT SUM(id) ns_DOT_metric FROM ns.transform",
        columns=[("ns_DOT_metric", BigIntType())],
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
        columns=[("id", IntegerType())],
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
        query="SELECT id FROM ns.source",
        columns=[("id", IntegerType())],
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
        columns=[("id", IntegerType())],
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT id FROM ns.source",
        columns=[("id", IntegerType())],
    )
    grandchild, grandchild_rev = _make_node(
        "ns.metric",
        NodeType.METRIC,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT SUM(id) ns_DOT_metric FROM ns.transform",
        columns=[("ns_DOT_metric", BigIntType())],
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
# Phase 3: Revalidation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revalidation_recovery_invalid_to_valid(session, current_user: User):
    """INVALID node with all parents now VALID and query resolves → WILL_RECOVER."""
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    # Transform was INVALID but parent is now VALID
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
        query="SELECT user_id, amount FROM ns.source",
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    await _persist(session, source, source_rev, transform, transform_rev)
    await _persist(session, _link(source, transform_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    impact = by_name["ns.transform"]
    assert impact.impact_type == ImpactType.WILL_RECOVER
    assert impact.current_status == NodeStatus.INVALID
    assert impact.predicted_status == NodeStatus.VALID
    assert transform_rev.status == NodeStatus.VALID


@pytest.mark.asyncio
async def test_revalidation_recovery_fails(session, current_user: User):
    """INVALID node, parent VALID, but query references nonexistent column → stays INVALID."""
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("user_id", IntegerType())],
    )
    # Transform references 'nonexistent' which doesn't exist on source
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
        query="SELECT nonexistent FROM ns.source",
        columns=[("nonexistent", StringType())],
    )
    await _persist(session, source, source_rev, transform, transform_rev)
    await _persist(session, _link(source, transform_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    impact = by_name["ns.transform"]
    assert impact.impact_type == ImpactType.WILL_INVALIDATE
    assert impact.predicted_status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_revalidation_invalid_parent_still_invalid_no_recovery(
    session,
    current_user: User,
):
    """INVALID node with one parent still INVALID → no recovery attempt."""
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.INVALID,
        current_user.id,
        columns=[("user_id", IntegerType())],
    )
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
        query="SELECT user_id FROM ns.source",
        columns=[("user_id", IntegerType())],
    )
    await _persist(session, source, source_rev, transform, transform_rev)
    await _persist(session, _link(source, transform_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    # Parent is INVALID → child gets WILL_INVALIDATE, not recovery
    assert by_name["ns.transform"].impact_type == ImpactType.WILL_INVALIDATE


@pytest.mark.asyncio
async def test_revalidation_column_type_change(session, current_user: User):
    """Parent column type changes (INT→BIGINT) → downstream columns updated."""
    session.add(NodeNamespace(namespace="ns"))

    # Source with BIGINT now (was INT before, but deploy updated it)
    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("user_id", BigIntType()), ("amount", DoubleType())],
    )
    # Transform still has old INT type
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT user_id, amount FROM ns.source",
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    await _persist(session, source, source_rev, transform, transform_rev)
    await _persist(session, _link(source, transform_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    impact = by_name["ns.transform"]
    assert impact.impact_type == ImpactType.MAY_AFFECT
    assert "Column types changed" in impact.impact_reason
    # The transform's column type should be updated to BIGINT
    col_types = {col.name: col.type for col in transform_rev.columns}
    assert isinstance(col_types["user_id"], BigIntType)


@pytest.mark.asyncio
async def test_revalidation_column_renamed_breaks_downstream(
    session,
    current_user: User,
):
    """Parent column renamed → downstream can't resolve → WILL_INVALIDATE."""
    session.add(NodeNamespace(namespace="ns"))

    # Source now has 'uid' instead of 'user_id'
    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("uid", IntegerType()), ("amount", DoubleType())],
    )
    # Transform still references 'user_id'
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT user_id, amount FROM ns.source",
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    await _persist(session, source, source_rev, transform, transform_rev)
    await _persist(session, _link(source, transform_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    assert by_name["ns.transform"].impact_type == ImpactType.WILL_INVALIDATE
    assert transform_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_revalidation_unchanged_columns_passthrough(
    session,
    current_user: User,
):
    """Parent query changed but column signature unchanged → node passes through."""
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT user_id, amount FROM ns.source",
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    await _persist(session, source, source_rev, transform, transform_rev)
    await _persist(session, _link(source, transform_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    # Columns unchanged, status unchanged → MAY_AFFECT passthrough
    assert by_name["ns.transform"].impact_type == ImpactType.MAY_AFFECT
    assert transform_rev.status == NodeStatus.VALID


@pytest.mark.asyncio
async def test_revalidation_source_node_skipped(session, current_user: User):
    """Source nodes as MAY_AFFECT → skipped (no query to validate), passes through."""
    session.add(NodeNamespace(namespace="ns"))

    parent_source, parent_source_rev = _make_node(
        "ns.catalog_source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("id", IntegerType())],
    )
    # Another source that depends on the first (unusual but possible)
    child_source, child_source_rev = _make_node(
        "ns.derived_source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("id", IntegerType())],
    )
    await _persist(
        session,
        parent_source,
        parent_source_rev,
        child_source,
        child_source_rev,
    )
    await _persist(session, _link(parent_source, child_source_rev))

    result = await propagate_impact(session, "ns", {"ns.catalog_source"})

    by_name = {r.name: r for r in result}
    assert "ns.derived_source" in by_name
    assert by_name["ns.derived_source"].impact_type == ImpactType.MAY_AFFECT
    # Status should not change
    assert child_source_rev.status == NodeStatus.VALID


@pytest.mark.asyncio
async def test_revalidation_three_level_cascade(session, current_user: User):
    """Three-level chain: source → transform → metric.

    Source column type changes (INT→BIGINT). Transform's output type changes.
    Metric sees transform's updated type and its output type changes too.
    """
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("user_id", BigIntType()), ("amount", DoubleType())],
    )
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT user_id, amount FROM ns.source",
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    metric, metric_rev = _make_node(
        "ns.metric",
        NodeType.METRIC,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT SUM(user_id) ns_DOT_metric FROM ns.transform",
        columns=[("ns_DOT_metric", BigIntType())],
    )
    await _persist(
        session,
        source,
        source_rev,
        transform,
        transform_rev,
        metric,
        metric_rev,
    )
    await _persist(
        session,
        _link(source, transform_rev),
        _link(transform, metric_rev),
    )

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    assert "ns.metric" in by_name

    # Transform should have updated column types
    transform_cols = {col.name: col.type for col in transform_rev.columns}
    assert isinstance(transform_cols["user_id"], BigIntType)

    # Metric should see the updated transform and still resolve
    assert by_name["ns.metric"].impact_type in (
        ImpactType.MAY_AFFECT,
        ImpactType.WILL_RECOVER,
    )


@pytest.mark.asyncio
async def test_revalidation_three_level_cascade_middle_breaks(
    session,
    current_user: User,
):
    """Three-level chain where middle node breaks.

    Source column renamed → transform can't resolve → WILL_INVALIDATE.
    Metric depends on transform → should also become WILL_INVALIDATE since
    its parent is now INVALID.
    """
    session.add(NodeNamespace(namespace="ns"))

    # Source renamed 'user_id' to 'uid'
    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("uid", IntegerType()), ("amount", DoubleType())],
    )
    # Transform still references 'user_id'
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT user_id, amount FROM ns.source",
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    metric, metric_rev = _make_node(
        "ns.metric",
        NodeType.METRIC,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT SUM(amount) ns_DOT_metric FROM ns.transform",
        columns=[("ns_DOT_metric", DoubleType())],
    )
    await _persist(
        session,
        source,
        source_rev,
        transform,
        transform_rev,
        metric,
        metric_rev,
    )
    await _persist(
        session,
        _link(source, transform_rev),
        _link(transform, metric_rev),
    )

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    assert "ns.metric" in by_name

    # Transform breaks — column renamed
    assert by_name["ns.transform"].impact_type == ImpactType.WILL_INVALIDATE
    assert transform_rev.status == NodeStatus.INVALID

    # Metric should also be WILL_INVALIDATE since its parent broke
    assert by_name["ns.metric"].impact_type == ImpactType.WILL_INVALIDATE
    assert metric_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_revalidation_recovery_with_column_type_change(
    session,
    current_user: User,
):
    """INVALID node recovers AND its column types change during recovery.

    Source was updated (INT→BIGINT). Transform was INVALID but now resolves
    with the new column types → WILL_RECOVER with updated columns.
    """
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("user_id", BigIntType()), ("amount", DoubleType())],
    )
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.INVALID,
        current_user.id,
        query="SELECT user_id, amount FROM ns.source",
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    await _persist(session, source, source_rev, transform, transform_rev)
    await _persist(session, _link(source, transform_rev))

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    impact = by_name["ns.transform"]
    assert impact.impact_type == ImpactType.WILL_RECOVER
    assert impact.current_status == NodeStatus.INVALID
    assert impact.predicted_status == NodeStatus.VALID
    assert transform_rev.status == NodeStatus.VALID
    # Column types should be updated during recovery
    col_types = {col.name: col.type for col in transform_rev.columns}
    assert isinstance(col_types["user_id"], BigIntType)


@pytest.mark.asyncio
async def test_column_type_change_cascades_through_three_levels(
    session,
    current_user: User,
):
    """Column type changes propagate through multiple levels.

    Source: user_id INT→BIGINT. Transform picks it up (INT→BIGINT).
    Metric's SUM(user_id) output type also changes accordingly.
    All nodes stay VALID, but types cascade downward.
    """
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("user_id", BigIntType()), ("amount", DoubleType())],
    )
    transform, transform_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT user_id, amount FROM ns.source",
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    metric, metric_rev = _make_node(
        "ns.metric",
        NodeType.METRIC,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT SUM(user_id) ns_DOT_metric FROM ns.transform",
        columns=[("ns_DOT_metric", IntegerType())],
    )
    await _persist(
        session,
        source,
        source_rev,
        transform,
        transform_rev,
        metric,
        metric_rev,
    )
    await _persist(
        session,
        _link(source, transform_rev),
        _link(transform, metric_rev),
    )

    result = await propagate_impact(session, "ns", {"ns.source"})

    by_name = {r.name: r for r in result}
    # Transform's user_id should update from INT to BIGINT
    assert by_name["ns.transform"].impact_type == ImpactType.MAY_AFFECT
    transform_cols = {col.name: col.type for col in transform_rev.columns}
    assert isinstance(transform_cols["user_id"], BigIntType)

    # Metric's output type should also update (SUM(BIGINT) → BIGINT)
    assert by_name["ns.metric"].impact_type == ImpactType.MAY_AFFECT
    metric_cols = {col.name: col.type for col in metric_rev.columns}
    assert isinstance(metric_cols["ns_DOT_metric"], BigIntType)


@pytest.mark.asyncio
async def test_mixed_valid_invalid_parents(session, current_user: User):
    """Node with one VALID parent and one INVALID parent → WILL_INVALIDATE.

    The INVALID parent is in failed_names, so its columns are excluded from the
    parent_columns_map. The child's query references both parents → fails.
    """
    session.add(NodeNamespace(namespace="ns"))

    valid_parent, valid_parent_rev = _make_node(
        "ns.source_a",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("id", IntegerType())],
    )
    invalid_parent, invalid_parent_rev = _make_node(
        "ns.source_b",
        NodeType.SOURCE,
        NodeStatus.INVALID,
        current_user.id,
        columns=[("name", StringType())],
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT a.id, b.name FROM ns.source_a a JOIN ns.source_b b ON a.id = b.id",
        columns=[("id", IntegerType()), ("name", StringType())],
    )
    await _persist(
        session,
        valid_parent,
        valid_parent_rev,
        invalid_parent,
        invalid_parent_rev,
        child,
        child_rev,
    )
    await _persist(
        session,
        _link(valid_parent, child_rev),
        _link(invalid_parent, child_rev),
    )

    result = await propagate_impact(
        session,
        "ns",
        {"ns.source_a", "ns.source_b"},
    )

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    assert by_name["ns.transform"].impact_type == ImpactType.WILL_INVALIDATE
    assert child_rev.status == NodeStatus.INVALID


@pytest.mark.asyncio
async def test_multiple_roots_same_downstream(session, current_user: User):
    """Two changed parents both affect the same child — caused_by lists both."""
    session.add(NodeNamespace(namespace="ns"))

    parent_a, parent_a_rev = _make_node(
        "ns.source_a",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("id", IntegerType())],
    )
    parent_b, parent_b_rev = _make_node(
        "ns.source_b",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("name", StringType())],
    )
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT a.id, b.name FROM ns.source_a a JOIN ns.source_b b ON a.id = b.id",
        columns=[("id", IntegerType()), ("name", StringType())],
    )
    await _persist(
        session,
        parent_a,
        parent_a_rev,
        parent_b,
        parent_b_rev,
        child,
        child_rev,
    )
    await _persist(
        session,
        _link(parent_a, child_rev),
        _link(parent_b, child_rev),
    )

    result = await propagate_impact(
        session,
        "ns",
        {"ns.source_a", "ns.source_b"},
    )

    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    # Both roots should appear in caused_by
    assert sorted(by_name["ns.transform"].caused_by) == [
        "ns.source_a",
        "ns.source_b",
    ]


@pytest.mark.asyncio
async def test_propagate_impact_dimension_link_stub(session, current_user: User):
    """Passing changed_link_node_names exercises the dimension link stub."""
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("id", IntegerType())],
    )
    await _persist(session, source, source_rev)

    result = await propagate_impact(
        session,
        "ns",
        set(),
        frozenset(),
        changed_link_node_names={"ns.source"},
    )
    assert result == []


def test_merge_impacts_deduplicates_by_severity():
    """_merge_impacts keeps the higher-severity impact when the same node appears in both."""
    parent_impact = DownstreamImpact(
        name="ns.child",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason="parent graph",
        depth=1,
        caused_by=["ns.root"],
        is_external=False,
    )
    link_impact = DownstreamImpact(
        name="ns.child",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.INVALID,
        impact_type=ImpactType.WILL_INVALIDATE,
        impact_reason="link graph",
        depth=1,
        caused_by=["ns.root"],
        is_external=False,
    )

    # Higher severity (WILL_INVALIDATE) wins over MAY_AFFECT
    merged = _merge_impacts([parent_impact], [link_impact])
    assert len(merged) == 1
    assert merged[0].impact_type == ImpactType.WILL_INVALIDATE
    assert merged[0].impact_reason == "link graph"


def test_merge_impacts_adds_new_from_link():
    """_merge_impacts adds link-only impacts that don't appear in parent graph."""
    parent_impact = DownstreamImpact(
        name="ns.a",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason="parent graph",
        depth=1,
        caused_by=["ns.root"],
        is_external=False,
    )
    link_only = DownstreamImpact(
        name="ns.b",
        node_type=NodeType.METRIC,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason="link graph",
        depth=1,
        caused_by=["ns.root"],
        is_external=False,
    )

    merged = _merge_impacts([parent_impact], [link_only])
    assert len(merged) == 2
    by_name = {m.name: m for m in merged}
    assert "ns.a" in by_name
    assert "ns.b" in by_name


def test_merge_impacts_lower_severity_kept():
    """_merge_impacts keeps parent impact when link impact has lower severity."""
    parent_impact = DownstreamImpact(
        name="ns.child",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.INVALID,
        impact_type=ImpactType.WILL_INVALIDATE,
        impact_reason="parent graph",
        depth=1,
        caused_by=["ns.root"],
        is_external=False,
    )
    link_impact = DownstreamImpact(
        name="ns.child",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason="link graph",
        depth=1,
        caused_by=["ns.root"],
        is_external=False,
    )

    merged = _merge_impacts([parent_impact], [link_impact])
    assert len(merged) == 1
    assert merged[0].impact_type == ImpactType.WILL_INVALIDATE
    assert merged[0].impact_reason == "parent graph"


@pytest.mark.asyncio
async def test_propagate_impact_unparseable_query_falls_back_gracefully(
    session,
    current_user: User,
):
    """When a downstream node has a query that fails threadpool parsing,
    the exception is caught (lines 381-382) and converted to None (line 404),
    so the node falls back to inline parse in _revalidate_single_node.

    We mock parse_query to raise for the child node to simulate an unparseable query.
    """
    session.add(NodeNamespace(namespace="ns"))

    source, source_rev = _make_node(
        "ns.source",
        NodeType.SOURCE,
        NodeStatus.VALID,
        current_user.id,
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    # Child has a valid-looking query, but we'll make parse_query raise for it
    child, child_rev = _make_node(
        "ns.transform",
        NodeType.TRANSFORM,
        NodeStatus.VALID,
        current_user.id,
        query="SELECT user_id, amount FROM ns.source",
        columns=[("user_id", IntegerType()), ("amount", DoubleType())],
    )
    await _persist(session, source, source_rev, child, child_rev)
    await _persist(session, _link(source, child_rev))

    original_parse_query = None

    def _failing_parse_query(query_str):
        """Simulate a parse failure for any query."""
        raise RuntimeError("Simulated ANTLR parse failure")

    with patch(
        "datajunction_server.internal.impact.parse_query",
        side_effect=_failing_parse_query,
    ):
        result = await propagate_impact(session, "ns", {"ns.source"})

    # The child should still appear in results — the exception in threadpool parse
    # is caught and the node falls back to inline parse (which also uses parse,
    # but _revalidate_single_node catches inline parse failures too).
    # The node should be handled gracefully, not crash.
    assert len(result) >= 1
    by_name = {r.name: r for r in result}
    assert "ns.transform" in by_name
    impact = by_name["ns.transform"]
    # The node should either be MAY_AFFECT (if inline parse succeeds)
    # or WILL_INVALIDATE (if inline parse also fails)
    assert impact.impact_type in (ImpactType.MAY_AFFECT, ImpactType.WILL_INVALIDATE)
