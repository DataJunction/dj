"""
Tests for the impact preview feature.

Covers:
- references_changed_columns / references_removed_dim (lightweight checks)
- compute_impact — column change, dim link removed, deleted node paths
- POST /nodes/{name}/impact-preview endpoint
- /deployments/impact downstream analysis (now powered by compute_impact)
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.internal.impact import (
    _validate_downstream_node,
    compute_impact,
    references_changed_columns,
    references_removed_dim,
)
from datajunction_server.models.impact_preview import NodeChange
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import _node_output_options


# ---------------------------------------------------------------------------
# references_changed_columns
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_references_changed_columns_true_for_existing_column(
    session: AsyncSession,
) -> None:
    from datajunction_server.database.node import Node

    node = await Node.get_by_name(
        session, "default.repair_orders_fact", options=_node_output_options(),
    )
    assert node is not None
    a_col = next(iter(c.name for c in node.current.columns))
    assert references_changed_columns(node, {a_col}) is True


@pytest.mark.asyncio
async def test_references_changed_columns_false_for_missing_column(
    session: AsyncSession,
) -> None:
    from datajunction_server.database.node import Node

    node = await Node.get_by_name(
        session, "default.repair_orders_fact", options=_node_output_options(),
    )
    assert node is not None
    assert references_changed_columns(node, {"__definitely_not_a_column__"}) is False


# ---------------------------------------------------------------------------
# references_removed_dim
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_references_removed_dim_true_via_parent(session: AsyncSession) -> None:
    """A METRIC or CUBE node whose parents include the removed dim returns True."""
    from datajunction_server.database.node import Node

    # avg_repair_price is a metric whose parent is repair_orders_fact (a transform)
    node = await Node.get_by_name(
        session, "default.avg_repair_price", options=_node_output_options(),
    )
    if node is None:
        pytest.skip("avg_repair_price not found")
    parent_names = {p.name for p in node.current.parents}
    if not parent_names:
        pytest.skip("Node has no parents")
    a_parent = next(iter(parent_names))
    assert references_removed_dim(node, {a_parent}) is True


@pytest.mark.asyncio
async def test_references_removed_dim_false_for_absent_dim(
    session: AsyncSession,
) -> None:
    from datajunction_server.database.node import Node

    node = await Node.get_by_name(
        session, "default.avg_repair_price", options=_node_output_options(),
    )
    if node is None:
        pytest.skip("avg_repair_price not found")
    assert references_removed_dim(node, {"__no_such_dim__"}) is False


# ---------------------------------------------------------------------------
# compute_impact — column change path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compute_impact_column_removal_impacts_downstream(
    session: AsyncSession,
) -> None:
    """Removing a real column from repair_orders_fact impacts downstream nodes."""
    from datajunction_server.database.node import Node

    node = await Node.get_by_name(
        session, "default.repair_orders_fact", options=_node_output_options(),
    )
    assert node is not None
    a_col = next(iter(c.name for c in node.current.columns))

    impacted = await compute_impact(
        session, {"default.repair_orders_fact": NodeChange(columns_removed=[a_col])},
    )
    assert len(impacted) > 0
    for n in impacted:
        assert n.impact_type in ("column", "dimension_link", "deleted_parent")
        assert len(n.caused_by) > 0


@pytest.mark.asyncio
async def test_compute_impact_nonexistent_column_no_impact(
    session: AsyncSession,
) -> None:
    """Removing a column that nothing references produces no impact."""
    impacted = await compute_impact(
        session,
        {"default.repair_orders_fact": NodeChange(columns_removed=["__ghost__"])},
    )
    assert impacted == []


@pytest.mark.asyncio
async def test_compute_impact_deleted_node_impacts_all_consumers(
    session: AsyncSession,
) -> None:
    """A deleted node unconditionally impacts all direct consumers."""
    impacted = await compute_impact(
        session, {"default.repair_orders_fact": NodeChange(is_deleted=True)},
    )
    assert len(impacted) > 0
    for n in impacted:
        assert n.impact_type == "deleted_parent"


@pytest.mark.asyncio
async def test_compute_impact_empty_input_returns_empty(session: AsyncSession) -> None:
    assert await compute_impact(session, {}) == []


@pytest.mark.asyncio
async def test_compute_impact_dim_link_removed_only_terminal_nodes_impacted(
    session: AsyncSession,
) -> None:
    """Intermediate source/transform nodes should not appear in the impacted list."""
    impacted = await compute_impact(
        session,
        {
            "default.repair_orders_fact": NodeChange(
                dim_links_removed=["default.hard_hat"],
            ),
        },
    )
    for n in impacted:
        assert n.node_type in (NodeType.METRIC, NodeType.CUBE, NodeType.DIMENSION)


# ---------------------------------------------------------------------------
# API endpoint: single-node impact preview
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_single_node_impact_preview_column_removal(
    client_with_roads: AsyncClient,
) -> None:
    response = await client_with_roads.post(
        "/nodes/default.repair_orders_fact/impact-preview",
        json={"columns_removed": ["hard_hat_id"]},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["node_diff"]["name"] == "default.repair_orders_fact"
    assert data["node_diff"]["change_type"] == "modified"
    assert "downstream_impact" in data


@pytest.mark.asyncio
async def test_single_node_impact_preview_deleted_node(
    client_with_roads: AsyncClient,
) -> None:
    response = await client_with_roads.post(
        "/nodes/default.repair_orders_fact/impact-preview",
        json={"is_deleted": True},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["node_diff"]["change_type"] == "deleted"
    assert len(data["downstream_impact"]) > 0
    for n in data["downstream_impact"]:
        assert n["impact_type"] == "deleted_parent"


@pytest.mark.asyncio
async def test_single_node_impact_preview_nonexistent_returns_404(
    client_with_roads: AsyncClient,
) -> None:
    response = await client_with_roads.post(
        "/nodes/default.does_not_exist/impact-preview",
        json={"columns_removed": ["foo"]},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_single_node_impact_preview_no_change_empty_impact(
    client_with_roads: AsyncClient,
) -> None:
    response = await client_with_roads.post(
        "/nodes/default.repair_orders_fact/impact-preview",
        json={},
    )
    assert response.status_code == 200
    assert response.json()["downstream_impact"] == []


# ---------------------------------------------------------------------------
# /deployments/impact now powered by compute_impact
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deployment_impact_column_removal_detected(
    client_with_roads: AsyncClient,
) -> None:
    """
    Deployment impact correctly detects a REMOVED column when a source node's
    column spec no longer includes a previously-existing column.
    """
    from datajunction_server.models.deployment import (
        ColumnSpec,
        SourceSpec,
        DeploymentSpec,
    )

    # repair_orders has hard_hat_id; removing it should be detected as a column removal
    spec = DeploymentSpec(
        namespace="default",
        nodes=[
            SourceSpec(
                name="repair_orders",
                catalog="default",
                schema_="roads",
                table="repair_orders",
                columns=[
                    ColumnSpec(name="repair_order_id", type="int"),
                    ColumnSpec(name="municipality_id", type="int"),
                    # hard_hat_id intentionally omitted
                    ColumnSpec(name="dispatcher_id", type="int"),
                    ColumnSpec(name="order_date", type="timestamp"),
                    ColumnSpec(name="dispatched_date", type="timestamp"),
                    ColumnSpec(name="required_date", type="timestamp"),
                ],
            ),
        ],
    )
    response = await client_with_roads.post(
        "/deployments/impact",
        json=spec.model_dump(by_alias=True),
    )
    assert response.status_code == 200
    data = response.json()

    # Find the repair_orders change in the changes list
    repair_orders_change = next(
        (c for c in data["changes"] if c["name"] == "default.repair_orders"), None,
    )
    assert repair_orders_change is not None, "repair_orders should appear in changes"
    assert repair_orders_change["operation"] == "update"

    # hard_hat_id should be detected as REMOVED
    removed_cols = [
        cc
        for cc in repair_orders_change["column_changes"]
        if cc["change_type"] == "removed"
    ]
    assert any(cc["column"] == "hard_hat_id" for cc in removed_cols), (
        f"hard_hat_id should be detected as removed; column_changes: {repair_orders_change['column_changes']}"
    )


# ---------------------------------------------------------------------------
# _validate_downstream_node
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_downstream_node_detects_missing_upstream_column(
    session: AsyncSession,
) -> None:
    """
    repair_orders_fact SELECTs hard_hat_id from repair_orders.
    Proposing that repair_orders no longer exposes hard_hat_id should cause
    repair_orders_fact to be flagged as impacted.
    """
    from datajunction_server.database.node import Node

    fact = await Node.get_by_name(
        session, "default.repair_orders_fact", options=_node_output_options(),
    )
    source = await Node.get_by_name(
        session, "default.repair_orders", options=_node_output_options(),
    )
    assert fact is not None and source is not None

    # Proposed state: repair_orders without hard_hat_id
    proposed_cols = [c for c in source.current.columns if c.name != "hard_hat_id"]
    is_impacted, new_cols = await _validate_downstream_node(
        session,
        fact,
        {"default.repair_orders": proposed_cols},
    )

    assert is_impacted is True


@pytest.mark.asyncio
async def test_validate_downstream_node_no_impact_when_column_intact(
    session: AsyncSession,
) -> None:
    """
    Proposing an upstream state that still includes all columns should not
    flag the child as impacted.
    """
    from datajunction_server.database.node import Node

    fact = await Node.get_by_name(
        session, "default.repair_orders_fact", options=_node_output_options(),
    )
    source = await Node.get_by_name(
        session, "default.repair_orders", options=_node_output_options(),
    )
    assert fact is not None and source is not None

    # Proposed state: repair_orders unchanged
    is_impacted, new_cols = await _validate_downstream_node(
        session,
        fact,
        {"default.repair_orders": list(source.current.columns)},
    )

    assert is_impacted is False
    assert len(new_cols) > 0


# ---------------------------------------------------------------------------
# compute_impact — multi-hop propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compute_impact_multihop_column_removal(
    session: AsyncSession,
) -> None:
    """
    Removing hard_hat_id from repair_orders (source) should cascade:
      repair_orders → repair_orders_fact (direct child, fails validation)
                    → downstream metrics (second hop, impacted via repair_orders_fact)

    Verifies that impact propagates beyond the immediate child.
    """
    impacted = await compute_impact(
        session,
        {"default.repair_orders": NodeChange(columns_removed=["hard_hat_id"])},
    )
    impacted_names = {n.name for n in impacted}

    # Direct child must be impacted
    assert "default.repair_orders_fact" in impacted_names

    # At least one metric downstream of repair_orders_fact must also be impacted
    metric_impacts = [n for n in impacted if n.node_type == NodeType.METRIC]
    assert len(metric_impacts) > 0, (
        "Expected at least one metric to be impacted via repair_orders_fact"
    )


@pytest.mark.asyncio
async def test_compute_impact_multihop_topo_order(
    session: AsyncSession,
) -> None:
    """
    The topo-sorted output should list repair_orders_fact before any metric
    that depends on it.
    """
    impacted = await compute_impact(
        session,
        {"default.repair_orders": NodeChange(columns_removed=["hard_hat_id"])},
    )
    impacted_names = [n.name for n in impacted]

    if "default.repair_orders_fact" not in impacted_names:
        pytest.skip("repair_orders_fact not impacted — graph may have changed")

    fact_idx = impacted_names.index("default.repair_orders_fact")
    for node in impacted:
        if node.node_type == NodeType.METRIC and node.name in impacted_names:
            metric_idx = impacted_names.index(node.name)
            assert fact_idx < metric_idx, (
                f"repair_orders_fact (idx {fact_idx}) should precede "
                f"metric {node.name} (idx {metric_idx}) in topo order"
            )


@pytest.mark.asyncio
async def test_compute_impact_multihop_caused_by_tracks_origin(
    session: AsyncSession,
) -> None:
    """
    Even for nodes impacted multiple hops away, caused_by should include
    the original changed node (default.repair_orders).
    """
    impacted = await compute_impact(
        session,
        {"default.repair_orders": NodeChange(columns_removed=["hard_hat_id"])},
    )
    for node in impacted:
        assert "default.repair_orders" in node.caused_by, (
            f"{node.name}.caused_by={node.caused_by} missing original changed node"
        )
