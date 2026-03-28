"""
Tests for the compute_impact BFS propagation engine and related helpers.

Section coverage:
  1a — Helper functions (pure, no session required)
  1b — _validate_downstream_node with ROADS data
  1c — compute_impact column propagation
  1d — compute_impact deletion path
"""

import pytest
from unittest.mock import MagicMock

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node
from datajunction_server.internal.impact import (
    _ExplicitDiffSpec,
    _normalize_type,
    compute_impact,
    references_changed_columns,
    references_removed_dim,
    _validate_downstream_node,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import _node_output_options


# ---------------------------------------------------------------------------
# Section 1a — Helper functions (pure / no DB required)
# ---------------------------------------------------------------------------


class TestHelperFunctions:
    """Pure helper function tests — no session required."""

    def test_references_changed_columns_true_for_existing_column(self):
        """Returns True when a removed column overlaps with node output columns."""
        node = MagicMock()
        col_hh = MagicMock()
        col_hh.name = "hard_hat_id"
        col_od = MagicMock()
        col_od.name = "order_date"
        node.current.columns = [col_hh, col_od]

        assert references_changed_columns(node, {"hard_hat_id"}) is True

    def test_references_changed_columns_false_for_unknown_column(self):
        """Returns False when the removed column set doesn't overlap with node columns."""
        node = MagicMock()
        col = MagicMock()
        col.name = "order_date"
        node.current.columns = [col]

        assert references_changed_columns(node, {"__ghost__"}) is False

    def test_references_removed_dim_false_when_no_required_dimensions(self):
        """Returns False for a metric with no required_dimensions."""
        node = MagicMock()
        node.type = NodeType.METRIC
        node.current.required_dimensions = []

        assert references_removed_dim(node, {"some.dimension"}) is False

    def test_references_removed_dim_false_for_non_metric_type(self):
        """Returns False for non-metric node types (only metrics can have required dims)."""
        node = MagicMock()
        node.type = NodeType.TRANSFORM

        assert references_removed_dim(node, {"any.dimension"}) is False

    def test_references_removed_dim_true_when_in_required_dimensions(self):
        """Returns True when the removed dim is in the metric's required_dimensions."""
        node = MagicMock()
        node.type = NodeType.METRIC
        dim = MagicMock()
        dim.name = "default.hard_hat"
        node.current.required_dimensions = [dim]

        assert references_removed_dim(node, {"default.hard_hat"}) is True

    def test_normalize_type_empty_and_none(self):
        """_normalize_type returns '' for None and for empty string."""
        assert _normalize_type(None) == ""
        assert _normalize_type("") == ""

    def test_normalize_type_aliases(self):
        """_normalize_type applies known type aliases correctly."""
        assert _normalize_type("long") == "bigint"
        assert _normalize_type("int64") == "bigint"
        assert _normalize_type("INTEGER") == "int"
        assert _normalize_type("string") == "varchar"
        assert _normalize_type("text") == "varchar"
        assert _normalize_type("BIGINT") == "bigint"
        assert _normalize_type("bool") == "boolean"
        assert _normalize_type("float32") == "float"

    def test_normalize_type_passthrough(self):
        """Unknown type strings are lowercased and returned as-is."""
        assert _normalize_type("timestamp") == "timestamp"
        assert _normalize_type("DOUBLE") == "double"


# ---------------------------------------------------------------------------
# Section 1b — _validate_downstream_node
# ---------------------------------------------------------------------------


class TestValidateDownstreamNode:
    """Tests for _validate_downstream_node using ROADS data from the session."""

    @pytest.mark.asyncio
    async def test_validate_downstream_node_impacted_when_upstream_column_removed(
        self,
        session: AsyncSession,
    ):
        """
        repair_orders_fact selects repair_orders.hard_hat_id.
        Removing hard_hat_id from the proposed upstream columns should mark the
        child as impacted.
        """
        repair_orders = await Node.get_by_name(
            session,
            "default.repair_orders",
            options=list(_node_output_options()),
        )
        repair_orders_fact = await Node.get_by_name(
            session,
            "default.repair_orders_fact",
            options=list(_node_output_options()),
        )
        assert repair_orders is not None
        assert repair_orders_fact is not None

        proposed_cols = [
            c for c in repair_orders.current.columns if c.name != "hard_hat_id"
        ]

        is_impacted, _ = await _validate_downstream_node(
            session,
            repair_orders_fact,
            {"default.repair_orders": proposed_cols},
        )

        assert is_impacted is True

    @pytest.mark.asyncio
    async def test_validate_downstream_node_not_impacted_when_columns_intact(
        self,
        session: AsyncSession,
    ):
        """
        When all upstream columns remain in place, repair_orders_fact is not impacted.
        """
        repair_orders = await Node.get_by_name(
            session,
            "default.repair_orders",
            options=list(_node_output_options()),
        )
        repair_orders_fact = await Node.get_by_name(
            session,
            "default.repair_orders_fact",
            options=list(_node_output_options()),
        )
        assert repair_orders is not None
        assert repair_orders_fact is not None

        proposed_cols = list(repair_orders.current.columns)

        is_impacted, _ = await _validate_downstream_node(
            session,
            repair_orders_fact,
            {"default.repair_orders": proposed_cols},
        )

        assert is_impacted is False


# ---------------------------------------------------------------------------
# Section 1c — compute_impact column propagation
# ---------------------------------------------------------------------------


class TestComputeImpactColumnPropagation:
    """BFS column-propagation tests for compute_impact."""

    @pytest.mark.asyncio
    async def test_compute_impact_empty_input(self, session: AsyncSession):
        """An empty changed_nodes dict yields no impacts."""
        results = [n async for n in compute_impact(session, {})]
        assert results == []

    @pytest.mark.asyncio
    async def test_compute_impact_new_node_no_downstream(self, session: AsyncSession):
        """existing_node=None (new node) → no downstream impacts yielded."""
        results = [
            n
            async for n in compute_impact(
                session,
                {
                    "brand_new.source": (
                        _ExplicitDiffSpec(node_type=NodeType.SOURCE),
                        None,
                        set(),
                    ),
                },
            )
        ]
        assert results == []

    @pytest.mark.asyncio
    async def test_compute_impact_column_removal_impacts_direct_child(
        self,
        session: AsyncSession,
    ):
        """Removing hard_hat_id from repair_orders impacts repair_orders_fact."""
        repair_orders = await Node.get_by_name(
            session,
            "default.repair_orders",
            options=list(_node_output_options()),
        )
        assert repair_orders is not None

        results = [
            n
            async for n in compute_impact(
                session,
                {
                    "default.repair_orders": (
                        _ExplicitDiffSpec(
                            node_type=NodeType.SOURCE,
                            explicit_columns_removed={"hard_hat_id"},
                        ),
                        repair_orders,
                        set(),
                    ),
                },
            )
        ]

        impacted_names = {r.name for r in results}
        assert "default.repair_orders_fact" in impacted_names

    @pytest.mark.asyncio
    async def test_compute_impact_column_removal_multihop(self, session: AsyncSession):
        """Column removal cascades to metrics downstream of repair_orders_fact."""
        repair_orders = await Node.get_by_name(
            session,
            "default.repair_orders",
            options=list(_node_output_options()),
        )
        assert repair_orders is not None

        results = [
            n
            async for n in compute_impact(
                session,
                {
                    "default.repair_orders": (
                        _ExplicitDiffSpec(
                            node_type=NodeType.SOURCE,
                            explicit_columns_removed={"hard_hat_id"},
                        ),
                        repair_orders,
                        set(),
                    ),
                },
            )
        ]

        metric_impacts = [r for r in results if r.node_type == NodeType.METRIC]
        assert len(metric_impacts) > 0, "Expected at least one metric to be impacted"

    @pytest.mark.asyncio
    async def test_compute_impact_column_removal_topo_order(
        self,
        session: AsyncSession,
    ):
        """repair_orders_fact appears before its downstream metrics in BFS output."""
        repair_orders = await Node.get_by_name(
            session,
            "default.repair_orders",
            options=list(_node_output_options()),
        )
        assert repair_orders is not None

        results = [
            n
            async for n in compute_impact(
                session,
                {
                    "default.repair_orders": (
                        _ExplicitDiffSpec(
                            node_type=NodeType.SOURCE,
                            explicit_columns_removed={"hard_hat_id"},
                        ),
                        repair_orders,
                        set(),
                    ),
                },
            )
        ]

        names = [r.name for r in results]
        assert "default.repair_orders_fact" in names, (
            "repair_orders_fact not in impacted nodes"
        )
        fact_idx = names.index("default.repair_orders_fact")
        # Metrics downstream of repair_orders_fact must appear after it in BFS order.
        for i, r in enumerate(results):
            if r.node_type == NodeType.METRIC:
                assert i >= fact_idx, (
                    f"Metric {r.name} at index {i} appeared before "
                    f"repair_orders_fact at index {fact_idx}"
                )

    @pytest.mark.asyncio
    async def test_compute_impact_column_removal_caused_by_tracks_origin(
        self,
        session: AsyncSession,
    ):
        """All impacted nodes have a non-empty caused_by list."""
        repair_orders = await Node.get_by_name(
            session,
            "default.repair_orders",
            options=list(_node_output_options()),
        )
        assert repair_orders is not None

        results = [
            n
            async for n in compute_impact(
                session,
                {
                    "default.repair_orders": (
                        _ExplicitDiffSpec(
                            node_type=NodeType.SOURCE,
                            explicit_columns_removed={"hard_hat_id"},
                        ),
                        repair_orders,
                        set(),
                    ),
                },
            )
        ]

        assert len(results) > 0
        for node in results:
            assert len(node.caused_by) > 0, f"Node {node.name} has empty caused_by"

    @pytest.mark.asyncio
    async def test_compute_impact_nonexistent_column_removal_no_impact(
        self,
        session: AsyncSession,
    ):
        """Removing a column that doesn't exist produces no downstream impact."""
        repair_orders = await Node.get_by_name(
            session,
            "default.repair_orders",
            options=list(_node_output_options()),
        )
        assert repair_orders is not None

        results = [
            n
            async for n in compute_impact(
                session,
                {
                    "default.repair_orders": (
                        _ExplicitDiffSpec(
                            node_type=NodeType.SOURCE,
                            explicit_columns_removed={"__nonexistent_ghost_column__"},
                        ),
                        repair_orders,
                        set(),
                    ),
                },
            )
        ]

        # Ghost column is not in existing columns → actually_removed is empty →
        # proposed_columns is None → BFS triggers no column-based impacts.
        column_impacts = [r for r in results if r.impact_type == "column"]
        assert len(column_impacts) == 0


# ---------------------------------------------------------------------------
# Section 1d — compute_impact deletion path
# ---------------------------------------------------------------------------


class TestComputeImpactDeletion:
    """Tests for compute_impact when a node is deleted (proposed_spec=None)."""

    @pytest.mark.asyncio
    async def test_compute_impact_deleted_node_impacts_all_direct_consumers(
        self,
        session: AsyncSession,
    ):
        """
        Deleting repair_orders_fact marks all direct metric consumers as
        deleted_parent impacted.
        """
        repair_orders_fact = await Node.get_by_name(
            session,
            "default.repair_orders_fact",
            options=list(_node_output_options()),
        )
        assert repair_orders_fact is not None

        results = [
            n
            async for n in compute_impact(
                session,
                {
                    "default.repair_orders_fact": (
                        None,  # deletion
                        repair_orders_fact,
                        set(),
                    ),
                },
            )
        ]

        assert len(results) > 0
        for node in results:
            assert node.impact_type == "deleted_parent", (
                f"Node {node.name} has impact_type={node.impact_type!r}, "
                "expected 'deleted_parent'"
            )

    @pytest.mark.asyncio
    async def test_compute_impact_deleted_node_propagates_unconditionally(
        self,
        session: AsyncSession,
    ):
        """
        Deleting repair_orders propagates unconditionally: repair_orders_fact
        is impacted and so are the metrics further downstream.
        """
        repair_orders = await Node.get_by_name(
            session,
            "default.repair_orders",
            options=list(_node_output_options()),
        )
        assert repair_orders is not None

        results = [
            n
            async for n in compute_impact(
                session,
                {
                    "default.repair_orders": (
                        None,  # deletion
                        repair_orders,
                        set(),
                    ),
                },
            )
        ]

        impacted_names = {r.name for r in results}
        assert "default.repair_orders_fact" in impacted_names
        metric_impacts = [r for r in results if r.node_type == NodeType.METRIC]
        assert len(metric_impacts) > 0, (
            "Expected metrics downstream of repair_orders_fact to be impacted"
        )
