"""Unit tests for internal.cube_materializations helper functions."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datajunction_server.construction.build_v3.types import BuildContext, GrainGroupSQL
from datajunction_server.internal.cube_materializations import (
    _v3_col_to_model_column,
    _v3_grain_group_to_measures_query,
)
from datajunction_server.models.node_type import NodeType


def _make_col(semantic_name, semantic_type="dimension", name="col", type_="str"):
    col = SimpleNamespace(
        semantic_name=semantic_name,
        semantic_type=semantic_type,
        name=name,
        type=type_,
    )
    return col


class TestV3ColToModelColumn:
    def test_no_separator_in_semantic_name(self):
        """Line 177->180 False: semantic_name has no dot → column/node stay None."""
        col = _make_col("nodot", semantic_type="dimension", name="nodot_alias")
        result = _v3_col_to_model_column(col)
        assert result.column is None
        assert result.node is None
        assert result.name == "nodot_alias"

    def test_empty_semantic_name(self):
        """Line 177->180 False: empty semantic_name → column/node stay None."""
        col = _make_col("", semantic_type="metric", name="m_hash")
        result = _v3_col_to_model_column(col)
        assert result.column is None
        assert result.node is None
        assert result.name == "m_hash"

    def test_dotted_dimension(self):
        """Dotted semantic_name → column/node extracted; name stays the v3 alias."""
        col = _make_col(
            "default.hard_hat.city",
            semantic_type="dimension",
            name="city",
        )
        result = _v3_col_to_model_column(col)
        assert result.column == "city"
        assert result.node == "default.hard_hat"
        assert result.name == "city"
        assert result.semantic_entity == "default.hard_hat.city"

    def test_metric_type_collapses_to_measure(self):
        """metric/metric_component/metric_input semantic types map to 'measure'."""
        for v3_type in ("metric", "metric_component", "metric_input"):
            col = _make_col(
                "default.orders:count_hash",
                semantic_type=v3_type,
                name="count_hash",
            )
            result = _v3_col_to_model_column(col)
            assert result.semantic_type == "measure"
            assert result.name == "count_hash"

    def test_role_qualified_dimension_passes_through_cleanly(self):
        """Role-qualified semantic name (``foo.dateint[role]``) keeps the v3 alias.

        The brackets only live in ``semantic_entity``; ``name`` is the SQL alias
        from v3 and must not contain bracket characters.
        """
        col = _make_col(
            "default.dim.dateint[event_date]",
            semantic_type="dimension",
            name="dateint",
        )
        result = _v3_col_to_model_column(col)
        assert result.name == "dateint"
        assert "[" not in result.name and "]" not in result.name
        assert result.semantic_entity == "default.dim.dateint[event_date]"


def _make_node(name, node_type, current=None):
    node = MagicMock()
    node.name = name
    node.type = node_type
    node.current = current
    return node


def _make_source_rev(catalog_name, schema, table):
    rev = MagicMock()
    if catalog_name is None:
        rev.catalog = None
    else:
        rev.catalog = MagicMock()
        rev.catalog.name = catalog_name
    rev.schema_ = schema
    rev.table = table
    return rev


def _make_ctx(nodes, parent_map):
    ctx = MagicMock(spec=BuildContext)
    ctx.nodes = nodes
    ctx.parent_map = parent_map
    return ctx


def _make_gg(parent_name, columns=None, grain=None, metrics=None, query=None):
    gg = MagicMock(spec=GrainGroupSQL)
    gg.parent_name = parent_name
    gg.columns = columns or []
    gg.grain = grain or []
    gg.metrics = metrics or []
    gg.sql = query or "SELECT 1"
    return gg


class TestV3GrainGroupToMeasuresQuery:
    """Tests for branches inside _v3_grain_group_to_measures_query."""

    @pytest.fixture
    def session(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_walk_missing_node_returns_early(self, session):
        """Line 246: _walk hits a parent_name not in ctx.nodes → returns without error."""
        fact_rev = MagicMock()
        fact_rev.name = "default.repair_orders_fact"
        fact_rev.version = "v1.0"
        fact_rev.display_name = "Fact"

        fact_node = _make_node(
            "default.repair_orders_fact",
            NodeType.TRANSFORM,
            fact_rev,
        )

        # parent_map says fact has a parent "default.missing", which is NOT in ctx.nodes
        ctx = _make_ctx(
            nodes={"default.repair_orders_fact": fact_node},
            parent_map={"default.repair_orders_fact": ["default.missing"]},
        )

        gg = _make_gg(
            parent_name="default.repair_orders_fact",
            columns=[],
            grain=[],
            metrics=[],
        )

        with patch("datajunction_server.utils.refresh_if_needed", AsyncMock()):
            result = await _v3_grain_group_to_measures_query(
                session,
                gg,
                ctx,
                decomposed_metrics={},
            )

        # Missing node in _walk → upstream_tables is empty; function still returns a MeasuresQuery
        assert result.upstream_tables == []

    @pytest.mark.asyncio
    async def test_source_node_missing_catalog_skipped(self, session):
        """Lines 254->264 False: source with no catalog → not added to upstream_tables."""
        source_rev = _make_source_rev(catalog_name=None, schema="roads", table="orders")
        source_node = _make_node("default.orders", NodeType.SOURCE, source_rev)

        fact_rev = MagicMock()
        fact_rev.name = "default.repair_orders_fact"
        fact_rev.version = "v1.0"
        fact_rev.display_name = "Fact"
        fact_node = _make_node(
            "default.repair_orders_fact",
            NodeType.TRANSFORM,
            fact_rev,
        )

        ctx = _make_ctx(
            nodes={
                "default.repair_orders_fact": fact_node,
                "default.orders": source_node,
            },
            parent_map={
                "default.repair_orders_fact": ["default.orders"],
                "default.orders": [],
            },
        )

        gg = _make_gg(
            parent_name="default.repair_orders_fact",
            columns=[],
            grain=[],
            metrics=[],
        )

        with patch("datajunction_server.utils.refresh_if_needed", AsyncMock()):
            result = await _v3_grain_group_to_measures_query(
                session,
                gg,
                ctx,
                decomposed_metrics={},
            )

        # No catalog → not appended
        assert result.upstream_tables == []

    @pytest.mark.asyncio
    async def test_source_node_duplicate_skipped(self, session):
        """Lines 254->264 False: same source visited via two paths → only added once."""
        source_rev = _make_source_rev("default", "roads", "repair_orders")
        source_node = _make_node("default.repair_orders", NodeType.SOURCE, source_rev)

        fact_rev = MagicMock()
        fact_rev.name = "default.repair_orders_fact"
        fact_rev.version = "v1.0"
        fact_rev.display_name = "Fact"
        fact_node = _make_node(
            "default.repair_orders_fact",
            NodeType.TRANSFORM,
            fact_rev,
        )

        # Two parents both point to the same source
        mid_rev = MagicMock()
        mid_rev.name = "default.mid"
        mid_node = _make_node("default.mid", NodeType.TRANSFORM, mid_rev)

        ctx = _make_ctx(
            nodes={
                "default.repair_orders_fact": fact_node,
                "default.mid": mid_node,
                "default.repair_orders": source_node,
            },
            parent_map={
                "default.repair_orders_fact": ["default.repair_orders", "default.mid"],
                "default.mid": ["default.repair_orders"],
                "default.repair_orders": [],
            },
        )

        gg = _make_gg(
            parent_name="default.repair_orders_fact",
            columns=[],
            grain=[],
            metrics=[],
        )

        with patch("datajunction_server.utils.refresh_if_needed", AsyncMock()):
            result = await _v3_grain_group_to_measures_query(
                session,
                gg,
                ctx,
                decomposed_metrics={},
            )

        # Source reached via two paths but only appended once
        assert result.upstream_tables == ["default.roads.repair_orders"]

    @pytest.mark.asyncio
    async def test_dim_column_no_semantic_name_skipped(self, session):
        """Line 278: dimension column with empty semantic_name → continue (not walked)."""
        fact_rev = MagicMock()
        fact_rev.name = "default.fact"
        fact_rev.version = "v1.0"
        fact_rev.display_name = "Fact"
        fact_node = _make_node("default.fact", NodeType.TRANSFORM, fact_rev)

        ctx = _make_ctx(
            nodes={"default.fact": fact_node},
            parent_map={"default.fact": []},
        )

        # Dimension column with empty semantic_name → line 278 continue
        empty_sem_col = _make_col("", semantic_type="dimension", name="d")
        gg = _make_gg(
            parent_name="default.fact",
            columns=[empty_sem_col],
            grain=[],
            metrics=[],
        )

        with patch("datajunction_server.utils.refresh_if_needed", AsyncMock()):
            result = await _v3_grain_group_to_measures_query(
                session,
                gg,
                ctx,
                decomposed_metrics={},
            )

        assert result.upstream_tables == []

    @pytest.mark.asyncio
    async def test_dim_column_no_dot_in_semantic_name_skipped(self, session):
        """Line 278: dimension column with semantic_name but no dot → continue."""
        fact_rev = MagicMock()
        fact_rev.name = "default.fact"
        fact_rev.version = "v1.0"
        fact_rev.display_name = "Fact"
        fact_node = _make_node("default.fact", NodeType.TRANSFORM, fact_rev)

        ctx = _make_ctx(
            nodes={"default.fact": fact_node},
            parent_map={"default.fact": []},
        )

        no_dot_col = _make_col("nodothere", semantic_type="dimension", name="d")
        gg = _make_gg(
            parent_name="default.fact",
            columns=[no_dot_col],
            grain=[],
            metrics=[],
        )

        with patch("datajunction_server.utils.refresh_if_needed", AsyncMock()):
            result = await _v3_grain_group_to_measures_query(
                session,
                gg,
                ctx,
                decomposed_metrics={},
            )

        assert result.upstream_tables == []
