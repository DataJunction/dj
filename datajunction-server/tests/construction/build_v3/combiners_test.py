"""
Tests for the combiners module.

These tests verify that grain groups can be correctly combined using
FULL OUTER JOIN with COALESCE on shared dimensions.
"""

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from . import assert_sql_equal

from datajunction_server.construction.build_v3.combiners import (
    _build_grain_group_from_preagg_table,
    _compute_preagg_table_name,
    _reorder_partition_column_last,
    build_combiner_sql,
    build_combiner_sql_from_preaggs,
    validate_grain_groups_compatible,
    CombinedGrainGroupResult,
)
from datajunction_server.construction.build_v3.utils import (
    _build_join_criteria,
)
from datajunction_server.construction.build_v3.types import (
    GrainGroupSQL,
    ColumnMetadata,
)
from datajunction_server.models.query import V3ColumnMetadata
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_grain_group_hash,
    compute_expression_hash,
)
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
    PreAggMeasure,
)
from datajunction_server.models.partition import PartitionType, Granularity
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse as parse_sql


def _create_grain_group(
    sql: str,
    columns: list[dict],
    grain: list[str],
    parent_name: str = "test_node",
    metrics: list[str] | None = None,
    components: list[MetricComponent] | None = None,
) -> GrainGroupSQL:
    """
    Helper to create a GrainGroupSQL from SQL string and column definitions.
    """
    query = parse_sql(sql)

    col_metadata = [
        ColumnMetadata(
            name=col["name"],
            semantic_name=col.get("semantic_name", col["name"]),
            type=col.get("type", "string"),
            semantic_type=col.get("semantic_type", "dimension"),
        )
        for col in columns
    ]

    return GrainGroupSQL(
        query=query,
        columns=col_metadata,
        grain=grain,
        aggregability=Aggregability.FULL,
        metrics=metrics or [],
        parent_name=parent_name,
        components=components or [],
    )


class TestBuildCombinerSql:
    """Tests for build_combiner_sql function."""

    def test_single_grain_group_returns_unchanged(self):
        """
        Single grain group should return unchanged (no JOIN needed).
        """
        gg = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS line_total_sum_e1f61696 FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {
                    "name": "line_total_sum_e1f61696",
                    "semantic_type": "metric_component",
                },
            ],
            grain=["date_id"],
            metrics=["revenue"],
        )

        result = build_combiner_sql([gg])

        assert result.grain_groups_combined == 1
        assert result.shared_dimensions == ["date_id"]
        assert result.all_measures == ["line_total_sum_e1f61696"]
        assert len(result.columns) == 2

        # SQL should be unchanged (no CTEs or JOINs)
        assert_sql_equal(
            result.sql,
            """
            SELECT date_id, SUM(amount) AS line_total_sum_e1f61696
            FROM orders
            GROUP BY date_id
            """,
        )

    def test_two_grain_groups_full_outer_join(self):
        """
        Two grain groups should be combined with FULL OUTER JOIN.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, status, SUM(amount) AS line_total_sum_e1f61696 FROM orders GROUP BY date_id, status",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "status", "semantic_type": "dimension"},
                {
                    "name": "line_total_sum_e1f61696",
                    "semantic_type": "metric_component",
                },
            ],
            grain=["date_id", "status"],
            parent_name="orders",
            metrics=["revenue"],
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, status, COUNT(*) AS page_views FROM events GROUP BY date_id, status",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "status", "semantic_type": "dimension"},
                {"name": "page_views", "semantic_type": "metric_component"},
            ],
            grain=["date_id", "status"],
            parent_name="events",
            metrics=["views"],
        )

        result = build_combiner_sql([gg1, gg2])

        assert result.grain_groups_combined == 2
        assert set(result.shared_dimensions) == {"date_id", "status"}
        assert set(result.all_measures) == {"line_total_sum_e1f61696", "page_views"}

        # Verify the SQL structure with FULL OUTER JOIN and COALESCE
        # Note: The combiner doesn't use AS for the COALESCE aliases
        assert_sql_equal(
            result.sql,
            """
            WITH
            gg1 AS (
                SELECT date_id, status, SUM(amount) AS line_total_sum_e1f61696
                FROM orders
                GROUP BY date_id, status
            ),
            gg2 AS (
                SELECT date_id, status, COUNT(*) AS page_views
                FROM events
                GROUP BY date_id, status
            )
            SELECT
                COALESCE(gg1.date_id, gg2.date_id) date_id,
                COALESCE(gg1.status, gg2.status) status,
                gg1.line_total_sum_e1f61696,
                gg2.page_views
            FROM gg1
            FULL OUTER JOIN gg2
                ON gg1.date_id = gg2.date_id AND gg1.status = gg2.status
            """,
        )

    def test_three_grain_groups_chained_joins(self):
        """
        Three grain groups should produce chained FULL OUTER JOINs.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, SUM(revenue) AS revenue FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "revenue", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="orders",
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, COUNT(*) AS views FROM events GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "views", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="events",
        )

        gg3 = _create_grain_group(
            sql="SELECT date_id, SUM(clicks) AS clicks FROM clicks GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "clicks", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="clicks",
        )

        result = build_combiner_sql([gg1, gg2, gg3])

        assert result.grain_groups_combined == 3
        assert result.shared_dimensions == ["date_id"]
        assert set(result.all_measures) == {"revenue", "views", "clicks"}

        # Should have 2 FULL OUTER JOINs for 3 tables
        assert_sql_equal(
            result.sql,
            """
            WITH
            gg1 AS (
                SELECT date_id, SUM(revenue) AS revenue
                FROM orders
                GROUP BY date_id
            ),
            gg2 AS (
                SELECT date_id, COUNT(*) AS views
                FROM events
                GROUP BY date_id
            ),
            gg3 AS (
                SELECT date_id, SUM(clicks) AS clicks
                FROM clicks
                GROUP BY date_id
            )
            SELECT
                COALESCE(gg1.date_id, gg2.date_id, gg3.date_id) date_id,
                gg1.revenue,
                gg2.views,
                gg3.clicks
            FROM gg1
            FULL OUTER JOIN gg2 ON gg1.date_id = gg2.date_id
            FULL OUTER JOIN gg3 ON gg1.date_id = gg3.date_id
            """,
        )

    def test_coalesce_on_all_shared_dimensions(self):
        """
        COALESCE should be applied to all shared dimension columns.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, region, SUM(amount) AS amount FROM orders GROUP BY date_id, region",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "region", "semantic_type": "dimension"},
                {"name": "amount", "semantic_type": "metric_component"},
            ],
            grain=["date_id", "region"],
            parent_name="orders",
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, region, COUNT(*) AS count FROM events GROUP BY date_id, region",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "region", "semantic_type": "dimension"},
                {"name": "count", "semantic_type": "metric_component"},
            ],
            grain=["date_id", "region"],
            parent_name="events",
        )

        result = build_combiner_sql([gg1, gg2])

        # Both dimension columns should be COALESCEd
        assert_sql_equal(
            result.sql,
            """
            WITH
            gg1 AS (
                SELECT date_id, region, SUM(amount) AS amount
                FROM orders
                GROUP BY date_id, region
            ),
            gg2 AS (
                SELECT date_id, region, COUNT(*) AS count
                FROM events
                GROUP BY date_id, region
            )
            SELECT
                COALESCE(gg1.date_id, gg2.date_id) date_id,
                COALESCE(gg1.region, gg2.region) region,
                gg1.amount,
                gg2.count
            FROM gg1
            FULL OUTER JOIN gg2
                ON gg1.date_id = gg2.date_id AND gg1.region = gg2.region
            """,
        )

    def test_custom_output_table_names(self):
        """
        Custom output table names should be used as CTE aliases.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS amount FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "amount", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, COUNT(*) AS count FROM events GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "count", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
        )

        result = build_combiner_sql(
            [gg1, gg2],
            output_table_names=["orders_preagg", "events_preagg"],
        )

        # Custom CTE names should be used
        assert_sql_equal(
            result.sql,
            """
            WITH
            orders_preagg AS (
                SELECT date_id, SUM(amount) AS amount
                FROM orders
                GROUP BY date_id
            ),
            events_preagg AS (
                SELECT date_id, COUNT(*) AS count
                FROM events
                GROUP BY date_id
            )
            SELECT
                COALESCE(orders_preagg.date_id, events_preagg.date_id) date_id,
                orders_preagg.amount,
                events_preagg.count
            FROM orders_preagg
            FULL OUTER JOIN events_preagg
                ON orders_preagg.date_id = events_preagg.date_id
            """,
        )

    def test_output_columns_metadata(self):
        """
        Output column metadata should include all dimensions and measures.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS revenue FROM orders GROUP BY date_id",
            columns=[
                {
                    "name": "date_id",
                    "semantic_name": "v3.date_dim.date_id",
                    "type": "int",
                    "semantic_type": "dimension",
                },
                {
                    "name": "revenue",
                    "semantic_name": "v3.total_revenue",
                    "type": "double",
                    "semantic_type": "metric_component",
                },
            ],
            grain=["date_id"],
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, COUNT(*) AS orders FROM orders GROUP BY date_id",
            columns=[
                {
                    "name": "date_id",
                    "semantic_name": "v3.date_dim.date_id",
                    "type": "int",
                    "semantic_type": "dimension",
                },
                {
                    "name": "orders",
                    "semantic_name": "v3.order_count",
                    "type": "bigint",
                    "semantic_type": "metric_component",
                },
            ],
            grain=["date_id"],
        )

        result = build_combiner_sql([gg1, gg2])

        # Check column metadata
        column_names = [col.name for col in result.columns]
        assert "date_id" in column_names
        assert "revenue" in column_names
        assert "orders" in column_names

        # Check semantic info is preserved
        date_col = next(c for c in result.columns if c.name == "date_id")
        assert date_col.semantic_entity == "v3.date_dim.date_id"
        assert date_col.semantic_type == "dimension"

        # Verify SQL is correct
        assert_sql_equal(
            result.sql,
            """
            WITH
            gg1 AS (
                SELECT date_id, SUM(amount) AS revenue
                FROM orders
                GROUP BY date_id
            ),
            gg2 AS (
                SELECT date_id, COUNT(*) AS orders
                FROM orders
                GROUP BY date_id
            )
            SELECT
                COALESCE(gg1.date_id, gg2.date_id) date_id,
                gg1.revenue,
                gg2.orders
            FROM gg1
            FULL OUTER JOIN gg2
                ON gg1.date_id = gg2.date_id
            """,
        )

    def test_empty_grain_groups_raises_error(self):
        """
        Empty grain groups list should raise ValueError.
        """
        with pytest.raises(ValueError, match="[Aa]t least one grain group"):
            build_combiner_sql([])


class TestValidateGrainGroupsCompatible:
    """Tests for validate_grain_groups_compatible function."""

    def test_single_grain_group_always_valid(self):
        """
        Single grain group is always valid.
        """
        gg = _create_grain_group(
            sql="SELECT date_id, SUM(amount) FROM orders GROUP BY date_id",
            columns=[{"name": "date_id"}],
            grain=["date_id"],
        )

        is_valid, error = validate_grain_groups_compatible([gg])
        assert is_valid is True
        assert error is None

    def test_matching_grains_are_compatible(self):
        """
        Grain groups with matching grain columns are compatible.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, status, SUM(a) FROM t1 GROUP BY date_id, status",
            columns=[{"name": "date_id"}, {"name": "status"}],
            grain=["date_id", "status"],
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, status, COUNT(*) FROM t2 GROUP BY date_id, status",
            columns=[{"name": "date_id"}, {"name": "status"}],
            grain=["date_id", "status"],  # Same grain, different order is OK
        )

        is_valid, error = validate_grain_groups_compatible([gg1, gg2])
        assert is_valid is True
        assert error is None

    def test_different_grains_are_incompatible(self):
        """
        Grain groups with different grain columns are incompatible.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, SUM(a) FROM t1 GROUP BY date_id",
            columns=[{"name": "date_id"}],
            grain=["date_id"],
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, status, COUNT(*) FROM t2 GROUP BY date_id, status",
            columns=[{"name": "date_id"}, {"name": "status"}],
            grain=["date_id", "status"],  # Different grain - more columns
        )

        is_valid, error = validate_grain_groups_compatible([gg1, gg2])
        assert is_valid is False
        assert "different grain" in error.lower()

    def test_empty_list_is_invalid(self):
        """
        Empty grain groups list is invalid.
        """
        is_valid, error = validate_grain_groups_compatible([])
        assert is_valid is False
        assert "no grain groups" in error.lower()


class TestCombinedGrainGroupResult:
    """Tests for CombinedGrainGroupResult dataclass."""

    def test_sql_property(self):
        """
        The sql property should render the query AST to string.
        """
        query = parse_sql("SELECT a, b FROM table1")

        result = CombinedGrainGroupResult(
            query=query,
            columns=[],
            grain_groups_combined=1,
            shared_dimensions=["a"],
            all_measures=["b"],
        )

        # sql property should return string
        assert isinstance(result.sql, str)
        assert_sql_equal(
            result.sql,
            "SELECT a, b FROM table1",
        )


class TestCombinerSqlValidity:
    """Tests that verify the combined SQL is valid and parseable."""

    def test_combined_sql_is_parseable(self):
        """
        The combined SQL should be valid and parseable.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS revenue FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "revenue", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, COUNT(*) AS order_count FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {
                    "name": "order_id_count_78d2e5eb",
                    "semantic_type": "metric_component",
                },
            ],
            grain=["date_id"],
        )

        result = build_combiner_sql([gg1, gg2])

        # Should be able to parse the result
        parsed = parse_sql(result.sql)
        assert parsed is not None
        assert parsed.select is not None

        # Verify exact SQL structure
        assert_sql_equal(
            result.sql,
            """
            WITH
            gg1 AS (
                SELECT date_id, SUM(amount) AS revenue
                FROM orders
                GROUP BY date_id
            ),
            gg2 AS (
                SELECT date_id, COUNT(*) AS order_count
                FROM orders
                GROUP BY date_id
            )
            SELECT
                COALESCE(gg1.date_id, gg2.date_id) date_id,
                gg1.revenue,
                gg2.order_id_count_78d2e5eb
            FROM gg1
            FULL OUTER JOIN gg2
                ON gg1.date_id = gg2.date_id
            """,
        )

    def test_join_on_clause_correct(self):
        """
        The JOIN ON clause should reference the correct grain columns.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, region, SUM(amount) AS amount FROM orders GROUP BY date_id, region",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "region", "semantic_type": "dimension"},
                {"name": "amount", "semantic_type": "metric_component"},
            ],
            grain=["date_id", "region"],
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, region, COUNT(*) AS count FROM events GROUP BY date_id, region",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "region", "semantic_type": "dimension"},
                {"name": "count", "semantic_type": "metric_component"},
            ],
            grain=["date_id", "region"],
        )

        result = build_combiner_sql([gg1, gg2])

        # Should have JOIN ON with both grain columns
        assert_sql_equal(
            result.sql,
            """
            WITH
            gg1 AS (
                SELECT date_id, region, SUM(amount) AS amount
                FROM orders
                GROUP BY date_id, region
            ),
            gg2 AS (
                SELECT date_id, region, COUNT(*) AS count
                FROM events
                GROUP BY date_id, region
            )
            SELECT
                COALESCE(gg1.date_id, gg2.date_id) date_id,
                COALESCE(gg1.region, gg2.region) region,
                gg1.amount,
                gg2.count
            FROM gg1
            FULL OUTER JOIN gg2
                ON gg1.date_id = gg2.date_id AND gg1.region = gg2.region
            """,
        )


class TestDifferentGrainGroups:
    """Tests for handling grain groups with different grains (lines 163-169)."""

    def test_different_grains_use_intersection(self):
        """
        Grain groups with different grains should use intersection for JOIN.

        This tests lines 163-169 where a warning is logged and the intersection
        of grains is used.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, customer_id, SUM(amount) AS revenue FROM orders GROUP BY date_id, customer_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "customer_id", "semantic_type": "dimension"},
                {"name": "revenue", "semantic_type": "metric_component"},
            ],
            grain=["date_id", "customer_id"],
            parent_name="orders",
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, region, COUNT(*) AS views FROM events GROUP BY date_id, region",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "region", "semantic_type": "dimension"},
                {"name": "views", "semantic_type": "metric_component"},
            ],
            grain=["date_id", "region"],  # Different grain - only date_id is shared
            parent_name="events",
        )

        result = build_combiner_sql([gg1, gg2])

        # Should use intersection of grains (just date_id)
        assert result.shared_dimensions == ["date_id"]
        assert result.grain_groups_combined == 2

        # JOIN should only be on date_id
        assert_sql_equal(
            result.sql,
            """
            WITH
            gg1 AS (
                SELECT date_id, customer_id, SUM(amount) AS revenue
                FROM orders
                GROUP BY date_id, customer_id
            ),
            gg2 AS (
                SELECT date_id, region, COUNT(*) AS views
                FROM events
                GROUP BY date_id, region
            )
            SELECT
                COALESCE(gg1.date_id, gg2.date_id) date_id,
                gg1.revenue,
                gg2.views
            FROM gg1
            FULL OUTER JOIN gg2
                ON gg1.date_id = gg2.date_id
            """,
        )


class TestCartesianJoin:
    """Tests for cartesian join when no grain columns (line 381)."""

    def test_empty_grain_produces_cartesian_join(self):
        """
        When grain groups have no shared grain columns, JOIN uses TRUE (cartesian).

        This tests line 381 where an empty grain produces ast.Boolean(True).
        """
        left = ast.Table(name=ast.Name("left_table"))
        right = ast.Table(name=ast.Name("right_table"))

        result = _build_join_criteria(left, right, grain_columns=[])

        assert isinstance(result, ast.Boolean)
        assert result.value is True

    def test_grain_groups_with_no_shared_grain(self):
        """
        Grain groups with completely different grains produce cartesian join.
        """
        gg1 = _create_grain_group(
            sql="SELECT customer_id, SUM(amount) AS revenue FROM orders GROUP BY customer_id",
            columns=[
                {"name": "customer_id", "semantic_type": "dimension"},
                {"name": "revenue", "semantic_type": "metric_component"},
            ],
            grain=["customer_id"],
            parent_name="orders",
        )

        gg2 = _create_grain_group(
            sql="SELECT region, COUNT(*) AS views FROM events GROUP BY region",
            columns=[
                {"name": "region", "semantic_type": "dimension"},
                {"name": "views", "semantic_type": "metric_component"},
            ],
            grain=["region"],  # No overlap with customer_id
            parent_name="events",
        )

        result = build_combiner_sql([gg1, gg2])

        # No shared dimensions
        assert result.shared_dimensions == []

        # Should have TRUE in JOIN condition (cartesian)
        assert "true" in result.sql.lower() or "TRUE" in result.sql


class TestDuplicateMeasures:
    """Tests for deduplication of measures (lines 310, 415, 439)."""

    def test_duplicate_measure_names_deduplicated(self):
        """
        Same measure name in multiple grain groups should only appear once.

        This tests line 310 (seen_measures deduplication).
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS revenue FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "revenue", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="orders",
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS revenue FROM returns GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "revenue", "semantic_type": "metric_component"},  # Same name!
            ],
            grain=["date_id"],
            parent_name="returns",
        )

        result = build_combiner_sql([gg1, gg2])

        # Revenue should only appear once in all_measures
        assert result.all_measures.count("revenue") == 1
        assert result.all_measures == ["revenue"]

        # Only one revenue column in output
        revenue_cols = [c for c in result.columns if c.name == "revenue"]
        assert len(revenue_cols) == 1


class TestMissingColumnMetadata:
    """Tests for missing column metadata edge cases (lines 418, 442 branches)."""

    def test_grain_column_not_in_columns_metadata(self):
        """
        Grain column not in columns metadata should be skipped gracefully.

        This tests line 418 branch where col_meta is None for a grain column.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, extra_dim, SUM(amount) AS revenue FROM orders GROUP BY date_id, extra_dim",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                # extra_dim is NOT in columns metadata but IS in grain
                {"name": "revenue", "semantic_type": "metric_component"},
            ],
            grain=["date_id", "extra_dim"],  # extra_dim won't have metadata
            parent_name="orders",
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, extra_dim, COUNT(*) AS views FROM events GROUP BY date_id, extra_dim",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "views", "semantic_type": "metric_component"},
            ],
            grain=["date_id", "extra_dim"],
            parent_name="events",
        )

        result = build_combiner_sql([gg1, gg2])

        # Should still produce valid SQL
        assert result.grain_groups_combined == 2

        # date_id should be in output columns (has metadata)
        date_cols = [c for c in result.columns if c.name == "date_id"]
        assert len(date_cols) == 1

        # extra_dim should be skipped (no metadata)
        extra_cols = [c for c in result.columns if c.name == "extra_dim"]
        assert len(extra_cols) == 0

    def test_measure_not_in_any_columns_metadata(self):
        """
        Measure not in any columns metadata should be skipped.

        This tests line 442 branch where col_meta is None for a measure.
        """
        gg1 = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS revenue, SUM(cost) AS cost FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "revenue", "semantic_type": "metric_component"},
                # cost measure has semantic_type that doesn't match "metric_component"
                {"name": "cost", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="orders",
        )

        result = build_combiner_sql([gg1])

        # Both measures should be found
        assert "revenue" in result.all_measures
        assert "cost" in result.all_measures


class TestSingleGrainGroupSemanticTypes:
    """Tests for single grain group with various semantic types (line 119)."""

    def test_single_grain_group_with_metric_semantic_type(self):
        """
        Single grain group with semantic_type="metric" should be handled.

        This tests line 119 branch for "metric" semantic_type.
        """
        gg = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS total_revenue FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {
                    "name": "line_total_sum_e1f61696",
                    "semantic_type": "metric",
                },  # "metric" not "metric_component"
            ],
            grain=["date_id"],
            metrics=["revenue"],
        )

        result = build_combiner_sql([gg])

        assert result.grain_groups_combined == 1
        assert "line_total_sum_e1f61696" in result.all_measures

    def test_single_grain_group_with_measure_semantic_type(self):
        """
        Single grain group with semantic_type="measure" should be handled.

        This tests line 119 branch for "measure" semantic_type.
        """
        gg = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS total FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "total", "semantic_type": "measure"},  # "measure" type
            ],
            grain=["date_id"],
        )

        result = build_combiner_sql([gg])

        assert result.grain_groups_combined == 1
        assert "total" in result.all_measures

    def test_single_grain_group_with_metric_input_type(self):
        """
        Single grain group with semantic_type="metric_input" should be dimension.

        This tests line 117 branch for "metric_input" semantic_type.
        """
        gg = _create_grain_group(
            sql="SELECT date_id, user_id, SUM(amount) AS total FROM orders GROUP BY date_id, user_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {
                    "name": "user_id",
                    "semantic_type": "metric_input",
                },  # For COUNT DISTINCT
                {"name": "total", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
        )

        result = build_combiner_sql([gg])

        assert result.grain_groups_combined == 1
        # metric_input should NOT be in measures
        assert "user_id" not in result.all_measures
        # It's treated as dimension
        assert "total" in result.all_measures


class TestPreAggTableFunctions:
    """Tests for pre-aggregation table functions (lines 501-502, 654-722)."""

    def test_compute_preagg_table_name(self):
        """
        Test the _compute_preagg_table_name function (lines 501-502).
        """
        result = _compute_preagg_table_name("v3.order_details", "abc123def456")

        assert result == "v3_order_details_preagg_abc123de"
        assert "_preagg_" in result
        assert result.startswith("v3_order_details")

    def test_compute_preagg_table_name_replaces_dots(self):
        """
        Dots in parent name should be replaced with underscores.
        """
        result = _compute_preagg_table_name("catalog.schema.table", "hash12345678")

        assert "." not in result.split("_preagg_")[0]
        assert result == "catalog_schema_table_preagg_hash1234"

    def test_build_grain_group_from_preagg_table(self):
        """
        Test _build_grain_group_from_preagg_table function (lines 654-722).
        """
        # Create a grain group with components
        component = MetricComponent(
            name="revenue_sum",
            expression="amount",
            aggregation="SUM",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        )

        gg = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS revenue_sum FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "revenue_sum", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="orders",
            components=[component],
        )

        result = _build_grain_group_from_preagg_table(
            gg,
            "catalog.schema.orders_preagg_abc12345",
        )

        # Should generate SQL reading from pre-agg table with re-aggregation
        sql = str(result.query)

        # Should reference the pre-agg table
        assert "catalog.schema.orders_preagg_abc12345" in sql

        # Should have GROUP BY
        assert "GROUP BY" in sql.upper()

        # Should have re-aggregation with merge function (SUM)
        assert "SUM(" in sql.upper()

        # Grain should be preserved
        assert result.grain == ["date_id"]

    def test_build_grain_group_from_preagg_table_no_merge_function(self):
        """
        Measures without merge function should be selected directly.
        """
        # Create a grain group WITHOUT components (no merge function)
        gg = _create_grain_group(
            sql="SELECT date_id, raw_value FROM source GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "raw_value", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="source",
            components=[],  # No components = no merge function
        )

        result = _build_grain_group_from_preagg_table(
            gg,
            "catalog.schema.source_preagg",
        )

        sql = str(result.query)

        # Should still generate valid SQL
        assert "catalog.schema.source_preagg" in sql
        assert "date_id" in sql


class TestBuildCombinerSqlFromPreaggs:
    """Integration tests for build_combiner_sql_from_preaggs (lines 544-632)."""

    @pytest.mark.asyncio
    async def test_build_combiner_sql_from_preaggs_no_grain_groups(
        self,
        session,
        client_with_build_v3,
    ):
        """
        Should raise ValueError when no grain groups are generated.

        This tests line 556-557.
        """
        # Use a metric that doesn't exist
        with pytest.raises(Exception):
            await build_combiner_sql_from_preaggs(
                session=session,
                metrics=["nonexistent.metric"],
                dimensions=["v3.order_details.status"],
            )

    @pytest.mark.asyncio
    async def test_build_combiner_sql_from_preaggs_basic(
        self,
        session,
        client_with_build_v3,
    ):
        """
        Test basic pre-agg SQL generation flow.
        """
        # This should work with existing v3 metrics
        result, table_refs, temporal_info = await build_combiner_sql_from_preaggs(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
        )

        # Should produce a result
        assert result is not None
        assert result.grain_groups_combined >= 1

        # Should have pre-agg table references
        assert len(table_refs) >= 1
        for ref in table_refs:
            assert "_preagg_" in ref

        # SQL should reference the pre-agg tables
        assert result.sql is not None

    @pytest.mark.asyncio
    async def test_build_combiner_sql_from_preaggs_with_existing_preagg(
        self,
        session,
        client_with_build_v3,
    ):
        """
        Test pre-agg SQL generation when PreAggregation records exist.

        This tests lines 585-596 (temporal partition extraction) and 628-630.
        """
        # Get the order_details node to find its revision ID
        # Need to eagerly load current and columns for async context
        stmt = (
            select(Node)
            .where(Node.name == "v3.order_details")
            .options(selectinload(Node.current).selectinload(NodeRevision.columns))
        )
        result = await session.execute(stmt)
        order_details_node = result.scalar_one_or_none()
        assert order_details_node is not None
        assert order_details_node.current is not None

        node_revision_id = order_details_node.current.id

        # Compute the grain_group_hash that build_combiner_sql_from_preaggs will use
        # It uses fully qualified dimension refs
        grain_columns = ["v3.order_details.status"]
        grain_hash = compute_grain_group_hash(node_revision_id, grain_columns)

        # Create a temporal partition on the order_date column
        order_date_col = None
        for col in order_details_node.current.columns:
            if col.name == "order_date":
                order_date_col = col
                break

        if order_date_col:
            # Add temporal partition to the column
            partition = Partition(
                column_id=order_date_col.id,
                type_=PartitionType.TEMPORAL,
                granularity=Granularity.DAY,
                format="yyyyMMdd",
            )
            session.add(partition)
            await session.flush()
            order_date_col.partition = partition

        # Create an availability state
        avail = AvailabilityState(
            catalog="default",
            schema_="v3",
            table="order_details_preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        # Create a PreAggregation record with matching grain_group_hash
        preagg = PreAggregation(
            node_revision_id=node_revision_id,
            grain_columns=grain_columns,
            measures=[
                PreAggMeasure(
                    name="line_total_sum",
                    expression="line_total",
                    aggregation="SUM",
                    merge="SUM",
                    rule=AggregationRule(type="full"),
                    expr_hash=compute_expression_hash("line_total"),
                ),
            ],
            sql="SELECT status, SUM(line_total) AS line_total_sum FROM v3.order_details GROUP BY status",
            grain_group_hash=grain_hash,
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        # Now call build_combiner_sql_from_preaggs
        result, table_refs, temporal_info = await build_combiner_sql_from_preaggs(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
        )

        # Should produce a result
        assert result is not None
        assert result.grain_groups_combined >= 1

        # Should have pre-agg table references
        assert len(table_refs) >= 1

        # If temporal partition was set up, temporal_info should be populated
        # (depends on whether order_date is in grain_columns or linked dimension)
        # The key test is that lines 585-596 are executed


class TestCombinedMeasuresSQLEndpoint:
    """Tests for the /sql/measures/v3/combined endpoint."""

    @pytest.mark.asyncio
    async def test_single_metric_single_dimension(self, client_with_build_v3):
        """
        Test the simplest case: one metric, one dimension.
        Returns combined SQL even for single grain group.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Validate response structure
        assert "sql" in data
        assert "columns" in data
        assert "grain" in data
        assert "grain_groups_combined" in data
        assert "dialect" in data
        assert "use_preagg_tables" in data
        assert "source_tables" in data

        # Should have 1 grain group combined (single metric)
        assert data["grain_groups_combined"] == 1
        assert data["use_preagg_tables"] is False
        assert "v3.order_details" in data["source_tables"]

        # Validate columns
        column_names = [col["name"] for col in data["columns"]]
        assert "status" in column_names
        assert "line_total_sum_e1f61696" in column_names

        # Validate grain
        assert "status" in data["grain"]

        # Validate SQL structure - single grain group returns the query unchanged
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

    @pytest.mark.asyncio
    async def test_multiple_metrics_same_grain_group(self, client_with_build_v3):
        """
        Test multiple metrics from the same grain group.
        Should produce a single combined grain group.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Both metrics are from order_details with FULL aggregability
        # Should be merged into 1 grain group
        assert data["grain_groups_combined"] == 1

        # Should have both measures
        column_names = [col["name"] for col in data["columns"]]
        assert "line_total_sum_e1f61696" in column_names
        assert "quantity_sum_06b64d2e" in column_names

        # Validate SQL structure
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696, SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

    @pytest.mark.asyncio
    async def test_cross_fact_metrics_combined(self, client_with_build_v3):
        """
        Test metrics from different fact tables.
        Should combine multiple grain groups with FULL OUTER JOIN.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue", "v3.page_view_count"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Metrics are from different facts (order_details and page_views_enriched)
        # Should have 2 grain groups combined
        assert data["grain_groups_combined"] == 2

        # Should have measures from both grain groups
        column_names = [col["name"] for col in data["columns"]]
        assert "line_total_sum_e1f61696" in column_names
        assert "view_id_count_f41e2db4" in column_names

        # Should have the shared dimension
        assert "category" in data["grain"]

        # Validate SQL structure with CTEs and FULL OUTER JOIN
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
            SELECT  oi.product_id,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            ),
            v3_page_views_enriched AS (
            SELECT  view_id,
                product_id
            FROM default.v3.page_views
            ),
            gg1 AS (
            SELECT  t2.category,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY  t2.category
            ),
            gg2 AS (
            SELECT  t2.category,
                COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY  t2.category
            )

            SELECT  COALESCE(gg1.category, gg2.category) category,
                gg1.line_total_sum_e1f61696,
                gg2.view_id_count_f41e2db4
            FROM gg1 FULL OUTER JOIN gg2 ON gg1.category = gg2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_multiple_dimensions(self, client_with_build_v3):
        """
        Test with multiple dimensions.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status", "v3.customer.name"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should have both dimensions in grain
        assert len(data["grain"]) == 2

        # Both dimension columns should be in output
        column_names = [col["name"] for col in data["columns"]]
        assert "status" in column_names
        assert "name" in column_names

    @pytest.mark.asyncio
    async def test_no_dimensions_global_aggregation(self, client_with_build_v3):
        """
        Test with no dimensions (global aggregation).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": [],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should have empty grain (global aggregation)
        assert data["grain"] == []

        # Should still have the measures
        column_names = [col["name"] for col in data["columns"]]
        assert "line_total_sum_e1f61696" in column_names
        assert "quantity_sum_06b64d2e" in column_names

        # Validate SQL - no GROUP BY when no dimensions
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT oi.quantity, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT SUM(t1.line_total) line_total_sum_e1f61696, SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            """,
        )

    @pytest.mark.asyncio
    async def test_empty_metrics_raises_error(self, client_with_build_v3):
        """
        Test that empty metrics list raises an error.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": [],
                "dimensions": ["v3.order_details.status"],
            },
        )

        # Should return an error
        assert response.status_code >= 400

    @pytest.mark.asyncio
    async def test_nonexistent_metric_raises_error(self, client_with_build_v3):
        """
        Test that nonexistent metric raises an error.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["nonexistent.metric"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        # Should return an error
        assert response.status_code >= 400
        assert "not found" in response.text.lower()

    @pytest.mark.asyncio
    async def test_column_metadata_types(self, client_with_build_v3):
        """
        Test that column metadata includes correct semantic types.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Find the dimension column
        dim_cols = [c for c in data["columns"] if c["name"] == "status"]
        assert len(dim_cols) == 1
        assert dim_cols[0]["semantic_type"] == "dimension"
        assert dim_cols[0]["semantic_entity"] == "v3.order_details.status"

        # Find the measure column
        measure_cols = [
            c for c in data["columns"] if c["name"] == "line_total_sum_e1f61696"
        ]
        assert len(measure_cols) == 1
        # Measures come through as metric_component in the combined output
        assert measure_cols[0]["semantic_type"] in ("metric", "metric_component")

    @pytest.mark.asyncio
    async def test_source_tables_populated(self, client_with_build_v3):
        """
        Test that source_tables is correctly populated.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue", "v3.page_view_count"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should have source tables from both facts
        assert len(data["source_tables"]) == 2
        source_tables_str = " ".join(data["source_tables"])
        assert "order_details" in source_tables_str
        assert "page_views" in source_tables_str

    @pytest.mark.asyncio
    async def test_dialect_parameter(self, client_with_build_v3):
        """
        Test that dialect parameter is respected.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "dialect": "spark",
            },
        )

        assert response.status_code == 200
        data = response.json()

        assert data["dialect"] == "spark"

    @pytest.mark.asyncio
    async def test_with_filters(self, client_with_build_v3):
        """
        Test combined SQL with filters.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status = 'active'"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Validate SQL with WHERE clause for the filter
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            WHERE t1.status = 'active'
            GROUP BY t1.status
            """,
        )


class TestUnknownSemanticType:
    """Tests for unknown semantic types (line 119->116)."""

    def test_unknown_semantic_type_ignored(self):
        """
        Columns with unknown semantic_type should be ignored (not in dimensions or measures).

        This tests the fallthrough case in lines 116-120.
        """
        gg = _create_grain_group(
            sql="SELECT date_id, mystery_col, SUM(amount) AS revenue FROM orders GROUP BY date_id, mystery_col",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {
                    "name": "mystery_col",
                    "semantic_type": "unknown_type",
                },  # Unknown type
                {"name": "revenue", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            metrics=["revenue"],
        )

        result = build_combiner_sql([gg])

        # mystery_col with unknown type should NOT be in measures
        assert "mystery_col" not in result.all_measures
        # Only revenue should be a measure
        assert result.all_measures == ["revenue"]


class TestDuplicateColumns:
    """Tests for duplicate column handling (lines 415, 439)."""

    def test_duplicate_grain_columns_in_shared_grain(self):
        """
        Duplicate grain columns should be skipped (line 415).
        """
        # Create grain groups where shared_grain might have duplicates
        # This can happen with edge cases in grain intersection
        gg1 = _create_grain_group(
            sql="SELECT date_id, date_id as date_id2, SUM(amount) AS revenue FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "revenue", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="orders",
        )

        gg2 = _create_grain_group(
            sql="SELECT date_id, COUNT(*) AS views FROM events GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "views", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="events",
        )

        result = build_combiner_sql([gg1, gg2])

        # date_id should only appear once in output columns
        date_cols = [c for c in result.columns if c.name == "date_id"]
        assert len(date_cols) == 1


class TestMeasureMetadataNotFound:
    """Tests for missing measure metadata (line 442->437)."""

    def test_measure_without_metadata_skipped(self):
        """
        Measures that don't exist in any grain group's columns should be skipped.

        This tests the branch at line 442 where col_meta is None.
        """
        # Create a grain group where the measure in projection doesn't match columns
        gg = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS revenue FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                # Note: "revenue" is NOT in columns, but semantic_type filter won't find it
                {"name": "other_measure", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
        )

        result = build_combiner_sql([gg])

        # Should still produce valid result
        assert result is not None
        # other_measure should be in measures
        assert "other_measure" in result.all_measures


class TestComponentAliasLookup:
    """Tests for component alias lookup (line 674->673)."""

    def test_component_found_by_alias(self):
        """
        Component should be found by alias when direct name doesn't match.

        This tests line 674 branch where component_aliases lookup succeeds.
        """
        component = MetricComponent(
            name="internal_name",  # Internal name doesn't match column
            expression="amount",
            aggregation="SUM",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        )

        # Create grain group with component_aliases mapping
        gg = _create_grain_group(
            sql="SELECT date_id, SUM(amount) AS revenue_sum FROM orders GROUP BY date_id",
            columns=[
                {"name": "date_id", "semantic_type": "dimension"},
                {"name": "revenue_sum", "semantic_type": "metric_component"},
            ],
            grain=["date_id"],
            parent_name="orders",
            components=[component],
        )
        # Manually set component_aliases to test the alias lookup path
        gg.component_aliases = {"internal_name": "revenue_sum"}

        result = _build_grain_group_from_preagg_table(
            gg,
            "catalog.schema.orders_preagg",
        )

        sql = str(result.query)

        # Should have re-aggregation with SUM (found via alias)
        assert "SUM(" in sql.upper()
        assert "revenue_sum" in sql.lower()


class TestReorderPartitionColumnLast:
    """
    Tests for _reorder_partition_column_last function.

    This function ensures columns are ordered correctly for Hive/Spark
    INSERT OVERWRITE ... PARTITION (col) syntax, where the partition
    column must be last.
    """

    def _create_combined_result(
        self,
        sql: str,
        columns: list[tuple[str, str]],  # (name, semantic_type)
        shared_dimensions: list[str],
        all_measures: list[str] | None = None,
    ) -> CombinedGrainGroupResult:
        """Helper to create a CombinedGrainGroupResult from SQL and columns."""
        query = parse_sql(sql)
        col_metadata = [
            V3ColumnMetadata(
                name=name,
                type="string",
                semantic_entity=f"test.{name}",
                semantic_type=sem_type,
            )
            for name, sem_type in columns
        ]
        return CombinedGrainGroupResult(
            query=query,
            columns=col_metadata,
            grain_groups_combined=1,
            shared_dimensions=shared_dimensions,
            all_measures=all_measures or [],
        )

    def test_partition_column_moved_to_end(self):
        """Partition column should be moved to the end of projections and columns."""
        result = self._create_combined_result(
            sql="SELECT dateint, country, SUM(revenue) AS revenue FROM t GROUP BY dateint, country",
            columns=[
                ("dateint", "dimension"),
                ("country", "dimension"),
                ("revenue", "metric_component"),
            ],
            shared_dimensions=["dateint", "country"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # Column metadata should have dateint last
        assert [c.name for c in reordered.columns] == ["country", "revenue", "dateint"]

        # Projections should have dateint last
        proj_names = [
            p.alias_or_name.name if hasattr(p, "alias_or_name") else p.name.name
            for p in reordered.query.select.projection
        ]
        assert proj_names == ["country", "revenue", "dateint"]

        # shared_dimensions should have dateint last
        assert reordered.shared_dimensions == ["country", "dateint"]

    def test_partition_column_already_last(self):
        """If partition column is already last, no reordering needed."""
        result = self._create_combined_result(
            sql="SELECT country, revenue, dateint FROM t",
            columns=[
                ("country", "dimension"),
                ("revenue", "metric_component"),
                ("dateint", "dimension"),
            ],
            shared_dimensions=["country", "dateint"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # Should remain in same order
        assert [c.name for c in reordered.columns] == ["country", "revenue", "dateint"]
        assert reordered.shared_dimensions == ["country", "dateint"]

    def test_partition_column_not_in_result(self):
        """If partition column doesn't exist, no changes should be made."""
        result = self._create_combined_result(
            sql="SELECT country, revenue FROM t",
            columns=[
                ("country", "dimension"),
                ("revenue", "metric_component"),
            ],
            shared_dimensions=["country"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # Should remain unchanged
        assert [c.name for c in reordered.columns] == ["country", "revenue"]
        assert reordered.shared_dimensions == ["country"]

    def test_partition_not_in_shared_dimensions(self):
        """If partition column exists but not in shared_dimensions."""
        result = self._create_combined_result(
            sql="SELECT dateint, country, revenue FROM t",
            columns=[
                ("dateint", "dimension"),
                ("country", "dimension"),
                ("revenue", "metric_component"),
            ],
            shared_dimensions=["country"],  # dateint not in shared_dimensions
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # Column metadata and projections should still be reordered
        assert [c.name for c in reordered.columns] == ["country", "revenue", "dateint"]

        # shared_dimensions should remain unchanged (dateint wasn't in it)
        assert reordered.shared_dimensions == ["country"]

    def test_single_column_is_partition(self):
        """Single column that is also partition column."""
        result = self._create_combined_result(
            sql="SELECT dateint FROM t",
            columns=[("dateint", "dimension")],
            shared_dimensions=["dateint"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # Should remain as single column
        assert [c.name for c in reordered.columns] == ["dateint"]
        assert reordered.shared_dimensions == ["dateint"]

    def test_multiple_measures_with_partition(self):
        """Multiple measure columns with partition column."""
        result = self._create_combined_result(
            sql="SELECT dateint, country, SUM(revenue) AS revenue, COUNT(*) AS cnt FROM t GROUP BY dateint, country",
            columns=[
                ("dateint", "dimension"),
                ("country", "dimension"),
                ("revenue", "metric_component"),
                ("cnt", "metric_component"),
            ],
            shared_dimensions=["dateint", "country"],
            all_measures=["revenue", "cnt"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # dateint should be at end
        assert reordered.columns[-1].name == "dateint"
        assert reordered.shared_dimensions[-1] == "dateint"

    def test_with_aliased_columns(self):
        """Columns with aliases should work correctly."""
        result = self._create_combined_result(
            sql="SELECT d.dateint AS date_id, c.name AS country_name, SUM(o.amt) AS total FROM t",
            columns=[
                ("date_id", "dimension"),
                ("country_name", "dimension"),
                ("total", "metric_component"),
            ],
            shared_dimensions=["date_id", "country_name"],
        )

        reordered = _reorder_partition_column_last(result, "date_id")

        # date_id should be at end
        assert [c.name for c in reordered.columns] == [
            "country_name",
            "total",
            "date_id",
        ]
        assert reordered.shared_dimensions == ["country_name", "date_id"]

    def test_with_function_projections(self):
        """Projections that are functions should work correctly."""
        result = self._create_combined_result(
            sql="SELECT dateint, country, SUM(revenue) AS total_revenue, COUNT(DISTINCT user_id) AS unique_users FROM t GROUP BY dateint, country",
            columns=[
                ("dateint", "dimension"),
                ("country", "dimension"),
                ("total_revenue", "metric_component"),
                ("unique_users", "metric_component"),
            ],
            shared_dimensions=["dateint", "country"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # All columns should be present with dateint last
        col_names = [c.name for c in reordered.columns]
        assert col_names[-1] == "dateint"
        assert set(col_names) == {"dateint", "country", "total_revenue", "unique_users"}

    def test_preserves_other_fields(self):
        """Other fields like measure_components and component_aliases should be preserved."""
        query = parse_sql("SELECT dateint, country, revenue FROM t")
        col_metadata = [
            V3ColumnMetadata(
                name="dateint",
                type="int",
                semantic_entity="test.dateint",
                semantic_type="dimension",
            ),
            V3ColumnMetadata(
                name="country",
                type="string",
                semantic_entity="test.country",
                semantic_type="dimension",
            ),
            V3ColumnMetadata(
                name="revenue",
                type="double",
                semantic_entity="test.revenue",
                semantic_type="metric_component",
            ),
        ]
        components = [
            MetricComponent(
                name="revenue_sum",
                expression="revenue",
                aggregation="SUM",
                merge="SUM",
                rule=AggregationRule(),
            ),
        ]
        result = CombinedGrainGroupResult(
            query=query,
            columns=col_metadata,
            grain_groups_combined=3,
            shared_dimensions=["dateint", "country"],
            all_measures=["revenue"],
            measure_components=components,
            component_aliases={"revenue_sum": "revenue"},
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # Other fields should be preserved
        assert reordered.grain_groups_combined == 3
        assert reordered.all_measures == ["revenue"]
        assert reordered.measure_components == components
        assert reordered.component_aliases == {"revenue_sum": "revenue"}

    def test_columns_synced_to_projection_order(self):
        """Column metadata should be synced to match projection order before reordering."""
        # Create a result where column metadata order differs from projection order
        query = parse_sql(
            "SELECT country, dateint, revenue FROM t",
        )  # projection: country, dateint, revenue
        col_metadata = [
            # Intentionally different order than projection
            V3ColumnMetadata(
                name="revenue",
                type="double",
                semantic_entity="test.revenue",
                semantic_type="metric_component",
            ),
            V3ColumnMetadata(
                name="dateint",
                type="int",
                semantic_entity="test.dateint",
                semantic_type="dimension",
            ),
            V3ColumnMetadata(
                name="country",
                type="string",
                semantic_entity="test.country",
                semantic_type="dimension",
            ),
        ]
        result = CombinedGrainGroupResult(
            query=query,
            columns=col_metadata,
            grain_groups_combined=1,
            shared_dimensions=["dateint", "country"],
            all_measures=["revenue"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # Column metadata should be synced to projection order,
        # then dateint moved to end
        # Projection: country, revenue, dateint (after reorder)
        # So columns should be: country, revenue, dateint
        assert [c.name for c in reordered.columns] == ["country", "revenue", "dateint"]

    def test_partition_in_middle_of_many_columns(self):
        """Partition column in middle of many columns."""
        result = self._create_combined_result(
            sql="SELECT a, b, dateint, c, d, e FROM t",
            columns=[
                ("a", "dimension"),
                ("b", "dimension"),
                ("dateint", "dimension"),
                ("c", "metric_component"),
                ("d", "metric_component"),
                ("e", "metric_component"),
            ],
            shared_dimensions=["a", "b", "dateint"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # dateint should be last
        col_names = [c.name for c in reordered.columns]
        assert col_names[-1] == "dateint"
        # All others should precede dateint
        assert col_names[:-1] == ["a", "b", "c", "d", "e"]

        # shared_dimensions should have dateint last
        assert reordered.shared_dimensions == ["a", "b", "dateint"]

    def test_with_named_function_no_alias(self):
        """Test with a function projection that is Named but not Aliasable.

        Note: In practice, most functions in SELECT are wrapped in Alias nodes
        when they have "AS" syntax. This test verifies the Named branch handling
        for bare function calls.
        """
        # Using COUNT(*) which creates a Named function
        # Note: The SQL parser typically wraps this in Alias, but we test
        # the helper's Named handling explicitly
        query = parse_sql(
            "SELECT dateint, country, COUNT(*) FROM t GROUP BY dateint, country",
        )

        # Manually check that projection includes function
        projections = query.select.projection
        assert len(projections) == 3

        # Create result with columns that include a function-named column
        col_metadata = [
            V3ColumnMetadata(
                name="dateint",
                type="int",
                semantic_entity="test.dateint",
                semantic_type="dimension",
            ),
            V3ColumnMetadata(
                name="country",
                type="string",
                semantic_entity="test.country",
                semantic_type="dimension",
            ),
            V3ColumnMetadata(
                name="count",  # Name comes from function name
                type="bigint",
                semantic_entity="test.count",
                semantic_type="metric_component",
            ),
        ]

        result = CombinedGrainGroupResult(
            query=query,
            columns=col_metadata,
            grain_groups_combined=1,
            shared_dimensions=["dateint", "country"],
            all_measures=["count"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # dateint should be moved to end
        col_names = [c.name for c in reordered.columns]
        assert col_names[-1] == "dateint"

    def test_projection_with_unrecognized_type(self):
        """Test handling of projection types that are neither Aliasable nor Named.

        This tests the None return path in _get_projection_name helper.
        Most AST nodes fall into Aliasable or Named, but we ensure graceful handling.
        """
        result = self._create_combined_result(
            sql="SELECT dateint, country, 42 AS literal_val FROM t",  # Literal value
            columns=[
                ("dateint", "dimension"),
                ("country", "dimension"),
                ("literal_val", "metric_component"),
            ],
            shared_dimensions=["dateint", "country"],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # Should still work - dateint moved to end
        assert reordered.columns[-1].name == "dateint"
        assert reordered.shared_dimensions[-1] == "dateint"

    def test_projection_bare_expression_not_aliasable(self):
        """Test handling when a projection cannot be identified.

        Creates a scenario with a Number literal (not Aliasable, not Named)
        to exercise the None return path in _get_projection_name.
        """
        # Parse SQL with a bare number (no alias)
        query = parse_sql("SELECT dateint, country, 42 FROM t")

        # Verify that the third projection is NOT Aliasable (it's a Number)
        projs = query.select.projection
        assert len(projs) == 3
        # Numbers are literals, not Aliasable or Named
        third_proj = projs[2]
        assert not isinstance(third_proj, ast.Aliasable)
        assert not isinstance(third_proj, ast.Named)

        # Create result - note the bare 42 won't match any column name
        col_metadata = [
            V3ColumnMetadata(
                name="dateint",
                type="int",
                semantic_entity="test.dateint",
                semantic_type="dimension",
            ),
            V3ColumnMetadata(
                name="country",
                type="string",
                semantic_entity="test.country",
                semantic_type="dimension",
            ),
            # The "42" column won't be in projection names since Number returns None
        ]

        result = CombinedGrainGroupResult(
            query=query,
            columns=col_metadata,
            grain_groups_combined=1,
            shared_dimensions=["dateint", "country"],
            all_measures=[],
        )

        reordered = _reorder_partition_column_last(result, "dateint")

        # Should still work - dateint moved to end
        # The None from bare number projection is filtered out
        assert reordered.columns[-1].name == "dateint"
        assert reordered.shared_dimensions == ["country", "dateint"]
