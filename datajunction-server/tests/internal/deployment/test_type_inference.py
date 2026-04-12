"""
Unit tests for the lightweight top-down type inference used by deployment propagation.

resolve_output_columns takes a SQL query string and a map of parent node columns,
and returns the output column names + types without any DB calls.
"""

import pytest

from datajunction_server.internal.deployment.type_inference import (
    resolve_output_columns,
    TypeResolutionError,
)
from datajunction_server.sql.parsing.types import (
    BigIntType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
    UnknownType,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col_map(*tables):
    """Build parent_columns_map from (table_name, [(col_name, type), ...]) tuples."""
    return {
        name: {col_name: col_type for col_name, col_type in cols}
        for name, cols in tables
    }


USERS_COLS = (
    "default.users",
    [
        ("user_id", IntegerType()),
        ("username", StringType()),
        ("email", StringType()),
        ("created_at", TimestampType()),
    ],
)

ORDERS_COLS = (
    "default.orders",
    [
        ("order_id", IntegerType()),
        ("user_id", IntegerType()),
        ("amount", DoubleType()),
        ("order_date", DateType()),
    ],
)


# ===================================================================
# 1. Simple SELECT from single table
# ===================================================================


class TestSimpleSelect:
    def test_select_columns(self):
        """SELECT user_id, username FROM default.users"""
        result = resolve_output_columns(
            "SELECT user_id, username FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("user_id", IntegerType())
        assert result[1] == ("username", StringType())

    def test_select_with_alias(self):
        """SELECT user_id AS id FROM default.users"""
        result = resolve_output_columns(
            "SELECT user_id AS id FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0] == ("id", IntegerType())

    def test_select_star(self):
        """SELECT * FROM default.users — should expand to all columns."""
        result = resolve_output_columns(
            "SELECT * FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 4
        col_names = [name for name, _ in result]
        assert "user_id" in col_names
        assert "username" in col_names

    def test_select_table_star(self):
        """SELECT u.* FROM default.users u"""
        result = resolve_output_columns(
            "SELECT u.* FROM default.users u",
            _col_map(USERS_COLS),
        )
        assert len(result) == 4


# ===================================================================
# 2. Multi-table / JOIN
# ===================================================================


class TestMultiTable:
    def test_qualified_columns(self):
        """SELECT u.user_id, o.amount FROM default.users u JOIN default.orders o ON ..."""
        result = resolve_output_columns(
            "SELECT u.user_id, o.amount "
            "FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("user_id", IntegerType())
        assert result[1] == ("amount", DoubleType())

    def test_unqualified_unambiguous(self):
        """SELECT username, amount FROM default.users u JOIN default.orders o ON ..."""
        result = resolve_output_columns(
            "SELECT username, amount "
            "FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("username", StringType())
        assert result[1] == ("amount", DoubleType())


# ===================================================================
# 3. Aggregation functions
# ===================================================================


class TestAggregations:
    def test_count_star(self):
        """SELECT COUNT(*) AS cnt FROM default.orders"""
        result = resolve_output_columns(
            "SELECT COUNT(*) AS cnt FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "cnt"
        # COUNT returns BigIntType
        assert isinstance(result[0][1], BigIntType)

    def test_sum(self):
        """SELECT SUM(amount) AS total FROM default.orders"""
        result = resolve_output_columns(
            "SELECT SUM(amount) AS total FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "total"
        # SUM of DoubleType → DoubleType
        assert isinstance(result[0][1], DoubleType)

    def test_avg(self):
        """SELECT AVG(amount) AS avg_amount FROM default.orders"""
        result = resolve_output_columns(
            "SELECT AVG(amount) AS avg_amount FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "avg_amount"

    def test_group_by_with_agg(self):
        """SELECT user_id, SUM(amount) AS total FROM default.orders GROUP BY user_id"""
        result = resolve_output_columns(
            "SELECT user_id, SUM(amount) AS total FROM default.orders GROUP BY user_id",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("user_id", IntegerType())
        assert result[1][0] == "total"


# ===================================================================
# 4. Expressions (arithmetic, CASE, CAST)
# ===================================================================


class TestExpressions:
    def test_arithmetic(self):
        """SELECT amount * 2 AS doubled FROM default.orders"""
        result = resolve_output_columns(
            "SELECT amount * 2 AS doubled FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "doubled"

    def test_cast(self):
        """SELECT CAST(user_id AS BIGINT) AS big_id FROM default.users"""
        result = resolve_output_columns(
            "SELECT CAST(user_id AS BIGINT) AS big_id FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "big_id"
        assert isinstance(result[0][1], BigIntType)

    def test_string_literal(self):
        """SELECT 'hello' AS greeting FROM default.users"""
        result = resolve_output_columns(
            "SELECT 'hello' AS greeting FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "greeting"
        assert isinstance(result[0][1], StringType)

    def test_numeric_literal(self):
        """SELECT 42 AS magic FROM default.users"""
        result = resolve_output_columns(
            "SELECT 42 AS magic FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "magic"


# ===================================================================
# 5. Subqueries
# ===================================================================


class TestSubqueries:
    def test_subquery(self):
        """SELECT x FROM (SELECT user_id AS x FROM default.users) sub"""
        result = resolve_output_columns(
            "SELECT x FROM (SELECT user_id AS x FROM default.users) sub",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0] == ("x", IntegerType())

    def test_nested_subquery(self):
        """SELECT y FROM (SELECT x AS y FROM (SELECT user_id AS x FROM default.users) a) b"""
        result = resolve_output_columns(
            "SELECT y FROM (SELECT x AS y FROM (SELECT user_id AS x FROM default.users) a) b",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0] == ("y", IntegerType())


# ===================================================================
# 6. CTEs
# ===================================================================


class TestCTEs:
    def test_simple_cte(self):
        """WITH cte AS (SELECT user_id FROM default.users) SELECT user_id FROM cte"""
        result = resolve_output_columns(
            "WITH cte AS (SELECT user_id FROM default.users) SELECT user_id FROM cte",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0] == ("user_id", IntegerType())

    def test_cte_with_alias(self):
        """WITH cte AS (SELECT user_id AS uid FROM default.users) SELECT uid FROM cte"""
        result = resolve_output_columns(
            "WITH cte AS (SELECT user_id AS uid FROM default.users) SELECT uid FROM cte",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0] == ("uid", IntegerType())

    def test_multiple_ctes(self):
        """
        WITH
          u AS (SELECT user_id FROM default.users),
          o AS (SELECT order_id, user_id FROM default.orders)
        SELECT u.user_id, o.order_id FROM u JOIN o ON u.user_id = o.user_id
        """
        result = resolve_output_columns(
            "WITH u AS (SELECT user_id FROM default.users), "
            "o AS (SELECT order_id, user_id FROM default.orders) "
            "SELECT u.user_id, o.order_id FROM u JOIN o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("user_id", IntegerType())
        assert result[1] == ("order_id", IntegerType())


# ===================================================================
# 7. Derived metrics (no FROM clause)
# ===================================================================


class TestDerivedMetrics:
    def test_single_metric_ref(self):
        """SELECT default.total_revenue — metric reference, no FROM."""
        metric_cols = _col_map(
            ("default.total_revenue", [("default_DOT_total_revenue", DoubleType())]),
        )
        result = resolve_output_columns(
            "SELECT default.total_revenue",
            metric_cols,
        )
        assert len(result) == 1
        assert isinstance(result[0][1], DoubleType)

    def test_multi_metric_expression(self):
        """SELECT default.total_revenue + default.order_count AS combined"""
        metric_cols = _col_map(
            ("default.total_revenue", [("default_DOT_total_revenue", DoubleType())]),
            ("default.order_count", [("default_DOT_order_count", BigIntType())]),
        )
        result = resolve_output_columns(
            "SELECT default.total_revenue + default.order_count AS combined",
            metric_cols,
        )
        assert len(result) == 1
        assert result[0][0] == "combined"


# ===================================================================
# 8. Error cases
# ===================================================================


# ===================================================================
# 10. CASE / WHEN expressions
# ===================================================================


class TestCaseWhen:
    def test_simple_case(self):
        """SELECT CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS tier"""
        result = resolve_output_columns(
            "SELECT CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS tier "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "tier"
        assert isinstance(result[0][1], StringType)

    def test_case_with_column_result(self):
        """CASE returns a column reference type."""
        result = resolve_output_columns(
            "SELECT CASE WHEN amount > 100 THEN amount ELSE 0 END AS adjusted "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "adjusted"


# ===================================================================
# 11. COALESCE / IF
# ===================================================================


class TestCoalesceIf:
    def test_coalesce(self):
        """SELECT COALESCE(email, username) AS contact FROM default.users"""
        result = resolve_output_columns(
            "SELECT COALESCE(email, username) AS contact FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "contact"
        assert isinstance(result[0][1], StringType)

    def test_if_expression(self):
        """SELECT IF(amount > 100, 'big', 'small') AS size FROM default.orders"""
        result = resolve_output_columns(
            "SELECT IF(amount > 100, 'big', 'small') AS size FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "size"


# ===================================================================
# 12. Window functions
# ===================================================================


class TestWindowFunctions:
    def test_sum_over_partition(self):
        """SELECT user_id, SUM(amount) OVER (PARTITION BY user_id) AS running"""
        result = resolve_output_columns(
            "SELECT user_id, SUM(amount) OVER (PARTITION BY user_id) AS running "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("user_id", IntegerType())
        assert result[1][0] == "running"
        assert isinstance(result[1][1], DoubleType)

    def test_row_number(self):
        """SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) AS rn"""
        result = resolve_output_columns(
            "SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) AS rn "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("user_id", IntegerType())
        assert result[1][0] == "rn"


# ===================================================================
# 13. DISTINCT
# ===================================================================


class TestDistinct:
    def test_select_distinct(self):
        """SELECT DISTINCT user_id FROM default.orders — types unchanged."""
        result = resolve_output_columns(
            "SELECT DISTINCT user_id FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0] == ("user_id", IntegerType())

    def test_count_distinct(self):
        """SELECT COUNT(DISTINCT user_id) AS unique_users FROM default.orders"""
        result = resolve_output_columns(
            "SELECT COUNT(DISTINCT user_id) AS unique_users FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "unique_users"
        assert isinstance(result[0][1], BigIntType)


# ===================================================================
# 14. Nested functions
# ===================================================================


class TestNestedFunctions:
    def test_upper_coalesce(self):
        """SELECT UPPER(COALESCE(username, 'unknown')) AS name FROM default.users"""
        result = resolve_output_columns(
            "SELECT UPPER(COALESCE(username, 'unknown')) AS name FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "name"
        assert isinstance(result[0][1], StringType)

    def test_cast_inside_sum(self):
        """SELECT SUM(CAST(user_id AS BIGINT)) AS total FROM default.users"""
        result = resolve_output_columns(
            "SELECT SUM(CAST(user_id AS BIGINT)) AS total FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "total"
        assert isinstance(result[0][1], BigIntType)


# ===================================================================
# 15. Self-join
# ===================================================================


class TestSelfJoin:
    def test_self_join(self):
        """SELECT a.user_id, b.username FROM default.users a JOIN default.users b ON ..."""
        result = resolve_output_columns(
            "SELECT a.user_id, b.username "
            "FROM default.users a "
            "JOIN default.users b ON a.user_id = b.user_id",
            _col_map(USERS_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("user_id", IntegerType())
        assert result[1] == ("username", StringType())


# ===================================================================
# 16. Mixed CTE + subquery
# ===================================================================


class TestMixedCTESubquery:
    def test_cte_used_in_subquery(self):
        """
        WITH cte AS (SELECT user_id, username FROM default.users)
        SELECT uid FROM (SELECT user_id AS uid FROM cte) sub
        """
        result = resolve_output_columns(
            "WITH cte AS (SELECT user_id, username FROM default.users) "
            "SELECT uid FROM (SELECT user_id AS uid FROM cte) sub",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0] == ("uid", IntegerType())


# ===================================================================
# 17. Derived metric + dimension attribute in same query
# ===================================================================


class TestDerivedMetricWithDimension:
    def test_metric_and_dimension_attr(self):
        """
        SELECT default.total_revenue, default.date_dim.week
        — both a metric ref and a dimension attribute ref (no FROM clause).
        """
        metric_and_dim_cols = _col_map(
            ("default.total_revenue", [("default_DOT_total_revenue", DoubleType())]),
            ("default.date_dim", [("week", StringType()), ("year", IntegerType())]),
        )
        result = resolve_output_columns(
            "SELECT default.total_revenue, default.date_dim.week",
            metric_and_dim_cols,
        )
        assert len(result) == 2
        assert isinstance(result[0][1], DoubleType)
        assert isinstance(result[1][1], StringType)

    def test_dimension_attr_in_case_with_from(self):
        """
        SELECT SUM(CASE WHEN default.date_dim.dateint = 20260101 THEN amount ELSE 0 END) AS filtered
        FROM default.orders
        — dimension attribute referenced inline in a query that HAS a FROM clause.
        """
        result = resolve_output_columns(
            "SELECT SUM(CASE WHEN default.date_dim.dateint = 20260101 "
            "THEN amount ELSE 0 END) AS filtered "
            "FROM default.orders",
            _col_map(
                ORDERS_COLS,
                (
                    "default.date_dim",
                    [("dateint", IntegerType()), ("year", IntegerType())],
                ),
            ),
        )
        assert len(result) == 1
        assert result[0][0] == "filtered"

    def test_dimension_attr_in_where_with_from(self):
        """
        SELECT SUM(amount) AS total
        FROM default.orders
        WHERE default.date_dim.year = 2026
        — dimension attribute in WHERE clause.
        """
        result = resolve_output_columns(
            "SELECT SUM(amount) AS total "
            "FROM default.orders "
            "WHERE default.date_dim.year = 2026",
            _col_map(
                ORDERS_COLS,
                (
                    "default.date_dim",
                    [("dateint", IntegerType()), ("year", IntegerType())],
                ),
            ),
        )
        assert len(result) == 1
        assert result[0][0] == "total"

    def test_dimension_attr_in_group_by_with_from(self):
        """
        SELECT default.date_dim.year, SUM(amount) AS total
        FROM default.orders
        GROUP BY default.date_dim.year
        — dimension attribute in both SELECT and GROUP BY.
        """
        result = resolve_output_columns(
            "SELECT default.date_dim.year, SUM(amount) AS total "
            "FROM default.orders "
            "GROUP BY default.date_dim.year",
            _col_map(
                ORDERS_COLS,
                (
                    "default.date_dim",
                    [("dateint", IntegerType()), ("year", IntegerType())],
                ),
            ),
        )
        assert len(result) == 2
        assert isinstance(result[0][1], IntegerType)
        assert result[1][0] == "total"

    def test_dimension_attr_without_dim_in_map(self):
        """
        SELECT default.date_dim.year, SUM(amount) AS total
        FROM default.orders
        GROUP BY default.date_dim.year
        — dimension node NOT in parent_columns_map → UnknownType fallback.
        """
        result = resolve_output_columns(
            "SELECT default.date_dim.year, SUM(amount) AS total "
            "FROM default.orders "
            "GROUP BY default.date_dim.year",
            _col_map(ORDERS_COLS),  # No date_dim in map
        )
        assert len(result) == 2
        assert isinstance(result[0][1], UnknownType)  # Can't resolve without dim node
        assert result[1][0] == "total"

    def test_dimension_attr_in_aggregation(self):
        """
        SELECT SUM(default.date_dim.dateint) AS sum_dates
        FROM default.orders
        — dimension attribute used as the aggregation argument itself.
        """
        result = resolve_output_columns(
            "SELECT SUM(default.date_dim.dateint) AS sum_dates FROM default.orders",
            _col_map(
                ORDERS_COLS,
                (
                    "default.date_dim",
                    [("dateint", IntegerType()), ("year", IntegerType())],
                ),
            ),
        )
        assert len(result) == 1
        assert result[0][0] == "sum_dates"
        # SUM(IntegerType) → BigIntType
        assert isinstance(result[0][1], BigIntType)


# ===================================================================
# 18. Deep namespace resolution
# ===================================================================


class TestDeepNamespaces:
    def test_deep_namespace_dimension_attr(self):
        """
        SELECT ads.report.dims.date.year, SUM(amount) AS total
        FROM default.orders
        GROUP BY ads.report.dims.date.year
        — deep namespace dimension attribute reference.
        """
        result = resolve_output_columns(
            "SELECT ads.report.dims.date.year, SUM(amount) AS total "
            "FROM default.orders "
            "GROUP BY ads.report.dims.date.year",
            _col_map(
                ORDERS_COLS,
                (
                    "ads.report.dims.date",
                    [
                        ("year", IntegerType()),
                        ("month", IntegerType()),
                        ("dateint", IntegerType()),
                    ],
                ),
            ),
        )
        assert len(result) == 2
        assert isinstance(result[0][1], IntegerType)
        assert result[1][0] == "total"

    def test_deep_namespace_derived_metric(self):
        """
        SELECT ads.report.metrics.total_revenue
        — derived metric with deep namespace.
        """
        result = resolve_output_columns(
            "SELECT ads.report.metrics.total_revenue",
            _col_map(
                (
                    "ads.report.metrics.total_revenue",
                    [("ads_DOT_report_DOT_metrics_DOT_total_revenue", DoubleType())],
                ),
            ),
        )
        assert len(result) == 1
        assert isinstance(result[0][1], DoubleType)

    def test_deep_namespace_dim_attr_in_aggregation(self):
        """
        SELECT SUM(ads.report.dims.date.dateint) AS sum_dates
        FROM default.orders
        """
        result = resolve_output_columns(
            "SELECT SUM(ads.report.dims.date.dateint) AS sum_dates FROM default.orders",
            _col_map(
                ORDERS_COLS,
                (
                    "ads.report.dims.date",
                    [("dateint", IntegerType()), ("year", IntegerType())],
                ),
            ),
        )
        assert len(result) == 1
        assert result[0][0] == "sum_dates"
        assert isinstance(result[0][1], BigIntType)

    def test_deep_namespace_not_in_map_fallback(self):
        """
        SELECT ads.report.dims.date.year FROM default.orders
        — deep namespace dim NOT in parent map → UnknownType.
        """
        result = resolve_output_columns(
            "SELECT ads.report.dims.date.year, amount FROM default.orders",
            _col_map(ORDERS_COLS),  # No deep dim in map
        )
        assert len(result) == 2
        assert isinstance(result[0][1], UnknownType)
        assert isinstance(result[1][1], DoubleType)

    def test_ambiguous_namespace_prefers_longest_match(self):
        """
        If both 'ads.report' and 'ads.report.dims' exist as nodes,
        a reference to 'ads.report.dims.date.year' should match
        'ads.report.dims' with column 'date' (if it exists), not
        'ads.report' with column 'dims'.
        """
        result = resolve_output_columns(
            "SELECT ads.report.dims.year FROM default.orders",
            _col_map(
                ORDERS_COLS,
                ("ads.report", [("dims", StringType())]),
                ("ads.report.dims", [("year", IntegerType())]),
            ),
        )
        assert len(result) == 1
        # Should match ads.report.dims (longer prefix) with column year
        assert isinstance(result[0][1], IntegerType)


# ===================================================================
# 19. UNION / SET operations
# ===================================================================


class TestSetOperations:
    def test_union(self):
        """
        SELECT user_id FROM default.users
        UNION
        SELECT user_id FROM default.orders
        — output should have user_id with consistent type.
        """
        result = resolve_output_columns(
            "SELECT user_id FROM default.users "
            "UNION "
            "SELECT user_id FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "user_id"
        assert isinstance(result[0][1], IntegerType)

    def test_except(self):
        """
        SELECT user_id FROM default.users
        EXCEPT
        SELECT user_id FROM default.orders
        — output takes types from the first SELECT.
        """
        result = resolve_output_columns(
            "SELECT user_id FROM default.users "
            "EXCEPT "
            "SELECT user_id FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "user_id"
        assert isinstance(result[0][1], IntegerType)

    def test_intersect(self):
        """
        SELECT user_id FROM default.users
        INTERSECT
        SELECT user_id FROM default.orders
        — output takes types from the first SELECT.
        """
        result = resolve_output_columns(
            "SELECT user_id FROM default.users "
            "INTERSECT "
            "SELECT user_id FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "user_id"
        assert isinstance(result[0][1], IntegerType)


# ===================================================================
# 19. LATERAL VIEW EXPLODE
# ===================================================================


ARRAY_NODE_COLS = (
    "default.events",
    [
        ("event_id", IntegerType()),
        (
            "tags",
            StringType(),
        ),  # In practice this would be array<string>, but stored as string type
    ],
)


class TestLateralViewExplode:
    def test_lateral_view_explode(self):
        """
        SELECT event_id, tag
        FROM default.events
        LATERAL VIEW EXPLODE(tags) t AS tag
        — the exploded column 'tag' should be in scope.
        """
        result = resolve_output_columns(
            "SELECT event_id, tag "
            "FROM default.events "
            "LATERAL VIEW EXPLODE(tags) t AS tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("event_id", IntegerType())
        # 'tag' comes from the lateral view — type may be StringType or inferred
        assert result[1][0] == "tag"

    def test_lateral_view_outer_explode(self):
        """
        SELECT event_id, tag
        FROM default.events
        LATERAL VIEW OUTER EXPLODE(tags) t AS tag
        """
        result = resolve_output_columns(
            "SELECT event_id, tag "
            "FROM default.events "
            "LATERAL VIEW OUTER EXPLODE(tags) t AS tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("event_id", IntegerType())
        assert result[1][0] == "tag"


# ===================================================================
# 20. HLL and other registered functions
# ===================================================================


class TestRegisteredFunctions:
    def test_hll_sketch_estimate(self):
        """SELECT hll_sketch_estimate(sketch_col) AS approx_count FROM default.sketches"""
        sketch_cols = _col_map(
            (
                "default.sketches",
                [("sketch_col", StringType()), ("dim_id", IntegerType())],
            ),
        )
        result = resolve_output_columns(
            "SELECT hll_sketch_estimate(sketch_col) AS approx_count FROM default.sketches",
            sketch_cols,
        )
        assert len(result) == 1
        assert result[0][0] == "approx_count"
        # HllSketchEstimate returns LongType
        from datajunction_server.sql.parsing.types import LongType

        assert isinstance(result[0][1], LongType)

    def test_concat(self):
        """SELECT CONCAT(username, '@', email) AS full_contact FROM default.users"""
        result = resolve_output_columns(
            "SELECT CONCAT(username, '@', email) AS full_contact FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "full_contact"
        assert isinstance(result[0][1], StringType)

    def test_length(self):
        """SELECT LENGTH(username) AS name_len FROM default.users"""
        result = resolve_output_columns(
            "SELECT LENGTH(username) AS name_len FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "name_len"

    def test_unregistered_function_fallback(self):
        """An unregistered function should fallback gracefully, not crash."""
        result = resolve_output_columns(
            "SELECT SOME_UNKNOWN_FUNC(user_id) AS x FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "x"
        # Should get UnknownType fallback rather than crashing
        assert isinstance(result[0][1], UnknownType)


# ===================================================================
# 23. Multi-level DJ node references (real-world DJ pattern)
# ===================================================================


NODE_C_COLS = (
    "default.node_c",
    [
        ("x", IntegerType()),
        ("y", StringType()),
    ],
)

NODE_D_COLS = (
    "default.node_d",
    [
        ("m", IntegerType()),
        ("n", DoubleType()),
    ],
)

# node_b's query: SELECT x, y, d.n FROM default.node_c JOIN default.node_d d ON node_c.x = d.m
# After resolution, node_b's output columns are (x: int, y: string, n: double)
NODE_B_COLS = (
    "default.node_b",
    [
        ("x", IntegerType()),
        ("y", StringType()),
        ("n", DoubleType()),
    ],
)


class TestMultiLevelNodeReferences:
    def test_node_referencing_other_nodes(self):
        """
        node_a: SELECT x, y, n FROM default.node_b
        node_b's columns are pre-resolved in the parent map.
        """
        result = resolve_output_columns(
            "SELECT x, y, n FROM default.node_b",
            _col_map(NODE_B_COLS),
        )
        assert len(result) == 3
        assert result[0] == ("x", IntegerType())
        assert result[1] == ("y", StringType())
        assert result[2] == ("n", DoubleType())

    def test_node_b_query_against_its_parents(self):
        """
        node_b: SELECT c.x, c.y, d.n
        FROM default.node_c c
        JOIN default.node_d d ON c.x = d.m
        — resolving node_b's query against its parents (node_c, node_d).
        """
        result = resolve_output_columns(
            "SELECT c.x, c.y, d.n "
            "FROM default.node_c c "
            "JOIN default.node_d d ON c.x = d.m",
            _col_map(NODE_C_COLS, NODE_D_COLS),
        )
        assert len(result) == 3
        assert result[0] == ("x", IntegerType())
        assert result[1] == ("y", StringType())
        assert result[2] == ("n", DoubleType())

    def test_complex_multi_node_with_aggregation(self):
        """
        SELECT node_b.x, SUM(node_b.n) AS total_n
        FROM default.node_b
        GROUP BY node_b.x
        """
        result = resolve_output_columns(
            "SELECT b.x, SUM(b.n) AS total_n FROM default.node_b b GROUP BY b.x",
            _col_map(NODE_B_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("x", IntegerType())
        assert result[1][0] == "total_n"
        assert isinstance(result[1][1], DoubleType)


# ===================================================================
# 24. Deeply nested expressions
# ===================================================================


class TestDeeplyNestedExpressions:
    def test_max_case_greatest_coalesce(self):
        """
        SELECT MAX(CASE
            WHEN GREATEST(COALESCE(amount, 0), 0) > 0
            THEN amount
            ELSE 0
        END) AS max_positive_amount
        FROM default.orders
        """
        result = resolve_output_columns(
            "SELECT MAX(CASE "
            "  WHEN GREATEST(COALESCE(amount, 0), 0) > 0 "
            "  THEN amount "
            "  ELSE 0 "
            "END) AS max_positive_amount "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "max_positive_amount"
        # MAX of DoubleType → DoubleType (amount is DoubleType)
        assert isinstance(result[0][1], DoubleType)

    def test_sum_case_when_with_nested_functions(self):
        """
        SELECT SUM(CASE
            WHEN LEAST(amount, 1000) = amount THEN amount
            ELSE 1000
        END) AS capped_total
        FROM default.orders
        """
        result = resolve_output_columns(
            "SELECT SUM(CASE "
            "  WHEN LEAST(amount, 1000) = amount THEN amount "
            "  ELSE 1000 "
            "END) AS capped_total "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "capped_total"

    def test_concat_ws_with_coalesce(self):
        """
        SELECT CONCAT_WS('-', COALESCE(username, 'unknown'), CAST(user_id AS STRING)) AS label
        FROM default.users
        """
        result = resolve_output_columns(
            "SELECT CONCAT_WS('-', COALESCE(username, 'unknown'), "
            "CAST(user_id AS STRING)) AS label "
            "FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "label"
        assert isinstance(result[0][1], StringType)

    def test_nested_aggregation_with_arithmetic(self):
        """
        SELECT
            user_id,
            SUM(amount) / COUNT(*) AS avg_manual,
            COUNT(DISTINCT order_id) AS unique_orders
        FROM default.orders
        GROUP BY user_id
        """
        result = resolve_output_columns(
            "SELECT user_id, "
            "SUM(amount) / COUNT(*) AS avg_manual, "
            "COUNT(DISTINCT order_id) AS unique_orders "
            "FROM default.orders "
            "GROUP BY user_id",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 3
        assert result[0] == ("user_id", IntegerType())
        assert result[1][0] == "avg_manual"
        assert result[2][0] == "unique_orders"
        assert isinstance(result[2][1], BigIntType)


# ===================================================================
# 25. CROSS JOIN UNNEST (Trino-style array flattening)
# ===================================================================


ARRAY_SOURCE_COLS = (
    "default.murals",
    [
        ("mural_id", IntegerType()),
        ("name", StringType()),
        ("colors", StringType()),  # array<struct<id, name>> stored as string type
    ],
)


class TestCrossJoinUnnest:
    def test_cross_join_unnest(self):
        """
        SELECT m.mural_id, t.color_name
        FROM default.murals m
        CROSS JOIN UNNEST(m.colors) t(color_id, color_name)
        """
        result = resolve_output_columns(
            "SELECT m.mural_id, t.color_name "
            "FROM default.murals m "
            "CROSS JOIN UNNEST(m.colors) t(color_id, color_name)",
            _col_map(ARRAY_SOURCE_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("mural_id", IntegerType())
        assert result[1][0] == "color_name"

    def test_cross_join_unnest_unqualified(self):
        """
        SELECT mural_id, color_name
        FROM default.murals
        CROSS JOIN UNNEST(colors) t(color_id, color_name)
        """
        result = resolve_output_columns(
            "SELECT mural_id, color_name "
            "FROM default.murals "
            "CROSS JOIN UNNEST(colors) t(color_id, color_name)",
            _col_map(ARRAY_SOURCE_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("mural_id", IntegerType())
        assert result[1][0] == "color_name"


# ===================================================================
# 26. POSEXPLODE (LATERAL VIEW with ordinal)
# ===================================================================


class TestPosExplode:
    def test_posexplode(self):
        """
        SELECT event_id, pos, tag
        FROM default.events
        LATERAL VIEW POSEXPLODE(tags) t AS pos, tag
        """
        result = resolve_output_columns(
            "SELECT event_id, pos, tag "
            "FROM default.events "
            "LATERAL VIEW POSEXPLODE(tags) t AS pos, tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert len(result) == 3
        assert result[0] == ("event_id", IntegerType())
        assert result[1][0] == "pos"
        assert result[2][0] == "tag"


# ===================================================================
# 27. RANGE and other table-valued functions in FROM
# ===================================================================


class TestTableValuedFunctions:
    def test_range_in_from(self):
        """
        SELECT id FROM RANGE(10) t(id)
        — RANGE produces a table with a single column.
        """
        result = resolve_output_columns(
            "SELECT id FROM RANGE(10) t(id)",
            {},  # No parent tables needed — RANGE is self-contained
        )
        assert len(result) == 1
        assert result[0][0] == "id"
        assert isinstance(result[0][1], UnknownType)

    def test_range_cross_join(self):
        """
        SELECT u.user_id, r.idx
        FROM default.users u
        CROSS JOIN RANGE(5) r(idx)
        """
        result = resolve_output_columns(
            "SELECT u.user_id, r.idx FROM default.users u CROSS JOIN RANGE(5) r(idx)",
            _col_map(USERS_COLS),
        )
        assert len(result) == 2
        assert result[0] == ("user_id", IntegerType())
        assert result[1][0] == "idx"
        assert isinstance(result[1][1], UnknownType)


# ===================================================================
# Ambiguous column error
# ===================================================================


class TestAmbiguousColumn:
    def test_ambiguous_column_errors(self):
        """
        SELECT user_id FROM default.users u, default.orders o
        — user_id exists in both, should raise.
        """
        with pytest.raises(TypeResolutionError, match="ambiguous"):
            resolve_output_columns(
                "SELECT user_id FROM default.users u, default.orders o",
                _col_map(USERS_COLS, ORDERS_COLS),
            )


# ===================================================================
# 20. Column only in WHERE, not SELECT
# ===================================================================


class TestWhereOnlyColumn:
    def test_where_column_doesnt_affect_output(self):
        """
        SELECT username FROM default.users WHERE user_id > 10
        — user_id is only in WHERE, output should have just username.
        """
        result = resolve_output_columns(
            "SELECT username FROM default.users WHERE user_id > 10",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0] == ("username", StringType())


# ===================================================================
# 21. NULL literal
# ===================================================================


class TestNullLiteral:
    def test_null_in_projection(self):
        """SELECT NULL AS placeholder FROM default.users"""
        result = resolve_output_columns(
            "SELECT NULL AS placeholder FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0][0] == "placeholder"


# ===================================================================
# 22. Column aliased same as another table's column
# ===================================================================


class TestConfusingAliases:
    def test_alias_shadows_other_column(self):
        """
        SELECT user_id AS order_id FROM default.users
        — output should be 'order_id' with IntegerType (from users.user_id).
        """
        result = resolve_output_columns(
            "SELECT user_id AS order_id FROM default.users",
            _col_map(USERS_COLS),
        )
        assert len(result) == 1
        assert result[0] == ("order_id", IntegerType())


# ===================================================================
# Error cases (original)
# ===================================================================


class TestErrors:
    def test_missing_table(self):
        """SELECT a FROM default.nonexistent — table not in parent_columns_map."""
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT a FROM default.nonexistent",
                _col_map(USERS_COLS),
            )

    def test_missing_column(self):
        """SELECT nonexistent FROM default.users — column not in table."""
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT nonexistent FROM default.users",
                _col_map(USERS_COLS),
            )

    def test_empty_parent_map(self):
        """No parents provided."""
        with pytest.raises(TypeResolutionError):
            resolve_output_columns(
                "SELECT a FROM default.users",
                {},
            )


# ===================================================================
# 9. Column signature comparison helper
# ===================================================================


class TestColumnSignatureComparison:
    """Tests for the helper that checks if a node's output columns changed."""

    def test_unchanged_columns(self):
        from datajunction_server.internal.deployment.type_inference import (
            columns_signature_changed,
        )

        old = [("user_id", IntegerType()), ("name", StringType())]
        new = [("user_id", IntegerType()), ("name", StringType())]
        assert columns_signature_changed(old, new) is False

    def test_type_changed(self):
        from datajunction_server.internal.deployment.type_inference import (
            columns_signature_changed,
        )

        old = [("user_id", IntegerType()), ("name", StringType())]
        new = [("user_id", BigIntType()), ("name", StringType())]
        assert columns_signature_changed(old, new) is True

    def test_column_added(self):
        from datajunction_server.internal.deployment.type_inference import (
            columns_signature_changed,
        )

        old = [("user_id", IntegerType())]
        new = [("user_id", IntegerType()), ("name", StringType())]
        assert columns_signature_changed(old, new) is True

    def test_column_removed(self):
        from datajunction_server.internal.deployment.type_inference import (
            columns_signature_changed,
        )

        old = [("user_id", IntegerType()), ("name", StringType())]
        new = [("user_id", IntegerType())]
        assert columns_signature_changed(old, new) is True

    def test_column_renamed(self):
        from datajunction_server.internal.deployment.type_inference import (
            columns_signature_changed,
        )

        old = [("user_id", IntegerType())]
        new = [("uid", IntegerType())]
        assert columns_signature_changed(old, new) is True

    def test_unknown_type_always_changed(self):
        """UnknownType in either old or new should be treated as changed."""
        from datajunction_server.internal.deployment.type_inference import (
            columns_signature_changed,
        )

        old = [("user_id", IntegerType())]
        new = [("user_id", UnknownType())]
        assert columns_signature_changed(old, new) is True

        old = [("user_id", UnknownType())]
        new = [("user_id", IntegerType())]
        assert columns_signature_changed(old, new) is True

        old = [("user_id", UnknownType())]
        new = [("user_id", UnknownType())]
        assert columns_signature_changed(old, new) is True
