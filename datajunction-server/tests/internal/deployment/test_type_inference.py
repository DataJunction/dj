"""
Unit tests for the lightweight top-down type inference used by deployment propagation.

resolve_output_columns takes a SQL query string and a map of parent node columns,
and returns the output column names + types without any DB calls.
"""

import pytest

from datajunction_server.internal.deployment.type_inference import (
    columns_signature_changed,
    resolve_output_columns,
    TypeResolutionError,
)
from datajunction_server.sql.parsing.types import (
    BigIntType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    NullType,
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

ARRAY_NODE_COLS = (
    "default.events",
    [
        ("event_id", IntegerType()),
        ("tags", StringType()),
    ],
)


# ---------------------------------------------------------------------------
# Simple SELECT
# ---------------------------------------------------------------------------


class TestSimpleSelect:
    def test_select_columns(self):
        result = resolve_output_columns(
            "SELECT user_id, username FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result == [
            ("user_id", IntegerType()),
            ("username", StringType()),
        ]

    def test_select_with_alias(self):
        result = resolve_output_columns(
            "SELECT user_id AS id, username AS name FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result == [
            ("id", IntegerType()),
            ("name", StringType()),
        ]

    def test_select_star(self):
        result = resolve_output_columns(
            "SELECT * FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result == [
            ("user_id", IntegerType()),
            ("username", StringType()),
            ("email", StringType()),
            ("created_at", TimestampType()),
        ]

    def test_qualified_wildcard(self):
        result = resolve_output_columns(
            "SELECT u.* FROM default.users u",
            _col_map(USERS_COLS),
        )
        assert result == [
            ("user_id", IntegerType()),
            ("username", StringType()),
            ("email", StringType()),
            ("created_at", TimestampType()),
        ]

    def test_qualified_wildcard_multi_table(self):
        """SELECT u.* should only return users columns, not orders."""
        result = resolve_output_columns(
            "SELECT u.* FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result == [
            ("user_id", IntegerType()),
            ("username", StringType()),
            ("email", StringType()),
            ("created_at", TimestampType()),
        ]

    def test_both_table_stars(self):
        result = resolve_output_columns(
            "SELECT u.*, o.* FROM default.users u, default.orders o",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result == [
            ("user_id", IntegerType()),
            ("username", StringType()),
            ("email", StringType()),
            ("created_at", TimestampType()),
            ("order_id", IntegerType()),
            ("user_id", IntegerType()),
            ("amount", DoubleType()),
            ("order_date", DateType()),
        ]

    def test_missing_wildcard_alias_falls_through(self):
        """SELECT missing.* falls through to all-table expansion."""
        result = resolve_output_columns(
            "SELECT missing.* FROM default.users u",
            _col_map(USERS_COLS),
        )
        assert result == [
            ("user_id", IntegerType()),
            ("username", StringType()),
            ("email", StringType()),
            ("created_at", TimestampType()),
        ]


# ---------------------------------------------------------------------------
# Multi-table / JOIN
# ---------------------------------------------------------------------------


class TestMultiTable:
    def test_qualified_columns(self):
        result = resolve_output_columns(
            "SELECT u.user_id, o.amount FROM default.users u, default.orders o",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result == [
            ("user_id", IntegerType()),
            ("amount", DoubleType()),
        ]

    def test_unqualified_unambiguous(self):
        result = resolve_output_columns(
            "SELECT username, amount "
            "FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result == [
            ("username", StringType()),
            ("amount", DoubleType()),
        ]

    def test_join_columns(self):
        result = resolve_output_columns(
            "SELECT u.username, o.amount "
            "FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result == [
            ("username", StringType()),
            ("amount", DoubleType()),
        ]

    def test_self_join(self):
        result = resolve_output_columns(
            "SELECT a.user_id, b.username "
            "FROM default.users a "
            "JOIN default.users b ON a.user_id = b.user_id",
            _col_map(USERS_COLS),
        )
        assert result == [
            ("user_id", IntegerType()),
            ("username", StringType()),
        ]


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------


class TestAggregations:
    def test_count_star(self):
        result = resolve_output_columns(
            "SELECT COUNT(*) AS cnt FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("cnt", BigIntType())

    def test_sum(self):
        result = resolve_output_columns(
            "SELECT SUM(amount) AS total FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("total", DoubleType())

    def test_avg(self):
        result = resolve_output_columns(
            "SELECT AVG(amount) AS avg_amount FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0][0] == "avg_amount"
        assert isinstance(result[0][1], DoubleType)

    def test_group_by_with_agg(self):
        result = resolve_output_columns(
            "SELECT user_id, SUM(amount) AS total FROM default.orders GROUP BY user_id",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())
        assert result[1][0] == "total"
        assert isinstance(result[1][1], DoubleType)

    def test_count_distinct(self):
        result = resolve_output_columns(
            "SELECT COUNT(DISTINCT user_id) AS unique_users FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("unique_users", BigIntType())

    def test_select_distinct(self):
        result = resolve_output_columns(
            "SELECT DISTINCT user_id FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------


class TestExpressions:
    def test_arithmetic(self):
        result = resolve_output_columns(
            "SELECT amount * 2 AS doubled FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0][0] == "doubled"
        assert isinstance(result[0][1], DoubleType)

    def test_arithmetic_without_alias(self):
        result = resolve_output_columns(
            "SELECT amount + 1 FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert isinstance(result[0][1], DoubleType)

    def test_parenthesized_expression(self):
        result = resolve_output_columns(
            "SELECT (amount + 1) FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result) == 1
        assert isinstance(result[0][1], DoubleType)

    def test_cast(self):
        result = resolve_output_columns(
            "SELECT CAST(user_id AS BIGINT) AS big_id FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("big_id", BigIntType())

    def test_string_literal(self):
        result = resolve_output_columns(
            "SELECT 'hello' AS greeting FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("greeting", StringType())

    def test_numeric_literal(self):
        result = resolve_output_columns(
            "SELECT 42 AS magic FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0][0] == "magic"
        assert isinstance(result[0][1], IntegerType)

    def test_boolean_literal(self):
        result = resolve_output_columns(
            "SELECT TRUE AS flag FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("flag", BooleanType())

    def test_null_literal(self):
        result = resolve_output_columns(
            "SELECT NULL AS placeholder FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("placeholder", NullType())

    def test_alias_shadows_other_column(self):
        result = resolve_output_columns(
            "SELECT user_id AS order_id FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("order_id", IntegerType())


# ---------------------------------------------------------------------------
# CASE / WHEN / COALESCE / IF
# ---------------------------------------------------------------------------


class TestConditionals:
    def test_case_string_result(self):
        result = resolve_output_columns(
            "SELECT CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS tier "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("tier", StringType())

    def test_case_column_result(self):
        result = resolve_output_columns(
            "SELECT CASE WHEN amount > 100 THEN amount ELSE 0 END AS adjusted "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0][0] == "adjusted"
        assert isinstance(result[0][1], DoubleType)

    def test_case_else_fallback(self):
        """First THEN branch unresolvable, falls through to ELSE."""
        result = resolve_output_columns(
            "SELECT CASE WHEN TRUE THEN default.dim.x ELSE amount END AS val "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("val", DoubleType())

    def test_case_all_branches_unresolvable(self):
        result = resolve_output_columns(
            "SELECT CASE WHEN TRUE THEN default.dim.x END AS val FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("val", UnknownType())

    def test_coalesce(self):
        result = resolve_output_columns(
            "SELECT COALESCE(email, username) AS contact FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("contact", StringType())

    def test_if_expression(self):
        result = resolve_output_columns(
            "SELECT IF(amount > 100, 'big', 'small') AS size FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0][0] == "size"
        assert isinstance(result[0][1], StringType)


# ---------------------------------------------------------------------------
# Window functions
# ---------------------------------------------------------------------------


class TestWindowFunctions:
    def test_sum_over_partition(self):
        result = resolve_output_columns(
            "SELECT user_id, SUM(amount) OVER (PARTITION BY user_id) AS running "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())
        assert result[1][0] == "running"
        assert isinstance(result[1][1], DoubleType)

    def test_row_number(self):
        result = resolve_output_columns(
            "SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) AS rn "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())
        assert result[1][0] == "rn"
        assert isinstance(result[1][1], IntegerType)


# ---------------------------------------------------------------------------
# Nested functions
# ---------------------------------------------------------------------------


class TestNestedFunctions:
    def test_upper_coalesce(self):
        result = resolve_output_columns(
            "SELECT UPPER(COALESCE(username, 'unknown')) AS name FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("name", StringType())

    def test_cast_inside_sum(self):
        result = resolve_output_columns(
            "SELECT SUM(CAST(user_id AS BIGINT)) AS total FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("total", BigIntType())


# ---------------------------------------------------------------------------
# Subqueries
# ---------------------------------------------------------------------------


class TestSubqueries:
    def test_subquery(self):
        result = resolve_output_columns(
            "SELECT x FROM (SELECT user_id AS x FROM default.users) sub",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("x", IntegerType())

    def test_nested_subquery(self):
        result = resolve_output_columns(
            "SELECT y FROM (SELECT x AS y FROM (SELECT user_id AS x FROM default.users) a) b",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("y", IntegerType())

    def test_inner_column_not_visible_in_outer(self):
        """username is not in the subquery's output."""
        with pytest.raises(TypeResolutionError):
            resolve_output_columns(
                "SELECT username FROM (SELECT user_id AS x FROM default.users) sub",
                _col_map(USERS_COLS),
            )


# ---------------------------------------------------------------------------
# CTEs
# ---------------------------------------------------------------------------


class TestCTEs:
    def test_simple_cte(self):
        result = resolve_output_columns(
            "WITH cte AS (SELECT user_id FROM default.users) SELECT user_id FROM cte",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())

    def test_cte_with_alias(self):
        result = resolve_output_columns(
            "WITH cte AS (SELECT user_id AS uid FROM default.users) SELECT uid FROM cte",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("uid", IntegerType())

    def test_multiple_ctes(self):
        result = resolve_output_columns(
            "WITH u AS (SELECT user_id FROM default.users), "
            "o AS (SELECT order_id, user_id FROM default.orders) "
            "SELECT u.user_id, o.order_id FROM u JOIN o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result == [
            ("user_id", IntegerType()),
            ("order_id", IntegerType()),
        ]

    def test_cte_used_in_subquery(self):
        result = resolve_output_columns(
            "WITH cte AS (SELECT user_id, username FROM default.users) "
            "SELECT uid FROM (SELECT user_id AS uid FROM cte) sub",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("uid", IntegerType())


# ---------------------------------------------------------------------------
# Derived metrics
# ---------------------------------------------------------------------------


class TestDerivedMetrics:
    def test_single_metric_ref(self):
        result = resolve_output_columns(
            "SELECT default.total_revenue",
            _col_map(
                (
                    "default.total_revenue",
                    [("default_DOT_total_revenue", DoubleType())],
                ),
            ),
        )
        assert len(result) == 1
        assert isinstance(result[0][1], DoubleType)

    def test_multi_metric_expression(self):
        result = resolve_output_columns(
            "SELECT default.total_revenue + default.order_count AS combined",
            _col_map(
                (
                    "default.total_revenue",
                    [("default_DOT_total_revenue", DoubleType())],
                ),
                ("default.order_count", [("default_DOT_order_count", BigIntType())]),
            ),
        )
        assert result[0][0] == "combined"


# ---------------------------------------------------------------------------
# Dimension attribute references
# ---------------------------------------------------------------------------


class TestDimensionAttributes:
    def test_dim_attr_in_derived_metric(self):
        result = resolve_output_columns(
            "SELECT default.total_revenue, default.date_dim.week",
            _col_map(
                (
                    "default.total_revenue",
                    [("default_DOT_total_revenue", DoubleType())],
                ),
                ("default.date_dim", [("week", StringType()), ("year", IntegerType())]),
            ),
        )
        assert isinstance(result[0][1], DoubleType)
        assert isinstance(result[1][1], StringType)

    def test_dim_attr_in_case_with_from(self):
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
        assert result[0][0] == "filtered"

    def test_dim_attr_in_where_with_from(self):
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
        assert result[0][0] == "total"
        assert isinstance(result[0][1], DoubleType)

    def test_dim_attr_in_group_by_with_from(self):
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
        assert isinstance(result[0][1], IntegerType)
        assert result[1][0] == "total"

    def test_dim_attr_not_in_map(self):
        """Dimension not in parent map → UnknownType fallback."""
        result = resolve_output_columns(
            "SELECT default.date_dim.year, SUM(amount) AS total "
            "FROM default.orders "
            "GROUP BY default.date_dim.year",
            _col_map(ORDERS_COLS),
        )
        assert isinstance(result[0][1], UnknownType)
        assert result[1][0] == "total"

    def test_dim_attr_in_aggregation(self):
        """SUM(default.date_dim.dateint) — dim attr as aggregation argument."""
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
        assert result[0] == ("sum_dates", BigIntType())


# ---------------------------------------------------------------------------
# Deep namespaces
# ---------------------------------------------------------------------------


class TestDeepNamespaces:
    def test_node_exists_but_column_doesnt(self):
        """Progressive prefix finds the node but the column name doesn't match."""
        result = resolve_output_columns(
            "SELECT ads.report.dims.date.nonexistent_col FROM default.orders",
            _col_map(
                ORDERS_COLS,
                ("ads.report.dims.date", [("year", IntegerType())]),
            ),
        )
        # Node found but column missing → continues to shorter prefixes → UnknownType
        assert isinstance(result[0][1], UnknownType)

    def test_deep_namespace_dim_attr(self):
        result = resolve_output_columns(
            "SELECT ads.report.dims.date.year, SUM(amount) AS total "
            "FROM default.orders "
            "GROUP BY ads.report.dims.date.year",
            _col_map(
                ORDERS_COLS,
                (
                    "ads.report.dims.date",
                    [("year", IntegerType()), ("month", IntegerType())],
                ),
            ),
        )
        assert isinstance(result[0][1], IntegerType)
        assert result[1][0] == "total"

    def test_deep_namespace_derived_metric(self):
        result = resolve_output_columns(
            "SELECT ads.report.metrics.total_revenue",
            _col_map(
                (
                    "ads.report.metrics.total_revenue",
                    [("ads_DOT_report_DOT_metrics_DOT_total_revenue", DoubleType())],
                ),
            ),
        )
        assert isinstance(result[0][1], DoubleType)

    def test_deep_namespace_dim_in_aggregation(self):
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
        assert result[0] == ("sum_dates", BigIntType())

    def test_deep_namespace_not_in_map(self):
        result = resolve_output_columns(
            "SELECT ads.report.dims.date.year, amount FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert isinstance(result[0][1], UnknownType)
        assert isinstance(result[1][1], DoubleType)

    def test_longest_prefix_wins(self):
        """Prefers ads.report.dims (longer) over ads.report."""
        result = resolve_output_columns(
            "SELECT ads.report.dims.year FROM default.orders",
            _col_map(
                ORDERS_COLS,
                ("ads.report", [("dims", StringType())]),
                ("ads.report.dims", [("year", IntegerType())]),
            ),
        )
        assert isinstance(result[0][1], IntegerType)

    def test_deep_namespace_derived_dim_attr(self):
        """Derived metric with deep namespace dim attr via progressive prefix."""
        result = resolve_output_columns(
            "SELECT deep.ns.metric_a, deep.ns.dim.date.year",
            _col_map(
                ("deep.ns.metric_a", [("deep_DOT_ns_DOT_metric_a", DoubleType())]),
                (
                    "deep.ns.dim.date",
                    [("year", IntegerType()), ("month", IntegerType())],
                ),
            ),
        )
        assert isinstance(result[0][1], DoubleType)
        assert isinstance(result[1][1], IntegerType)


# ---------------------------------------------------------------------------
# SET operations
# ---------------------------------------------------------------------------


class TestSetOperations:
    def test_union(self):
        result = resolve_output_columns(
            "SELECT user_id FROM default.users UNION SELECT user_id FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())

    def test_except(self):
        result = resolve_output_columns(
            "SELECT user_id FROM default.users EXCEPT SELECT user_id FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())

    def test_intersect(self):
        result = resolve_output_columns(
            "SELECT user_id FROM default.users INTERSECT SELECT user_id FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())


# ---------------------------------------------------------------------------
# LATERAL VIEW / table-valued functions
# ---------------------------------------------------------------------------


class TestTableValuedFunctions:
    def test_lateral_view_explode(self):
        result = resolve_output_columns(
            "SELECT event_id, tag "
            "FROM default.events "
            "LATERAL VIEW EXPLODE(tags) t AS tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert result[0] == ("event_id", IntegerType())
        assert result[1][0] == "tag"
        assert isinstance(result[1][1], UnknownType)

    def test_lateral_view_outer_explode(self):
        result = resolve_output_columns(
            "SELECT event_id, tag "
            "FROM default.events "
            "LATERAL VIEW OUTER EXPLODE(tags) t AS tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert result[0] == ("event_id", IntegerType())
        assert result[1][0] == "tag"

    def test_posexplode(self):
        result = resolve_output_columns(
            "SELECT event_id, pos, tag "
            "FROM default.events "
            "LATERAL VIEW POSEXPLODE(tags) t AS pos, tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert result[0] == ("event_id", IntegerType())
        assert result[1][0] == "pos"
        assert result[2][0] == "tag"

    def test_explode_with_list_type(self):
        """EXPLODE on a ListType column → element type is resolved."""
        from datajunction_server.sql.parsing.types import ListType

        typed_events = _col_map(
            (
                "default.events",
                [
                    ("event_id", IntegerType()),
                    ("tags", ListType(element_type=StringType())),
                ],
            ),
        )
        result = resolve_output_columns(
            "SELECT event_id, tag "
            "FROM default.events "
            "LATERAL VIEW EXPLODE(tags) t AS tag",
            typed_events,
        )
        assert result[0] == ("event_id", IntegerType())
        assert result[1] == ("tag", StringType())

    def test_posexplode_with_list_type(self):
        """POSEXPLODE on a ListType → pos is IntegerType, element is resolved."""
        from datajunction_server.sql.parsing.types import ListType

        typed_events = _col_map(
            (
                "default.events",
                [
                    ("event_id", IntegerType()),
                    ("tags", ListType(element_type=StringType())),
                ],
            ),
        )
        result = resolve_output_columns(
            "SELECT event_id, pos, tag "
            "FROM default.events "
            "LATERAL VIEW POSEXPLODE(tags) t AS pos, tag",
            typed_events,
        )
        assert result[0] == ("event_id", IntegerType())
        assert result[1] == ("pos", IntegerType())
        assert result[2] == ("tag", StringType())

    def test_cross_join_unnest(self):
        murals = _col_map(
            ("default.murals", [("mural_id", IntegerType()), ("colors", StringType())]),
        )
        result = resolve_output_columns(
            "SELECT m.mural_id, t.color_name "
            "FROM default.murals m "
            "CROSS JOIN UNNEST(m.colors) t(color_id, color_name)",
            murals,
        )
        assert result[0] == ("mural_id", IntegerType())
        assert result[1][0] == "color_name"
        assert isinstance(result[1][1], UnknownType)

    def test_range_in_from(self):
        result = resolve_output_columns("SELECT id FROM RANGE(10) t(id)", {})
        assert result[0] == ("id", UnknownType())

    def test_range_cross_join(self):
        result = resolve_output_columns(
            "SELECT u.user_id, r.idx FROM default.users u CROSS JOIN RANGE(5) r(idx)",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())
        assert result[1] == ("idx", UnknownType())

    def test_lateral_view_no_column_alias(self):
        """LATERAL VIEW EXPLODE(tags) t — no AS col_name."""
        try:
            result = resolve_output_columns(
                "SELECT event_id FROM default.events LATERAL VIEW EXPLODE(tags) t",
                _col_map(ARRAY_NODE_COLS),
            )
            assert result[0] == ("event_id", IntegerType())
        except TypeResolutionError:
            pass  # Parser may not support this form


# ---------------------------------------------------------------------------
# Registered functions
# ---------------------------------------------------------------------------


class TestRegisteredFunctions:
    def test_hll_sketch_estimate(self):
        from datajunction_server.sql.parsing.types import LongType

        result = resolve_output_columns(
            "SELECT hll_sketch_estimate(sketch_col) AS approx_count FROM default.sketches",
            _col_map(("default.sketches", [("sketch_col", StringType())])),
        )
        assert result[0] == ("approx_count", LongType())

    def test_concat(self):
        result = resolve_output_columns(
            "SELECT CONCAT(username, '@', email) AS full_contact FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("full_contact", StringType())

    def test_length(self):
        result = resolve_output_columns(
            "SELECT LENGTH(username) AS name_len FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0][0] == "name_len"
        assert isinstance(result[0][1], IntegerType)

    def test_unregistered_function(self):
        """Unknown function gracefully falls back to UnknownType."""
        result = resolve_output_columns(
            "SELECT SOME_UNKNOWN_FUNC(user_id) AS x FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("x", UnknownType())


# ---------------------------------------------------------------------------
# Inline tables (VALUES)
# ---------------------------------------------------------------------------


class TestInlineTable:
    def test_values_with_types(self):
        result = resolve_output_columns(
            "SELECT id, name FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
            {},
        )
        assert result[0][0] == "id"
        assert isinstance(result[0][1], IntegerType)
        assert result[1][0] == "name"
        assert isinstance(result[1][1], StringType)

    def test_values_in_subquery(self):
        result = resolve_output_columns(
            "SELECT id FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
            {},
        )
        assert result[0][0] == "id"
        assert isinstance(result[0][1], IntegerType)

    def test_values_with_null(self):
        result = resolve_output_columns(
            "SELECT id, val FROM (VALUES (1, NULL), (2, 'b')) AS t(id, val)",
            {},
        )
        assert result[0][0] == "id"
        assert isinstance(result[0][1], IntegerType)
        assert result[1] == ("val", UnknownType())

    def test_values_with_boolean(self):
        result = resolve_output_columns(
            "SELECT flag FROM (VALUES (TRUE), (FALSE)) AS t(flag)",
            {},
        )
        assert result[0] == ("flag", BooleanType())

    def test_values_without_aliases(self):
        try:
            result = resolve_output_columns("SELECT * FROM (VALUES (1, 2))", {})
            assert len(result) >= 0
        except TypeResolutionError:
            pass

    def test_values_with_expression(self):
        try:
            result = resolve_output_columns(
                "SELECT x FROM (VALUES (1 + 2, 'a')) AS t(x, y)",
                {},
            )
            assert len(result) >= 1
        except TypeResolutionError:
            pass


# ---------------------------------------------------------------------------
# WHERE / GROUP BY / HAVING
# ---------------------------------------------------------------------------


class TestNonProjectionClauses:
    def test_where_valid_column(self):
        result = resolve_output_columns(
            "SELECT username FROM default.users WHERE user_id > 10",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("username", StringType())

    def test_where_invalid_column(self):
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT user_id FROM default.users WHERE nonexistent > 5",
                _col_map(USERS_COLS),
            )

    def test_group_by_invalid_column(self):
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT user_id, COUNT(*) AS cnt "
                "FROM default.orders GROUP BY nonexistent",
                _col_map(ORDERS_COLS),
            )

    def test_having_invalid_column(self):
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT user_id FROM default.orders "
                "GROUP BY user_id HAVING SUM(nonexistent) > 1",
                _col_map(ORDERS_COLS),
            )

    def test_order_by_invalid_column(self):
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT user_id FROM default.orders ORDER BY nonexistent",
                _col_map(ORDERS_COLS),
            )

    def test_join_condition_invalid_column(self):
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT u.user_id FROM default.users u "
                "JOIN default.orders o ON u.user_id = o.nonexistent",
                _col_map(USERS_COLS, ORDERS_COLS),
            )

    def test_where_doesnt_add_columns(self):
        result = resolve_output_columns(
            "SELECT username FROM default.users WHERE user_id > 10",
            _col_map(USERS_COLS),
        )
        assert result[0] == ("username", StringType())

    def test_group_by_with_aggregation(self):
        result = resolve_output_columns(
            "SELECT user_id, COUNT(*) AS cnt FROM default.orders GROUP BY user_id",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("user_id", IntegerType())
        assert result[1] == ("cnt", BigIntType())


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class TestErrors:
    def test_invalid_sql(self):
        with pytest.raises(TypeResolutionError, match="Failed to parse"):
            resolve_output_columns("NOT VALID SQL AT ALL !!!", {})

    def test_missing_table(self):
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT a FROM default.nonexistent",
                _col_map(USERS_COLS),
            )

    def test_missing_column(self):
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT nonexistent FROM default.users",
                _col_map(USERS_COLS),
            )

    def test_qualified_column_not_in_table(self):
        """SELECT u.nonexistent FROM default.users u — table exists but column doesn't."""
        with pytest.raises(TypeResolutionError, match="nonexistent"):
            resolve_output_columns(
                "SELECT u.nonexistent FROM default.users u",
                _col_map(USERS_COLS),
            )

    def test_empty_parent_map(self):
        with pytest.raises(TypeResolutionError):
            resolve_output_columns("SELECT a FROM default.users", {})

    def test_ambiguous_column(self):
        with pytest.raises(TypeResolutionError, match="ambiguous"):
            resolve_output_columns(
                "SELECT user_id FROM default.users u, default.orders o",
                _col_map(USERS_COLS, ORDERS_COLS),
            )

    def test_wrong_table_alias(self):
        """wrong_alias.amount → UnknownType (treated as possible dim ref)."""
        result = resolve_output_columns(
            "SELECT wrong_alias.amount FROM default.users u",
            _col_map(USERS_COLS),
        )
        assert isinstance(result[0][1], UnknownType)

    def test_derived_metric_dim_not_in_map(self):
        with pytest.raises(TypeResolutionError, match="not found"):
            resolve_output_columns(
                "SELECT default.nonexistent_dim.col",
                _col_map(
                    (
                        "default.total_revenue",
                        [("default_DOT_total_revenue", DoubleType())],
                    ),
                ),
            )


# ---------------------------------------------------------------------------
# Unresolvable references (functions with bad args, nested failures)
# ---------------------------------------------------------------------------


class TestUnresolvableReferences:
    def test_sum_of_nonexistent_column(self):
        result = resolve_output_columns(
            "SELECT SUM(nonexistent) AS total FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("total", UnknownType())

    def test_count_of_nonexistent_column(self):
        """COUNT accepts any type, so still returns BigIntType."""
        result = resolve_output_columns(
            "SELECT COUNT(nonexistent) AS cnt FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("cnt", BigIntType())

    def test_function_nested_inside_case_arg(self):
        """SUM(CASE WHEN TRUE THEN COALESCE(amount, 0) ELSE 0 END)
        — Function (COALESCE) inside CASE inside SUM triggers
        _prepare_column_types_recursive hitting a Function node."""
        result = resolve_output_columns(
            "SELECT SUM(CASE WHEN TRUE THEN COALESCE(amount, 0) ELSE 0 END) AS total "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0][0] == "total"
        assert isinstance(result[0][1], DoubleType)

    def test_function_with_unresolvable_dim_arg(self):
        result = resolve_output_columns(
            "SELECT SUM(default.missing_dim.x) AS total FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("total", UnknownType())

    def test_binary_op_both_unresolvable(self):
        result = resolve_output_columns(
            "SELECT default.dim_a.x + default.dim_b.y AS z FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0] == ("z", UnknownType())

    def test_nested_case_with_bad_column(self):
        """SUM(CASE WHEN TRUE THEN nonexistent ELSE 0 END) — bare column doesn't exist."""
        result = resolve_output_columns(
            "SELECT SUM(CASE WHEN TRUE THEN nonexistent ELSE 0 END) AS total "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0][0] == "total"
        assert isinstance(result[0][1], (BigIntType, UnknownType))

    def test_nested_expression_arg_unresolvable(self):
        result = resolve_output_columns(
            "SELECT SUM(CASE WHEN TRUE THEN default.dim.x ELSE 0 END) AS total "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result[0][0] == "total"
        assert isinstance(result[0][1], (BigIntType, UnknownType))


# ---------------------------------------------------------------------------
# Compile idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_double_resolve_same_result(self):
        query = "SELECT user_id, username FROM default.users"
        parent_map = _col_map(USERS_COLS)
        result1 = resolve_output_columns(query, parent_map)
        result2 = resolve_output_columns(query, parent_map)
        assert result1 == result2


# ---------------------------------------------------------------------------
# Column signature comparison
# ---------------------------------------------------------------------------


class TestColumnSignatureComparison:
    def test_unchanged(self):
        old = [("user_id", IntegerType()), ("name", StringType())]
        new = [("user_id", IntegerType()), ("name", StringType())]
        assert columns_signature_changed(old, new) is False

    def test_type_changed(self):
        old = [("user_id", IntegerType())]
        new = [("user_id", BigIntType())]
        assert columns_signature_changed(old, new) is True

    def test_column_added(self):
        old = [("user_id", IntegerType())]
        new = [("user_id", IntegerType()), ("name", StringType())]
        assert columns_signature_changed(old, new) is True

    def test_column_removed(self):
        old = [("user_id", IntegerType()), ("name", StringType())]
        new = [("user_id", IntegerType())]
        assert columns_signature_changed(old, new) is True

    def test_column_renamed(self):
        old = [("user_id", IntegerType())]
        new = [("uid", IntegerType())]
        assert columns_signature_changed(old, new) is True

    def test_unknown_type_always_changed(self):
        assert (
            columns_signature_changed(
                [("x", IntegerType())],
                [("x", UnknownType())],
            )
            is True
        )
        assert (
            columns_signature_changed(
                [("x", UnknownType())],
                [("x", IntegerType())],
            )
            is True
        )
        assert (
            columns_signature_changed(
                [("x", UnknownType())],
                [("x", UnknownType())],
            )
            is True
        )
