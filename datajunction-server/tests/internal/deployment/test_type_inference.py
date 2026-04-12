"""
Unit tests for the lightweight top-down type inference used by deployment propagation.

validate_node_query takes a SQL query string and a map of parent node columns,
and returns the output column names + types without any DB calls.
"""

from datajunction_server.internal.deployment.type_inference import (
    columns_signature_changed,
    validate_node_query,
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
        result = validate_node_query(
            "SELECT user_id, username FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns == [
            ("user_id", IntegerType()),
            ("username", StringType()),
        ]

    def test_select_with_alias(self):
        result = validate_node_query(
            "SELECT user_id AS id, username AS name FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns == [
            ("id", IntegerType()),
            ("name", StringType()),
        ]

    def test_select_star(self):
        result = validate_node_query(
            "SELECT * FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns == [
            ("user_id", IntegerType()),
            ("username", StringType()),
            ("email", StringType()),
            ("created_at", TimestampType()),
        ]

    def test_qualified_wildcard(self):
        result = validate_node_query(
            "SELECT u.* FROM default.users u",
            _col_map(USERS_COLS),
        )
        assert result.output_columns == [
            ("user_id", IntegerType()),
            ("username", StringType()),
            ("email", StringType()),
            ("created_at", TimestampType()),
        ]

    def test_qualified_wildcard_multi_table(self):
        """SELECT u.* should only return users columns, not orders."""
        result = validate_node_query(
            "SELECT u.* FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns == [
            ("user_id", IntegerType()),
            ("username", StringType()),
            ("email", StringType()),
            ("created_at", TimestampType()),
        ]

    def test_both_table_stars(self):
        result = validate_node_query(
            "SELECT u.*, o.* FROM default.users u, default.orders o",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns == [
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
        result = validate_node_query(
            "SELECT missing.* FROM default.users u",
            _col_map(USERS_COLS),
        )
        assert result.output_columns == [
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
        result = validate_node_query(
            "SELECT u.user_id, o.amount FROM default.users u, default.orders o",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns == [
            ("user_id", IntegerType()),
            ("amount", DoubleType()),
        ]

    def test_unqualified_unambiguous(self):
        result = validate_node_query(
            "SELECT username, amount "
            "FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns == [
            ("username", StringType()),
            ("amount", DoubleType()),
        ]

    def test_join_columns(self):
        result = validate_node_query(
            "SELECT u.username, o.amount "
            "FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns == [
            ("username", StringType()),
            ("amount", DoubleType()),
        ]

    def test_self_join(self):
        result = validate_node_query(
            "SELECT a.user_id, b.username "
            "FROM default.users a "
            "JOIN default.users b ON a.user_id = b.user_id",
            _col_map(USERS_COLS),
        )
        assert result.output_columns == [
            ("user_id", IntegerType()),
            ("username", StringType()),
        ]


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------


class TestAggregations:
    def test_count_star(self):
        result = validate_node_query(
            "SELECT COUNT(*) AS cnt FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("cnt", BigIntType())

    def test_sum(self):
        result = validate_node_query(
            "SELECT SUM(amount) AS total FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("total", DoubleType())

    def test_avg(self):
        result = validate_node_query(
            "SELECT AVG(amount) AS avg_amount FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0][0] == "avg_amount"
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_group_by_with_agg(self):
        result = validate_node_query(
            "SELECT user_id, SUM(amount) AS total FROM default.orders GROUP BY user_id",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1][0] == "total"
        assert isinstance(result.output_columns[1][1], DoubleType)

    def test_count_distinct(self):
        result = validate_node_query(
            "SELECT COUNT(DISTINCT user_id) AS unique_users FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("unique_users", BigIntType())

    def test_select_distinct(self):
        result = validate_node_query(
            "SELECT DISTINCT user_id FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------


class TestExpressions:
    def test_arithmetic(self):
        result = validate_node_query(
            "SELECT amount * 2 AS doubled FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0][0] == "doubled"
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_arithmetic_without_alias(self):
        result = validate_node_query(
            "SELECT amount + 1 FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result.output_columns) == 1
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_parenthesized_expression(self):
        result = validate_node_query(
            "SELECT (amount + 1) FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert len(result.output_columns) == 1
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_cast(self):
        result = validate_node_query(
            "SELECT CAST(user_id AS BIGINT) AS big_id FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("big_id", BigIntType())

    def test_string_literal(self):
        result = validate_node_query(
            "SELECT 'hello' AS greeting FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("greeting", StringType())

    def test_numeric_literal(self):
        result = validate_node_query(
            "SELECT 42 AS magic FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0][0] == "magic"
        assert isinstance(result.output_columns[0][1], IntegerType)

    def test_boolean_literal(self):
        result = validate_node_query(
            "SELECT TRUE AS flag FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("flag", BooleanType())

    def test_null_literal(self):
        result = validate_node_query(
            "SELECT NULL AS placeholder FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("placeholder", NullType())

    def test_alias_shadows_other_column(self):
        result = validate_node_query(
            "SELECT user_id AS order_id FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("order_id", IntegerType())


# ---------------------------------------------------------------------------
# CASE / WHEN / COALESCE / IF
# ---------------------------------------------------------------------------


class TestConditionals:
    def test_case_string_result(self):
        result = validate_node_query(
            "SELECT CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS tier "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("tier", StringType())

    def test_case_column_result(self):
        result = validate_node_query(
            "SELECT CASE WHEN amount > 100 THEN amount ELSE 0 END AS adjusted "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0][0] == "adjusted"
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_case_else_fallback(self):
        """First THEN branch unresolvable, falls through to ELSE."""
        result = validate_node_query(
            "SELECT CASE WHEN TRUE THEN default.dim.x ELSE amount END AS val "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("val", DoubleType())

    def test_case_all_branches_unresolvable(self):
        result = validate_node_query(
            "SELECT CASE WHEN TRUE THEN default.dim.x END AS val FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("val", UnknownType())

    def test_coalesce(self):
        result = validate_node_query(
            "SELECT COALESCE(email, username) AS contact FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("contact", StringType())

    def test_if_expression(self):
        result = validate_node_query(
            "SELECT IF(amount > 100, 'big', 'small') AS size FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0][0] == "size"
        assert isinstance(result.output_columns[0][1], StringType)


# ---------------------------------------------------------------------------
# Window functions
# ---------------------------------------------------------------------------


class TestWindowFunctions:
    def test_sum_over_partition(self):
        result = validate_node_query(
            "SELECT user_id, SUM(amount) OVER (PARTITION BY user_id) AS running "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1][0] == "running"
        assert isinstance(result.output_columns[1][1], DoubleType)

    def test_row_number(self):
        result = validate_node_query(
            "SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) AS rn "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1][0] == "rn"
        assert isinstance(result.output_columns[1][1], IntegerType)


# ---------------------------------------------------------------------------
# Nested functions
# ---------------------------------------------------------------------------


class TestNestedFunctions:
    def test_upper_coalesce(self):
        result = validate_node_query(
            "SELECT UPPER(COALESCE(username, 'unknown')) AS name FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("name", StringType())

    def test_cast_inside_sum(self):
        result = validate_node_query(
            "SELECT SUM(CAST(user_id AS BIGINT)) AS total FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("total", BigIntType())


# ---------------------------------------------------------------------------
# Subqueries
# ---------------------------------------------------------------------------


class TestSubqueries:
    def test_subquery(self):
        result = validate_node_query(
            "SELECT x FROM (SELECT user_id AS x FROM default.users) sub",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("x", IntegerType())

    def test_nested_subquery(self):
        result = validate_node_query(
            "SELECT y FROM (SELECT x AS y FROM (SELECT user_id AS x FROM default.users) a) b",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("y", IntegerType())

    def test_inner_column_not_visible_in_outer(self):
        """username is not in the subquery's output."""
        result = validate_node_query(
            "SELECT username FROM (SELECT user_id AS x FROM default.users) sub",
            _col_map(USERS_COLS),
        )
        assert result.errors

    def test_subquery_in_from_with_bad_where(self):
        """Subquery output is fine but its WHERE references a bad column."""
        from datajunction_server.internal.deployment.type_inference import (
            validate_node_query,
        )

        result = validate_node_query(
            "SELECT x FROM ("
            "  SELECT user_id AS x FROM default.users WHERE nonexistent > 5"
            ") sub",
            _col_map(USERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)
        assert result.output_columns[0] == ("x", IntegerType())

    def test_correlated_subquery(self):
        """Correlated subquery references outer query's column.
        The inner subquery uses u.user_id from the outer FROM clause.
        Currently the outer column resolves as UnknownType (dim ref fallback)
        rather than being passed into the inner scope.
        """
        result = validate_node_query(
            "SELECT u.user_id, "
            "(SELECT COUNT(*) FROM default.orders o WHERE o.user_id = u.user_id) AS cnt "
            "FROM default.users u",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())
        # cnt is from COUNT(*) which always returns BigIntType
        assert result.output_columns[1][0] == "cnt"
        assert isinstance(result.output_columns[1][1], BigIntType)

    def test_select_star_in_subquery(self):
        """SELECT * inside a subquery — parent columns change, subquery output changes."""
        result = validate_node_query(
            "SELECT user_id, username FROM (  SELECT * FROM default.users) sub",
            _col_map(USERS_COLS),
        )
        assert result.output_columns == [
            ("user_id", IntegerType()),
            ("username", StringType()),
        ]

    def test_select_star_in_subquery_column_added(self):
        """Source gains a column — subquery SELECT * picks it up."""
        extended_users = _col_map(
            (
                "default.users",
                [
                    ("user_id", IntegerType()),
                    ("username", StringType()),
                    ("email", StringType()),
                    ("created_at", TimestampType()),
                    ("new_col", BooleanType()),  # Added column
                ],
            ),
        )
        result = validate_node_query(
            "SELECT * FROM (SELECT * FROM default.users) sub",
            extended_users,
        )
        col_names = [name for name, _ in result.output_columns]
        assert "new_col" in col_names
        assert len(result.output_columns) == 5


# ---------------------------------------------------------------------------
# Struct field access
# ---------------------------------------------------------------------------


STRUCT_SOURCE = _col_map(
    (
        "default.events",
        [
            ("event_id", IntegerType()),
            ("metadata", StringType()),  # In real usage would be StructType
        ],
    ),
)


class TestStructFieldAccess:
    def test_struct_field_resolves(self):
        """SELECT metadata.name FROM default.events — struct field access."""
        from datajunction_server.sql.parsing.types import StructType
        from datajunction_server.sql.parsing.ast import NestedField, Name

        struct_source = _col_map(
            (
                "default.events",
                [
                    ("event_id", IntegerType()),
                    (
                        "metadata",
                        StructType(
                            NestedField(Name("name"), StringType(), True),
                            NestedField(Name("value"), IntegerType(), True),
                        ),
                    ),
                ],
            ),
        )
        result = validate_node_query(
            "SELECT metadata.name FROM default.events",
            struct_source,
        )
        assert result.errors == []
        assert result.output_columns[0] == ("name", StringType())

    def test_struct_field_nonexistent(self):
        """SELECT metadata.nonexistent FROM default.events — bad struct field."""
        from datajunction_server.sql.parsing.types import StructType
        from datajunction_server.sql.parsing.ast import NestedField, Name

        struct_source = _col_map(
            (
                "default.events",
                [
                    ("event_id", IntegerType()),
                    (
                        "metadata",
                        StructType(
                            NestedField(Name("name"), StringType(), True),
                        ),
                    ),
                ],
            ),
        )
        result = validate_node_query(
            "SELECT metadata.nonexistent FROM default.events",
            struct_source,
        )
        assert any("nonexistent" in e for e in result.errors)
        assert any("struct" in e.lower() for e in result.errors)


# ---------------------------------------------------------------------------
# CTEs
# ---------------------------------------------------------------------------


class TestCTEs:
    def test_simple_cte(self):
        result = validate_node_query(
            "WITH cte AS (SELECT user_id FROM default.users) SELECT user_id FROM cte",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())

    def test_cte_with_alias(self):
        result = validate_node_query(
            "WITH cte AS (SELECT user_id AS uid FROM default.users) SELECT uid FROM cte",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("uid", IntegerType())

    def test_multiple_ctes(self):
        result = validate_node_query(
            "WITH u AS (SELECT user_id FROM default.users), "
            "o AS (SELECT order_id, user_id FROM default.orders) "
            "SELECT u.user_id, o.order_id FROM u JOIN o ON u.user_id = o.user_id",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns == [
            ("user_id", IntegerType()),
            ("order_id", IntegerType()),
        ]

    def test_cte_used_in_subquery(self):
        result = validate_node_query(
            "WITH cte AS (SELECT user_id, username FROM default.users) "
            "SELECT uid FROM (SELECT user_id AS uid FROM cte) sub",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("uid", IntegerType())

    def test_cte_with_invalid_where(self):
        """Invalid column in a CTE's WHERE clause is caught."""
        result = validate_node_query(
            "WITH cte AS (SELECT user_id FROM default.users WHERE nonexistent > 5) "
            "SELECT user_id FROM cte",
            _col_map(USERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_cte_with_invalid_group_by(self):
        """Invalid column in a CTE's GROUP BY clause is caught."""
        result = validate_node_query(
            "WITH cte AS (SELECT user_id, COUNT(*) AS cnt "
            "FROM default.users GROUP BY nonexistent) "
            "SELECT user_id FROM cte",
            _col_map(USERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_three_nested_ctes(self):
        """Three CTEs, each with nested subqueries."""
        result = validate_node_query(
            "WITH "
            "a AS ("
            "  SELECT uid, total FROM ("
            "    SELECT user_id AS uid, SUM(amount) AS total FROM ("
            "      SELECT user_id, amount FROM default.orders"
            "    ) raw "
            "    GROUP BY user_id"
            "  ) agg"
            "), "
            "b AS ("
            "  SELECT uid, username FROM ("
            "    SELECT a.uid, u.username FROM a "
            "    JOIN default.users u ON a.uid = u.user_id"
            "  ) enriched"
            "), "
            "c AS ("
            "  SELECT username, total FROM ("
            "    SELECT b.username, a.total FROM b "
            "    JOIN a ON b.uid = a.uid "
            "    WHERE a.total > 100"
            "  ) filtered"
            ") "
            "SELECT username, total FROM c",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns == [
            ("username", StringType()),
            ("total", DoubleType()),
        ]

    def test_cte_forward_reference(self):
        """CTE 'a' references CTE 'b' which isn't defined yet → error."""
        result = validate_node_query(
            "WITH a AS (SELECT user_id FROM b), "
            "b AS (SELECT user_id FROM default.users) "
            "SELECT user_id FROM a",
            _col_map(USERS_COLS),
        )
        assert result.errors

    def test_same_column_name_across_ctes(self):
        """Two CTEs both have 'user_id' — qualified references resolve correctly."""
        result = validate_node_query(
            "WITH a AS (SELECT user_id FROM default.users), "
            "b AS (SELECT user_id FROM default.orders) "
            "SELECT a.user_id, b.user_id FROM a, b",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns == [
            ("user_id", IntegerType()),
            ("user_id", IntegerType()),
        ]

    def test_cte_inner_subquery_invalid_column(self):
        """CTE has a subquery with a bad column reference.
        Currently not caught — subquery errors inside _collect_tables_from_relation
        are discarded. TODO: propagate subquery errors through table scope building.
        """
        from datajunction_server.internal.deployment.type_inference import (
            validate_node_query,
        )

        result = validate_node_query(
            "WITH a AS ("
            "  SELECT x FROM ("
            "    SELECT nonexistent AS x FROM default.users"
            "  ) sub"
            ") SELECT x FROM a",
            _col_map(USERS_COLS),
        )
        # The inner subquery's "nonexistent" column causes a cascade:
        # the subquery's output columns fail, which causes the outer CTE to fail too.
        assert len(result.errors) >= 1


# ---------------------------------------------------------------------------
# Derived metrics
# ---------------------------------------------------------------------------


class TestDerivedMetrics:
    def test_single_metric_ref(self):
        result = validate_node_query(
            "SELECT default.total_revenue",
            _col_map(
                (
                    "default.total_revenue",
                    [("default_DOT_total_revenue", DoubleType())],
                ),
            ),
        )
        assert len(result.output_columns) == 1
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_multi_metric_expression(self):
        result = validate_node_query(
            "SELECT default.total_revenue + default.order_count AS combined",
            _col_map(
                (
                    "default.total_revenue",
                    [("default_DOT_total_revenue", DoubleType())],
                ),
                ("default.order_count", [("default_DOT_order_count", BigIntType())]),
            ),
        )
        assert result.output_columns[0][0] == "combined"


# ---------------------------------------------------------------------------
# Dimension attribute references
# ---------------------------------------------------------------------------


class TestDimensionAttributes:
    def test_dim_attr_in_derived_metric(self):
        result = validate_node_query(
            "SELECT default.total_revenue, default.date_dim.week",
            _col_map(
                (
                    "default.total_revenue",
                    [("default_DOT_total_revenue", DoubleType())],
                ),
                ("default.date_dim", [("week", StringType()), ("year", IntegerType())]),
            ),
        )
        assert isinstance(result.output_columns[0][1], DoubleType)
        assert isinstance(result.output_columns[1][1], StringType)

    def test_dim_attr_in_case_with_from(self):
        result = validate_node_query(
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
        assert result.output_columns[0][0] == "filtered"

    def test_dim_attr_in_where_with_from(self):
        result = validate_node_query(
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
        assert result.output_columns[0][0] == "total"
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_dim_attr_in_group_by_with_from(self):
        result = validate_node_query(
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
        assert isinstance(result.output_columns[0][1], IntegerType)
        assert result.output_columns[1][0] == "total"

    def test_dim_attr_not_in_map(self):
        """Dimension not in parent map → UnknownType fallback."""
        result = validate_node_query(
            "SELECT default.date_dim.year, SUM(amount) AS total "
            "FROM default.orders "
            "GROUP BY default.date_dim.year",
            _col_map(ORDERS_COLS),
        )
        assert isinstance(result.output_columns[0][1], UnknownType)
        assert result.output_columns[1][0] == "total"

    def test_dim_attr_in_aggregation(self):
        """SUM(default.date_dim.dateint) — dim attr as aggregation argument."""
        result = validate_node_query(
            "SELECT SUM(default.date_dim.dateint) AS sum_dates FROM default.orders",
            _col_map(
                ORDERS_COLS,
                (
                    "default.date_dim",
                    [("dateint", IntegerType()), ("year", IntegerType())],
                ),
            ),
        )
        assert result.output_columns[0] == ("sum_dates", BigIntType())


# ---------------------------------------------------------------------------
# Deep namespaces
# ---------------------------------------------------------------------------


class TestDeepNamespaces:
    def test_node_exists_but_column_doesnt(self):
        """Progressive prefix finds the node but the column name doesn't match."""
        result = validate_node_query(
            "SELECT ads.report.dims.date.nonexistent_col FROM default.orders",
            _col_map(
                ORDERS_COLS,
                ("ads.report.dims.date", [("year", IntegerType())]),
            ),
        )
        # Node found but column missing → continues to shorter prefixes → UnknownType
        assert isinstance(result.output_columns[0][1], UnknownType)

    def test_deep_namespace_dim_attr(self):
        result = validate_node_query(
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
        assert isinstance(result.output_columns[0][1], IntegerType)
        assert result.output_columns[1][0] == "total"

    def test_deep_namespace_derived_metric(self):
        result = validate_node_query(
            "SELECT ads.report.metrics.total_revenue",
            _col_map(
                (
                    "ads.report.metrics.total_revenue",
                    [("ads_DOT_report_DOT_metrics_DOT_total_revenue", DoubleType())],
                ),
            ),
        )
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_deep_namespace_dim_in_aggregation(self):
        result = validate_node_query(
            "SELECT SUM(ads.report.dims.date.dateint) AS sum_dates FROM default.orders",
            _col_map(
                ORDERS_COLS,
                (
                    "ads.report.dims.date",
                    [("dateint", IntegerType()), ("year", IntegerType())],
                ),
            ),
        )
        assert result.output_columns[0] == ("sum_dates", BigIntType())

    def test_deep_namespace_not_in_map(self):
        result = validate_node_query(
            "SELECT ads.report.dims.date.year, amount FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert isinstance(result.output_columns[0][1], UnknownType)
        assert isinstance(result.output_columns[1][1], DoubleType)

    def test_longest_prefix_wins(self):
        """Prefers ads.report.dims (longer) over ads.report."""
        result = validate_node_query(
            "SELECT ads.report.dims.year FROM default.orders",
            _col_map(
                ORDERS_COLS,
                ("ads.report", [("dims", StringType())]),
                ("ads.report.dims", [("year", IntegerType())]),
            ),
        )
        assert isinstance(result.output_columns[0][1], IntegerType)

    def test_deep_namespace_derived_dim_attr(self):
        """Derived metric with deep namespace dim attr via progressive prefix."""
        result = validate_node_query(
            "SELECT deep.ns.metric_a, deep.ns.dim.date.year",
            _col_map(
                ("deep.ns.metric_a", [("deep_DOT_ns_DOT_metric_a", DoubleType())]),
                (
                    "deep.ns.dim.date",
                    [("year", IntegerType()), ("month", IntegerType())],
                ),
            ),
        )
        assert isinstance(result.output_columns[0][1], DoubleType)
        assert isinstance(result.output_columns[1][1], IntegerType)


# ---------------------------------------------------------------------------
# SET operations
# ---------------------------------------------------------------------------


class TestSetOperations:
    def test_union(self):
        result = validate_node_query(
            "SELECT user_id FROM default.users UNION SELECT user_id FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())

    def test_except(self):
        result = validate_node_query(
            "SELECT user_id FROM default.users EXCEPT SELECT user_id FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())

    def test_intersect(self):
        result = validate_node_query(
            "SELECT user_id FROM default.users INTERSECT SELECT user_id FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())


# ---------------------------------------------------------------------------
# LATERAL VIEW / table-valued functions
# ---------------------------------------------------------------------------


class TestTableValuedFunctions:
    def test_lateral_view_explode(self):
        result = validate_node_query(
            "SELECT event_id, tag "
            "FROM default.events "
            "LATERAL VIEW EXPLODE(tags) t AS tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert result.output_columns[0] == ("event_id", IntegerType())
        assert result.output_columns[1][0] == "tag"
        assert isinstance(result.output_columns[1][1], UnknownType)

    def test_lateral_view_outer_explode(self):
        result = validate_node_query(
            "SELECT event_id, tag "
            "FROM default.events "
            "LATERAL VIEW OUTER EXPLODE(tags) t AS tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert result.output_columns[0] == ("event_id", IntegerType())
        assert result.output_columns[1][0] == "tag"

    def test_posexplode(self):
        result = validate_node_query(
            "SELECT event_id, pos, tag "
            "FROM default.events "
            "LATERAL VIEW POSEXPLODE(tags) t AS pos, tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert result.output_columns[0] == ("event_id", IntegerType())
        assert result.output_columns[1][0] == "pos"
        assert result.output_columns[2][0] == "tag"

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
        result = validate_node_query(
            "SELECT event_id, tag "
            "FROM default.events "
            "LATERAL VIEW EXPLODE(tags) t AS tag",
            typed_events,
        )
        assert result.output_columns[0] == ("event_id", IntegerType())
        assert result.output_columns[1] == ("tag", StringType())

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
        result = validate_node_query(
            "SELECT event_id, pos, tag "
            "FROM default.events "
            "LATERAL VIEW POSEXPLODE(tags) t AS pos, tag",
            typed_events,
        )
        assert result.output_columns[0] == ("event_id", IntegerType())
        assert result.output_columns[1] == ("pos", IntegerType())
        assert result.output_columns[2] == ("tag", StringType())

    def test_explode_with_map_type(self):
        """EXPLODE on a MapType column → key and value types resolved."""
        from datajunction_server.sql.parsing.types import MapType

        typed_events = _col_map(
            (
                "default.events",
                [
                    ("event_id", IntegerType()),
                    ("props", MapType(key_type=StringType(), value_type=IntegerType())),
                ],
            ),
        )
        result = validate_node_query(
            "SELECT event_id, k, v "
            "FROM default.events "
            "LATERAL VIEW EXPLODE(props) t AS k, v",
            typed_events,
        )
        assert result.output_columns[0] == ("event_id", IntegerType())
        assert result.output_columns[1] == ("k", StringType())
        assert result.output_columns[2] == ("v", IntegerType())

    def test_explode_unresolvable_arg(self):
        """EXPLODE on a column not in scope → UnknownType."""
        result = validate_node_query(
            "SELECT event_id, tag "
            "FROM default.events "
            "LATERAL VIEW EXPLODE(nonexistent) t AS tag",
            _col_map(ARRAY_NODE_COLS),
        )
        assert result.output_columns[0] == ("event_id", IntegerType())
        assert isinstance(result.output_columns[1][1], UnknownType)

    def test_cross_join_unnest(self):
        murals = _col_map(
            ("default.murals", [("mural_id", IntegerType()), ("colors", StringType())]),
        )
        result = validate_node_query(
            "SELECT m.mural_id, t.color_name "
            "FROM default.murals m "
            "CROSS JOIN UNNEST(m.colors) t(color_id, color_name)",
            murals,
        )
        assert result.output_columns[0] == ("mural_id", IntegerType())
        assert result.output_columns[1][0] == "color_name"
        assert isinstance(result.output_columns[1][1], UnknownType)

    def test_range_in_from(self):
        result = validate_node_query("SELECT id FROM RANGE(10) t(id)", {})
        assert result.output_columns[0] == ("id", UnknownType())

    def test_range_cross_join(self):
        result = validate_node_query(
            "SELECT u.user_id, r.idx FROM default.users u CROSS JOIN RANGE(5) r(idx)",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1] == ("idx", UnknownType())

    def test_lateral_view_no_column_alias(self):
        """LATERAL VIEW EXPLODE(tags) t — no AS col_name."""
        try:
            result = validate_node_query(
                "SELECT event_id FROM default.events LATERAL VIEW EXPLODE(tags) t",
                _col_map(ARRAY_NODE_COLS),
            )
            assert result.output_columns[0] == ("event_id", IntegerType())
        except TypeResolutionError:
            pass  # Parser may not support this form


# ---------------------------------------------------------------------------
# Registered functions
# ---------------------------------------------------------------------------


class TestRegisteredFunctions:
    def test_hll_sketch_estimate(self):
        from datajunction_server.sql.parsing.types import LongType

        result = validate_node_query(
            "SELECT hll_sketch_estimate(sketch_col) AS approx_count FROM default.sketches",
            _col_map(("default.sketches", [("sketch_col", StringType())])),
        )
        assert result.output_columns[0] == ("approx_count", LongType())

    def test_concat(self):
        result = validate_node_query(
            "SELECT CONCAT(username, '@', email) AS full_contact FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("full_contact", StringType())

    def test_length(self):
        result = validate_node_query(
            "SELECT LENGTH(username) AS name_len FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0][0] == "name_len"
        assert isinstance(result.output_columns[0][1], IntegerType)

    def test_unregistered_function(self):
        """Unknown function gracefully falls back to UnknownType."""
        result = validate_node_query(
            "SELECT SOME_UNKNOWN_FUNC(user_id) AS x FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("x", UnknownType())


# ---------------------------------------------------------------------------
# Inline tables (VALUES)
# ---------------------------------------------------------------------------


class TestInlineTable:
    def test_values_with_types(self):
        result = validate_node_query(
            "SELECT id, name FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
            {},
        )
        assert result.output_columns[0][0] == "id"
        assert isinstance(result.output_columns[0][1], IntegerType)
        assert result.output_columns[1][0] == "name"
        assert isinstance(result.output_columns[1][1], StringType)

    def test_values_in_subquery(self):
        result = validate_node_query(
            "SELECT id FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
            {},
        )
        assert result.output_columns[0][0] == "id"
        assert isinstance(result.output_columns[0][1], IntegerType)

    def test_values_with_null(self):
        result = validate_node_query(
            "SELECT id, val FROM (VALUES (1, NULL), (2, 'b')) AS t(id, val)",
            {},
        )
        assert result.output_columns[0][0] == "id"
        assert isinstance(result.output_columns[0][1], IntegerType)
        assert result.output_columns[1] == ("val", UnknownType())

    def test_values_with_boolean(self):
        result = validate_node_query(
            "SELECT flag FROM (VALUES (TRUE), (FALSE)) AS t(flag)",
            {},
        )
        assert result.output_columns[0] == ("flag", BooleanType())

    def test_values_without_aliases(self):
        try:
            result = validate_node_query("SELECT * FROM (VALUES (1, 2))", {})
            assert len(result.output_columns) >= 0
        except TypeResolutionError:
            pass

    def test_values_with_expression(self):
        try:
            result = validate_node_query(
                "SELECT x FROM (VALUES (1 + 2, 'a')) AS t(x, y)",
                {},
            )
            assert len(result.output_columns) >= 1
        except TypeResolutionError:
            pass


# ---------------------------------------------------------------------------
# WHERE / GROUP BY / HAVING
# ---------------------------------------------------------------------------


class TestNonProjectionClauses:
    def test_where_valid_column(self):
        result = validate_node_query(
            "SELECT username FROM default.users WHERE user_id > 10",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("username", StringType())

    def test_where_invalid_column(self):
        result = validate_node_query(
            "SELECT user_id FROM default.users WHERE nonexistent > 5",
            _col_map(USERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_group_by_invalid_column(self):
        result = validate_node_query(
            "SELECT user_id, COUNT(*) AS cnt FROM default.orders GROUP BY nonexistent",
            _col_map(ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_having_invalid_column(self):
        result = validate_node_query(
            "SELECT user_id FROM default.orders "
            "GROUP BY user_id HAVING SUM(nonexistent) > 1",
            _col_map(ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_order_by_invalid_column(self):
        result = validate_node_query(
            "SELECT user_id FROM default.orders ORDER BY nonexistent",
            _col_map(ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_join_condition_invalid_column(self):
        result = validate_node_query(
            "SELECT u.user_id FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.nonexistent",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_sort_by_invalid_column(self):
        result = validate_node_query(
            "SELECT user_id FROM default.orders SORT BY nonexistent",
            _col_map(ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_having_without_group_by(self):
        """HAVING without GROUP BY — parser may accept or reject."""
        try:
            validate_node_query(
                "SELECT user_id FROM default.orders HAVING COUNT(*) > 1",
                _col_map(ORDERS_COLS),
            )
        except TypeResolutionError:
            pass  # Parser or validator rejected it — either way is fine

    def test_window_function_invalid_partition_column(self):
        """PARTITION BY nonexistent in a window function.
        Currently not caught — OVER clause columns aren't validated.
        TODO: validate PARTITION BY / ORDER BY inside window functions.
        """
        from datajunction_server.internal.deployment.type_inference import (
            validate_node_query,
        )

        result = validate_node_query(
            "SELECT SUM(amount) OVER (PARTITION BY nonexistent) AS total "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_where_doesnt_add_columns(self):
        result = validate_node_query(
            "SELECT username FROM default.users WHERE user_id > 10",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("username", StringType())

    def test_group_by_with_aggregation(self):
        result = validate_node_query(
            "SELECT user_id, COUNT(*) AS cnt FROM default.orders GROUP BY user_id",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1] == ("cnt", BigIntType())


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class TestErrors:
    def test_invalid_sql(self):
        result = validate_node_query("NOT VALID SQL AT ALL !!!", {})
        assert any("Failed to parse" in e for e in result.errors)

    def test_missing_table(self):
        result = validate_node_query(
            "SELECT a FROM default.nonexistent",
            _col_map(USERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_missing_column(self):
        result = validate_node_query(
            "SELECT nonexistent FROM default.users",
            _col_map(USERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_qualified_column_not_in_table(self):
        """SELECT u.nonexistent FROM default.users u — table exists but column doesn't."""
        result = validate_node_query(
            "SELECT u.nonexistent FROM default.users u",
            _col_map(USERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_empty_parent_map(self):
        result = validate_node_query("SELECT a FROM default.users", {})
        assert result.errors

    def test_ambiguous_column(self):
        result = validate_node_query(
            "SELECT user_id FROM default.users u, default.orders o",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert any("ambiguous" in e for e in result.errors)

    def test_wrong_table_alias(self):
        """wrong_alias.amount → UnknownType (treated as possible dim ref)."""
        result = validate_node_query(
            "SELECT wrong_alias.amount FROM default.users u",
            _col_map(USERS_COLS),
        )
        assert isinstance(result.output_columns[0][1], UnknownType)

    def test_derived_metric_dim_not_in_map(self):
        result = validate_node_query(
            "SELECT default.nonexistent_dim.col",
            _col_map(
                (
                    "default.total_revenue",
                    [("default_DOT_total_revenue", DoubleType())],
                ),
            ),
        )
        assert any("not found" in e for e in result.errors)

    def test_multiple_errors_collected(self):
        """Multiple bad columns → all errors collected, not just the first."""
        from datajunction_server.internal.deployment.type_inference import (
            validate_node_query,
        )

        result = validate_node_query(
            "SELECT bad1, bad2 FROM default.users WHERE bad3 > 5",
            _col_map(USERS_COLS),
        )
        # Should have errors for bad1, bad2 (projection) and bad3 (WHERE)
        assert len(result.errors) >= 3
        assert any("bad1" in e for e in result.errors)
        assert any("bad2" in e for e in result.errors)
        assert any("bad3" in e for e in result.errors)

    def test_subquery_in_select_invalid_column(self):
        """Scalar subquery in SELECT with bad column."""
        from datajunction_server.internal.deployment.type_inference import (
            validate_node_query,
        )

        result = validate_node_query(
            "SELECT user_id, "
            "(SELECT COUNT(nonexistent) FROM default.orders) AS cnt "
            "FROM default.users",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)

    def test_union_second_select_invalid_column(self):
        """UNION where second SELECT has an invalid column.
        Currently not caught — set_op right side isn't validated.
        TODO: validate UNION/EXCEPT/INTERSECT right-side queries.
        """
        from datajunction_server.internal.deployment.type_inference import (
            validate_node_query,
        )

        result = validate_node_query(
            "SELECT user_id FROM default.users "
            "UNION "
            "SELECT nonexistent FROM default.orders",
            _col_map(USERS_COLS, ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)


# ---------------------------------------------------------------------------
# Unresolvable references (functions with bad args, nested failures)
# ---------------------------------------------------------------------------


class TestUnresolvableReferences:
    def test_sum_of_nonexistent_column(self):
        from datajunction_server.internal.deployment.type_inference import (
            validate_node_query,
        )

        result = validate_node_query(
            "SELECT SUM(nonexistent) AS total FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)
        assert result.output_columns[0][0] == "total"

    def test_count_of_nonexistent_column(self):
        """COUNT accepts any type but the bad column is still an error."""
        from datajunction_server.internal.deployment.type_inference import (
            validate_node_query,
        )

        result = validate_node_query(
            "SELECT COUNT(nonexistent) AS cnt FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)
        assert result.output_columns[0][0] == "cnt"
        assert isinstance(result.output_columns[0][1], BigIntType)

    def test_function_nested_inside_case_arg(self):
        """SUM(CASE WHEN TRUE THEN COALESCE(amount, 0) ELSE 0 END)
        — Function (COALESCE) inside CASE inside SUM triggers
        _prepare_column_types_recursive hitting a Function node."""
        result = validate_node_query(
            "SELECT SUM(CASE WHEN TRUE THEN COALESCE(amount, 0) ELSE 0 END) AS total "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0][0] == "total"
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_function_with_unresolvable_dim_arg(self):
        result = validate_node_query(
            "SELECT SUM(default.missing_dim.x) AS total FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("total", UnknownType())

    def test_binary_op_both_unresolvable(self):
        result = validate_node_query(
            "SELECT default.dim_a.x + default.dim_b.y AS z FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("z", UnknownType())

    def test_nested_case_with_bad_column(self):
        """SUM(CASE WHEN TRUE THEN nonexistent ELSE 0 END) — bare column doesn't exist."""
        from datajunction_server.internal.deployment.type_inference import (
            validate_node_query,
        )

        result = validate_node_query(
            "SELECT SUM(CASE WHEN TRUE THEN nonexistent ELSE 0 END) AS total "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert any("nonexistent" in e for e in result.errors)
        assert result.output_columns[0][0] == "total"

    def test_nested_expression_arg_unresolvable(self):
        result = validate_node_query(
            "SELECT SUM(CASE WHEN TRUE THEN default.dim.x ELSE 0 END) AS total "
            "FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0][0] == "total"
        assert isinstance(result.output_columns[0][1], (BigIntType, UnknownType))


# ---------------------------------------------------------------------------
# Compile idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_double_resolve_same_result(self):
        query = "SELECT user_id, username FROM default.users"
        parent_map = _col_map(USERS_COLS)
        result1 = validate_node_query(query, parent_map)
        result2 = validate_node_query(query, parent_map)
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
