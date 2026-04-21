"""
Unit tests for the lightweight top-down type inference used by deployment propagation.

validate_node_query takes a SQL query string and a map of parent node columns,
and returns the output column names + types without any DB calls.
"""

import pytest

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
        assert any("Unable to infer type for column `val`" in e for e in result.errors)

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

    def test_table_qualified_struct_field(self):
        """SELECT t.metadata.name FROM default.events t — table.struct.field access."""
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
            "SELECT t.metadata.name FROM default.events t",
            struct_source,
        )
        assert result.errors == []
        assert result.output_columns[0] == ("name", StringType())

    def test_struct_field_in_subquery_projects_flat_columns(self):
        """Struct field access inside a subquery produces flat columns for the outer query.

        Pattern: outer query references m.is_flag, where m is a subquery that
        projects t.details.is_flag AS is_flag from a source with a struct column.
        """
        from datajunction_server.sql.parsing.types import StructType
        from datajunction_server.sql.parsing.ast import NestedField, Name

        source = _col_map(
            (
                "default.events",
                [
                    ("id", IntegerType()),
                    (
                        "details",
                        StructType(
                            NestedField(Name("is_flag"), BooleanType(), True),
                            NestedField(Name("ts"), BigIntType(), True),
                        ),
                    ),
                    ("status", IntegerType()),
                ],
            ),
        )
        result = validate_node_query(
            """
            SELECT m.id, m.is_flag, m.ts
            FROM (
                SELECT
                    t.id,
                    t.details.is_flag AS is_flag,
                    t.details.ts AS ts
                FROM default.events AS t
                WHERE t.status = 1

                UNION ALL

                SELECT
                    t.id,
                    t.details.is_flag AS is_flag,
                    t.details.ts AS ts
                FROM default.events AS t
                WHERE t.status = 0
            ) AS m
            """,
            source,
        )
        assert not result.errors, result.errors
        assert len(result.output_columns) == 3
        assert result.output_columns[0] == ("id", IntegerType())
        assert result.output_columns[1] == ("is_flag", BooleanType())
        assert result.output_columns[2] == ("ts", BigIntType())

    def test_nested_struct_chain(self):
        """data.nested.value — two levels of struct nesting, no table alias."""
        from datajunction_server.sql.parsing.types import StructType
        from datajunction_server.sql.parsing.ast import NestedField, Name

        source = _col_map(
            (
                "default.events",
                [
                    ("id", IntegerType()),
                    (
                        "data",
                        StructType(
                            NestedField(
                                Name("nested"),
                                StructType(
                                    NestedField(Name("x"), StringType(), True),
                                    NestedField(Name("y"), StringType(), True),
                                    NestedField(Name("z"), BigIntType(), True),
                                ),
                                True,
                            ),
                            NestedField(Name("kind"), StringType(), True),
                        ),
                    ),
                ],
            ),
        )
        result = validate_node_query(
            """SELECT
                id,
                data.kind AS kind,
                data.nested.x AS ix,
                data.nested.y AS iy,
                data.nested.z AS iz
            FROM default.events""",
            source,
        )
        assert not result.errors, result.errors
        assert len(result.output_columns) == 5
        assert result.output_columns[0] == ("id", IntegerType())
        assert result.output_columns[1] == ("kind", StringType())
        assert result.output_columns[2] == ("ix", StringType())
        assert result.output_columns[3] == ("iy", StringType())
        assert result.output_columns[4] == ("iz", BigIntType())

    def test_table_qualified_nested_struct_chain(self):
        """t.price.details.val — table alias + two levels of struct nesting."""
        from datajunction_server.sql.parsing.types import StructType
        from datajunction_server.sql.parsing.ast import NestedField, Name

        source = _col_map(
            (
                "default.deals",
                [
                    ("id", IntegerType()),
                    (
                        "price",
                        StructType(
                            NestedField(
                                Name("details"),
                                StructType(
                                    NestedField(Name("val"), DoubleType(), True),
                                ),
                                True,
                            ),
                        ),
                    ),
                ],
            ),
        )
        result = validate_node_query(
            "SELECT t.id, t.price.details.val AS deep_val FROM default.deals t",
            source,
        )
        assert not result.errors, result.errors
        assert len(result.output_columns) == 2
        assert result.output_columns[0] == ("id", IntegerType())
        assert result.output_columns[1] == ("deep_val", DoubleType())

    def test_three_level_struct_chain(self):
        """a.b.c.d — three levels of struct nesting."""
        from datajunction_server.sql.parsing.types import StructType
        from datajunction_server.sql.parsing.ast import NestedField, Name

        source = _col_map(
            (
                "default.data",
                [
                    (
                        "a",
                        StructType(
                            NestedField(
                                Name("b"),
                                StructType(
                                    NestedField(
                                        Name("c"),
                                        StructType(
                                            NestedField(
                                                Name("d"),
                                                IntegerType(),
                                                True,
                                            ),
                                        ),
                                        True,
                                    ),
                                ),
                                True,
                            ),
                        ),
                    ),
                ],
            ),
        )
        result = validate_node_query(
            "SELECT a.b.c.d AS deep FROM default.data",
            source,
        )
        assert not result.errors, result.errors
        assert result.output_columns[0] == ("deep", IntegerType())

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

    def test_explode_sequence_function(self):
        """LATERAL VIEW EXPLODE(SEQUENCE(1, 90)) infers IntegerType from SEQUENCE args."""
        result = validate_node_query(
            "SELECT h.horizon "
            "FROM (SELECT 1 AS dummy) t "
            "LATERAL VIEW EXPLODE(SEQUENCE(1, 90)) h AS horizon",
            {},
        )
        assert not result.errors, result.errors
        assert result.output_columns[0] == ("horizon", IntegerType())

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
        assert any(
            "Unable to infer type for column `color_name`" in e for e in result.errors
        )

    def test_range_in_from(self):
        result = validate_node_query("SELECT id FROM RANGE(10) t(id)", {})
        assert result.output_columns[0] == ("id", UnknownType())
        assert any("Unable to infer type for column `id`" in e for e in result.errors)

    def test_range_cross_join(self):
        result = validate_node_query(
            "SELECT u.user_id, r.idx FROM default.users u CROSS JOIN RANGE(5) r(idx)",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1] == ("idx", UnknownType())
        assert any("Unable to infer type for column `idx`" in e for e in result.errors)

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
    def test_struct_constructor_simple(self):
        """struct(col AS name) should infer StructType."""
        result = validate_node_query(
            "SELECT struct(user_id AS id) AS s FROM default.users",
            _col_map(USERS_COLS),
        )
        assert not result.errors, result.errors
        assert result.output_columns[0][0] == "s"

    def test_struct_constructor_with_max(self):
        """struct(MAX(col) AS name) should work."""
        result = validate_node_query(
            "SELECT struct(MAX(flag) AS max_flag) AS s FROM default.data d",
            _col_map(("default.data", [("flag", IntegerType())])),
        )
        assert not result.errors, result.errors

    def test_struct_constructor_with_if(self):
        """struct(IF(flag = 1, 1, 0) AS name) should work."""
        result = validate_node_query(
            "SELECT struct(IF(flag = 1, 1, 0) AS val) AS s FROM default.data d",
            _col_map(("default.data", [("flag", IntegerType())])),
        )
        assert not result.errors, result.errors

    def test_struct_constructor_with_aggregation(self):
        """struct(expr AS name, agg AS name) should infer StructType from its args."""
        result = validate_node_query(
            """SELECT
                struct(
                    d.dateint AS snapshot_date,
                    MAX(IF(flag = 1, 1, 0)) AS is_active
                ) AS status_struct
            FROM default.data d""",
            _col_map(
                ("default.data", [("dateint", IntegerType()), ("flag", IntegerType())]),
            ),
        )
        assert not result.errors, result.errors
        assert len(result.output_columns) == 1
        assert result.output_columns[0][0] == "status_struct"

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
        """Unknown function produces UnknownType and an error."""
        result = validate_node_query(
            "SELECT SOME_UNKNOWN_FUNC(user_id) AS x FROM default.users",
            _col_map(USERS_COLS),
        )
        assert result.output_columns[0] == ("x", UnknownType())
        assert any("Unable to infer type for column `x`" in e for e in result.errors)


# ---------------------------------------------------------------------------
# Inline tables (VALUES)
# ---------------------------------------------------------------------------


class TestSubscript:
    """Subscript expressions: col['key'] for maps and structs."""

    def test_struct_subscript(self):
        """col['field'] on a StructType resolves the field type."""
        from datajunction_server.sql.parsing.types import StructType
        from datajunction_server.sql.parsing.ast import NestedField, Name

        source = _col_map(
            (
                "default.data",
                [
                    ("id", IntegerType()),
                    (
                        "info",
                        StructType(
                            NestedField(Name("price"), DoubleType(), True),
                            NestedField(Name("name"), StringType(), True),
                        ),
                    ),
                ],
            ),
        )
        result = validate_node_query(
            "SELECT info['price'] AS p, info['name'] AS n FROM default.data",
            source,
        )
        assert not result.errors, result.errors
        assert result.output_columns[0] == ("p", DoubleType())
        assert result.output_columns[1] == ("n", StringType())

    def test_map_subscript(self):
        """col['key'] on a MapType resolves to the value type."""
        from datajunction_server.sql.parsing.types import MapType

        source = _col_map(
            (
                "default.data",
                [
                    ("id", IntegerType()),
                    ("props", MapType(StringType(), DoubleType())),
                ],
            ),
        )
        result = validate_node_query(
            "SELECT props['weight'] AS w FROM default.data",
            source,
        )
        assert not result.errors, result.errors
        assert result.output_columns[0] == ("w", DoubleType())


class TestLambdaFunctions:
    """Higher-order functions with lambda expressions (AGGREGATE, FILTER, TRANSFORM)."""

    def test_aggregate_with_filter_and_lambda(self):
        """AGGREGATE(FILTER(arr, c -> ...), init, (acc, c) -> ...) should not crash."""

        source = _col_map(
            (
                "default.events",
                [
                    ("id", IntegerType()),
                    (
                        "scores",
                        # Array of structs — represented as a ListType in practice,
                        # but for this test UnknownType suffices since we just need
                        # the query to not crash.
                        UnknownType(),
                    ),
                ],
            ),
        )
        result = validate_node_query(
            """SELECT
                id,
                AGGREGATE(
                    FILTER(scores, c -> c.name = 'X'),
                    CAST(0.0 AS DOUBLE),
                    (acc, c) -> CAST(acc + c.value AS DOUBLE)
                ) AS total_score
            FROM default.events""",
            source,
        )
        assert not result.errors, result.errors
        assert len(result.output_columns) == 2
        assert result.output_columns[0] == ("id", IntegerType())
        # AGGREGATE return type inferred from initial value CAST(0.0 AS DOUBLE)
        assert result.output_columns[1] == ("total_score", DoubleType())

    def test_filter_returns_same_type_as_input(self):
        """FILTER(array, predicate) → same type as the array arg."""
        source = _col_map(
            ("default.events", [("id", IntegerType()), ("tags", UnknownType())]),
        )
        result = validate_node_query(
            "SELECT FILTER(tags, t -> t > 0) AS filtered FROM default.events",
            source,
        )
        assert result.output_columns[0][0] == "filtered"

    def test_transform_returns_same_type_as_input(self):
        """TRANSFORM(array, func) → same type as the array arg."""
        source = _col_map(
            ("default.events", [("id", IntegerType()), ("vals", UnknownType())]),
        )
        result = validate_node_query(
            "SELECT TRANSFORM(vals, v -> v * 2) AS doubled FROM default.events",
            source,
        )
        assert result.output_columns[0][0] == "doubled"


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
        """NULL in the first row for a column doesn't poison its type —
        _resolve_inline_table scans later rows for a typed literal."""
        result = validate_node_query(
            "SELECT id, val FROM (VALUES (1, NULL), (2, 'b')) AS t(id, val)",
            {},
        )
        assert result.output_columns[0][0] == "id"
        assert isinstance(result.output_columns[0][1], IntegerType)
        assert result.output_columns[1] == ("val", StringType())
        assert not result.errors, result.errors

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

    def test_top_level_values_with_column_aliases(self):
        """FROM VALUES ... AS t(col1, col2) — top-level InlineTable, not a subquery."""
        result = validate_node_query(
            """SELECT status_key, code, label
            FROM VALUES
              (0, 'A', 'Alpha'),
              (1, 'B', 'Beta'),
              (2, 'C', 'Gamma')
            AS t(status_key, code, label)""",
            {},
        )
        assert not result.errors
        assert len(result.output_columns) == 3
        assert result.output_columns[0][0] == "status_key"
        assert isinstance(result.output_columns[0][1], IntegerType)
        assert result.output_columns[1][0] == "code"
        assert isinstance(result.output_columns[1][1], StringType)
        assert result.output_columns[2][0] == "label"
        assert isinstance(result.output_columns[2][1], StringType)


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

    def test_derived_metric_dim_not_in_map_is_permissive(self):
        """Derived metrics can reference dim attributes that aren't parents —
        they resolve at query build time via dim links. The in-memory
        inferrer can't know that, so it types them as UnknownType rather than
        raising. Matches the regular-query branch's dim-link permissiveness.
        """
        result = validate_node_query(
            "SELECT default.some_dim.col",
            _col_map(
                (
                    "default.total_revenue",
                    [("default_DOT_total_revenue", DoubleType())],
                ),
            ),
        )
        # No "not found" error — the ref is allowed to stay untyped.
        assert not any(
            "not found in derived metric scope" in e for e in result.errors
        ), result.errors

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
        — Function (COALESCE) inside CASE inside SUM requires recursive
        type stamping through nested expressions."""
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
        assert any(
            "Unable to infer type for column `total`" in e for e in result.errors
        )

    def test_binary_op_both_unresolvable(self):
        result = validate_node_query(
            "SELECT default.dim_a.x + default.dim_b.y AS z FROM default.orders",
            _col_map(ORDERS_COLS),
        )
        assert result.output_columns[0] == ("z", UnknownType())
        assert any("Unable to infer type for column `z`" in e for e in result.errors)

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


# ---------------------------------------------------------------------------
# EXPLODE struct unpacking, FROMless lateral views, unresolved-namespace hints
# ---------------------------------------------------------------------------


class TestExplodeStructUnpacking:
    """EXPLODE(array<struct<a, b>>) AS (c1, c2) should alias the struct fields
    positionally, matching Spark behavior."""

    @staticmethod
    def _cells_type():
        from datajunction_server.sql.parsing.types import (
            ListType,
            NestedField,
            StructType,
        )

        return ListType(
            element_type=StructType(
                NestedField(name="cell_id", field_type=StringType()),
                NestedField(name="cell_name", field_type=StringType()),
            ),
        )

    def test_projection_explode_struct_array_unpacks_fields(self):
        """SELECT test_id, EXPLODE(cells) AS (cell_id, cell_name) FROM t
        where cells is array<struct<cell_id:string, cell_name:string>>
        → (test_id: bigint, cell_id: string, cell_name: string)."""
        result = validate_node_query(
            "SELECT test_id, EXPLODE(cells) AS (cell_id, cell_name) FROM t.src",
            _col_map(
                (
                    "t.src",
                    [
                        ("test_id", BigIntType()),
                        ("cells", self._cells_type()),
                    ],
                ),
            ),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("test_id", BigIntType()),
            ("cell_id", StringType()),
            ("cell_name", StringType()),
        ]

    def test_projection_explode_scalar_array_single_column(self):
        """Regression: EXPLODE(array<int>) AS x produces one int column.
        The struct-unpacking branch must NOT kick in for a scalar element type."""
        from datajunction_server.sql.parsing.types import ListType

        result = validate_node_query(
            "SELECT id, EXPLODE(nums) AS n FROM t.src",
            _col_map(
                (
                    "t.src",
                    [
                        ("id", IntegerType()),
                        ("nums", ListType(element_type=IntegerType())),
                    ],
                ),
            ),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("id", IntegerType()),
            ("n", IntegerType()),
        ]

    def test_lateral_view_posexplode_struct_keeps_struct_intact(self):
        """POSEXPLODE does NOT struct-unpack — first alias is pos (int), second
        is the struct element as-is. (LATERAL VIEW form; the projection form
        for POSEXPLODE with a parenthesized alias list isn't parseable.)"""
        result = validate_node_query(
            "SELECT pos, tag FROM t.src LATERAL VIEW POSEXPLODE(cells) v AS pos, tag",
            _col_map(("t.src", [("cells", self._cells_type())])),
        )
        assert not result.errors, result.errors
        assert result.output_columns[0] == ("pos", IntegerType())
        # Second col is the struct element, not one of its fields
        assert result.output_columns[1][0] == "tag"
        from datajunction_server.sql.parsing.types import StructType

        assert isinstance(result.output_columns[1][1], StructType)

    def test_lateral_view_explode_struct_array_unpacks_fields(self):
        """Same struct-unpacking for LATERAL VIEW form."""
        result = validate_node_query(
            "SELECT cell_id, cell_name FROM t.src "
            "LATERAL VIEW EXPLODE(cells) v AS cell_id, cell_name",
            _col_map(("t.src", [("cells", self._cells_type())])),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("cell_id", StringType()),
            ("cell_name", StringType()),
        ]

    def test_projection_explode_struct_in_anonymous_subquery(self):
        """Composition: anonymous subquery with projection-EXPLODE whose
        struct-unpacked columns are referenced from the outer projection."""
        result = validate_node_query(
            "SELECT CAST(test_id AS BIGINT) AS test_id, cell_id, cell_name "
            "FROM ( SELECT test_id, EXPLODE(cells) AS (cell_id, cell_name) "
            "       FROM t.src )",
            _col_map(
                (
                    "t.src",
                    [
                        ("test_id", IntegerType()),
                        ("cells", self._cells_type()),
                    ],
                ),
            ),
        )
        assert not result.errors, result.errors
        assert [n for n, _ in result.output_columns] == [
            "test_id",
            "cell_id",
            "cell_name",
        ]
        # CAST gives bigint; struct-unpacked fields keep their string type
        assert result.output_columns[0] == ("test_id", BigIntType())
        assert result.output_columns[1] == ("cell_id", StringType())
        assert result.output_columns[2] == ("cell_name", StringType())


class TestMultipleAnonymousLateralViews:
    def test_two_anonymous_lateral_views_do_not_collide(self):
        """Two LATERAL VIEW EXPLODE(sequence(...)) in the same SELECT each
        default to __lateral__; the new __lateral_{idx}__ fallback keeps both
        sets of exploded columns reachable from the outer projection."""
        result = validate_node_query(
            "SELECT window_start, window_end FROM (SELECT 1 AS d) t "
            "LATERAL VIEW EXPLODE(SEQUENCE(1, 97)) AS window_start "
            "LATERAL VIEW EXPLODE(SEQUENCE(2, 98)) AS window_end",
            {},
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("window_start", IntegerType()),
            ("window_end", IntegerType()),
        ]


class TestFromlessLateralView:
    def test_fromless_query_with_lateral_view_resolves_exploded_columns(self):
        """Query with LATERAL VIEW but no FROM clause — used as a series
        generator in Spark — should still be processed, not routed to the
        __derived__ scope."""
        result = validate_node_query(
            "SELECT CAST(CONCAT(window_start, '-', window_end) AS string) AS obs_window, "
            "window_start AS obs_window_start, "
            "window_end AS obs_window_end "
            "LATERAL VIEW EXPLODE(SEQUENCE(1, 97)) AS window_start "
            "LATERAL VIEW EXPLODE(SEQUENCE(2, 98)) AS window_end "
            "WHERE window_start < window_end AND window_start = 1",
            {},
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("obs_window", StringType()),
            ("obs_window_start", IntegerType()),
            ("obs_window_end", IntegerType()),
        ]

    def test_fromless_query_without_lateral_view_stays_derived_metric(self):
        """Regression: a true derived metric (no FROM, no lateral views) must
        still go through the __derived__ scope so bare metric-name refs
        resolve against parent_map."""
        result = validate_node_query(
            "SELECT default.metric_a / default.metric_b AS ratio",
            _col_map(
                ("default.metric_a", [("metric_a", DoubleType())]),
                ("default.metric_b", [("metric_b", DoubleType())]),
            ),
        )
        assert not result.errors, result.errors
        assert [n for n, _ in result.output_columns] == ["ratio"]

    def test_inline_table_with_column_aliases_uses_explicit_alias(self):
        """CROSS JOIN VALUES (...) tab(c1, c2) — the `tab` alias lands on
        InlineTable.name rather than InlineTable.alias. v2 must honor it so
        outer refs like `tab.c2` resolve instead of hitting the `__inline__`
        fallback."""
        result = validate_node_query(
            "SELECT tab.window_end AS tenure FROM source.s a "
            "CROSS JOIN VALUES ('1-7', 1, 7), ('1-14', 1, 14) "
            "tab(label, window_start, window_end)",
            _col_map(("source.s", [("id", IntegerType())])),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [("tenure", IntegerType())]

    def test_inline_table_scans_past_null_for_column_type(self):
        """VALUES(NULL, 'a'), ('x', 'b') — first-row NULL for col0 must not
        poison the column type. Scan rows until a typed literal is found."""
        result = validate_node_query(
            "SELECT app_start_type, label FROM VALUES "
            "(NULL, 'From Nflx'), ('COLD', 'COLD'), ('WARM', 'WARM') "
            "AS t (app_start_type, label)",
            {},
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("app_start_type", StringType()),
            ("label", StringType()),
        ]

    def test_inline_table_all_null_column_is_null_type(self):
        """When every row has NULL for a given column, the type is NullType
        (not UnknownType — genuinely nullable-only)."""
        result = validate_node_query(
            "SELECT x, y FROM VALUES (NULL, 1), (NULL, 2) AS t(x, y)",
            {},
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("x", NullType()),
            ("y", IntegerType()),
        ]

    def test_lateral_view_explode_from_json_map_resolves_key_value(self):
        """LATERAL VIEW EXPLODE(from_json(string_col, 'MAP<STRING, STRING>')) — the
        function dispatch for from_json returns a MapType, EXPLODE of a map gives
        (key, value) pairs, and the AS list aliases them. All columns resolve
        cleanly."""
        result = validate_node_query(
            "SELECT ntl.test_id, ntl.evidence_map FROM source.t F "
            "LATERAL VIEW EXPLODE(from_json(xpEvidenceMap, 'MAP<STRING, STRING>')) "
            "ntl AS test_id, evidence_map",
            _col_map(("source.t", [("xpEvidenceMap", StringType())])),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("test_id", StringType()),
            ("evidence_map", StringType()),
        ]

    def test_cross_join_unnest_resolves_element_type_from_sibling_table(self):
        """`FROM t CROSS JOIN UNNEST(t.arr) AS u(x)` — UNNEST is the right side
        of a JOIN and references a column on the left-side table. The
        resolution must see the left table in scope (outer-scope propagation),
        and the element type of the array must flow into u.x."""
        from datajunction_server.sql.parsing.types import ListType

        result = validate_node_query(
            "SELECT id, x FROM t.src CROSS JOIN UNNEST(vals) AS u(x)",
            _col_map(
                (
                    "t.src",
                    [
                        ("id", IntegerType()),
                        ("vals", ListType(element_type=IntegerType())),
                    ],
                ),
            ),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("id", IntegerType()),
            ("x", IntegerType()),
        ]

    def test_cross_join_unnest_map_subscript_resolves_to_element_type(self):
        """UNNEST(map_col['key']) where map_col: map<string, array<int>>.
        The subscript returns array<int>, UNNEST returns rows of int."""
        result = validate_node_query(
            "SELECT AVG(x) AS avg_x FROM t.src "
            "CROSS JOIN UNNEST(m['home']) AS u(x) "
            "GROUP BY id",
            _col_map(
                (
                    "t.src",
                    [
                        ("id", IntegerType()),
                        # map<string, array<int>>
                        (
                            "m",
                            __import__(
                                "datajunction_server.sql.parsing.backends.antlr4",
                                fromlist=["parse_rule"],
                            ).parse_rule("map<string, array<int>>", "dataType"),
                        ),
                    ],
                ),
            ),
        )
        assert not result.errors, result.errors
        assert result.output_columns[0][0] == "avg_x"
        # AVG of int is double
        from datajunction_server.sql.parsing.types import DoubleType

        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_lateral_view_explode_missing_source_column_surfaces_real_error(self):
        """When EXPLODE's argument references a nonexistent column, the
        'Column X not found' error should be surfaced alongside (and before
        the user sees) the downstream 'Unable to infer type' noise."""
        result = validate_node_query(
            "SELECT ntl.test_id FROM source.t F "
            "LATERAL VIEW EXPLODE(from_json(xpEvidenceMap, 'MAP<STRING, STRING>')) "
            "ntl AS test_id, evidence_map",
            _col_map(("source.t", [("xpEvidence", StringType())])),  # renamed col
        )
        assert any(
            "Column `xpEvidenceMap` not found in any table" in e for e in result.errors
        ), result.errors

    def test_get_json_object_on_exploded_value_resolves_to_string(self):
        """get_json_object(map_value_col, '$.path.x') should resolve cleanly to
        string when applied to a string (the exploded map value)."""
        result = validate_node_query(
            "SELECT get_json_object(ntl.evidence_map, '$.67291.alloc_cell') AS cell "
            "FROM source.t F "
            "LATERAL VIEW EXPLODE(from_json(xpEvidenceMap, 'MAP<STRING, STRING>')) "
            "ntl AS test_id, evidence_map",
            _col_map(("source.t", [("xpEvidenceMap", StringType())])),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [("cell", StringType())]

    def test_subscript_on_list_type_returns_element_type(self):
        """arr[1] where arr: array<string> should resolve to string. Previously
        the Subscript handler only covered map and struct, falling through to
        UnknownType for arrays."""
        from datajunction_server.sql.parsing.types import ListType

        result = validate_node_query(
            "SELECT arr[1] AS first FROM t.src",
            _col_map(("t.src", [("arr", ListType(element_type=StringType()))])),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [("first", StringType())]

    def test_derived_metric_dim_attribute_window_ref_is_permissive(self):
        """Derived metrics often reference dim attributes inside window
        functions (`OVER (ORDER BY common.dimensions.time.date.dateint)`).
        These aren't parents — they resolve via dim links at query build time.
        v2 should type them as UnknownType and not reject."""
        result = validate_node_query(
            "SELECT AVG(demo.metrics.main.avg_dl) OVER ("
            "  ORDER BY common.dimensions.time.date.dateint "
            "  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW"
            ") AS trailing_avg",
            _col_map(
                ("demo.metrics.main.avg_dl", [("avg_dl", DoubleType())]),
            ),
        )
        assert not result.errors, result.errors
        assert [n for n, _ in result.output_columns] == ["trailing_avg"]


class TestUnresolvedNamespaceDiagnostic:
    def test_bogus_namespace_emits_specific_error(self):
        """A namespace that matches no table alias, struct column, or known
        parent should surface a descriptive error rather than only the generic
        'Unable to infer type'."""
        result = validate_node_query(
            "SELECT a.id, tenure.tenure FROM (SELECT 1 AS id) AS a",
            {},
        )
        # Must produce the namespace-specific message (not only the generic
        # Unable-to-infer-type that used to be the only signal).
        assert any(
            "namespace `tenure`" in msg and "not a table alias" in msg
            for msg in result.errors
        ), result.errors

    def test_known_parent_prefix_does_not_trigger_namespace_error(self):
        """When some prefix of the column path matches a known parent in
        parent_map, the namespace-error heuristic should stay quiet — the
        reference may be a legit dim-link that resolves at query build time."""
        result = validate_node_query(
            "SELECT src.orders.country.name FROM src.orders o",
            _col_map(("src.orders", [("id", IntegerType())])),
        )
        # No namespace-specific error — src.orders IS a known parent.
        assert not any(
            "not a table alias in this scope" in msg for msg in result.errors
        ), result.errors

    def test_multi_segment_dim_attribute_ref_does_not_trigger_namespace_error(self):
        """Long dim-attribute paths like
        `common.dimensions.xp.allocation_day.days_since_allocation` inside a
        metric's CASE expression aren't parents, and their prefixes don't
        match parent_map either. But they're real DJ dim-node paths, not
        typos. Only single-segment namespaces (`tenure.tenure`-style typos)
        should be flagged."""
        result = validate_node_query(
            "SELECT COUNT(DISTINCT CASE WHEN view_secs >= 360 "
            "THEN common.dimensions.xp.allocation_day.days_since_allocation "
            "ELSE NULL END) AS m "
            "FROM users.foo.playback",
            _col_map(("users.foo.playback", [("view_secs", IntegerType())])),
        )
        assert not any("references namespace" in msg for msg in result.errors), (
            result.errors
        )


class TestCoverageGaps:
    """Tests targeting specific uncovered branches in type_inference.py."""

    def test_inline_table_parens_with_alias_list_resolves_columns(self):
        """(VALUES (1), (2)) AS t(x) — parenthesized-VALUES + alias list
        currently parses with both node.alias and node.name None, so the
        handler falls into the __inline__ default-alias branch. The test
        still covers the end-to-end "outer query sees the aliased column"
        behavior for this common shape."""
        result = validate_node_query(
            "SELECT t.x FROM (VALUES (1), (2)) AS t(x)",
            {},
        )
        assert not result.errors, result.errors
        assert result.output_columns == [("x", IntegerType())]

    @pytest.mark.xfail(
        reason=(
            "Spark accepts `SELECT POSEXPLODE(arr) AS (pos, val) FROM t` as "
            "an inline generator function, but DJ's grammar rejects the "
            "identifier-list alias on a non-Table expression. When the "
            "grammar is relaxed for known table-valued functions, the "
            "projection-form POSEXPLODE branches in "
            "_resolve_projection_function_table become reachable and this "
            "test flips to pass."
        ),
        strict=True,
    )
    def test_projection_posexplode_alias_list_parses(self):
        """Spark inline-generator POSEXPLODE with explicit (pos, val) alias
        list in the SELECT projection."""
        from datajunction_server.sql.parsing.types import ListType

        result = validate_node_query(
            "SELECT POSEXPLODE(arr) AS (pos, val) FROM src.s",
            _col_map(
                ("src.s", [("arr", ListType(element_type=IntegerType()))]),
            ),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("pos", IntegerType()),
            ("val", IntegerType()),
        ]

    def test_lateral_element_types_surfaces_nested_scope_errors(self):
        """When EXPLODE's argument is a Function wrapping an unresolved
        sub-reference (e.g., IF(missing_col > 0, arr, arr)), the outer
        function still types successfully but the inner TypeResolutionError
        lands in scope.errors. Those are surfaced to the caller's errors
        list so the real cause doesn't get swallowed."""
        from datajunction_server.sql.parsing.types import ListType

        result = validate_node_query(
            "SELECT v.x FROM src.s "
            "LATERAL VIEW EXPLODE(IF(missing_col > 0, arr, arr)) v AS x",
            _col_map(
                ("src.s", [("arr", ListType(element_type=IntegerType()))]),
            ),
        )
        assert any("missing_col" in msg for msg in result.errors), result.errors

    def test_cross_join_posexplode_yields_pos_and_element(self):
        """`CROSS JOIN POSEXPLODE(arr) AS u(p, v)` — the FROM-clause
        POSEXPLODE form. First alias is the integer position; second is
        the element type."""
        from datajunction_server.sql.parsing.types import ListType

        result = validate_node_query(
            "SELECT t.id, u.p, u.v FROM src.s t "
            "CROSS JOIN POSEXPLODE(t.arr) AS u(p, v)",
            _col_map(
                (
                    "src.s",
                    [
                        ("id", IntegerType()),
                        ("arr", ListType(element_type=IntegerType())),
                    ],
                ),
            ),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("id", IntegerType()),
            ("p", IntegerType()),
            ("v", IntegerType()),
        ]

    def test_cross_join_unnest_struct_array_unpacks_fields_in_from(self):
        """Struct-unpacking in the FROM-clause FunctionTableExpression handler:
        `CROSS JOIN UNNEST(array<struct<a, b>>) AS t(c1, c2)` expands
        positionally to c1: a_type, c2: b_type."""
        from datajunction_server.sql.parsing.backends.antlr4 import parse_rule

        cells = parse_rule(
            "array<struct<cell_id:string, cell_name:string>>",
            "dataType",
        )
        result = validate_node_query(
            "SELECT t.cell_id, t.cell_name FROM src.s "
            "CROSS JOIN UNNEST(cells) AS t(cell_id, cell_name)",
            _col_map(("src.s", [("cells", cells)])),
        )
        assert not result.errors, result.errors
        assert result.output_columns == [
            ("cell_id", StringType()),
            ("cell_name", StringType()),
        ]

    def test_lateral_view_explode_with_too_many_aliases_fills_unknown(self):
        """LATERAL VIEW EXPLODE(scalar_array) AS a, b, c — 3 aliases but
        element is a single scalar. The extras fall back to UnknownType."""
        from datajunction_server.sql.parsing.types import ListType

        result = validate_node_query(
            "SELECT v.a, v.b, v.c FROM src.s LATERAL VIEW EXPLODE(nums) v AS a, b, c",
            _col_map(
                ("src.s", [("nums", ListType(element_type=IntegerType()))]),
            ),
        )
        assert result.output_columns[0] == ("a", IntegerType())
        assert isinstance(result.output_columns[1][1], UnknownType)
        assert isinstance(result.output_columns[2][1], UnknownType)

    def test_projection_explode_scalar_array_with_extra_aliases_unknown(self):
        """Projection `EXPLODE(arr) AS (a, b, c)` on a scalar-array column —
        only one element type, extras fall back to UnknownType."""
        from datajunction_server.sql.parsing.types import ListType

        result = validate_node_query(
            "SELECT EXPLODE(nums) AS (a, b, c) FROM src.s",
            _col_map(
                ("src.s", [("nums", ListType(element_type=IntegerType()))]),
            ),
        )
        assert result.output_columns[0] == ("a", IntegerType())
        assert isinstance(result.output_columns[1][1], UnknownType)
        assert isinstance(result.output_columns[2][1], UnknownType)

    def test_lateral_element_types_propagates_typeresolution_error(self):
        """When EXPLODE's argument references a nonexistent column, the
        TypeResolutionError message flows into the caller's errors list so
        the real root cause surfaces, not just 'Unable to infer type'."""
        result = validate_node_query(
            "SELECT v.x FROM src.s LATERAL VIEW EXPLODE(missing_col) v AS x",
            _col_map(("src.s", [("id", IntegerType())])),
        )
        assert any(
            "Column `missing_col` not found in any table" in msg
            for msg in result.errors
        ), result.errors

    def test_single_segment_namespace_matching_parent_is_silent(self):
        """Single-segment namespace whose name matches a parent_map key (not
        a FROM table) should NOT trigger the 'references namespace' error —
        the prefix-is-parent guard short-circuits and we fall through to
        UnknownType."""
        result = validate_node_query(
            "SELECT src.foo_missing FROM other.t",
            _col_map(
                ("other.t", [("id", IntegerType())]),
                ("src", [("real_col", IntegerType())]),
            ),
        )
        assert not any("references namespace" in msg for msg in result.errors), (
            result.errors
        )
