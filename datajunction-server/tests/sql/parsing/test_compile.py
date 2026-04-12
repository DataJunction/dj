"""
Unit tests for Query.compile and related AST compilation.

These tests mock the DB layer by pre-populating CompileContext.dependencies_cache
with fake Node objects, so they run fast without hitting Postgres.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datajunction_server.errors import DJException, ErrorCode
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import (
    BigIntType,
    DateType,
    DoubleType,
    IntegerType,
    ListType,
    StringType,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Helpers to build mock Node / NodeRevision / Column objects
# ---------------------------------------------------------------------------


class FakeColumn:
    """Lightweight stand-in for database.column.Column."""

    def __init__(self, name: str, col_type, **kwargs):
        self.name = name
        self.type = col_type
        self.attributes: list = []
        self.dimension = None
        self.dimension_id = None
        self.dimension_column = None
        self.partition = None
        for k, v in kwargs.items():
            setattr(self, k, v)


class FakeNodeRevision:
    """Lightweight stand-in for database.node.NodeRevision."""

    def __init__(self, name, node_type, columns, query="", node=None):
        self.name = name
        self.type = node_type
        self.query = query
        self.status = "valid"
        self.node = node
        self.columns = columns
        self.dimension_links = []
        self.parents = []
        self.materializations = []
        self.availability = []
        # Column.compile checks isinstance(dj_node, NodeRevision) and if not,
        # accesses dj_node.current. We set .current = self so both paths work.
        self.current = self


class FakeNode:
    """Lightweight stand-in for database.node.Node."""

    def __init__(self, name, node_type, current=None):
        self.name = name
        self.type = node_type
        self.current = current


def _make_db_column(name: str, col_type, **kwargs):
    """Create a FakeColumn with the given name and type."""
    return FakeColumn(name, col_type, **kwargs)


def _make_node(
    name: str,
    node_type: NodeType,
    columns: list[tuple[str, object]],
    query: str = "",
):
    """
    Create a fake Node with a current NodeRevision containing the given columns.

    Returns a FakeNode that can be placed in dependencies_cache.
    Table.compile expects cache entries to have .current → revision with .columns.
    """
    db_columns = [FakeColumn(col_name, col_type) for col_name, col_type in columns]
    node = FakeNode(name, node_type)
    revision = FakeNodeRevision(name, node_type, db_columns, query=query, node=node)
    node.current = revision
    return node


def _make_ctx(
    dependencies: dict | None = None,
    column_overrides: dict | None = None,
) -> ast.CompileContext:
    """Create a CompileContext with a mock async session and pre-populated cache."""
    session = AsyncMock()
    session.refresh = AsyncMock()

    # Mock session.execute() so that get_by_names / get_by_name don't crash.
    # The batch load path in Query.compile calls Node.get_by_names which does
    # await session.execute(query) → result.unique().scalars().all()
    # Note: unique() and scalars() are SYNC methods on the result object.
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_unique = MagicMock()
    mock_unique.scalars.return_value = mock_scalars
    mock_unique.scalar_one.side_effect = Exception("not found")
    mock_unique.scalar_one_or_none.return_value = None
    mock_result = MagicMock()
    mock_result.unique.return_value = mock_unique
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)

    ctx = ast.CompileContext(
        session=session,
        exception=DJException(),
        dependencies_cache=dependencies or {},
        column_overrides=column_overrides or {},
    )
    return ctx


# ---------------------------------------------------------------------------
# Standard source / transform / dimension nodes for reuse
# ---------------------------------------------------------------------------

USERS_NODE = _make_node(
    "default.users",
    NodeType.SOURCE,
    [
        ("user_id", IntegerType()),
        ("username", StringType()),
        ("email", StringType()),
        ("created_at", TimestampType()),
    ],
)

ORDERS_NODE = _make_node(
    "default.orders",
    NodeType.TRANSFORM,
    [
        ("order_id", IntegerType()),
        ("user_id", IntegerType()),
        ("amount", DoubleType()),
        ("order_date", DateType()),
    ],
)

DATE_DIM_NODE = _make_node(
    "default.date_dim",
    NodeType.DIMENSION,
    [
        ("date_id", IntegerType()),
        ("year", IntegerType()),
        ("month", IntegerType()),
        ("day", IntegerType()),
        ("week", StringType()),
    ],
)

STRUCT_NODE = _make_node(
    "default.events",
    NodeType.SOURCE,
    [
        ("event_id", IntegerType()),
        (
            "metadata",
            StructType(
                ast.NestedField(ast.Name("name"), StringType(), True),
                ast.NestedField(ast.Name("value"), IntegerType(), True),
            ),
        ),
        ("tags", ListType(element_type=StringType())),
    ],
)


def _make_metric_node(name: str, col_name: str, col_type, query: str = ""):
    """Create a metric node (single output column)."""
    return _make_node(name, NodeType.METRIC, [(col_name, col_type)], query=query)


REVENUE_METRIC = _make_metric_node(
    "default.total_revenue",
    "default_DOT_total_revenue",
    DoubleType(),
    "SELECT SUM(amount) default_DOT_total_revenue FROM default.orders",
)

ORDER_COUNT_METRIC = _make_metric_node(
    "default.order_count",
    "default_DOT_order_count",
    BigIntType(),
    "SELECT COUNT(*) default_DOT_order_count FROM default.orders",
)


# ===================================================================
# 1. Simple FROM query — columns get types from parent
# ===================================================================


class TestSimpleFromQuery:
    @pytest.mark.asyncio
    async def test_single_table_column_types(self):
        """SELECT user_id, username FROM default.users → types from source columns."""
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse("SELECT user_id, username FROM default.users")
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 2
        assert isinstance(query.columns[0].type, IntegerType)
        assert isinstance(query.columns[1].type, StringType)

    @pytest.mark.asyncio
    async def test_aliased_columns(self):
        """SELECT user_id AS id, username AS name FROM default.users"""
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse("SELECT user_id AS id, username AS name FROM default.users")
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 2
        assert query.columns[0].alias_or_name.name == "id"
        assert isinstance(query.columns[0].type, IntegerType)
        assert query.columns[1].alias_or_name.name == "name"
        assert isinstance(query.columns[1].type, StringType)

    @pytest.mark.asyncio
    async def test_star_select(self):
        """SELECT * FROM default.users — compile succeeds, wildcard stays as-is.
        Note: wildcard expansion happens in the build pipeline, not compile."""
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse("SELECT * FROM default.users")
        await query.compile(ctx)

        assert not ctx.exception.errors
        # The underlying Table should have all 4 columns loaded
        tables = list(query.find_all(ast.Table))
        dj_tables = [t for t in tables if t.dj_node is not None]
        assert len(dj_tables) == 1
        assert len(dj_tables[0].columns) == 4

    @pytest.mark.asyncio
    async def test_missing_column_error(self):
        """SELECT nonexistent FROM default.users → error."""
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse("SELECT nonexistent FROM default.users")
        await query.compile(ctx)

        # Should produce either INVALID_COLUMN or a compilation error
        assert len(ctx.exception.errors) >= 1


# ===================================================================
# 2. Multi-table query — correct table ownership
# ===================================================================


class TestMultiTableQuery:
    @pytest.mark.asyncio
    async def test_qualified_columns_from_two_tables(self):
        """
        SELECT u.user_id, o.amount
        FROM default.users u, default.orders o
        """
        ctx = _make_ctx(
            dependencies={
                "default.users": USERS_NODE,
                "default.orders": ORDERS_NODE,
            },
        )
        query = parse(
            "SELECT u.user_id, o.amount FROM default.users u, default.orders o",
        )
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 2
        assert isinstance(query.columns[0].type, IntegerType)
        assert isinstance(query.columns[1].type, DoubleType)

    @pytest.mark.asyncio
    async def test_join_columns(self):
        """
        SELECT u.username, o.amount
        FROM default.users u
        JOIN default.orders o ON u.user_id = o.user_id
        """
        ctx = _make_ctx(
            dependencies={
                "default.users": USERS_NODE,
                "default.orders": ORDERS_NODE,
            },
        )
        query = parse(
            "SELECT u.username, o.amount "
            "FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
        )
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 2
        assert isinstance(query.columns[0].type, StringType)
        assert isinstance(query.columns[1].type, DoubleType)

    @pytest.mark.asyncio
    async def test_ambiguous_column_error(self):
        """
        SELECT user_id FROM default.users u, default.orders o
        — user_id exists in both tables, should produce error.
        """
        ctx = _make_ctx(
            dependencies={
                "default.users": USERS_NODE,
                "default.orders": ORDERS_NODE,
            },
        )
        query = parse(
            "SELECT user_id FROM default.users u, default.orders o",
        )
        await query.compile(ctx)

        ambiguous_errors = [
            e
            for e in ctx.exception.errors
            if "multiple tables" in (e.message or "").lower()
        ]
        assert len(ambiguous_errors) >= 1


# ===================================================================
# 3. Derived metric — no FROM clause, metric references
# ===================================================================


class TestDerivedMetric:
    @pytest.mark.asyncio
    async def test_single_metric_reference(self):
        """SELECT default.total_revenue (derived metric, no FROM)."""
        ctx = _make_ctx(dependencies={"default.total_revenue": REVENUE_METRIC})

        # For derived metrics, Column.compile calls get_dj_node for the metric.
        # We need to mock get_dj_node to return the metric's NodeRevision.
        with patch(
            "datajunction_server.sql.parsing.ast.get_dj_node",
            new_callable=AsyncMock,
        ) as mock_get:
            mock_get.return_value = REVENUE_METRIC.current
            query = parse("SELECT default.total_revenue")
            await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 1
        assert isinstance(query.columns[0].type, DoubleType)

    @pytest.mark.asyncio
    async def test_multi_metric_derived(self):
        """SELECT default.total_revenue + default.order_count (two metric refs)."""
        ctx = _make_ctx(
            dependencies={
                "default.total_revenue": REVENUE_METRIC,
                "default.order_count": ORDER_COUNT_METRIC,
            },
        )

        async def mock_get_dj_node(session, name, kinds=None, current=True):
            mapping = {
                "default.total_revenue": REVENUE_METRIC.current,
                "default.order_count": ORDER_COUNT_METRIC.current,
            }
            from datajunction_server.errors import DJError, DJErrorException

            if name not in mapping:
                raise DJErrorException(
                    DJError(code=ErrorCode.UNKNOWN_NODE, message=f"No node `{name}`"),
                )
            return mapping[name]

        with patch(
            "datajunction_server.sql.parsing.ast.get_dj_node",
            side_effect=mock_get_dj_node,
        ):
            query = parse(
                "SELECT default.total_revenue + default.order_count AS combined",
            )
            await query.compile(ctx)

        # Both metrics should be resolved — output is a single expression column
        assert len(query.columns) == 1
        # Column should be compiled (both metric types resolved)
        assert query.columns[0].is_compiled()


# ===================================================================
# 4. Dimension attribute references in derived metrics
# ===================================================================


class TestDimensionAttributeRef:
    @pytest.mark.asyncio
    async def test_dimension_attribute_in_derived_metric(self):
        """
        SELECT default.total_revenue
        — with ORDER BY default.date_dim.week
        The dimension attribute should resolve from the dimension node.
        """
        ctx = _make_ctx(
            dependencies={
                "default.total_revenue": REVENUE_METRIC,
                "default.date_dim": DATE_DIM_NODE,
            },
        )

        async def mock_get_dj_node(session, name, kinds=None, current=True):
            mapping = {
                "default.total_revenue": REVENUE_METRIC.current,
                "default.date_dim": DATE_DIM_NODE.current,
            }
            from datajunction_server.errors import DJError, DJErrorException

            if name not in mapping:
                raise DJErrorException(
                    DJError(code=ErrorCode.UNKNOWN_NODE, message=f"No node `{name}`"),
                )
            return mapping[name]

        with patch(
            "datajunction_server.sql.parsing.ast.get_dj_node",
            side_effect=mock_get_dj_node,
        ):
            # Dimension attribute reference: default.date_dim.week
            query = parse("SELECT default.total_revenue, default.date_dim.week")
            await query.compile(ctx)

        # Both columns should compile without errors
        compiled_cols = [c for c in query.columns if c.is_compiled()]
        assert len(compiled_cols) == 2


# ===================================================================
# 5. CTEs (bake_ctes + cte_mapping)
# ===================================================================


class TestCTEs:
    @pytest.mark.asyncio
    async def test_cte_column_propagation(self):
        """
        WITH cte AS (SELECT user_id, username FROM default.users)
        SELECT user_id, username FROM cte
        — columns from CTE should propagate.
        """
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse(
            "WITH cte AS (SELECT user_id, username FROM default.users) "
            "SELECT user_id, username FROM cte",
        )
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 2
        assert isinstance(query.columns[0].type, IntegerType)
        assert isinstance(query.columns[1].type, StringType)

    @pytest.mark.asyncio
    async def test_cte_with_alias(self):
        """
        WITH cte AS (SELECT user_id AS uid FROM default.users)
        SELECT uid FROM cte
        """
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse(
            "WITH cte AS (SELECT user_id AS uid FROM default.users) "
            "SELECT uid FROM cte",
        )
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 1
        assert isinstance(query.columns[0].type, IntegerType)

    @pytest.mark.asyncio
    async def test_multiple_ctes(self):
        """
        WITH
          u AS (SELECT user_id FROM default.users),
          o AS (SELECT order_id, user_id FROM default.orders)
        SELECT u.user_id, o.order_id FROM u JOIN o ON u.user_id = o.user_id
        """
        ctx = _make_ctx(
            dependencies={
                "default.users": USERS_NODE,
                "default.orders": ORDERS_NODE,
            },
        )
        query = parse(
            "WITH u AS (SELECT user_id FROM default.users), "
            "o AS (SELECT order_id, user_id FROM default.orders) "
            "SELECT u.user_id, o.order_id FROM u JOIN o ON u.user_id = o.user_id",
        )
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 2


# ===================================================================
# 6. Subquery scoping — columns shouldn't leak across boundaries
# ===================================================================


class TestSubqueryScoping:
    @pytest.mark.asyncio
    async def test_subquery_columns_propagate(self):
        """
        SELECT x FROM (SELECT user_id AS x FROM default.users) sub
        — inner alias should be visible in outer query.
        """
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse(
            "SELECT x FROM (SELECT user_id AS x FROM default.users) sub",
        )
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 1
        assert isinstance(query.columns[0].type, IntegerType)

    @pytest.mark.asyncio
    async def test_inner_column_not_visible_in_outer(self):
        """
        SELECT username FROM (SELECT user_id AS x FROM default.users) sub
        — username is not in the subquery's output, should error.
        """
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse(
            "SELECT username FROM (SELECT user_id AS x FROM default.users) sub",
        )
        await query.compile(ctx)

        invalid_col_errors = [
            e for e in ctx.exception.errors if e.code == ErrorCode.INVALID_COLUMN
        ]
        assert len(invalid_col_errors) >= 1

    @pytest.mark.asyncio
    async def test_nested_subquery(self):
        """
        SELECT y FROM (
            SELECT x AS y FROM (
                SELECT user_id AS x FROM default.users
            ) inner_sub
        ) outer_sub
        """
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse(
            "SELECT y FROM ("
            "  SELECT x AS y FROM ("
            "    SELECT user_id AS x FROM default.users"
            "  ) inner_sub"
            ") outer_sub",
        )
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 1
        assert isinstance(query.columns[0].type, IntegerType)


# ===================================================================
# 7. Struct field resolution (struct_col.field)
# ===================================================================


class TestStructFieldResolution:
    @pytest.mark.asyncio
    async def test_struct_field_access(self):
        """
        SELECT metadata.name FROM default.events
        — metadata is a StructType, .name should resolve to StringType.
        """
        ctx = _make_ctx(dependencies={"default.events": STRUCT_NODE})
        query = parse("SELECT metadata.name FROM default.events")
        await query.compile(ctx)

        # Struct field resolution should work
        assert len(query.columns) == 1
        # The column should be compiled (may be a struct ref or resolved string)
        compiled_cols = [c for c in query.columns if c.is_compiled()]
        assert len(compiled_cols) == 1

    @pytest.mark.asyncio
    async def test_struct_field_with_alias(self):
        """SELECT metadata.name AS meta_name FROM default.events"""
        ctx = _make_ctx(dependencies={"default.events": STRUCT_NODE})
        query = parse("SELECT metadata.name AS meta_name FROM default.events")
        await query.compile(ctx)

        assert len(query.columns) == 1
        assert query.columns[0].alias_or_name.name == "meta_name"


# ===================================================================
# 8. InlineTable / VALUES expressions
# ===================================================================


class TestInlineTable:
    @pytest.mark.asyncio
    async def test_values_expression(self):
        """
        SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)
        — the inner subquery (InlineTable) should have columns from the alias list.
        """
        ctx = _make_ctx()
        query = parse("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
        await query.compile(ctx)

        # The inner Query wrapping the InlineTable should have 2 columns
        inner_queries = [q for q in query.find_all(ast.Query) if q is not query]
        assert len(inner_queries) == 1
        assert len(inner_queries[0].columns) == 2

    @pytest.mark.asyncio
    async def test_values_in_subquery(self):
        """
        SELECT id FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)
        """
        ctx = _make_ctx()
        query = parse("SELECT id FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
        await query.compile(ctx)

        assert len(query.columns) == 1


# ===================================================================
# 9. Lambda parameters
# ===================================================================


class TestLambdaParameters:
    @pytest.mark.asyncio
    async def test_lambda_in_transform(self):
        """
        SELECT TRANSFORM(tags, x -> UPPER(x)) FROM default.events

        Note: compile() currently DOES produce an INVALID_COLUMN error for
        lambda parameters because it doesn't understand lambda scoping.
        The validate_node_data function filters these out by checking local
        aliases. This test documents the current behavior.
        """
        ctx = _make_ctx(dependencies={"default.events": STRUCT_NODE})
        query = parse("SELECT TRANSFORM(tags, x -> UPPER(x)) FROM default.events")
        await query.compile(ctx)

        # Currently compile produces an error for 'x' — it doesn't know about
        # lambda scope. validate_node_data filters this out later.
        lambda_errors = [
            e
            for e in ctx.exception.errors
            if e.code == ErrorCode.INVALID_COLUMN and "`x`" in (e.message or "")
        ]
        assert len(lambda_errors) == 1  # Known limitation of compile()


# ===================================================================
# 10. Column overrides for dry-run / impact preview
# ===================================================================


class TestColumnOverrides:
    @pytest.mark.asyncio
    async def test_override_replaces_db_columns(self):
        """
        When column_overrides is set for a table, compile should use the
        overridden columns instead of the node's actual columns.
        """
        # Override: default.users has different columns than what's in the node
        override_columns = [
            _make_db_column("user_id", BigIntType()),  # Changed from IntegerType
            _make_db_column("display_name", StringType()),  # New column
        ]
        ctx = _make_ctx(
            dependencies={"default.users": USERS_NODE},
            column_overrides={"default.users": override_columns},
        )
        query = parse("SELECT user_id, display_name FROM default.users")
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 2
        # user_id should be BigIntType (from override), not IntegerType
        assert isinstance(query.columns[0].type, BigIntType)
        assert isinstance(query.columns[1].type, StringType)

    @pytest.mark.asyncio
    async def test_override_only_affects_specified_table(self):
        """
        Column overrides for one table shouldn't affect another table.
        """
        override_columns = [
            _make_db_column("user_id", BigIntType()),
        ]
        ctx = _make_ctx(
            dependencies={
                "default.users": USERS_NODE,
                "default.orders": ORDERS_NODE,
            },
            column_overrides={"default.users": override_columns},
        )
        query = parse(
            "SELECT u.user_id, o.amount "
            "FROM default.users u "
            "JOIN default.orders o ON u.user_id = o.user_id",
        )
        await query.compile(ctx)

        assert not ctx.exception.errors
        # users.user_id → BigIntType (override), orders.amount → DoubleType (original)
        assert isinstance(query.columns[0].type, BigIntType)
        assert isinstance(query.columns[1].type, DoubleType)


# ===================================================================
# 11. Error cases
# ===================================================================


class TestErrorCases:
    @pytest.mark.asyncio
    async def test_missing_table_node(self):
        """SELECT a FROM default.nonexistent — table doesn't exist.
        The batch load returns empty, then Table.compile falls through
        to get_dj_node which also fails."""
        ctx = _make_ctx()

        with patch(
            "datajunction_server.sql.parsing.ast.get_dj_node",
            new_callable=AsyncMock,
        ) as mock_get:
            from datajunction_server.errors import DJError, DJErrorException

            mock_get.side_effect = DJErrorException(
                DJError(
                    code=ErrorCode.UNKNOWN_NODE,
                    message="No node `default.nonexistent` exists",
                ),
            )
            query = parse("SELECT a FROM default.nonexistent")
            await query.compile(ctx)

        assert len(ctx.exception.errors) >= 1

    @pytest.mark.asyncio
    async def test_column_from_wrong_table_alias(self):
        """SELECT wrong_alias.amount FROM default.users u — wrong_alias isn't in FROM."""
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse("SELECT wrong_alias.amount FROM default.users u")
        await query.compile(ctx)

        # 'wrong_alias' is not a valid table alias — should produce an error
        assert len(ctx.exception.errors) >= 1


# ===================================================================
# 12. Compile idempotency
# ===================================================================


class TestCompileIdempotency:
    @pytest.mark.asyncio
    async def test_double_compile_is_noop(self):
        """Compiling the same query twice should produce the same result."""
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse("SELECT user_id, username FROM default.users")
        await query.compile(ctx)

        first_columns = list(query.columns)
        first_types = [c.type for c in first_columns]

        # Compile again
        await query.compile(ctx)

        assert len(query.columns) == len(first_columns)
        for c1, c2 in zip(first_types, [c.type for c in query.columns]):
            assert str(c1) == str(c2)


# ===================================================================
# 13. WHERE / GROUP BY / HAVING don't affect output columns
# ===================================================================


class TestNonProjectionClauses:
    @pytest.mark.asyncio
    async def test_where_clause_doesnt_add_columns(self):
        """
        SELECT username FROM default.users WHERE user_id > 10
        — output should only have username, not user_id.
        """
        ctx = _make_ctx(dependencies={"default.users": USERS_NODE})
        query = parse("SELECT username FROM default.users WHERE user_id > 10")
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 1
        assert query.columns[0].alias_or_name.name == "username"

    @pytest.mark.asyncio
    async def test_group_by_with_aggregation(self):
        """
        SELECT user_id, COUNT(*) AS cnt
        FROM default.orders
        GROUP BY user_id
        """
        ctx = _make_ctx(dependencies={"default.orders": ORDERS_NODE})
        query = parse(
            "SELECT user_id, COUNT(*) AS cnt FROM default.orders GROUP BY user_id",
        )
        await query.compile(ctx)

        assert not ctx.exception.errors
        assert len(query.columns) == 2
        assert isinstance(query.columns[0].type, IntegerType)
