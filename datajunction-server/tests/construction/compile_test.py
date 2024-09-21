"""
Tests for compiling nodes
"""

# pylint: disable=too-many-lines
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJException
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException


@pytest.mark.asyncio
async def test_get_table_node_is_none(construction_session: AsyncSession):
    """
    Test a nonexistent table node with compound exception ignore
    """

    query = parse("select x from purchases")
    ctx = CompileContext(
        session=construction_session,
        exception=DJException(),
    )
    await query.compile(ctx)

    assert "No node `purchases`" in str(ctx.exception.errors)


@pytest.mark.asyncio
async def test_missing_references(construction_session: AsyncSession):
    """
    Test getting dependencies from a query that has dangling references
    """
    query = parse("select a, b, c from does_not_exist")
    exception = DJException()
    context = CompileContext(
        session=construction_session,
        exception=exception,
    )
    _, missing_references = await query.extract_dependencies(context)
    assert missing_references


@pytest.mark.asyncio
async def test_catching_dangling_refs_in_extract_dependencies(
    construction_session: AsyncSession,
):
    """
    Test getting dependencies from a query that has dangling references when set not to raise
    """
    query = parse("select a, b, c from does_not_exist")
    exception = DJException()
    context = CompileContext(
        session=construction_session,
        exception=exception,
    )
    _, danglers = await query.extract_dependencies(context)
    assert "does_not_exist" in danglers


@pytest.mark.asyncio
async def test_raising_on_extract_from_node_with_no_query():
    """
    Test parsing an empty query fails
    """
    with pytest.raises(DJParseException) as exc_info:
        parse(None)
    assert "Empty query provided!" in str(exc_info.value)


@pytest.mark.asyncio
async def test_raise_on_unnamed_subquery_in_implicit_join(
    construction_session: AsyncSession,
):
    """
    Test raising on an unnamed subquery in an implicit join
    """
    query = parse(
        "SELECT country FROM basic.transform.country_agg, "
        "(SELECT country FROM basic.transform.country_agg)",
    )

    context = CompileContext(
        session=construction_session,
        exception=DJException(),
    )
    await query.extract_dependencies(context)
    assert (
        "Column `country` found in multiple tables. Consider using fully qualified name."
        in str(
            context.exception.errors,
        )
    )


@pytest.mark.asyncio
async def test_raise_on_ambiguous_column(construction_session: AsyncSession):
    """
    Test raising on ambiguous column
    """
    query = parse(
        "SELECT country FROM basic.transform.country_agg a "
        "LEFT JOIN basic.dimension.countries b on a.country = b.country",
    )
    context = CompileContext(
        session=construction_session,
        exception=DJException(),
    )
    await query.compile(context)
    assert (
        "Column `country` found in multiple tables. Consider using fully qualified name."
        in str(
            context.exception.errors,
        )
    )


@pytest.mark.asyncio
async def test_compile_node(construction_session: AsyncSession):
    """
    Test compiling a node
    """
    node_a = Node(name="A", current_version="1")
    node_a_rev = NodeRevision(
        node=node_a,
        version="1",
        query="SELECT country FROM basic.transform.country_agg",
    )
    query_ast = parse(node_a_rev.query)
    ctx = CompileContext(session=construction_session, exception=DJException())
    await query_ast.compile(ctx)


@pytest.mark.asyncio
async def test_raise_on_compile_node_with_no_query(construction_session: AsyncSession):
    """
    Test raising when compiling a node that has no query
    """
    node_a = Node(name="A", current_version="1")
    node_a_rev = NodeRevision(node=node_a, version="1")

    with pytest.raises(DJException) as exc_info:
        query_ast = parse(node_a_rev.query)
        ctx = CompileContext(session=construction_session, exception=DJException())
        await query_ast.compile(ctx)

    assert "Empty query provided" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.skip(reason="DJ should not validate query correctness")
async def test_raise_on_unjoinable_automatic_dimension_groupby(
    construction_session: AsyncSession,
):
    """
    Test raising where a dimension node is automatically detected but unjoinable
    """
    node_a = Node(name="A", current_version="1")
    node_a_rev = NodeRevision(
        node=node_a,
        version="1",
        query=(
            "SELECT country FROM basic.transform.country_agg "
            "GROUP BY basic.dimension.countries.country"
        ),
    )

    query_ast = parse(node_a_rev.query)
    ctx = CompileContext(session=construction_session, exception=DJException())
    await query_ast.compile(ctx)

    assert (
        "Column `basic.dimension.countries.country` does not exist on any valid table."
        in str(
            ctx.exception.errors,
        )
    )


@pytest.mark.asyncio
async def test_raise_on_having_without_a_groupby(construction_session: AsyncSession):
    """
    Test raising when using a having without a groupby
    """
    node_a = Node(name="A", current_version="1")
    node_a_rev = NodeRevision(
        node=node_a,
        version="1",
        query=(
            "SELECT country FROM basic.transform.country_agg " "HAVING country='US'"
        ),
    )

    query_ast = parse(node_a_rev.query)
    ctx = CompileContext(session=construction_session, exception=DJException())
    await query_ast.compile(ctx)

    assert "HAVING without a GROUP BY is not allowed" in str(ctx.exception.errors)


@pytest.mark.asyncio
async def test_having(construction_session: AsyncSession):
    """
    Test using having
    """
    node_a = Node(name="A", current_version="1")
    node_a_rev = NodeRevision(
        node=node_a,
        version="1",
        query=(
            "SELECT order_date, status FROM dbt.source.jaffle_shop.orders "
            "GROUP BY dbt.dimension.customers.id "
            "HAVING dbt.dimension.customers.id=1"
        ),
    )
    query_ast = parse(node_a_rev.query)
    ctx = CompileContext(session=construction_session, exception=DJException())
    await query_ast.compile(ctx)
