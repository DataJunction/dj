# pylint: disable=too-many-lines,too-many-statements
"""
testing ast Nodes and their methods
"""
from typing import cast

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.errors import DJException
from datajunction_server.sql.parsing import ast, types
from datajunction_server.sql.parsing.backends.antlr4 import parse
from tests.sql.utils import compare_query_strings


@pytest.mark.asyncio
async def test_ast_compile_table(
    session: AsyncSession,
    client_with_roads: AsyncClient,  # pylint: disable=unused-argument
):
    """
    Test compiling the primary table from a query

    Includes client_with_roads fixture so that roads examples are loaded into session
    """
    query = parse("SELECT hard_hat_id, last_name, first_name FROM default.hard_hats")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.select.from_.relations[0].primary.compile(ctx)  # type: ignore
    assert not exc.errors

    node = query.select.from_.relations[  # type: ignore  # pylint: disable=protected-access
        0
    ].primary._dj_node
    assert node
    assert node.name == "default.hard_hats"


@pytest.mark.asyncio
async def test_ast_compile_table_missing_node(session: AsyncSession):
    """
    Test compiling a table when the node is missing
    """
    query = parse("SELECT a FROM foo")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.select.from_.relations[0].primary.compile(ctx)  # type: ignore
    assert "No node `foo` exists of kind" in exc.errors[0].message

    query = parse("SELECT a FROM foo, bar, baz")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.select.from_.relations[0].primary.compile(ctx)  # type: ignore
    assert "No node `foo` exists of kind" in exc.errors[0].message
    await query.select.from_.relations[1].primary.compile(ctx)  # type: ignore
    assert "No node `bar` exists of kind" in exc.errors[1].message
    await query.select.from_.relations[2].primary.compile(ctx)  # type: ignore
    assert "No node `baz` exists of kind" in exc.errors[2].message

    query = parse("SELECT a FROM foo LEFT JOIN bar")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.select.from_.relations[0].primary.compile(ctx)  # type: ignore
    assert "No node `foo` exists of kind" in exc.errors[0].message
    await query.select.from_.relations[0].extensions[0].right.compile(ctx)  # type: ignore
    assert "No node `bar` exists of kind" in exc.errors[1].message

    query = parse("SELECT a FROM foo LEFT JOIN (SELECT b FROM bar) b")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.select.from_.relations[0].primary.compile(ctx)  # type: ignore
    assert "No node `foo` exists of kind" in exc.errors[0].message
    await (
        query.select.from_.relations[0].extensions[0].right.select.from_.relations  # type: ignore
    )[0].primary.compile(
        ctx,
    )
    assert "No node `bar` exists of kind" in exc.errors[1].message


@pytest.mark.asyncio
async def test_ast_compile_query(
    session: AsyncSession,
    client_with_roads: AsyncClient,  # pylint: disable=unused-argument
):
    """
    Test compiling an entire query
    """
    query = parse("SELECT hard_hat_id, last_name, first_name FROM default.hard_hats")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)
    assert not exc.errors

    node = query.select.from_.relations[  # type: ignore  # pylint: disable=protected-access
        0
    ].primary._dj_node
    assert node
    assert node.name == "default.hard_hats"


@pytest.mark.asyncio
async def test_ast_compile_query_missing_columns(
    session: AsyncSession,
    client_with_roads: AsyncClient,  # pylint: disable=unused-argument
):
    """
    Test compiling a query with missing columns
    """
    query = parse("SELECT hard_hat_id, column_foo, column_bar FROM default.hard_hats")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)
    assert (
        "Column `column_foo` does not exist on any valid table."
        in exc.errors[0].message
    )
    assert (
        "Column `column_bar` does not exist on any valid table."
        in exc.errors[1].message
    )

    node = query.select.from_.relations[  # type: ignore  # pylint: disable=protected-access
        0
    ].primary._dj_node
    assert node
    assert node.name == "default.hard_hats"


@pytest.mark.asyncio
async def test_ast_compile_missing_references(session: AsyncSession):
    """
    Test getting dependencies from a query that has dangling references when set not to raise
    """
    query = parse("select a, b, c from does_not_exist")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)
    _, danglers = await query.extract_dependencies(ctx)
    assert "does_not_exist" in danglers


@pytest.mark.asyncio
async def test_ast_compile_raise_on_ambiguous_column(
    session: AsyncSession,
    client_with_basic: AsyncClient,  # pylint: disable=unused-argument
):
    """
    Test raising on ambiguous column
    """
    query = parse(
        "SELECT country FROM basic.transform.country_agg a "
        "LEFT JOIN basic.dimension.countries b on a.country = b.country",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)
    assert (
        "Column `country` found in multiple tables. Consider using fully qualified name."
        in exc.errors[0].message
    )


@pytest.mark.asyncio
async def test_ast_compile_having(
    session: AsyncSession,
    client_with_dbt: AsyncClient,  # pylint: disable=unused-argument
):
    """
    Test using having
    """
    query = parse(
        "SELECT order_date, status FROM dbt.source.jaffle_shop.orders "
        "GROUP BY dbt.dimension.customers.id "
        "HAVING dbt.dimension.customers.id=1",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)

    node = query.select.from_.relations[0].primary._dj_node  # type: ignore  # pylint: disable=protected-access
    assert node
    assert node.name == "dbt.source.jaffle_shop.orders"


@pytest.mark.asyncio
async def test_ast_compile_explode(session: AsyncSession):
    """
    Test explode
    """
    query_str = """
        SELECT EXPLODE(foo.my_map) AS (col1, col2)
        FROM (SELECT MAP(1.0, '2', 3.0, '4') AS my_map) AS foo
    """
    query = parse(query_str)
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)
    assert not exc.errors

    assert query.columns[0].name == ast.Name(  # type: ignore
        name="col1",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].name == ast.Name(  # type: ignore
        name="col2",
        quote_style="",
        namespace=None,
    )
    assert isinstance(query.columns[0].type, types.FloatType)
    assert isinstance(query.columns[1].type, types.StringType)

    assert compare_query_strings(str(query), query_str)


@pytest.mark.asyncio
async def test_ast_compile_lateral_view_explode1(session: AsyncSession):
    """
    Test lateral view explode
    """

    query = parse(
        """SELECT a, b, c, c_age.col, d_age.col
    FROM (SELECT 1 as a, 2 as b, 3 as c, ARRAY(30,60) as d, ARRAY(40,80) as e) AS foo
    LATERAL VIEW EXPLODE(d) c_age
    LATERAL VIEW EXPLODE(e) d_age;
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)

    assert not exc.errors

    assert query.columns[0].is_compiled()
    assert query.columns[1].is_compiled()
    assert query.columns[2].is_compiled()
    assert query.columns[3].is_compiled()
    assert query.columns[4].is_compiled()
    assert query.columns[0].name == ast.Name(  # type: ignore
        name="a",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].name == ast.Name(  # type: ignore
        name="b",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].name == ast.Name(  # type: ignore
        name="c",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].name == ast.Name(  # type: ignore
        name="col",
        quote_style="",
        namespace=ast.Name(name="c_age", quote_style="", namespace=None),
    )
    assert query.columns[4].name == ast.Name(  # type: ignore
        name="col",
        quote_style="",
        namespace=ast.Name(name="d_age", quote_style="", namespace=None),
    )
    assert isinstance(query.columns[0].type, types.IntegerType)
    assert isinstance(query.columns[1].type, types.IntegerType)
    assert isinstance(query.columns[2].type, types.IntegerType)
    assert isinstance(query.columns[3].type, types.IntegerType)
    assert isinstance(query.columns[4].type, types.IntegerType)
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].table.alias_or_name == ast.Name(  # type: ignore
        name="c_age",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].table.alias_or_name == ast.Name(  # type: ignore
        name="d_age",
        quote_style="",
        namespace=None,
    )


@pytest.mark.asyncio
async def test_ast_compile_lateral_view_explode2(session: AsyncSession):
    """
    Test lateral view explode
    """

    query = parse(
        """SELECT a, b, c, c_age, d_age
    FROM (SELECT 1 as a, 2 as b, 3 as c, ARRAY(30,60) as d, ARRAY(40,80) as e) AS foo
    LATERAL VIEW EXPLODE(d) AS c_age
    LATERAL VIEW EXPLODE(e) AS d_age;""",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)

    assert query.columns[0].is_compiled()
    assert query.columns[1].is_compiled()
    assert query.columns[2].is_compiled()
    assert query.columns[3].is_compiled()
    assert query.columns[4].is_compiled()
    assert query.columns[0].name == ast.Name(  # type: ignore
        name="a",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].name == ast.Name(  # type: ignore
        name="b",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].name == ast.Name(  # type: ignore
        name="c",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].name == ast.Name(  # type: ignore
        name="c_age",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].name == ast.Name(  # type: ignore
        name="d_age",
        quote_style="",
        namespace=None,
    )
    assert isinstance(query.columns[0].type, types.IntegerType)
    assert isinstance(query.columns[1].type, types.IntegerType)
    assert isinstance(query.columns[2].type, types.IntegerType)
    assert isinstance(query.columns[3].type, types.IntegerType)
    assert isinstance(query.columns[4].type, types.IntegerType)
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )


@pytest.mark.asyncio
async def test_ast_compile_lateral_view_explode3(session: AsyncSession):
    """
    Test lateral view explode of array constant
    """

    query = parse(
        """SELECT a, b, c, d, e, v.en
    FROM (SELECT 1 as a, 2 as b, 3 as c) AS foo
    LATERAL VIEW EXPLODE(ARRAY(30, 60)) AS d
    LATERAL VIEW EXPLODE(ARRAY(40, 80)) AS e
    LATERAL VIEW EXPLODE(ARRAY(100, 200)) v AS en;""",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)
    assert parse(str(query)) == query
    assert "LATERAL VIEW EXPLODE(ARRAY(100, 200)) v AS  en" in str(query)

    assert query.columns[0].is_compiled()
    assert query.columns[1].is_compiled()
    assert query.columns[2].is_compiled()
    assert query.columns[3].is_compiled()
    assert query.columns[4].is_compiled()
    assert query.columns[5].is_compiled()
    assert query.columns[0].name == ast.Name(  # type: ignore
        name="a",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].name == ast.Name(  # type: ignore
        name="b",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].name == ast.Name(  # type: ignore
        name="c",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].name == ast.Name(  # type: ignore
        name="d",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].name == ast.Name(  # type: ignore
        name="e",
        quote_style="",
        namespace=None,
    )
    assert query.columns[5].name == ast.Name(  # type: ignore
        name="en",
        quote_style="",
        namespace=ast.Name(name="v"),
    )
    assert isinstance(query.columns[0].type, types.IntegerType)
    assert isinstance(query.columns[1].type, types.IntegerType)
    assert isinstance(query.columns[2].type, types.IntegerType)
    assert isinstance(query.columns[3].type, types.IntegerType)
    assert isinstance(query.columns[4].type, types.IntegerType)
    assert isinstance(query.columns[5].type, types.IntegerType)
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )
    assert query.columns[5].table.alias_or_name == ast.Name(  # type: ignore
        name="v",
        quote_style="",
        namespace=None,
    )


@pytest.mark.asyncio
async def test_ast_compile_lateral_view_explode4(
    session: AsyncSession,
    client: AsyncClient,
):
    """
    Test lateral view explode of an upstream column
    """
    await client.post("/namespaces/default/")
    response = await client.post("/catalogs/", json={"name": "default"})
    assert response.status_code in (200, 201)
    response = await client.post(
        "/nodes/source/",
        json={
            "columns": [
                {"name": "a", "type": "int"},
            ],
            "description": "Placeholder source node",
            "mode": "published",
            "name": "default.a",
            "catalog": "default",
            "schema_": "a",
            "table": "a",
        },
    )
    assert response.status_code in (200, 201)
    response = await client.post(
        "/nodes/transform/",
        json={
            "description": "A projection with an array",
            "query": "SELECT ARRAY(30, 60) as foo_array FROM default.a",
            "mode": "published",
            "name": "default.foo_array_example",
        },
    )
    assert response.status_code in (200, 201)

    query = parse(
        """
        SELECT foo_array, a, b
        FROM default.foo_array_example
        LATERAL VIEW EXPLODE(foo_array) AS a
        LATERAL VIEW EXPLODE(foo_array) AS b;
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)

    assert query.columns[0].is_compiled()
    assert query.columns[1].is_compiled()
    assert query.columns[2].is_compiled()
    assert query.columns[0].name == ast.Name(  # type: ignore
        name="foo_array",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].name == ast.Name(  # type: ignore
        name="a",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].name == ast.Name(  # type: ignore
        name="b",
        quote_style="",
        namespace=None,
    )
    assert isinstance(query.columns[0].type, types.ListType)
    assert isinstance(query.columns[1].type, types.IntegerType)
    assert isinstance(query.columns[2].type, types.IntegerType)
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="foo_array_example",
        quote_style="",
        namespace=ast.Name(name="default", quote_style="", namespace=None),
    )
    assert query.columns[1].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )


@pytest.mark.asyncio
async def test_ast_compile_lateral_view_explode5(session: AsyncSession):
    """
    Test both a lateral and horizontal explode
    """

    query = parse(
        """SELECT a, b, c, d.col, e.col, EXPLODE(ARRAY(30, 60))
    FROM (SELECT 1 as a, 2 as b, 3 as c) AS foo
    LATERAL VIEW EXPLODE(ARRAY(30, 60)) d
    LATERAL VIEW EXPLODE(ARRAY(40, 80)) e;
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)

    assert not exc.errors

    assert query.columns[0].is_compiled()
    assert query.columns[1].is_compiled()
    assert query.columns[2].is_compiled()
    assert query.columns[3].is_compiled()
    assert query.columns[4].is_compiled()
    assert query.columns[0].name == ast.Name(  # type: ignore
        name="a",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].name == ast.Name(  # type: ignore
        name="b",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].name == ast.Name(  # type: ignore
        name="c",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].name == ast.Name(  # type: ignore
        name="col",
        quote_style="",
        namespace=ast.Name(name="d", quote_style="", namespace=None),
    )
    assert query.columns[4].name == ast.Name(  # type: ignore
        name="col",
        quote_style="",
        namespace=ast.Name(name="e", quote_style="", namespace=None),
    )
    assert query.columns[5].name == ast.Name(  # type: ignore
        name="col",
        quote_style="",
        namespace=None,
    )
    assert isinstance(query.columns[0].type, types.IntegerType)
    assert isinstance(query.columns[1].type, types.IntegerType)
    assert isinstance(query.columns[2].type, types.IntegerType)
    assert isinstance(query.columns[3].type, types.IntegerType)
    assert isinstance(query.columns[4].type, types.IntegerType)
    assert isinstance(query.columns[5].type, types.IntegerType)
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].table.alias_or_name == ast.Name(  # type: ignore
        name="d",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].table.alias_or_name == ast.Name(  # type: ignore
        name="e",
        quote_style="",
        namespace=None,
    )
    assert query.columns[5].table is None  # type: ignore


@pytest.mark.asyncio
async def test_ast_compile_lateral_view_explode6(session: AsyncSession):
    """
    Test lateral view explode of a map (table aliased)
    """

    query = parse(
        """SELECT a, b, c, c_age.key, c_age.value, d_age.key, d_age.value
    FROM (
      SELECT 1 as a, 2 as b, 3 as c, MAP('a',1,'b',2) as d, MAP('c',1,'d',2) as e
    ) AS foo
    LATERAL VIEW EXPLODE(d) c_age
    LATERAL VIEW EXPLODE(e) d_age;
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)

    assert not exc.errors

    assert query.columns[0].is_compiled()
    assert query.columns[1].is_compiled()
    assert query.columns[2].is_compiled()
    assert query.columns[3].is_compiled()
    assert query.columns[4].is_compiled()
    assert query.columns[5].is_compiled()
    assert query.columns[6].is_compiled()
    assert query.columns[0].name == ast.Name(  # type: ignore
        name="a",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].name == ast.Name(  # type: ignore
        name="b",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].name == ast.Name(  # type: ignore
        name="c",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].name == ast.Name(  # type: ignore
        name="key",
        quote_style="",
        namespace=ast.Name("c_age"),
    )
    assert query.columns[4].name == ast.Name(  # type: ignore
        name="value",
        quote_style="",
        namespace=ast.Name("c_age"),
    )
    assert query.columns[5].name == ast.Name(  # type: ignore
        name="key",
        quote_style="",
        namespace=ast.Name("d_age"),
    )
    assert query.columns[6].name == ast.Name(  # type: ignore
        name="value",
        quote_style="",
        namespace=ast.Name("d_age"),
    )
    assert isinstance(query.columns[0].type, types.IntegerType)
    assert isinstance(query.columns[1].type, types.IntegerType)
    assert isinstance(query.columns[2].type, types.IntegerType)
    assert isinstance(query.columns[3].type, types.StringType)
    assert isinstance(query.columns[4].type, types.IntegerType)
    assert isinstance(query.columns[5].type, types.StringType)
    assert isinstance(query.columns[6].type, types.IntegerType)
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].table.alias_or_name == ast.Name(  # type: ignore
        name="c_age",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].table.alias_or_name == ast.Name(  # type: ignore
        name="c_age",
        quote_style="",
        namespace=None,
    )
    assert query.columns[5].table.alias_or_name == ast.Name(  # type: ignore
        name="d_age",
        quote_style="",
        namespace=None,
    )
    assert query.columns[6].table.alias_or_name == ast.Name(  # type: ignore
        name="d_age",
        quote_style="",
        namespace=None,
    )


@pytest.mark.asyncio
async def test_ast_compile_lateral_view_explode7(session: AsyncSession):
    """
    Test lateral view explode of a map (column aliased)
    """

    query = parse(
        """SELECT a, b, c, k1, v1, k2, v2
    FROM (
      SELECT 1 as a, 2 as b, 3 as c, MAP('a',1,'b',2) as d, MAP('c',1,'d',2) as e
    ) AS foo
    LATERAL VIEW EXPLODE(d) AS k1, v1
    LATERAL VIEW EXPLODE(e) AS k2, v2;
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)

    assert not exc.errors

    assert query.columns[0].is_compiled()
    assert query.columns[1].is_compiled()
    assert query.columns[2].is_compiled()
    assert query.columns[3].is_compiled()
    assert query.columns[4].is_compiled()
    assert query.columns[5].is_compiled()
    assert query.columns[6].is_compiled()
    assert query.columns[0].name == ast.Name(  # type: ignore
        name="a",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].name == ast.Name(  # type: ignore
        name="b",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].name == ast.Name(  # type: ignore
        name="c",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].name == ast.Name(  # type: ignore
        name="k1",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].name == ast.Name(  # type: ignore
        name="v1",
        quote_style="",
        namespace=None,
    )
    assert query.columns[5].name == ast.Name(  # type: ignore
        name="k2",
        quote_style="",
        namespace=None,
    )
    assert query.columns[6].name == ast.Name(  # type: ignore
        name="v2",
        quote_style="",
        namespace=None,
    )
    assert isinstance(query.columns[0].type, types.IntegerType)
    assert isinstance(query.columns[1].type, types.IntegerType)
    assert isinstance(query.columns[2].type, types.IntegerType)
    assert isinstance(query.columns[3].type, types.StringType)
    assert isinstance(query.columns[4].type, types.IntegerType)
    assert isinstance(query.columns[5].type, types.StringType)
    assert isinstance(query.columns[6].type, types.IntegerType)
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )
    assert query.columns[5].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )
    assert query.columns[6].table.alias_or_name == ast.Name(  # type: ignore
        name="EXPLODE",
        quote_style="",
        namespace=None,
    )


@pytest.mark.asyncio
async def test_ast_compile_lateral_view_explode8(session: AsyncSession):
    """
    Test lateral view explode of a map (both table and column aliased)
    """

    query = parse(
        """SELECT a, b, c, c_age.k1, c_age.v1, d_age.k2, d_age.v2
    FROM (
      SELECT 1 as a, 2 as b, 3 as c, MAP('a',1,'b',2) as d, MAP('c',1,'d',2) as e
    ) AS foo
    LATERAL VIEW EXPLODE(d) c_age AS k1, v1
    LATERAL VIEW EXPLODE(e) d_age AS k2, v2;
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)

    assert not exc.errors

    assert query.columns[0].is_compiled()
    assert query.columns[1].is_compiled()
    assert query.columns[2].is_compiled()
    assert query.columns[3].is_compiled()
    assert query.columns[4].is_compiled()
    assert query.columns[5].is_compiled()
    assert query.columns[6].is_compiled()
    assert query.columns[0].name == ast.Name(  # type: ignore
        name="a",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].name == ast.Name(  # type: ignore
        name="b",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].name == ast.Name(  # type: ignore
        name="c",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].name == ast.Name(  # type: ignore
        name="k1",
        quote_style="",
        namespace=ast.Name("c_age"),
    )
    assert query.columns[4].name == ast.Name(  # type: ignore
        name="v1",
        quote_style="",
        namespace=ast.Name("c_age"),
    )
    assert query.columns[5].name == ast.Name(  # type: ignore
        name="k2",
        quote_style="",
        namespace=ast.Name("d_age"),
    )
    assert query.columns[6].name == ast.Name(  # type: ignore
        name="v2",
        quote_style="",
        namespace=ast.Name("d_age"),
    )
    assert isinstance(query.columns[0].type, types.IntegerType)
    assert isinstance(query.columns[1].type, types.IntegerType)
    assert isinstance(query.columns[2].type, types.IntegerType)
    assert isinstance(query.columns[3].type, types.StringType)
    assert isinstance(query.columns[4].type, types.IntegerType)
    assert isinstance(query.columns[5].type, types.StringType)
    assert isinstance(query.columns[6].type, types.IntegerType)
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[1].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[2].table.alias_or_name == ast.Name(  # type: ignore
        name="foo",
        quote_style="",
        namespace=None,
    )
    assert query.columns[3].table.alias_or_name == ast.Name(  # type: ignore
        name="c_age",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].table.alias_or_name == ast.Name(  # type: ignore
        name="c_age",
        quote_style="",
        namespace=None,
    )
    assert query.columns[5].table.alias_or_name == ast.Name(  # type: ignore
        name="d_age",
        quote_style="",
        namespace=None,
    )
    assert query.columns[6].table.alias_or_name == ast.Name(  # type: ignore
        name="d_age",
        quote_style="",
        namespace=None,
    )


@pytest.mark.asyncio
async def test_ast_compile_inline_table(session: AsyncSession):
    """
    Test parsing and compiling an inline table with VALUES (...)
    """
    query_str_explicit_columns = """SELECT
  w.a AS one,
  w.b AS two,
  w.c AS three,
  w.d AS four
FROM VALUES
  ('1', 1, 2, 1),
  ('11', 1, 3, 1),
  ('111', 1, 2, 1),
  ('1111', 1, 3, 1),
  ('11111', 1, 4, 1),
  ('111111', 1, 5, 1) AS w(a, b, c, d)"""
    expected_columns = [
        ("one", types.StringType()),
        ("two", types.IntegerType()),
        ("three", types.IntegerType()),
        ("four", types.IntegerType()),
    ]
    expected_values = [
        [
            ast.String(value="'1'"),
            ast.Number(value=1, _type=None),
            ast.Number(value=2, _type=None),
            ast.Number(value=1, _type=None),
        ],
        [
            ast.String(value="'11'"),
            ast.Number(value=1, _type=None),
            ast.Number(value=3, _type=None),
            ast.Number(value=1, _type=None),
        ],
        [
            ast.String(value="'111'"),
            ast.Number(value=1, _type=None),
            ast.Number(value=2, _type=None),
            ast.Number(value=1, _type=None),
        ],
        [
            ast.String(value="'1111'"),
            ast.Number(value=1, _type=None),
            ast.Number(value=3, _type=None),
            ast.Number(value=1, _type=None),
        ],
        [
            ast.String(value="'11111'"),
            ast.Number(value=1, _type=None),
            ast.Number(value=4, _type=None),
            ast.Number(value=1, _type=None),
        ],
        [
            ast.String(value="'111111'"),
            ast.Number(value=1, _type=None),
            ast.Number(value=5, _type=None),
            ast.Number(value=1, _type=None),
        ],
    ]
    expected_table_name = ast.Name(  # type: ignore
        name="w",
        quote_style="",
        namespace=None,
    )
    query = parse(query_str_explicit_columns)
    exc = DJException()
    assert not exc.errors

    ctx = ast.CompileContext(session=session, exception=exc)
    assert parse(str(query)) == query
    assert compare_query_strings(str(query), query_str_explicit_columns)

    await query.compile(ctx)
    assert [
        (col.alias_or_name.name, col.type) for col in query.select.projection  # type: ignore
    ] == expected_columns
    assert query.select.from_.relations[0].primary.values == expected_values  # type: ignore
    assert query.columns[0].table.alias_or_name == expected_table_name  # type: ignore

    query_str_implicit_columns = """SELECT
  w.col1 AS one,
  w.col2 AS two,
  w.col3 AS three,
  w.col4 AS four
FROM VALUES
  ('1', 1, 2, 1),
  ('11', 1, 3, 1),
  ('111', 1, 2, 1),
  ('1111', 1, 3, 1),
  ('11111', 1, 4, 1),
  ('111111', 1, 5, 1) AS w"""
    query = parse(query_str_implicit_columns)
    exc = DJException()
    assert not exc.errors

    ctx = ast.CompileContext(session=session, exception=exc)
    assert parse(str(query)) == query
    assert compare_query_strings(str(query), query_str_implicit_columns)

    await query.compile(ctx)
    assert [
        (col.alias_or_name.name, col.type) for col in query.select.projection  # type: ignore
    ] == expected_columns
    assert query.select.from_.relations[0].primary.values == expected_values  # type: ignore
    assert query.columns[0].table.alias_or_name == expected_table_name  # type: ignore

    query_str = """SELECT tab.source FROM VALUES ('a'), ('b'), ('c') AS tab(source)"""
    query = parse(query_str)
    exc = DJException()
    assert not exc.errors
    assert str(parse(str(query))) == str(parse(query_str))

    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)
    assert [
        (col.alias_or_name.name, col.type) for col in query.select.projection  # type: ignore
    ] == [("source", types.StringType())]
    assert [
        val[0].value for val in query.select.from_.relations[0].primary.values  # type: ignore
    ] == ["'a'", "'b'", "'c'"]
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="tab",
        quote_style="",
        namespace=None,
    )

    query_str = """SELECT tab.col1 FROM VALUES ('a'), ('b'), ('c') AS tab"""
    query = parse(query_str)
    exc = DJException()
    assert not exc.errors
    assert str(parse(str(query))) == str(parse(query_str))

    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)
    assert [
        (col.alias_or_name.name, col.type) for col in query.select.projection  # type: ignore
    ] == [("col1", types.StringType())]
    assert [
        val[0].value for val in query.select.from_.relations[0].primary.values  # type: ignore
    ] == ["'a'", "'b'", "'c'"]
    assert query.columns[0].table.alias_or_name == ast.Name(  # type: ignore
        name="tab",
        quote_style="",
        namespace=None,
    )


@pytest.mark.asyncio
async def test_ast_subscript_handling(session: AsyncSession):
    """
    Test parsing a query with subscripts
    """
    query_str = """SELECT
  w.a[1]['a'] as a_,
  w.a[1]['b'] as b_
FROM VALUES
  (array(named_struct('a', 1, 'b', 2)), array(named_struct('a', 300, 'b', 20))) AS w(a)"""
    query = parse(str(query_str))
    assert str(parse(str(query))) == str(parse(str(query_str)))
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await query.compile(ctx)
    assert not exc.errors
    assert [
        (col.alias_or_name.name, col.type) for col in query.select.projection  # type: ignore
    ] == [("a_", types.IntegerType()), ("b_", types.IntegerType())]


@pytest.mark.asyncio
async def test_ast_hints():
    """
    Test that parsing a query with hints will yield an AST that includes the hints
    """
    # Test join hints for broadcast joins
    query_str = (
        """SELECT /*+ BROADCAST(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key"""
    )
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "BROADCAST"
    assert [col.name.name for col in query.select.hints[0].parameters] == ["t1"]
    assert "/*+ BROADCAST(t1) */" in str(query)

    query_str = (
        """SELECT /*+ BROADCASTJOIN(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key"""
    )
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "BROADCASTJOIN"
    assert [col.name.name for col in query.select.hints[0].parameters] == ["t1"]
    assert "/*+ BROADCASTJOIN(t1) */" in str(query)

    query_str = (
        """SELECT /*+ MAPJOIN(t2) */ * FROM t1 right JOIN t2 ON t1.key = t2.key"""
    )
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "MAPJOIN"
    assert [col.name.name for col in query.select.hints[0].parameters] == ["t2"]
    assert "/*+ MAPJOIN(t2) */" in str(query)

    # Test join hints for shuffle sort merge join
    query_str = (
        """SELECT /*+ SHUFFLE_MERGE(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key"""
    )
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "SHUFFLE_MERGE"
    assert [col.name.name for col in query.select.hints[0].parameters] == ["t1"]
    assert "/*+ SHUFFLE_MERGE(t1) */" in str(query)

    query_str = (
        """SELECT /*+ MERGEJOIN(t2) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key"""
    )
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "MERGEJOIN"
    assert [col.name.name for col in query.select.hints[0].parameters] == ["t2"]
    assert "/*+ MERGEJOIN(t2) */" in str(query)

    query_str = """SELECT /*+ MERGE(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "MERGE"
    assert [col.name.name for col in query.select.hints[0].parameters] == ["t1"]
    assert "/*+ MERGE(t1) */" in str(query)

    # Test join hints for shuffle hash join
    query_str = (
        """SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key"""
    )
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "SHUFFLE_HASH"
    assert [col.name.name for col in query.select.hints[0].parameters] == ["t1"]
    assert "/*+ SHUFFLE_HASH(t1) */" in str(query)

    # Test join hints for shuffle-and-replicate nested loop join
    query_str = "SELECT /*+ SHUFFLE_REPLICATE_NL(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key"
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "SHUFFLE_REPLICATE_NL"
    assert [col.name.name for col in query.select.hints[0].parameters] == ["t1"]
    assert "/*+ SHUFFLE_REPLICATE_NL(t1) */" in str(query)

    # Test multiple join hints
    query_str = (
        "SELECT /*+ BROADCAST(t1), MERGE(t1, t2) */ * FROM t1 "
        "INNER JOIN t2 ON t1.key = t2.key"
    )
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "BROADCAST"
    assert [col.name.name for col in query.select.hints[0].parameters] == ["t1"]
    assert cast(ast.Hint, query.select.hints[1]).name.name == "MERGE"
    assert [col.name.name for col in query.select.hints[1].parameters] == ["t1", "t2"]
    assert "/*+ BROADCAST(t1), MERGE(t1, t2) */" in str(query)

    # Test partitioning hints
    query_str = """SELECT /*+ REPARTITION(c) */ c, b, a FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "REPARTITION"
    assert [str(col) for col in query.select.hints[0].parameters] == ["c"]
    assert "/*+ REPARTITION(c) */" in str(query)

    query_str = """SELECT /*+ COALESCE(3) */ * FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "COALESCE"
    assert [str(col) for col in query.select.hints[0].parameters] == ["3"]
    assert "/*+ COALESCE(3) */" in str(query)

    query_str = """SELECT /*+ REPARTITION(3) */ * FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "REPARTITION"
    assert [str(col) for col in query.select.hints[0].parameters] == ["3"]
    assert "/*+ REPARTITION(3) */" in str(query)

    query_str = """SELECT /*+ REPARTITION(3, c) */ * FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "REPARTITION"
    assert [str(col) for col in query.select.hints[0].parameters] == ["3", "c"]
    assert "/*+ REPARTITION(3, c) */" in str(query)

    query_str = """SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "REPARTITION_BY_RANGE"
    assert [str(col) for col in query.select.hints[0].parameters] == ["c"]
    assert "/*+ REPARTITION_BY_RANGE(c) */" in str(query)

    query_str = """SELECT /*+ REPARTITION_BY_RANGE(3, c) */ * FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "REPARTITION_BY_RANGE"
    assert [str(col) for col in query.select.hints[0].parameters] == ["3", "c"]
    assert "/*+ REPARTITION_BY_RANGE(3, c) */" in str(query)

    query_str = """SELECT /*+ REBALANCE */ * FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "REBALANCE"
    assert [str(col) for col in query.select.hints[0].parameters] == []
    assert "/*+ REBALANCE */" in str(query)

    query_str = """SELECT /*+ REBALANCE(3) */ * FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "REBALANCE"
    assert [str(col) for col in query.select.hints[0].parameters] == ["3"]
    assert "/*+ REBALANCE(3) */" in str(query)

    query_str = """SELECT /*+ REBALANCE(c) */ * FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "REBALANCE"
    assert [str(col) for col in query.select.hints[0].parameters] == ["c"]
    assert "/*+ REBALANCE(c) */" in str(query)

    query_str = """SELECT /*+ REBALANCE(3, c) */ * FROM t"""
    query = parse(str(query_str))
    assert cast(ast.Hint, query.select.hints[0]).name.name == "REBALANCE"
    assert [str(col) for col in query.select.hints[0].parameters] == ["3", "c"]
    assert "/*+ REBALANCE(3, c) */" in str(query)
