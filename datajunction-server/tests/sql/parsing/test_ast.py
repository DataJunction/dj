# pylint: disable=too-many-lines
"""
testing ast Nodes and their methods
"""

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from datajunction_server.errors import DJException
from datajunction_server.sql.parsing import ast, types
from datajunction_server.sql.parsing.backends.antlr4 import parse
from tests.sql.utils import compare_query_strings


def test_ast_compile_table(
    session: Session,
    client_with_roads: TestClient,  # pylint: disable=unused-argument
):
    """
    Test compiling the primary table from a query

    Includes client_with_roads fixture so that roads examples are loaded into session
    """
    query = parse("SELECT hard_hat_id, last_name, first_name FROM default.hard_hats")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.select.from_.relations[0].primary.compile(ctx)  # type: ignore
    assert not exc.errors

    node = query.select.from_.relations[  # type: ignore  # pylint: disable=protected-access
        0
    ].primary._dj_node
    assert node
    assert node.name == "default.hard_hats"


def test_ast_compile_table_missing_node(session):
    """
    Test compiling a table when the node is missing
    """
    query = parse("SELECT a FROM foo")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.select.from_.relations[0].primary.compile(ctx)
    assert "No node `foo` exists of kind" in exc.errors[0].message

    query = parse("SELECT a FROM foo, bar, baz")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.select.from_.relations[0].primary.compile(ctx)
    assert "No node `foo` exists of kind" in exc.errors[0].message
    query.select.from_.relations[1].primary.compile(ctx)
    assert "No node `bar` exists of kind" in exc.errors[1].message
    query.select.from_.relations[2].primary.compile(ctx)
    assert "No node `baz` exists of kind" in exc.errors[2].message

    query = parse("SELECT a FROM foo LEFT JOIN bar")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.select.from_.relations[0].primary.compile(ctx)
    assert "No node `foo` exists of kind" in exc.errors[0].message
    query.select.from_.relations[0].extensions[0].right.compile(ctx)
    assert "No node `bar` exists of kind" in exc.errors[1].message

    query = parse("SELECT a FROM foo LEFT JOIN (SELECT b FROM bar) b")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.select.from_.relations[0].primary.compile(ctx)
    assert "No node `foo` exists of kind" in exc.errors[0].message
    query.select.from_.relations[0].extensions[0].right.select.from_.relations[
        0
    ].primary.compile(ctx)
    assert "No node `bar` exists of kind" in exc.errors[1].message


def test_ast_compile_query(
    session: Session,
    client_with_roads: TestClient,  # pylint: disable=unused-argument
):
    """
    Test compiling an entire query
    """
    query = parse("SELECT hard_hat_id, last_name, first_name FROM default.hard_hats")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors

    node = query.select.from_.relations[  # type: ignore  # pylint: disable=protected-access
        0
    ].primary._dj_node
    assert node
    assert node.name == "default.hard_hats"


def test_ast_compile_query_missing_columns(
    session: Session,
    client_with_roads: TestClient,  # pylint: disable=unused-argument
):
    """
    Test compiling a query with missing columns
    """
    query = parse("SELECT hard_hat_id, column_foo, column_bar FROM default.hard_hats")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
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


def test_ast_compile_missing_references(session: Session):
    """
    Test getting dependencies from a query that has dangling references when set not to raise
    """
    query = parse("select a, b, c from does_not_exist")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    _, danglers = query.extract_dependencies(ctx)
    assert "does_not_exist" in danglers


def test_ast_compile_raise_on_ambiguous_column(
    session: Session,
    client_with_basic: TestClient,  # pylint: disable=unused-argument
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
    query.compile(ctx)
    assert (
        "Column `country` found in multiple tables. Consider using fully qualified name."
        in exc.errors[0].message
    )


def test_ast_compile_having(
    session: Session,
    client_with_dbt: TestClient,  # pylint: disable=unused-argument
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
    query.compile(ctx)
    assert not exc.errors

    node = query.select.from_.relations[0].primary._dj_node  # type: ignore  # pylint: disable=protected-access
    assert node
    assert node.name == "dbt.source.jaffle_shop.orders"


def test_ast_compile_lateral_view_explode1(session: Session):
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
    query.compile(ctx)

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


def test_ast_compile_lateral_view_explode2(session: Session):
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
    query.compile(ctx)

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


def test_ast_compile_lateral_view_explode3(session: Session):
    """
    Test lateral view explode of array constant
    """

    query = parse(
        """SELECT a, b, c, d, e
    FROM (SELECT 1 as a, 2 as b, 3 as c) AS foo
    LATERAL VIEW EXPLODE(ARRAY(30, 60)) AS d
    LATERAL VIEW EXPLODE(ARRAY(40, 80)) AS e;""",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)

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
        name="d",
        quote_style="",
        namespace=None,
    )
    assert query.columns[4].name == ast.Name(  # type: ignore
        name="e",
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


def test_ast_compile_lateral_view_explode4(session: Session, client: TestClient):
    """
    Test lateral view explode of an upstream column
    """
    client.post("/namespaces/default/")
    response = client.post("/catalogs/", json={"name": "default"})
    assert response.ok
    response = client.post(
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
    assert response.ok
    response = client.post(
        "/nodes/transform/",
        json={
            "description": "A projection with an array",
            "query": "SELECT ARRAY(30, 60) as foo_array FROM default.a",
            "mode": "published",
            "name": "default.foo_array_example",
        },
    )
    assert response.ok

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
    query.compile(ctx)

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


def test_ast_compile_lateral_view_explode5(session: Session):
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
    query.compile(ctx)

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


def test_ast_compile_lateral_view_explode6(session: Session):
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
    query.compile(ctx)

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


def test_ast_compile_lateral_view_explode7(session: Session):
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
    query.compile(ctx)

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


def test_ast_compile_lateral_view_explode8(session: Session):
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
    query.compile(ctx)

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


def test_ast_compile_inline_table(session: Session):
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

    query.compile(ctx)
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

    query.compile(ctx)
    assert [
        (col.alias_or_name.name, col.type) for col in query.select.projection  # type: ignore
    ] == expected_columns
    assert query.select.from_.relations[0].primary.values == expected_values  # type: ignore
    assert query.columns[0].table.alias_or_name == expected_table_name  # type: ignore
