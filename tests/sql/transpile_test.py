"""
Tests for ``djqs.sql.transpile``.
"""

# pylint: disable=line-too-long

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table as SqlaTable
from sqlalchemy.sql import select
from sqloxide import parse_sql

from djqs.models.column import Column
from djqs.models.database import Database
from djqs.models.node import Node
from djqs.models.query import Query  # pylint: disable=unused-import
from djqs.models.table import Table
from djqs.sql.transpile import (
    get_expression,
    get_function,
    get_query,
    get_select_for_node,
    get_value,
)
from djqs.typing import ColumnType, Expression, Function

from .utils import query_to_string


def test_get_select_for_node_materialized(mocker: MockerFixture) -> None:
    """
    Test ``get_select_for_node`` when the node is materialized.
    """
    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(name="A")

    child = Node(
        name="B",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[Column(name="cnt", type=ColumnType.INT)],
            ),
        ],
        query="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE B (cnt INTEGER)")
    mocker.patch("djqs.models.database.create_engine", return_value=engine)

    assert (
        query_to_string(get_select_for_node(child, database))
        == 'SELECT "B".cnt \nFROM "B"'
    )


def test_get_select_for_node_not_materialized(mocker: MockerFixture) -> None:
    """
    Test ``get_select_for_node`` when the node is not materialized.
    """
    database = Database(id=1, name="db", URI="sqlite://")

    parent = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A_slow",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
                cost=1,
            ),
            Table(
                database=database,
                table="A_fast",
                columns=[Column(name="one", type=ColumnType.STR)],
                cost=0.1,
            ),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A_slow (one TEXT, two TEXT)")
    connection.execute("CREATE TABLE A_fast (one TEXT)")
    mocker.patch("djqs.models.database.create_engine", return_value=engine)

    child = Node(
        name="B",
        query="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    space = " "

    assert (
        query_to_string(get_select_for_node(child, database))
        == f'''SELECT count('*') AS cnt{space}
FROM (SELECT "A_fast".one AS one{space}
FROM "A_fast") AS "A"'''
    )

    # unnamed expression
    child.query = "SELECT COUNT(*) FROM A"

    assert (
        query_to_string(get_select_for_node(child, database))
        == f'''SELECT count('*') AS count_1{space}
FROM (SELECT "A_fast".one AS one{space}
FROM "A_fast") AS "A"'''
    )


def test_get_select_for_node_choose_slow(mocker: MockerFixture) -> None:
    """
    Test ``get_select_for_node`` when the slow table has the columns needed.
    """
    database = Database(id=1, name="db", URI="sqlite://")

    parent = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A_slow",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
                cost=1,
            ),
            Table(
                database=database,
                table="A_fast",
                columns=[Column(name="one", type=ColumnType.STR)],
                cost=0.1,
            ),
        ],
        columns=[
            Column(name="one", type=ColumnType.STR),
            Column(name="two", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A_slow (one TEXT, two TEXT)")
    connection.execute("CREATE TABLE A_fast (one TEXT)")
    mocker.patch("djqs.models.database.create_engine", return_value=engine)

    child = Node(
        name="B",
        query="SELECT COUNT(*) AS cnt FROM A WHERE two = 'test'",
        parents=[parent],
    )

    space = " "

    assert (
        query_to_string(get_select_for_node(child, database))
        == f"""SELECT count('*') AS cnt{space}
FROM (SELECT "A_slow".one AS one, "A_slow".two AS two{space}
FROM "A_slow") AS "A"{space}
WHERE "A".two = test"""
    )


def test_get_select_for_node_projection(mocker: MockerFixture) -> None:
    """
    Test ``get_select_for_node`` with a column projection.
    """
    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="one", type=ColumnType.STR),
            Column(name="two", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("djqs.models.database.create_engine", return_value=engine)

    child = Node(
        name="B",
        query="SELECT one, MAX(two), 3, 4.0, 'five' FROM A",
        parents=[parent],
    )

    space = " "

    assert (
        query_to_string(get_select_for_node(child, database))
        == f'''SELECT "A".one, max("A".two) AS max_1, 3, 4.0, five{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )


def test_get_select_for_node_where(mocker: MockerFixture) -> None:
    """
    Test ``get_select_for_node`` with a where clause.
    """
    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="one", type=ColumnType.STR),
            Column(name="two", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("djqs.models.database.create_engine", return_value=engine)

    child = Node(
        name="B",
        query="SELECT one, MAX(two), 3, 4.0, 'five' FROM A WHERE one > 10",
        parents=[parent],
    )

    space = " "

    assert (
        query_to_string(get_select_for_node(child, database))
        == f"""SELECT "A".one, max("A".two) AS max_1, 3, 4.0, five{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"{space}
WHERE "A".one > 10"""
    )


def test_get_select_for_node_groupby(mocker: MockerFixture) -> None:
    """
    Test ``get_select_for_node`` with a group by clause.
    """
    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="one", type=ColumnType.STR),
            Column(name="two", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("djqs.models.database.create_engine", return_value=engine)

    child = Node(
        name="B",
        query="SELECT one, MAX(two) FROM A GROUP BY one",
        parents=[parent],
    )

    space = " "

    assert (
        query_to_string(get_select_for_node(child, database))
        == f"""SELECT "A".one, max("A".two) AS max_1{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A" GROUP BY "A".one"""
    )


def test_get_select_for_node_limit(mocker: MockerFixture) -> None:
    """
    Test ``get_select_for_node`` with a limit clause.
    """
    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="one", type=ColumnType.STR),
            Column(name="two", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("djqs.models.database.create_engine", return_value=engine)

    child = Node(
        name="B",
        query="SELECT one, MAX(two) FROM A LIMIT 10",
        parents=[parent],
    )

    space = " "

    assert (
        query_to_string(get_select_for_node(child, database))
        == f"""SELECT "A".one, max("A".two) AS max_1{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"
 LIMIT 10 OFFSET 0"""
    )


def test_get_query_with_no_parents() -> None:
    """
    Test ``get_query`` on a query without parents.
    """
    database = Database(id=1, name="sqlite", URI="sqlite://")
    tree = parse_sql("SELECT 1 AS foo, 'two' AS bar", dialect="ansi")
    assert (
        query_to_string(get_query(None, [], tree, database))
        == "SELECT 1 AS foo, 'two' AS bar"
    )


def test_get_query_invalid() -> None:
    """
    Test ``get_query`` on an invalid query.
    """
    database = Database(id=1, name="sqlite", URI="sqlite://")

    tree = parse_sql("SELECT foo", dialect="ansi")
    with pytest.raises(Exception) as excinfo:
        get_query(None, [], tree, database)
    assert str(excinfo.value) == "Unable to return identifier without a source"

    tree = parse_sql("SELECT foo.bar", dialect="ansi")
    with pytest.raises(Exception) as excinfo:
        get_query(None, [], tree, database)
    assert str(excinfo.value) == "Unable to return identifier without a source"


def test_get_value() -> None:
    """
    Test ``get_value``.
    """
    assert get_value({"Number": ("1", False)}) == 1
    assert get_value({"Number": ("2.0", False)}) == 2.0
    assert str(get_value({"SingleQuotedString": "test"})) == "test"
    assert get_value({"Boolean": True})
    assert not get_value({"Boolean": False})


def test_get_select_for_node_with_join(mocker: MockerFixture) -> None:
    """
    Test ``get_select_for_node`` when the node query has a join.
    """
    database = Database(id=1, name="db", URI="sqlite://")

    parent_1 = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="one", type=ColumnType.STR),
            Column(name="two", type=ColumnType.STR),
        ],
    )
    parent_2 = Node(
        name="B",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[
                    Column(name="two", type=ColumnType.STR),
                    Column(name="three", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="two", type=ColumnType.STR),
            Column(name="three", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    connection.execute("CREATE TABLE B (two TEXT, three TEXT)")
    mocker.patch("djqs.models.database.create_engine", return_value=engine)

    child = Node(
        name="B",
        query="SELECT COUNT(*) FROM A JOIN B ON A.two = B.two WHERE B.three > 1",
        parents=[parent_1, parent_2],
    )

    space = " "

    assert (
        query_to_string(get_select_for_node(child, database))
        == f"""SELECT count('*') AS count_1{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A" JOIN (SELECT "B".two AS two, "B".three AS three{space}
FROM "B") AS "B" ON "A".two = "B".two{space}
WHERE "B".three > 1"""
    )


def test_date_trunc() -> None:
    """
    Test transpiling the ``DATE_TRUNC`` function.
    """
    engine = create_engine("sqlite://")
    connection = engine.connect()
    connection.execute("CREATE TABLE A (col TEXT)")
    source = select(SqlaTable("A", MetaData(bind=engine), autoload=True))

    function: Function = {
        "args": [
            {"Unnamed": {"Expr": {"Value": {"SingleQuotedString": "second"}}}},
            {
                "Unnamed": {
                    "Expr": {"Identifier": {"quote_style": None, "value": "col"}},
                },
            },
        ],
        "distinct": False,
        "name": [{"quote_style": None, "value": "DATE_TRUNC"}],
        "over": None,
    }

    assert (
        query_to_string(get_function(function, source, dialect="postgresql"))
        == "date_trunc('second', anon_1.col)"
    )


def test_date_trunc_invalid() -> None:
    """
    Test invalid uses of ``DATE_TRUNC``.
    """
    engine = create_engine("sqlite://")
    connection = engine.connect()
    connection.execute("CREATE TABLE A (col TEXT)")
    source = select(SqlaTable("A", MetaData(bind=engine), autoload=True))

    function: Function = {
        "args": [
            {"Unnamed": {"Expr": {"Value": {"SingleQuotedString": "second"}}}},
            {
                "Unnamed": {
                    "Expr": {"Identifier": {"quote_style": None, "value": "col"}},
                },
            },
        ],
        "distinct": False,
        "name": [{"quote_style": None, "value": "DATE_TRUNC"}],
        "over": None,
    }

    with pytest.raises(Exception) as excinfo:
        query_to_string(get_function(function, source))
    assert str(excinfo.value) == "A dialect is needed for `DATE_TRUNC`"

    with pytest.raises(Exception) as excinfo:
        query_to_string(get_function(function, source, dialect="unknown"))
    assert (
        str(excinfo.value)
        == """Dialect "unknown" doesn't support `DATE_TRUNC`
The following error happened:
- The function "DATE_TRUNC" hasn't been implemented for dialect "unknown" in DJ yet. You can file an issue at https://github.com/DataJunction/djqs/issues/new?title=DATE_TRUNC+for+unknown to request it to be added, or use the documentation at https://github.com/DataJunction/djqs/blob/main/docs/functions.rst#date_trunc to implement it. (error code: 1)"""
    )

    function = {
        "args": [
            {"Unnamed": {"Expr": {"Value": {"SingleQuotedString": "invalid"}}}},
            {
                "Unnamed": {
                    "Expr": {"Identifier": {"quote_style": None, "value": "col"}},
                },
            },
        ],
        "distinct": False,
        "name": [{"quote_style": None, "value": "DATE_TRUNC"}],
        "over": None,
    }
    with pytest.raises(Exception) as excinfo:
        query_to_string(get_function(function, source, dialect="sqlite"))
    assert (
        str(excinfo.value)
        == """Resolution "invalid" not supported by dialect "sqlite"
The following error happened:
- The resolution "invalid" in the `DATE_TRUNC` function hasn't been implemented in DJ for the dialect "sqlite" yet. You can file an issue at https://github.com/DataJunction/djqs/issues/new?title=Resolution+missing+for+sqlite:+invalid to request it to be added, or use the documentation at https://github.com/DataJunction/djqs/blob/main/docs/functions.rst#date_trunc to implement it. (error code: 1)"""
    )

    with pytest.raises(Exception) as excinfo:
        query_to_string(get_function(function, source, dialect="druid"))
    assert (
        str(excinfo.value)
        == """Resolution "invalid" not supported by dialect "druid"
The following error happened:
- The resolution "invalid" in the `DATE_TRUNC` function hasn't been implemented in DJ for the dialect "druid" yet. You can file an issue at https://github.com/DataJunction/djqs/issues/new?title=Resolution+missing+for+druid:+invalid to request it to be added, or use the documentation at https://github.com/DataJunction/djqs/blob/main/docs/functions.rst#date_trunc to implement it. (error code: 1)"""
    )


@pytest.mark.parametrize(
    "resolution,expected",
    [
        # simple ones
        ("second", "datetime(strftime('%Y-%m-%dT%H:%M:%S', anon_1.col))"),
        ("minute", "datetime(strftime('%Y-%m-%dT%H:%M:00', anon_1.col))"),
        ("hour", "datetime(strftime('%Y-%m-%dT%H:00:00', anon_1.col))"),
        # simpler ones
        ("day", "datetime(anon_1.col, 'start of day')"),
        ("month", "datetime(anon_1.col, 'start of month')"),
        ("year", "datetime(anon_1.col, 'start of year')"),
        # weird ones
        (
            "week",
            "datetime(anon_1.col, '1 day', 'weekday 0', '-7 days', 'start of day')",
        ),
        (
            "quarter",
            "datetime(anon_1.col, 'start of month', "
            "printf('-%d month', (strftime('%m', anon_1.col) - 1) % 3))",
        ),
    ],
)
def test_date_trunc_sqlite(resolution: str, expected: str) -> None:
    """
    Tests for the SQLite version of ``DATE_TRUNC``.
    """
    engine = create_engine("sqlite://")
    connection = engine.connect()
    connection.execute("CREATE TABLE A (col TEXT)")
    source = select(SqlaTable("A", MetaData(bind=engine), autoload=True))

    function: Function = {
        "args": [
            {"Unnamed": {"Expr": {"Value": {"SingleQuotedString": resolution}}}},
            {
                "Unnamed": {
                    "Expr": {"Identifier": {"quote_style": None, "value": "col"}},
                },
            },
        ],
        "distinct": False,
        "name": [{"quote_style": None, "value": "DATE_TRUNC"}],
        "over": None,
    }

    assert query_to_string(get_function(function, source, dialect="sqlite")) == expected


@pytest.mark.parametrize(
    "resolution,expected",
    [
        ("second", "time_floor(CAST(anon_1.col AS TIMESTAMP), 'PT1S')"),
        ("minute", "time_floor(CAST(anon_1.col AS TIMESTAMP), 'PT1M')"),
        ("hour", "time_floor(CAST(anon_1.col AS TIMESTAMP), 'PT1H')"),
        ("day", "time_floor(CAST(anon_1.col AS TIMESTAMP), 'P1D')"),
        ("week", "time_floor(CAST(anon_1.col AS TIMESTAMP), 'P1W')"),
        ("month", "time_floor(CAST(anon_1.col AS TIMESTAMP), 'P1M')"),
        ("quarter", "time_floor(CAST(anon_1.col AS TIMESTAMP), 'P3M')"),
        ("year", "time_floor(CAST(anon_1.col AS TIMESTAMP), 'P1Y')"),
    ],
)
def test_date_trunc_druid(resolution: str, expected: str) -> None:
    """
    Tests for the Druid version of ``DATE_TRUNC``.
    """
    engine = create_engine("sqlite://")
    connection = engine.connect()
    connection.execute("CREATE TABLE A (col TEXT)")
    source = select(SqlaTable("A", MetaData(bind=engine), autoload=True))

    function: Function = {
        "args": [
            {"Unnamed": {"Expr": {"Value": {"SingleQuotedString": resolution}}}},
            {
                "Unnamed": {
                    "Expr": {"Identifier": {"quote_style": None, "value": "col"}},
                },
            },
        ],
        "distinct": False,
        "name": [{"quote_style": None, "value": "DATE_TRUNC"}],
        "over": None,
    }

    assert query_to_string(get_function(function, source, dialect="druid")) == expected


def test_case() -> None:
    """
    Test for the ``CASE`` statement.
    """
    engine = create_engine("sqlite://")
    connection = engine.connect()
    connection.execute("CREATE TABLE A (col TEXT)")
    source = select(SqlaTable("A", MetaData(bind=engine), autoload=True))

    expression: Expression = {
        "Case": {
            "conditions": [
                {
                    "BinaryOp": {
                        "left": {"Identifier": {"quote_style": None, "value": "col"}},
                        "op": "Eq",
                        "right": {"Value": {"Number": ("1", False)}},
                    },
                },
                {
                    "BinaryOp": {
                        "left": {"Identifier": {"quote_style": None, "value": "col"}},
                        "op": "Eq",
                        "right": {"Value": {"Number": ("2", False)}},
                    },
                },
            ],
            "else_result": {"Value": {"SingleQuotedString": "a " "lot"}},
            "operand": None,
            "results": [
                {"Value": {"SingleQuotedString": "one"}},
                {"Value": {"SingleQuotedString": "two"}},
            ],
        },
    }
    assert query_to_string(get_expression(expression, source)) == (
        "CASE WHEN (anon_1.col = 1) THEN one WHEN (anon_1.col = 2) "
        "THEN two ELSE a lot END"
    )


def test_unary_op() -> None:
    """
    Test for unary operators.
    """
    engine = create_engine("sqlite://")
    connection = engine.connect()
    connection.execute("CREATE TABLE A (col TEXT)")
    source = select(SqlaTable("A", MetaData(bind=engine), autoload=True))

    expression: Expression = {
        "UnaryOp": {"op": "Minus", "expr": {"Value": {"Number": ("2.0", False)}}},
    }
    assert get_expression(expression, source) == -2.0


def test_wildcard_projection_unimplemented() -> None:
    """
    Test for projection with a wildcard.
    """
    database = Database(id=1, name="sqlite", URI="sqlite://")
    tree = parse_sql("SELECT * FROM a", dialect="ansi")
    with pytest.raises(NotImplementedError) as excinfo:
        get_query(None, [], tree, database)
    assert str(excinfo.value) == "Expand wildcard into a list of columns"
