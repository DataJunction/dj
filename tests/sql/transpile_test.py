"""
Tests for ``datajunction.sql.transpile``.
"""

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine import create_engine
from sqlalchemy.sql import Select
from sqloxide import parse_sql

from datajunction.models.database import Column, Database, Table
from datajunction.models.node import Node
from datajunction.models.query import Query  # pylint: disable=unused-import
from datajunction.sql.transpile import get_query, get_select_for_node
from datajunction.typing import ColumnType


def query_to_string(query: Select) -> str:
    """
    Helper function to compile a SQLAlchemy query to a string.
    """
    return str(query.compile(compile_kwargs={"literal_binds": True}))


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
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE B (cnt INTEGER)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

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
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    child = Node(
        name="B",
        expression="SELECT COUNT(*) AS cnt FROM A",
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
    child.expression = "SELECT COUNT(*) FROM A"

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
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A_slow (one TEXT, two TEXT)")
    connection.execute("CREATE TABLE A_fast (one TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    child = Node(
        name="B",
        expression="SELECT COUNT(*) AS cnt FROM A WHERE two = 'test'",
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
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    child = Node(
        name="B",
        expression="SELECT one, MAX(two), 3, 4.0, 'five' FROM A",
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
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    child = Node(
        name="B",
        expression="SELECT one, MAX(two), 3, 4.0, 'five' FROM A WHERE one > 10",
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
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    child = Node(
        name="B",
        expression="SELECT one, MAX(two) FROM A GROUP BY one",
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
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    child = Node(
        name="B",
        expression="SELECT one, MAX(two) FROM A LIMIT 10",
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
