"""
Tests for ``datajunction.sql.transpile``.
"""

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine import create_engine

from datajunction.models.database import Column, Database, Table
from datajunction.models.node import Node
from datajunction.models.query import Query  # pylint: disable=unused-import
from datajunction.sql.transpile import (
    get_filter,
    get_query_for_node,
    get_select_for_node,
)
from datajunction.typing import ColumnType


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

    assert str(get_select_for_node(child, database)) == 'SELECT "B".cnt \nFROM "B"'


def test_get_select_for_node_not_materialized(mocker: MockerFixture) -> None:
    """
    Test ``get_select_for_node`` when the node is not materialized.
    """
    database_1 = Database(id=1, name="slow", URI="sqlite://", cost=1.0)
    database_2 = Database(id=2, name="fast", URI="sqlite://", cost=0.1)

    parent = Node(
        name="A",
        tables=[
            Table(
                database=database_1,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
            Table(
                database=database_2,
                table="A",
                columns=[Column(name="one", type=ColumnType.STR)],
            ),
        ],
    )

    engine = create_engine(database_1.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    child = Node(
        name="B",
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    space = " "

    assert (
        str(get_select_for_node(child, database_1))
        == f'''SELECT count(?) AS cnt{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )

    # unnamed expression
    child.expression = "SELECT COUNT(*) FROM A"

    assert (
        str(get_select_for_node(child, database_1))
        == f'''SELECT count(?) AS count_1{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )


def test_get_query_projection(mocker: MockerFixture) -> None:
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
        str(get_select_for_node(child, database))
        == f'''SELECT "A".one AS one_1, max("A".two) AS max_1, 3, 4.0, five{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )


def test_get_query_for_node(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node``.
    """
    database_1 = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(name="A")

    child = Node(
        name="B",
        tables=[
            Table(
                database=database_1,
                table="B",
                columns=[Column(name="cnt", type=ColumnType.INT)],
            ),
        ],
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    engine = create_engine(database_1.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE B (cnt INTEGER)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    create_query = get_query_for_node(child, [], [])
    assert create_query.database_id == 1
    assert create_query.submitted_query == 'SELECT "B".cnt \nFROM "B"'


def test_get_query_for_node_no_databases(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node``.
    """
    database_1 = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(name="A")

    child = Node(
        name="B",
        tables=[
            Table(
                database=database_1,
                table="B",
                columns=[Column(name="one", type=ColumnType.STR)],
            ),
        ],
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    mocker.patch("datajunction.sql.transpile.get_computable_databases", return_value=[])

    with pytest.raises(Exception) as excinfo:
        get_query_for_node(child, [], [])
    assert str(excinfo.value) == "Unable to compute B (no common database)"


def test_get_filter(mocker: MockerFixture) -> None:
    """
    Test ``get_filter``.
    """
    greater_than = mocker.MagicMock()
    mocker.patch("datajunction.sql.transpile.COMPARISONS", new={">": greater_than})
    column_a = mocker.MagicMock()
    columns = {"a": column_a}

    get_filter(columns, "a>0")
    greater_than.assert_called_with(column_a, 0)

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "invalid")
    assert str(excinfo.value) == "Invalid filter: invalid"

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "b>0")
    assert str(excinfo.value) == "Invalid column name: b"

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "a>=0")
    assert str(excinfo.value) == "Invalid operation: >= (valid: >)"

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "a>open('/etc/passwd').read()")
    assert str(excinfo.value) == "Invalid value: open('/etc/passwd').read()"
