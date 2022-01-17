"""
Tests for ``datajunction.sql.transpile``.
"""

from pytest_mock import MockerFixture
from sqlalchemy.engine import create_engine

from datajunction.models.database import Column, Database, Table
from datajunction.models.node import Node
from datajunction.models.query import Query  # pylint: disable=unused-import
from datajunction.sql.transpile import get_query_for_node


def test_get_query_for_node_materialized(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node`` when the node is materialized.
    """
    database_1 = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(name="A")

    child = Node(
        name="B",
        tables=[
            Table(
                database=database_1,
                table="B",
                columns=[Column(name="one", type="str")],
            ),
        ],
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    engine = create_engine(database_1.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE B (cnt INTEGER)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    assert str(get_query_for_node(child)) == 'SELECT "B".cnt \nFROM "B"'


def test_get_query_for_node_not_materialized(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node`` when the node is not materialized.
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
                    Column(name="one", type="str"),
                    Column(name="two", type="str"),
                ],
            ),
            Table(
                database=database_2,
                table="A",
                columns=[Column(name="one", type="str")],
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
        str(get_query_for_node(child))
        == f'''SELECT count(?) AS cnt{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )

    # unnamed expression
    child.expression = "SELECT COUNT(*) FROM A"

    assert (
        str(get_query_for_node(child))
        == f'''SELECT count(?) AS count_1{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )
