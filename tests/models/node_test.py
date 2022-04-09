"""
Tests for ``datajunction.models.node``.
"""

# pylint: disable=use-implicit-booleaness-not-comparison

from sqlmodel import Session

from datajunction.models.column import Column
from datajunction.models.database import Database
from datajunction.models.node import Node
from datajunction.models.table import Table
from datajunction.typing import ColumnType


def test_node_relationship(session: Session) -> None:
    """
    Test the n:n self-referential relationships.
    """
    node_a = Node(name="A")
    node_b = Node(name="B")
    node_c = Node(name="C", parents=[node_a, node_b])

    session.add(node_c)

    assert node_a.children == [node_c]
    assert node_b.children == [node_c]
    assert node_c.children == []

    assert node_a.parents == []
    assert node_b.parents == []
    assert node_c.parents == [node_a, node_b]


def test_node_columns(session: Session) -> None:
    """
    Test that the node schema is derived from its tables.
    """
    database = Database(name="test", URI="sqlite://")

    table_a = Table(
        database_id=database.id,
        table="A",
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT),
        ],
    )

    table_b = Table(
        database_id=database.id,
        table="B",
        columns=[Column(name="ds", type=ColumnType.DATETIME)],
    )

    node = Node(name="C", tables=[table_a, table_b])

    session.add(node)

    assert node.columns == [
        Column(name="ds", type=ColumnType.DATETIME),
        Column(name="user_id", type=ColumnType.INT),
    ]


def test_node_schema_downstream_nodes(session: Session) -> None:
    """
    Test computing the schema of downstream nodes.
    """

    node_a = Node(
        name="A",
        tables=[
            Table(
                database=Database(name="test", URI="sqlite://"),
                table="A",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="foo", type=ColumnType.FLOAT),
                ],
            ),
        ],
    )

    node_b = Node(
        name="B",
        expression="SELECT ds, COUNT(*) AS cnt, MAX(foo) FROM A GROUP BY ds",
        parents=[node_a],
    )

    session.add(node_b)

    assert node_b.columns == [
        Column(name="ds", type=ColumnType.STR),
        Column(name="cnt", type=ColumnType.INT),
        Column(name="_col0", type=ColumnType.FLOAT),
    ]
