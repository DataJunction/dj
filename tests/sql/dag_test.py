"""
Tests for ``dj.sql.dag``.
"""

from dj.models.column import Column
from dj.models.database import Database
from dj.models.node import Node, NodeRevision, NodeType
from dj.models.table import Table
from dj.sql.dag import get_dimensions
from dj.typing import ColumnType


def test_get_dimensions() -> None:
    """
    Test ``get_dimensions``.
    """
    database = Database(id=1, name="one", URI="sqlite://")

    dimension_ref = Node(name="B", type=NodeType.DIMENSION, current_version="1")
    dimension = NodeRevision(
        node=dimension_ref,
        version="1",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="attribute", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="attribute", type=ColumnType.STR),
        ],
    )
    dimension_ref.current = dimension

    parent_ref = Node(name="A", current_version="1")
    parent = NodeRevision(
        node=parent_ref,
        version="1",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="b_id", type=ColumnType.INT, dimension=dimension_ref),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="b_id", type=ColumnType.INT, dimension=dimension_ref),
        ],
    )
    parent_ref.current = parent

    child_ref = Node(name="C", current_version="1")
    child = NodeRevision(
        node=child_ref,
        version="1",
        query="SELECT COUNT(*) FROM A",
        parents=[parent_ref],
    )
    child_ref.current = child

    assert get_dimensions(child_ref) == ["B.attribute", "B.id"]
