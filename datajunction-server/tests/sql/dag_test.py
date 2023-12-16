"""
Tests for ``datajunction_server.sql.dag``.
"""

from sqlmodel import Session

from datajunction_server.models.column import Column
from datajunction_server.models.database import Database
from datajunction_server.models.node import (
    DimensionAttributeOutput,
    Node,
    NodeRevision,
    NodeType,
)
from datajunction_server.models.table import Table
from datajunction_server.sql.dag import get_dimensions
from datajunction_server.sql.parsing.types import IntegerType, StringType


def test_get_dimensions(session: Session) -> None:
    """
    Test ``get_dimensions``.
    """
    database = Database(id=1, name="one", URI="sqlite://")
    session.add(database)

    dimension_ref = Node(name="B", type=NodeType.DIMENSION, current_version="1")
    dimension = NodeRevision(
        node=dimension_ref,
        display_name="B",
        version="1",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[
                    Column(name="id", type=IntegerType()),
                    Column(name="attribute", type=StringType()),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=IntegerType()),
            Column(name="attribute", type=StringType()),
        ],
    )
    dimension_ref.current = dimension
    session.add(dimension)
    session.add(dimension_ref)

    parent_ref = Node(name="A", current_version="1")
    parent = NodeRevision(
        node=parent_ref,
        display_name="A",
        version="1",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="ds", type=StringType()),
                    Column(name="b_id", type=IntegerType(), dimension=dimension_ref),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=StringType()),
            Column(name="b_id", type=IntegerType(), dimension=dimension_ref),
        ],
    )
    parent_ref.current = parent
    session.add(parent)
    session.add(parent_ref)

    child_ref = Node(name="C", current_version="1", type=NodeType.METRIC)
    child = NodeRevision(
        node=child_ref,
        display_name="C",
        version="1",
        query="SELECT COUNT(*) FROM A",
        parents=[parent_ref],
        type=NodeType.METRIC,
    )
    child_ref.current = child
    session.add(child)
    session.add(child_ref)
    session.commit()

    assert get_dimensions(session, child_ref) == [
        DimensionAttributeOutput(
            name="B.attribute",
            node_name="B",
            node_display_name="B",
            is_primary_key=False,
            type="string",
            path=[],
        ),
        DimensionAttributeOutput(
            name="B.id",
            node_name="B",
            node_display_name="B",
            is_primary_key=False,
            type="int",
            path=[],
        ),
    ]
