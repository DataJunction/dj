"""
Tests for ``datajunction_server.sql.dag``.
"""

from sqlalchemy.orm import Session

from datajunction_server.database.column import Column
from datajunction_server.database.database import Database
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.node import DimensionAttributeOutput, NodeType
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
        name=dimension_ref.name,
        type=dimension_ref.type,
        display_name="B",
        version="1",
        columns=[
            Column(name="id", type=IntegerType(), order=0),
            Column(name="attribute", type=StringType(), order=1),
        ],
    )
    dimension_ref.current = dimension
    session.add(dimension)
    session.add(dimension_ref)

    parent_ref = Node(name="A", current_version="1", type=NodeType.SOURCE)
    parent = NodeRevision(
        node=parent_ref,
        name=parent_ref.name,
        type=parent_ref.type,
        display_name="A",
        version="1",
        columns=[
            Column(name="ds", type=StringType(), order=0),
            Column(name="b_id", type=IntegerType(), dimension=dimension_ref, order=1),
        ],
    )
    parent_ref.current = parent
    session.add(parent)
    session.add(parent_ref)

    child_ref = Node(name="C", current_version="1", type=NodeType.METRIC)
    child = NodeRevision(
        node=child_ref,
        name=child_ref.name,
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
            path=["A.b_id"],
        ),
        DimensionAttributeOutput(
            name="B.id",
            node_name="B",
            node_display_name="B",
            is_primary_key=False,
            type="int",
            path=["A.b_id"],
        ),
    ]
