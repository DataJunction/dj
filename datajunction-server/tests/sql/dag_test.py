"""
Tests for ``datajunction_server.sql.dag``.
"""
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.column import Column
from datajunction_server.database.database import Database
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.models.node import DimensionAttributeOutput, NodeType
from datajunction_server.sql.dag import get_dimensions, topological_sort
from datajunction_server.sql.parsing.types import IntegerType, StringType


@pytest.mark.asyncio
async def test_get_dimensions(session: AsyncSession, current_user: User) -> None:
    """
    Test ``get_dimensions``.
    """
    database = Database(id=1, name="one", URI="sqlite://")
    session.add(database)

    dimension_ref = Node(
        name="B",
        type=NodeType.DIMENSION,
        current_version="1",
        created_by_id=current_user.id,
    )
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
        created_by_id=current_user.id,
    )
    dimension_ref.current = dimension
    session.add(dimension)
    session.add(dimension_ref)

    parent_ref = Node(
        name="A",
        current_version="1",
        type=NodeType.SOURCE,
        created_by_id=current_user.id,
    )
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
        created_by_id=current_user.id,
    )
    parent_ref.current = parent
    session.add(parent)
    session.add(parent_ref)

    child_ref = Node(
        name="C",
        current_version="1",
        type=NodeType.METRIC,
        created_by_id=current_user.id,
    )
    child = NodeRevision(
        node=child_ref,
        name=child_ref.name,
        display_name="C",
        version="1",
        query="SELECT COUNT(*) FROM A",
        parents=[parent_ref],
        type=NodeType.METRIC,
        created_by_id=current_user.id,
    )
    child_ref.current = child
    session.add(child)
    session.add(child_ref)
    await session.commit()

    assert await get_dimensions(session, child_ref) == [
        DimensionAttributeOutput(
            name="B.attribute",
            node_name="B",
            node_display_name="B",
            is_primary_key=False,
            type="string",
            path=["A.b_id"],
            filter_only=False,
        ),
        DimensionAttributeOutput(
            name="B.id",
            node_name="B",
            node_display_name="B",
            is_primary_key=False,
            type="int",
            path=["A.b_id"],
            filter_only=False,
        ),
    ]


@pytest.mark.asyncio
async def test_topological_sort(session: AsyncSession) -> None:
    """
    Test ``topological_sort``.
    """
    node_a = Node(name="test.A", type=NodeType.TRANSFORM)
    node_rev_a = NodeRevision(
        node=node_a,
        name=node_a.name,
        parents=[],
    )
    node_a.current = node_rev_a
    session.add(node_a)
    session.add(node_rev_a)

    node_b = Node(name="test.B", type=NodeType.TRANSFORM)
    node_rev_b = NodeRevision(
        node=node_b,
        name=node_b.name,
        parents=[node_a],
    )
    node_b.current = node_rev_b
    session.add(node_b)
    session.add(node_rev_b)

    node_c = Node(name="test.C", type=NodeType.TRANSFORM)
    node_rev_c = NodeRevision(
        node=node_c,
        name=node_c.name,
        parents=[],
    )
    node_c.current = node_rev_c
    session.add(node_c)
    session.add(node_rev_c)

    node_d = Node(name="test.D", type=NodeType.TRANSFORM)
    node_rev_c.parents = [node_b, node_d]
    node_rev_d = NodeRevision(
        node=node_d,
        name=node_d.name,
        parents=[node_a],
    )
    node_d.current = node_rev_d
    session.add(node_d)
    session.add(node_rev_d)

    node_e = Node(name="test.E", type=NodeType.TRANSFORM)
    node_rev_e = NodeRevision(
        node=node_e,
        name=node_e.name,
        parents=[node_c, node_b],
    )
    node_e.current = node_rev_e
    session.add(node_e)
    session.add(node_rev_e)

    node_f = Node(name="test.F", type=NodeType.TRANSFORM)
    node_rev_d.parents.append(node_f)
    node_rev_f = NodeRevision(
        node=node_f,
        name=node_f.name,
        parents=[node_e],
    )
    node_f.current = node_rev_f
    session.add(node_f)
    session.add(node_rev_f)

    ordering = topological_sort([node_a, node_b, node_c, node_d, node_e])
    assert [node.name for node in ordering] == [
        node_a.name,
        node_d.name,
        node_b.name,
        node_c.name,
        node_e.name,
    ]
    with pytest.raises(DJException) as exc_info:
        topological_sort([node_a, node_b, node_c, node_d, node_e, node_f])
    assert "Graph has at least one cycle" in str(exc_info)
