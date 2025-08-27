"""
Tests for ``datajunction_server.sql.dag``.
"""

import datetime
from unittest.mock import MagicMock, patch
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.column import Column
from datajunction_server.database.database import Database
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.errors import DJException
from datajunction_server.models.node import DimensionAttributeOutput, NodeType
from datajunction_server.sql.dag import (
    get_dimensions,
    get_downstream_nodes,
    topological_sort,
    get_dimension_dag_indegree,
)
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
            properties=[],
            type="string",
            path=["A.b_id"],
            filter_only=False,
        ),
        DimensionAttributeOutput(
            name="B.id",
            node_name="B",
            node_display_name="B",
            properties=[],
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


@pytest.mark.asyncio
class TestGetDimensionDagIndegree:
    """
    Tests for ``get_dimension_dag_indegree``.
    """

    @pytest.fixture(autouse=True)
    async def dimension_test_graph(self, session: AsyncSession, current_user: User):
        """
        Creates a reusable test graph with dimensions and facts.
        """
        dim1 = Node(
            name="default.dim1",
            type=NodeType.DIMENSION,
            created_by_id=current_user.id,
        )
        dim1_rev = NodeRevision(
            node=dim1,
            type=dim1.type,
            name=dim1.name,
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="attribute1", type=StringType(), order=1),
            ],
        )

        dim2 = Node(
            name="default.dim2",
            type=NodeType.DIMENSION,
            created_by_id=current_user.id,
        )
        dim2_rev = NodeRevision(
            node=dim2,
            type=dim2.type,
            name=dim2.name,
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="attribute2", type=StringType(), order=1),
            ],
        )
        dim3 = Node(
            name="default.dim3",
            type=NodeType.DIMENSION,
            created_by_id=current_user.id,
        )
        dim3_rev = NodeRevision(
            node=dim3,
            type=dim3.type,
            name=dim3.name,
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
            ],
        )
        session.add_all([dim1, dim1_rev, dim2, dim2_rev, dim3, dim3_rev])
        await session.flush()

        fact = Node(
            name="default.fact1",
            type=NodeType.TRANSFORM,
            created_by_id=current_user.id,
        )
        fact_rev = NodeRevision(
            node=fact,
            type=fact.type,
            name=fact.name,
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="dim1_id", type=IntegerType(), order=1),
                Column(name="dim2_id", type=IntegerType(), order=2),
            ],
        )

        fact2 = Node(
            name="default.fact2",
            type=NodeType.TRANSFORM,
            created_by_id=current_user.id,
        )
        fact2_rev = NodeRevision(
            node=fact2,
            type=fact2.type,
            name=fact2.name,
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="dim1_id", type=IntegerType(), order=1),
            ],
        )
        session.add_all([fact, fact_rev, fact2, fact2_rev])
        await session.flush()

        link1 = DimensionLink(
            dimension_id=dim1.id,
            node_revision_id=fact_rev.id,
            join_sql="default.fact1.dim1_id = default.dim1.id",
        )
        link2 = DimensionLink(
            dimension_id=dim2.id,
            node_revision_id=fact_rev.id,
            join_sql="default.fact1.dim1_id = default.dim2.id",
        )
        link3 = DimensionLink(
            dimension_id=dim2.id,
            node_revision_id=fact2_rev.id,
            join_sql="default.fact2.dim1_id = default.dim2.id",
        )

        session.add_all([link1, link2, link3])
        await session.commit()

        dim4 = Node(
            name="default.deactivated_dim",
            type=NodeType.DIMENSION,
            created_by_id=current_user.id,
            deactivated_at=datetime.datetime.now(datetime.timezone.utc),
        )
        session.add(dim4)
        await session.commit()

    @pytest.mark.parametrize(
        "node_names, expected",
        [
            # dim1 linked once, dim2 linked twice
            (["default.dim1", "default.dim2"], {"default.dim1": 1, "default.dim2": 2}),
            # dim3 not linked
            (["default.dim3"], {"default.dim3": 0}),
            # Non-dimension node: should return 0
            (["default.fact1"], {"default.fact1": 0}),
            # Nonexistent node: should skip
            (["nonexistent.dim"], {}),
            # Deactivated dimension should not be included
            (
                ["default.dim1", "default.fact1", "default.deactivated_dim"],
                {"default.dim1": 1, "default.fact1": 0},
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_get_dimension_dag_indegree(
        self,
        session: AsyncSession,
        dimension_test_graph,
        node_names: list[str],
        expected: dict[str, int],
    ):
        """
        Check that ``get_dimension_dag_indegree`` returns the correct indegree
        counts for dimension nodes.
        """
        result = await get_dimension_dag_indegree(session, node_names)
        assert result == expected

    @pytest.mark.asyncio
    async def test_node_downstreams_with_fanout(
        self,
        module__session: AsyncSession,
        module__client_with_roads,
    ):
        """
        Test getting downstream nodes with the BFS and recursive CTE approaches yield the same results.
        """
        expected_nodes = set(
            [
                "default.regional_level_agg",
                "default.repair_orders_fact",
                "default.repair_order",
                "default.discounted_orders_rate",
                "default.total_repair_order_discounts",
                "default.avg_repair_order_discounts",
                "default.avg_time_to_dispatch",
                "default.regional_repair_efficiency",
                "default.num_repair_orders",
                "default.avg_repair_price",
                "default.total_repair_cost",
            ],
        )

        # BFS
        min_fanout_settings = MagicMock()
        min_fanout_settings.fanout_threshold = 1
        min_fanout_settings.max_concurrency = 5
        min_fanout_settings.node_list_max = 10000
        with patch("datajunction_server.sql.dag.settings", min_fanout_settings):
            downstreams = await get_downstream_nodes(
                module__session,
                "default.repair_orders",
            )
            assert {ds.name for ds in downstreams} == expected_nodes

            # Only look for downstreams up to depth 1
            downstreams = await get_downstream_nodes(
                module__session,
                "default.repair_orders",
                depth=1,
            )
            assert {ds.name for ds in downstreams} == {
                "default.regional_level_agg",
                "default.repair_order",
                "default.repair_orders_fact",
            }

        # Recursive CTE
        max_fanout_settings = MagicMock()
        max_fanout_settings.fanout_threshold = 100
        max_fanout_settings.max_concurrency = 5
        max_fanout_settings.node_list_max = 10000
        with patch("datajunction_server.sql.dag.settings", max_fanout_settings):
            downstreams = await get_downstream_nodes(
                module__session,
                "default.repair_orders",
            )
            assert {ds.name for ds in downstreams} == expected_nodes

        # Maximum number of downstream nodes returned
        max_node_list_settings = MagicMock()
        max_node_list_settings.fanout_threshold = 1
        max_node_list_settings.max_concurrency = 5
        max_node_list_settings.node_list_max = 5
        with patch("datajunction_server.sql.dag.settings", max_node_list_settings):
            downstreams = await get_downstream_nodes(
                module__session,
                "default.repair_orders",
            )
            assert (
                len({ds.name for ds in downstreams})
                == max_node_list_settings.node_list_max
            )
