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
    get_nodes_with_common_dimensions,
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
        node_b.name,
        node_d.name,
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
                "default.num_unique_hard_hats_approx",
                "default.avg_repair_price",
                "default.total_repair_cost",
            ],
        )

        # BFS
        min_fanout_settings = MagicMock()
        min_fanout_settings.fanout_threshold = 1
        min_fanout_settings.reader_db.pool_size = 20
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
        max_fanout_settings.reader_db.pool_size = 20
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
        max_node_list_settings.reader_db.pool_size = 20
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

        # Test deactivated
        await module__client_with_roads.delete(
            "/nodes/default.regional_repair_efficiency",
        )
        with patch("datajunction_server.sql.dag.settings", min_fanout_settings):
            downstreams = await get_downstream_nodes(
                module__session,
                "default.repair_orders",
                include_deactivated=True,
            )
            assert {ds.name for ds in downstreams} == expected_nodes

        # Test cubes
        await module__client_with_roads.post(
            "/nodes/cube/",
            json={
                "metrics": ["default.num_repair_orders", "default.avg_repair_price"],
                "dimensions": ["default.hard_hat.country"],
                "filters": ["default.hard_hat.state='AZ'"],
                "description": "Cube of various metrics related to repairs",
                "mode": "published",
                "name": "default.repairs_cube",
            },
        )
        with patch("datajunction_server.sql.dag.settings", min_fanout_settings):
            downstreams = await get_downstream_nodes(
                module__session,
                "default.repair_orders",
                include_cubes=False,
                include_deactivated=False,
            )
            assert {ds.name for ds in downstreams} == expected_nodes - {
                "default.regional_repair_efficiency",
            }

        with patch("datajunction_server.sql.dag.settings", min_fanout_settings):
            downstreams = await get_downstream_nodes(
                module__session,
                "default.repair_orders",
                node_type=NodeType.METRIC,
            )
            assert {ds.name for ds in downstreams} == {
                "default.avg_repair_order_discounts",
                "default.avg_repair_price",
                "default.avg_time_to_dispatch",
                "default.discounted_orders_rate",
                "default.num_repair_orders",
                "default.num_unique_hard_hats_approx",
                "default.regional_repair_efficiency",
                "default.total_repair_cost",
                "default.total_repair_order_discounts",
            }


@pytest.mark.asyncio
class TestGetNodesWithCommonDimensions:
    """
    Tests for ``get_nodes_with_common_dimensions``.
    """

    @pytest.fixture
    async def common_dimensions_test_graph(
        self,
        session: AsyncSession,
        current_user: User,
    ):
        """
        Creates a test graph with dimensions and nodes linked via reference links
        and join dimension links.
        """
        # Create dimensions
        dim1 = Node(
            name="default.dim1",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        dim1_rev = NodeRevision(
            node=dim1,
            type=dim1.type,
            name=dim1.name,
            version="1",
            display_name="Dimension 1",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="attribute1", type=StringType(), order=1),
            ],
        )
        dim1.current = dim1_rev

        dim2 = Node(
            name="default.dim2",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        dim2_rev = NodeRevision(
            node=dim2,
            type=dim2.type,
            name=dim2.name,
            version="1",
            display_name="Dimension 2",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="attribute2", type=StringType(), order=1),
            ],
        )
        dim2.current = dim2_rev

        dim3 = Node(
            name="default.dim3",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        dim3_rev = NodeRevision(
            node=dim3,
            type=dim3.type,
            name=dim3.name,
            version="1",
            display_name="Dimension 3",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
            ],
        )
        dim3.current = dim3_rev

        session.add_all([dim1, dim1_rev, dim2, dim2_rev, dim3, dim3_rev])
        await session.flush()

        # Create source node linked to dim1 and dim2 via reference links
        source1 = Node(
            name="default.source1",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        source1_rev = NodeRevision(
            node=source1,
            type=source1.type,
            name=source1.name,
            version="1",
            display_name="Source 1",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(
                    name="dim1_id",
                    type=IntegerType(),
                    dimension=dim1,
                    dimension_column="id",
                    order=1,
                ),
                Column(
                    name="dim2_id",
                    type=IntegerType(),
                    dimension=dim2,
                    dimension_column="id",
                    order=2,
                ),
            ],
        )
        source1.current = source1_rev

        # Create source node linked to only dim1 via reference link
        source2 = Node(
            name="default.source2",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        source2_rev = NodeRevision(
            node=source2,
            type=source2.type,
            name=source2.name,
            version="1",
            display_name="Source 2",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(
                    name="dim1_id",
                    type=IntegerType(),
                    dimension=dim1,
                    dimension_column="id",
                    order=1,
                ),
            ],
        )
        source2.current = source2_rev

        # Create transform node linked to dim1 and dim2 via join dimension link
        transform1 = Node(
            name="default.transform1",
            type=NodeType.TRANSFORM,
            current_version="1",
            created_by_id=current_user.id,
        )
        transform1_rev = NodeRevision(
            node=transform1,
            type=transform1.type,
            name=transform1.name,
            version="1",
            display_name="Transform 1",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="value", type=IntegerType(), order=1),
            ],
        )
        transform1.current = transform1_rev

        # Create a source node linked to dim2 and dim3 (will be parent of metric1)
        source3 = Node(
            name="default.source3",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        source3_rev = NodeRevision(
            node=source3,
            type=source3.type,
            name=source3.name,
            version="1",
            display_name="Source 3",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(
                    name="dim2_id",
                    type=IntegerType(),
                    dimension=dim2,
                    dimension_column="id",
                    order=1,
                ),
                Column(
                    name="dim3_id",
                    type=IntegerType(),
                    dimension=dim3,
                    dimension_column="id",
                    order=2,
                ),
                Column(name="amount", type=IntegerType(), order=3),
            ],
        )
        source3.current = source3_rev

        session.add_all(
            [
                source1,
                source1_rev,
                source2,
                source2_rev,
                transform1,
                transform1_rev,
                source3,
                source3_rev,
            ],
        )
        await session.flush()

        # Create metric node - metrics don't have direct dimension links,
        # they inherit dimensions from their parent nodes
        metric1 = Node(
            name="default.metric1",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        metric1_rev = NodeRevision(
            node=metric1,
            type=metric1.type,
            name=metric1.name,
            version="1",
            display_name="Metric 1",
            query="SELECT SUM(amount) FROM default.source3",
            parents=[source3],  # Metric inherits dim2 and dim3 from source3
            created_by_id=current_user.id,
            columns=[
                Column(name="value", type=IntegerType(), order=0),
            ],
        )
        metric1.current = metric1_rev

        session.add_all([metric1, metric1_rev])
        await session.flush()

        # Create dimension links for transform1 (links to dim1 and dim2)
        link1 = DimensionLink(
            dimension_id=dim1.id,
            node_revision_id=transform1_rev.id,
            join_sql="default.transform1.id = default.dim1.id",
        )
        link2 = DimensionLink(
            dimension_id=dim2.id,
            node_revision_id=transform1_rev.id,
            join_sql="default.transform1.id = default.dim2.id",
        )

        session.add_all([link1, link2])
        await session.commit()

        return {
            "dim1": dim1,
            "dim2": dim2,
            "dim3": dim3,
            "source1": source1,
            "source2": source2,
            "source3": source3,
            "transform1": transform1,
            "metric1": metric1,
        }

    @pytest.mark.asyncio
    async def test_empty_dimensions_list(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test that an empty dimensions list returns an empty result.
        """
        result = await get_nodes_with_common_dimensions(session, [])
        assert result == []

    @pytest.mark.asyncio
    async def test_single_dimension_via_column(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test finding nodes linked to a single dimension via reference link.
        """
        graph = common_dimensions_test_graph
        result = await get_nodes_with_common_dimensions(session, [graph["dim1"]])
        result_names = {node.name for node in result}

        # source1 and source2 are linked to dim1 via reference links
        # transform1 is linked to dim1 via join link
        assert "default.source1" in result_names
        assert "default.source2" in result_names
        assert "default.transform1" in result_names

    @pytest.mark.asyncio
    async def test_single_dimension_via_dimension_link(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test finding nodes linked to a single dimension via join link.
        """
        graph = common_dimensions_test_graph
        result = await get_nodes_with_common_dimensions(session, [graph["dim3"]])
        result_names = {node.name for node in result}
        assert result_names == {"default.metric1", "default.source3"}

    @pytest.mark.asyncio
    async def test_multiple_dimensions_intersection(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test finding nodes linked to multiple dimensions (intersection).
        """
        graph = common_dimensions_test_graph
        result = await get_nodes_with_common_dimensions(
            session,
            [graph["dim1"], graph["dim2"]],
        )
        result_names = {node.name for node in result}

        # source1 is linked to both dim1 and dim2 via reference links
        # transform1 is linked to both dim1 and dim2 via join links
        # source2 is only linked to dim1, so should not be included
        assert "default.source1" in result_names
        assert "default.transform1" in result_names
        assert "default.source2" not in result_names

    @pytest.mark.asyncio
    async def test_no_common_dimensions(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test that no nodes are returned when no nodes share all dimensions.
        """
        graph = common_dimensions_test_graph
        # dim1 and dim3 have no nodes in common
        result = await get_nodes_with_common_dimensions(
            session,
            [graph["dim1"], graph["dim3"]],
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_filter_by_node_type(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test filtering results by node type.
        """
        graph = common_dimensions_test_graph
        # Get only source nodes linked to dim1
        result = await get_nodes_with_common_dimensions(
            session,
            [graph["dim1"]],
            node_types=[NodeType.SOURCE],
        )
        result_names = {node.name for node in result}

        assert "default.source1" in result_names
        assert "default.source2" in result_names
        assert "default.transform1" not in result_names

    @pytest.mark.asyncio
    async def test_filter_by_multiple_node_types(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test filtering results by multiple node types.
        """
        graph = common_dimensions_test_graph
        result = await get_nodes_with_common_dimensions(
            session,
            [graph["dim2"]],
            node_types=[NodeType.TRANSFORM, NodeType.SOURCE],
        )
        result_names = {node.name for node in result}
        assert result_names == {
            # transform1 is linked to dim2 via join link
            "default.transform1",
            # source1 and source3 are linked to dim2 via reference links
            "default.source1",
            "default.source3",
        }

    @pytest.mark.asyncio
    async def test_mixed_column_and_dimension_link(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test that nodes linked via both Column and DimensionLink are found,
        including metrics that inherit dimensions from their parents.
        """
        graph = common_dimensions_test_graph
        result = await get_nodes_with_common_dimensions(session, [graph["dim2"]])
        result_names = {node.name for node in result}

        # source1 linked via reference link
        # source3 linked via reference link
        # transform1 linked via join link
        # metric1 inherits dim2 from source3 (its parent)
        assert result_names == {
            "default.source1",
            "default.source3",
            "default.transform1",
            "default.metric1",
        }

    @pytest.mark.asyncio
    async def test_metrics_inherit_dimensions_from_parents(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test that metrics inherit dimensions from their parent nodes.
        Metrics don't have direct dimension links, but they should be included
        in results when their parents have all the required dimensions.
        """
        graph = common_dimensions_test_graph
        # dim2 and dim3 are the dimensions that source3 (metric1's parent) has
        result = await get_nodes_with_common_dimensions(
            session,
            [graph["dim2"], graph["dim3"]],
        )
        result_names = {node.name for node in result}
        assert result_names == {"default.source3", "default.metric1"}

    @pytest.mark.asyncio
    async def test_filter_metrics_by_node_type(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test that metrics can be filtered by node type even when they
        inherit dimensions from parents.
        """
        graph = common_dimensions_test_graph
        result = await get_nodes_with_common_dimensions(
            session,
            [graph["dim2"], graph["dim3"]],
            node_types=[NodeType.METRIC],
        )
        result_names = {node.name for node in result}
        assert result_names == {"default.metric1"}

    @pytest.mark.asyncio
    async def test_metric_parent_missing_dimension(
        self,
        session: AsyncSession,
        common_dimensions_test_graph,
    ):
        """
        Test that metrics are not included if their parent doesn't have
        all required dimensions.
        """
        graph = common_dimensions_test_graph
        result = await get_nodes_with_common_dimensions(
            session,
            [graph["dim1"], graph["dim2"], graph["dim3"]],
        )
        result_names = {node.name for node in result}
        assert result_names == set()

    @pytest.fixture
    async def dimension_hierarchy_graph(
        self,
        session: AsyncSession,
        current_user: User,
    ):
        """
        Creates a test graph with a dimension hierarchy:
        transform1 -> dim_a -> dim_b -> dim_c

        When searching for nodes with dim_c, the transform should be found
        because it transitively links to dim_c through the hierarchy.
        """
        # Create dimension C (leaf)
        dim_c = Node(
            name="default.dim_c",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        dim_c_rev = NodeRevision(
            node=dim_c,
            type=dim_c.type,
            name=dim_c.name,
            version="1",
            display_name="Dimension C",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="attr_c", type=StringType(), order=1),
            ],
        )
        dim_c.current = dim_c_rev

        session.add_all([dim_c, dim_c_rev])
        await session.flush()

        # Create dimension B (links to C)
        dim_b = Node(
            name="default.dim_b",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        dim_b_rev = NodeRevision(
            node=dim_b,
            type=dim_b.type,
            name=dim_b.name,
            version="1",
            display_name="Dimension B",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="attr_b", type=StringType(), order=1),
            ],
        )
        dim_b.current = dim_b_rev

        session.add_all([dim_b, dim_b_rev])
        await session.flush()

        # Create dimension A (links to B)
        dim_a = Node(
            name="default.dim_a",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        dim_a_rev = NodeRevision(
            node=dim_a,
            type=dim_a.type,
            name=dim_a.name,
            version="1",
            display_name="Dimension A",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="attr_a", type=StringType(), order=1),
            ],
        )
        dim_a.current = dim_a_rev

        session.add_all([dim_a, dim_a_rev])
        await session.flush()

        # Create transform that links to dim_a
        transform1 = Node(
            name="default.transform_hier",
            type=NodeType.TRANSFORM,
            current_version="1",
            created_by_id=current_user.id,
        )
        transform1_rev = NodeRevision(
            node=transform1,
            type=transform1.type,
            name=transform1.name,
            version="1",
            display_name="Transform Hierarchy",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="value", type=IntegerType(), order=1),
            ],
        )
        transform1.current = transform1_rev

        session.add_all([transform1, transform1_rev])
        await session.flush()

        # Create metric that has transform1 as parent
        metric1 = Node(
            name="default.metric_hier",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        metric1_rev = NodeRevision(
            node=metric1,
            type=metric1.type,
            name=metric1.name,
            version="1",
            display_name="Metric Hierarchy",
            query="SELECT SUM(value) FROM default.transform_hier",
            parents=[transform1],
            created_by_id=current_user.id,
            columns=[
                Column(name="total", type=IntegerType(), order=0),
            ],
        )
        metric1.current = metric1_rev

        session.add_all([metric1, metric1_rev])
        await session.flush()

        # Create dimension links to form the hierarchy:
        # transform1 -> dim_a -> dim_b -> dim_c
        link_transform_to_a = DimensionLink(
            dimension_id=dim_a.id,
            node_revision_id=transform1_rev.id,
            join_sql="default.transform_hier.id = default.dim_a.id",
        )
        link_a_to_b = DimensionLink(
            dimension_id=dim_b.id,
            node_revision_id=dim_a_rev.id,
            join_sql="default.dim_a.id = default.dim_b.id",
        )
        link_b_to_c = DimensionLink(
            dimension_id=dim_c.id,
            node_revision_id=dim_b_rev.id,
            join_sql="default.dim_b.id = default.dim_c.id",
        )

        session.add_all([link_transform_to_a, link_a_to_b, link_b_to_c])
        await session.commit()

        return {
            "dim_a": dim_a,
            "dim_b": dim_b,
            "dim_c": dim_c,
            "transform1": transform1,
            "metric1": metric1,
        }

    @pytest.mark.asyncio
    async def test_dimension_hierarchy_traversal(
        self,
        session: AsyncSession,
        dimension_hierarchy_graph,
    ):
        """
        Test that searching for a dimension at the end of a hierarchy
        finds nodes that link to dimensions earlier in the chain.

        Hierarchy: transform1 -> dim_a -> dim_b -> dim_c
        Searching for dim_c should find transform1 (and metric1 via parent).
        """
        graph = dimension_hierarchy_graph

        # Search for dim_c - should find transform1 through the hierarchy
        result = await get_nodes_with_common_dimensions(session, [graph["dim_c"]])
        result_names = {node.name for node in result}

        # transform1 links to dim_a, dim_a links to dim_b, dim_b links to dim_c
        # So transform1 transitively "has" dim_c
        assert "default.transform_hier" in result_names
        # metric1 inherits from transform1
        assert "default.metric_hier" in result_names
        # dim_a and dim_b are also in the path
        assert "default.dim_a" in result_names
        assert "default.dim_b" in result_names

    @pytest.mark.asyncio
    async def test_dimension_hierarchy_middle(
        self,
        session: AsyncSession,
        dimension_hierarchy_graph,
    ):
        """
        Test searching for a dimension in the middle of a hierarchy.

        Hierarchy: transform1 -> dim_a -> dim_b -> dim_c
        Searching for dim_b should find transform1 and dim_a.
        """
        graph = dimension_hierarchy_graph

        result = await get_nodes_with_common_dimensions(session, [graph["dim_b"]])
        result_names = {node.name for node in result}

        # transform1 links to dim_a, dim_a links to dim_b
        assert "default.transform_hier" in result_names
        assert "default.metric_hier" in result_names
        assert "default.dim_a" in result_names

        # dim_c is after dim_b in the chain, so shouldn't be found
        assert "default.dim_c" not in result_names
