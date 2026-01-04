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
    get_metric_parents_map,
    get_nodes_with_common_dimensions,
    get_shared_dimensions,
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


@pytest.mark.asyncio
class TestGetMetricParentsMap:
    """
    Tests for ``get_metric_parents_map``.
    """

    @pytest.fixture
    async def metric_parents_test_graph(
        self,
        session: AsyncSession,
        current_user: User,
    ):
        """
        Creates a test graph with metrics and their parent nodes.

        Graph structure:
        - source1 (SOURCE) <- metric1 (regular metric with single parent)
        - source2, source3 (SOURCE) <- metric2 (regular metric with multiple parents)
        - source4 (SOURCE) <- base_metric (base metric)
        - base_metric <- derived_metric1 (derived metric referencing a base metric)
        - base_metric <- derived_metric2 (another derived metric referencing the same base)
        - source5, source6 (SOURCE) <- base_metric2 (base metric with multiple parents)
        - base_metric2 <- derived_metric3 (derived metric with multi-parent base)
        """
        # Create source nodes
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
                Column(name="value", type=IntegerType(), order=1),
            ],
        )
        source1.current = source1_rev

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
                Column(name="amount", type=IntegerType(), order=1),
            ],
        )
        source2.current = source2_rev

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
                Column(name="count", type=IntegerType(), order=1),
            ],
        )
        source3.current = source3_rev

        source4 = Node(
            name="default.source4",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        source4_rev = NodeRevision(
            node=source4,
            type=source4.type,
            name=source4.name,
            version="1",
            display_name="Source 4",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="metric_value", type=IntegerType(), order=1),
            ],
        )
        source4.current = source4_rev

        source5 = Node(
            name="default.source5",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        source5_rev = NodeRevision(
            node=source5,
            type=source5.type,
            name=source5.name,
            version="1",
            display_name="Source 5",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="data", type=IntegerType(), order=1),
            ],
        )
        source5.current = source5_rev

        source6 = Node(
            name="default.source6",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        source6_rev = NodeRevision(
            node=source6,
            type=source6.type,
            name=source6.name,
            version="1",
            display_name="Source 6",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="info", type=IntegerType(), order=1),
            ],
        )
        source6.current = source6_rev

        session.add_all(
            [
                source1,
                source1_rev,
                source2,
                source2_rev,
                source3,
                source3_rev,
                source4,
                source4_rev,
                source5,
                source5_rev,
                source6,
                source6_rev,
            ],
        )
        await session.flush()

        # Create metric1: single source parent
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
            query="SELECT SUM(value) FROM default.source1",
            parents=[source1],
            created_by_id=current_user.id,
            columns=[Column(name="total", type=IntegerType(), order=0)],
        )
        metric1.current = metric1_rev

        # Create metric2: multiple source parents
        metric2 = Node(
            name="default.metric2",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        metric2_rev = NodeRevision(
            node=metric2,
            type=metric2.type,
            name=metric2.name,
            version="1",
            display_name="Metric 2",
            query="SELECT SUM(amount) + SUM(count) FROM default.source2, default.source3",
            parents=[source2, source3],
            created_by_id=current_user.id,
            columns=[Column(name="combined", type=IntegerType(), order=0)],
        )
        metric2.current = metric2_rev

        # Create base_metric: used by derived metrics
        base_metric = Node(
            name="default.base_metric",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        base_metric_rev = NodeRevision(
            node=base_metric,
            type=base_metric.type,
            name=base_metric.name,
            version="1",
            display_name="Base Metric",
            query="SELECT SUM(metric_value) FROM default.source4",
            parents=[source4],
            created_by_id=current_user.id,
            columns=[Column(name="base_total", type=IntegerType(), order=0)],
        )
        base_metric.current = base_metric_rev

        # Create derived_metric1: references base_metric
        derived_metric1 = Node(
            name="default.derived_metric1",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        derived_metric1_rev = NodeRevision(
            node=derived_metric1,
            type=derived_metric1.type,
            name=derived_metric1.name,
            version="1",
            display_name="Derived Metric 1",
            query="SELECT default.base_metric * 2",
            parents=[base_metric],
            created_by_id=current_user.id,
            columns=[Column(name="doubled", type=IntegerType(), order=0)],
        )
        derived_metric1.current = derived_metric1_rev

        # Create derived_metric2: also references base_metric
        derived_metric2 = Node(
            name="default.derived_metric2",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        derived_metric2_rev = NodeRevision(
            node=derived_metric2,
            type=derived_metric2.type,
            name=derived_metric2.name,
            version="1",
            display_name="Derived Metric 2",
            query="SELECT default.base_metric / 100",
            parents=[base_metric],
            created_by_id=current_user.id,
            columns=[Column(name="percentage", type=IntegerType(), order=0)],
        )
        derived_metric2.current = derived_metric2_rev

        # Create base_metric2: has multiple parents
        base_metric2 = Node(
            name="default.base_metric2",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        base_metric2_rev = NodeRevision(
            node=base_metric2,
            type=base_metric2.type,
            name=base_metric2.name,
            version="1",
            display_name="Base Metric 2",
            query="SELECT SUM(data) + SUM(info) FROM default.source5, default.source6",
            parents=[source5, source6],
            created_by_id=current_user.id,
            columns=[Column(name="multi_base", type=IntegerType(), order=0)],
        )
        base_metric2.current = base_metric2_rev

        # Create derived_metric3: references base_metric2 (which has multiple parents)
        derived_metric3 = Node(
            name="default.derived_metric3",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        derived_metric3_rev = NodeRevision(
            node=derived_metric3,
            type=derived_metric3.type,
            name=derived_metric3.name,
            version="1",
            display_name="Derived Metric 3",
            query="SELECT default.base_metric2 * 10",
            parents=[base_metric2],
            created_by_id=current_user.id,
            columns=[Column(name="scaled", type=IntegerType(), order=0)],
        )
        derived_metric3.current = derived_metric3_rev

        session.add_all(
            [
                metric1,
                metric1_rev,
                metric2,
                metric2_rev,
                base_metric,
                base_metric_rev,
                derived_metric1,
                derived_metric1_rev,
                derived_metric2,
                derived_metric2_rev,
                base_metric2,
                base_metric2_rev,
                derived_metric3,
                derived_metric3_rev,
            ],
        )
        await session.commit()

        return {
            "source1": source1,
            "source2": source2,
            "source3": source3,
            "source4": source4,
            "source5": source5,
            "source6": source6,
            "metric1": metric1,
            "metric2": metric2,
            "base_metric": base_metric,
            "derived_metric1": derived_metric1,
            "derived_metric2": derived_metric2,
            "base_metric2": base_metric2,
            "derived_metric3": derived_metric3,
        }

    @pytest.mark.asyncio
    async def test_empty_input(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test that an empty input list returns an empty dict.
        """
        result = await get_metric_parents_map(session, [])
        assert result == {}

    @pytest.mark.asyncio
    async def test_single_metric_single_parent(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test a metric with a single non-metric parent.
        """
        graph = metric_parents_test_graph
        result = await get_metric_parents_map(session, [graph["metric1"]])

        assert "default.metric1" in result
        parent_names = {p.name for p in result["default.metric1"]}
        assert parent_names == {"default.source1"}

    @pytest.mark.asyncio
    async def test_single_metric_multiple_parents(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test a metric with multiple non-metric parents.
        """
        graph = metric_parents_test_graph
        result = await get_metric_parents_map(session, [graph["metric2"]])

        assert "default.metric2" in result
        parent_names = {p.name for p in result["default.metric2"]}
        assert parent_names == {"default.source2", "default.source3"}

    @pytest.mark.asyncio
    async def test_multiple_regular_metrics(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test multiple regular metrics (each with non-metric parents).
        """
        graph = metric_parents_test_graph
        result = await get_metric_parents_map(
            session,
            [graph["metric1"], graph["metric2"]],
        )

        assert len(result) == 2

        metric1_parents = {p.name for p in result["default.metric1"]}
        assert metric1_parents == {"default.source1"}

        metric2_parents = {p.name for p in result["default.metric2"]}
        assert metric2_parents == {"default.source2", "default.source3"}

    @pytest.mark.asyncio
    async def test_derived_metric_single_parent_base(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test a derived metric that references a base metric with a single parent.
        Should return the base metric's non-metric parent.
        """
        graph = metric_parents_test_graph
        result = await get_metric_parents_map(session, [graph["derived_metric1"]])

        assert "default.derived_metric1" in result
        parent_names = {p.name for p in result["default.derived_metric1"]}
        # derived_metric1 -> base_metric -> source4
        assert parent_names == {"default.source4"}

    @pytest.mark.asyncio
    async def test_derived_metric_multiple_parent_base(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test a derived metric that references a base metric with multiple parents.
        Should return all of the base metric's non-metric parents.
        """
        graph = metric_parents_test_graph
        result = await get_metric_parents_map(session, [graph["derived_metric3"]])

        assert "default.derived_metric3" in result
        parent_names = {p.name for p in result["default.derived_metric3"]}
        # derived_metric3 -> base_metric2 -> source5, source6
        assert parent_names == {"default.source5", "default.source6"}

    @pytest.mark.asyncio
    async def test_multiple_derived_metrics_same_base(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test multiple derived metrics that reference the same base metric.
        Both should get the same base metric's parents.
        """
        graph = metric_parents_test_graph
        result = await get_metric_parents_map(
            session,
            [graph["derived_metric1"], graph["derived_metric2"]],
        )

        assert len(result) == 2

        # Both derived metrics reference base_metric, which has source4 as parent
        dm1_parents = {p.name for p in result["default.derived_metric1"]}
        dm2_parents = {p.name for p in result["default.derived_metric2"]}

        assert dm1_parents == {"default.source4"}
        assert dm2_parents == {"default.source4"}

    @pytest.mark.asyncio
    async def test_mix_of_regular_and_derived_metrics(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test a mix of regular and derived metrics.
        """
        graph = metric_parents_test_graph
        result = await get_metric_parents_map(
            session,
            [graph["metric1"], graph["derived_metric1"], graph["derived_metric3"]],
        )

        assert len(result) == 3

        # metric1 -> source1
        metric1_parents = {p.name for p in result["default.metric1"]}
        assert metric1_parents == {"default.source1"}

        # derived_metric1 -> base_metric -> source4
        dm1_parents = {p.name for p in result["default.derived_metric1"]}
        assert dm1_parents == {"default.source4"}

        # derived_metric3 -> base_metric2 -> source5, source6
        dm3_parents = {p.name for p in result["default.derived_metric3"]}
        assert dm3_parents == {"default.source5", "default.source6"}

    @pytest.mark.asyncio
    async def test_base_metric_directly(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test querying a base metric directly (not as a derived metric).
        Should return its direct non-metric parents.
        """
        graph = metric_parents_test_graph
        result = await get_metric_parents_map(session, [graph["base_metric"]])

        assert "default.base_metric" in result
        parent_names = {p.name for p in result["default.base_metric"]}
        assert parent_names == {"default.source4"}

    @pytest.mark.asyncio
    async def test_all_metrics(
        self,
        session: AsyncSession,
        metric_parents_test_graph,
    ):
        """
        Test querying all metrics at once.
        """
        graph = metric_parents_test_graph
        all_metrics = [
            graph["metric1"],
            graph["metric2"],
            graph["base_metric"],
            graph["derived_metric1"],
            graph["derived_metric2"],
            graph["base_metric2"],
            graph["derived_metric3"],
        ]
        result = await get_metric_parents_map(session, all_metrics)

        assert len(result) == 7

        # Verify each metric's parents
        assert {p.name for p in result["default.metric1"]} == {"default.source1"}
        assert {p.name for p in result["default.metric2"]} == {
            "default.source2",
            "default.source3",
        }
        assert {p.name for p in result["default.base_metric"]} == {"default.source4"}
        assert {p.name for p in result["default.derived_metric1"]} == {
            "default.source4",
        }
        assert {p.name for p in result["default.derived_metric2"]} == {
            "default.source4",
        }
        assert {p.name for p in result["default.base_metric2"]} == {
            "default.source5",
            "default.source6",
        }
        assert {p.name for p in result["default.derived_metric3"]} == {
            "default.source5",
            "default.source6",
        }


class TestGetSharedDimensions:
    """
    Tests for ``get_shared_dimensions`` with derived metrics.

    This test class creates a graph with dimension links to properly test
    how shared dimensions are computed for derived metrics.
    """

    @pytest.fixture
    async def shared_dims_test_graph(
        self,
        session: AsyncSession,
        current_user: User,
    ):
        """
        Creates a test graph with metrics, dimensions, and dimension links.

        Graph structure:
        Dimensions:
        - dim_date (DIMENSION): id, day, week, month
        - dim_customer (DIMENSION): id, name, region
        - dim_warehouse (DIMENSION): id, location (NO overlap with date/customer)

        Sources with dimension links:
        - orders_source (SOURCE): id, amount, date_id->dim_date, customer_id->dim_customer
        - events_source (SOURCE): id, count, date_id->dim_date, customer_id->dim_customer
        - inventory_source (SOURCE): id, quantity, warehouse_id->dim_warehouse (different dims!)

        Metrics:
        - revenue (METRIC): SUM(amount) FROM orders_source
        - orders_count (METRIC): COUNT(*) FROM orders_source
        - page_views (METRIC): SUM(count) FROM events_source
        - inventory_total (METRIC): SUM(quantity) FROM inventory_source

        Derived Metrics:
        - revenue_per_order: revenue / orders_count (same source - full dim intersection)
        - revenue_per_pageview: revenue / page_views (cross-source with shared dims)
        - derived_from_inventory: inventory_total * 2 (different dims than orders/events)
        """
        # Create dimension nodes
        dim_date = Node(
            name="shared_dims.dim_date",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        dim_date_rev = NodeRevision(
            node=dim_date,
            type=dim_date.type,
            name=dim_date.name,
            version="1",
            display_name="Date Dimension",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="day", type=StringType(), order=1),
                Column(name="week", type=StringType(), order=2),
                Column(name="month", type=StringType(), order=3),
            ],
        )
        dim_date.current = dim_date_rev

        dim_customer = Node(
            name="shared_dims.dim_customer",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        dim_customer_rev = NodeRevision(
            node=dim_customer,
            type=dim_customer.type,
            name=dim_customer.name,
            version="1",
            display_name="Customer Dimension",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="name", type=StringType(), order=1),
                Column(name="region", type=StringType(), order=2),
            ],
        )
        dim_customer.current = dim_customer_rev

        dim_warehouse = Node(
            name="shared_dims.dim_warehouse",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        dim_warehouse_rev = NodeRevision(
            node=dim_warehouse,
            type=dim_warehouse.type,
            name=dim_warehouse.name,
            version="1",
            display_name="Warehouse Dimension",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="location", type=StringType(), order=1),
            ],
        )
        dim_warehouse.current = dim_warehouse_rev

        session.add_all(
            [
                dim_date,
                dim_date_rev,
                dim_customer,
                dim_customer_rev,
                dim_warehouse,
                dim_warehouse_rev,
            ],
        )
        await session.flush()

        # Create source nodes with dimension links
        orders_source = Node(
            name="shared_dims.orders_source",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        orders_source_rev = NodeRevision(
            node=orders_source,
            type=orders_source.type,
            name=orders_source.name,
            version="1",
            display_name="Orders Source",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="amount", type=IntegerType(), order=1),
                Column(name="date_id", type=IntegerType(), dimension=dim_date, order=2),
                Column(
                    name="customer_id",
                    type=IntegerType(),
                    dimension=dim_customer,
                    order=3,
                ),
            ],
        )
        orders_source.current = orders_source_rev

        events_source = Node(
            name="shared_dims.events_source",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        events_source_rev = NodeRevision(
            node=events_source,
            type=events_source.type,
            name=events_source.name,
            version="1",
            display_name="Events Source",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="count", type=IntegerType(), order=1),
                Column(name="date_id", type=IntegerType(), dimension=dim_date, order=2),
                Column(
                    name="customer_id",
                    type=IntegerType(),
                    dimension=dim_customer,
                    order=3,
                ),
            ],
        )
        events_source.current = events_source_rev

        inventory_source = Node(
            name="shared_dims.inventory_source",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        inventory_source_rev = NodeRevision(
            node=inventory_source,
            type=inventory_source.type,
            name=inventory_source.name,
            version="1",
            display_name="Inventory Source",
            created_by_id=current_user.id,
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="quantity", type=IntegerType(), order=1),
                Column(
                    name="warehouse_id",
                    type=IntegerType(),
                    dimension=dim_warehouse,
                    order=2,
                ),
            ],
        )
        inventory_source.current = inventory_source_rev

        session.add_all(
            [
                orders_source,
                orders_source_rev,
                events_source,
                events_source_rev,
                inventory_source,
                inventory_source_rev,
            ],
        )
        await session.flush()

        # Create base metrics
        revenue = Node(
            name="shared_dims.revenue",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        revenue_rev = NodeRevision(
            node=revenue,
            type=revenue.type,
            name=revenue.name,
            version="1",
            display_name="Revenue",
            query="SELECT SUM(amount) FROM shared_dims.orders_source",
            parents=[orders_source],
            created_by_id=current_user.id,
            columns=[Column(name="total_revenue", type=IntegerType(), order=0)],
        )
        revenue.current = revenue_rev

        orders_count = Node(
            name="shared_dims.orders_count",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        orders_count_rev = NodeRevision(
            node=orders_count,
            type=orders_count.type,
            name=orders_count.name,
            version="1",
            display_name="Orders Count",
            query="SELECT COUNT(*) FROM shared_dims.orders_source",
            parents=[orders_source],
            created_by_id=current_user.id,
            columns=[Column(name="order_count", type=IntegerType(), order=0)],
        )
        orders_count.current = orders_count_rev

        page_views = Node(
            name="shared_dims.page_views",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        page_views_rev = NodeRevision(
            node=page_views,
            type=page_views.type,
            name=page_views.name,
            version="1",
            display_name="Page Views",
            query="SELECT SUM(count) FROM shared_dims.events_source",
            parents=[events_source],
            created_by_id=current_user.id,
            columns=[Column(name="total_views", type=IntegerType(), order=0)],
        )
        page_views.current = page_views_rev

        inventory_total = Node(
            name="shared_dims.inventory_total",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        inventory_total_rev = NodeRevision(
            node=inventory_total,
            type=inventory_total.type,
            name=inventory_total.name,
            version="1",
            display_name="Inventory Total",
            query="SELECT SUM(quantity) FROM shared_dims.inventory_source",
            parents=[inventory_source],
            created_by_id=current_user.id,
            columns=[Column(name="total_inventory", type=IntegerType(), order=0)],
        )
        inventory_total.current = inventory_total_rev

        session.add_all(
            [
                revenue,
                revenue_rev,
                orders_count,
                orders_count_rev,
                page_views,
                page_views_rev,
                inventory_total,
                inventory_total_rev,
            ],
        )
        await session.flush()

        # Create derived metrics
        revenue_per_order = Node(
            name="shared_dims.revenue_per_order",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        revenue_per_order_rev = NodeRevision(
            node=revenue_per_order,
            type=revenue_per_order.type,
            name=revenue_per_order.name,
            version="1",
            display_name="Revenue Per Order",
            query="SELECT shared_dims.revenue / shared_dims.orders_count",
            parents=[revenue, orders_count],
            created_by_id=current_user.id,
            columns=[Column(name="avg_revenue", type=IntegerType(), order=0)],
        )
        revenue_per_order.current = revenue_per_order_rev

        revenue_per_pageview = Node(
            name="shared_dims.revenue_per_pageview",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        revenue_per_pageview_rev = NodeRevision(
            node=revenue_per_pageview,
            type=revenue_per_pageview.type,
            name=revenue_per_pageview.name,
            version="1",
            display_name="Revenue Per Pageview",
            query="SELECT shared_dims.revenue / shared_dims.page_views",
            parents=[revenue, page_views],
            created_by_id=current_user.id,
            columns=[Column(name="revenue_per_view", type=IntegerType(), order=0)],
        )
        revenue_per_pageview.current = revenue_per_pageview_rev

        derived_from_inventory = Node(
            name="shared_dims.derived_inventory",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        derived_from_inventory_rev = NodeRevision(
            node=derived_from_inventory,
            type=derived_from_inventory.type,
            name=derived_from_inventory.name,
            version="1",
            display_name="Derived Inventory",
            query="SELECT shared_dims.inventory_total * 2",
            parents=[inventory_total],
            created_by_id=current_user.id,
            columns=[Column(name="doubled_inventory", type=IntegerType(), order=0)],
        )
        derived_from_inventory.current = derived_from_inventory_rev

        session.add_all(
            [
                revenue_per_order,
                revenue_per_order_rev,
                revenue_per_pageview,
                revenue_per_pageview_rev,
                derived_from_inventory,
                derived_from_inventory_rev,
            ],
        )
        await session.flush()

        return {
            "dim_date": dim_date,
            "dim_customer": dim_customer,
            "dim_warehouse": dim_warehouse,
            "orders_source": orders_source,
            "events_source": events_source,
            "inventory_source": inventory_source,
            "revenue": revenue,
            "orders_count": orders_count,
            "page_views": page_views,
            "inventory_total": inventory_total,
            "revenue_per_order": revenue_per_order,
            "revenue_per_pageview": revenue_per_pageview,
            "derived_inventory": derived_from_inventory,
        }

    @pytest.mark.asyncio
    async def test_empty_input(
        self,
        session: AsyncSession,
        shared_dims_test_graph,
    ):
        """
        Test that an empty list of metrics returns an empty list.
        """
        result = await get_shared_dimensions(session, [])
        assert result == []

    @pytest.mark.asyncio
    async def test_single_base_metric_returns_all_dimensions(
        self,
        session: AsyncSession,
        shared_dims_test_graph,
    ):
        """
        Test that a single base metric returns all its dimensions.
        """
        graph = shared_dims_test_graph
        result = await get_shared_dimensions(session, [graph["revenue"]])

        dim_names = {d.name for d in result}
        # revenue comes from orders_source which has date and customer dimensions
        assert "shared_dims.dim_date.id" in dim_names
        assert "shared_dims.dim_date.day" in dim_names
        assert "shared_dims.dim_date.week" in dim_names
        assert "shared_dims.dim_date.month" in dim_names
        assert "shared_dims.dim_customer.id" in dim_names
        assert "shared_dims.dim_customer.name" in dim_names
        assert "shared_dims.dim_customer.region" in dim_names

    @pytest.mark.asyncio
    async def test_derived_metric_same_source_inherits_all_dimensions(
        self,
        session: AsyncSession,
        shared_dims_test_graph,
    ):
        """
        Test that a derived metric from same-source base metrics has all dimensions.
        revenue_per_order = revenue / orders_count, both from orders_source.
        """
        graph = shared_dims_test_graph
        result = await get_shared_dimensions(session, [graph["revenue_per_order"]])

        dim_names = {d.name for d in result}
        # Both base metrics come from orders_source -> date + customer dims
        assert "shared_dims.dim_date.id" in dim_names
        assert "shared_dims.dim_customer.id" in dim_names

    @pytest.mark.asyncio
    async def test_derived_metric_cross_source_with_shared_dimensions(
        self,
        session: AsyncSession,
        shared_dims_test_graph,
    ):
        """
        Test that a derived metric from different sources with shared dims
        returns the intersection.
        revenue_per_pageview = revenue (orders) / page_views (events).
        Both sources share date and customer dimensions.
        """
        graph = shared_dims_test_graph
        result = await get_shared_dimensions(session, [graph["revenue_per_pageview"]])

        dim_names = {d.name for d in result}
        # Both orders and events sources have date and customer dims
        assert "shared_dims.dim_date.id" in dim_names
        assert "shared_dims.dim_customer.id" in dim_names
        # Warehouse should NOT be present (only inventory has it)
        assert "shared_dims.dim_warehouse.id" not in dim_names

    @pytest.mark.asyncio
    async def test_derived_metric_with_no_shared_dimensions_with_other_metric(
        self,
        session: AsyncSession,
        shared_dims_test_graph,
    ):
        """
        Test that querying metrics with no shared dimensions returns empty list.
        revenue (date + customer) vs derived_inventory (warehouse only)
        """
        graph = shared_dims_test_graph
        result = await get_shared_dimensions(
            session,
            [graph["revenue"], graph["derived_inventory"]],
        )

        # No shared dimensions between orders-based and inventory-based metrics
        assert result == []

    @pytest.mark.asyncio
    async def test_multiple_metrics_from_same_source(
        self,
        session: AsyncSession,
        shared_dims_test_graph,
    ):
        """
        Test that multiple metrics from the same source share all dimensions.
        revenue and orders_count both come from orders_source.
        """
        graph = shared_dims_test_graph
        result = await get_shared_dimensions(
            session,
            [graph["revenue"], graph["orders_count"]],
        )

        dim_names = {d.name for d in result}
        # Both from orders_source -> should have all date + customer dims
        assert "shared_dims.dim_date.id" in dim_names
        assert "shared_dims.dim_date.day" in dim_names
        assert "shared_dims.dim_customer.id" in dim_names
        assert "shared_dims.dim_customer.name" in dim_names

    @pytest.mark.asyncio
    async def test_multiple_metrics_from_different_sources_with_overlap(
        self,
        session: AsyncSession,
        shared_dims_test_graph,
    ):
        """
        Test that multiple metrics from different sources return intersection.
        revenue (orders) and page_views (events) share date + customer.
        """
        graph = shared_dims_test_graph
        result = await get_shared_dimensions(
            session,
            [graph["revenue"], graph["page_views"]],
        )

        dim_names = {d.name for d in result}
        # Intersection of orders and events dimensions
        assert "shared_dims.dim_date.id" in dim_names
        assert "shared_dims.dim_customer.id" in dim_names

    @pytest.mark.asyncio
    async def test_derived_metric_inherits_union_of_base_metric_dimensions(
        self,
        session: AsyncSession,
        shared_dims_test_graph,
    ):
        """
        Test that a single derived metric's dimensions are the union of its
        base metrics' dimensions (which happens to be intersection for same dims).

        revenue_per_pageview has parents: revenue (orders) and page_views (events).
        Both share date and customer dimensions.
        """
        graph = shared_dims_test_graph

        # Get dims for derived metric alone
        derived_dims = await get_shared_dimensions(
            session,
            [graph["revenue_per_pageview"]],
        )

        # Get dims for each base metric
        revenue_dims = await get_shared_dimensions(session, [graph["revenue"]])
        pageviews_dims = await get_shared_dimensions(session, [graph["page_views"]])

        derived_dim_names = {d.name for d in derived_dims}
        revenue_dim_names = {d.name for d in revenue_dims}
        pageviews_dim_names = {d.name for d in pageviews_dims}

        # Derived metric should have union of its base metrics' dimensions
        # Since both have the same dims, the union equals each individual set
        assert derived_dim_names == revenue_dim_names
        assert derived_dim_names == pageviews_dim_names

    @pytest.mark.asyncio
    async def test_inventory_metric_has_only_warehouse_dimension(
        self,
        session: AsyncSession,
        shared_dims_test_graph,
    ):
        """
        Test that inventory-based metric only has warehouse dimension.
        """
        graph = shared_dims_test_graph
        result = await get_shared_dimensions(session, [graph["inventory_total"]])

        dim_names = {d.name for d in result}
        # Only warehouse dimension
        assert "shared_dims.dim_warehouse.id" in dim_names
        assert "shared_dims.dim_warehouse.location" in dim_names
        # No date or customer
        assert "shared_dims.dim_date.id" not in dim_names
        assert "shared_dims.dim_customer.id" not in dim_names
