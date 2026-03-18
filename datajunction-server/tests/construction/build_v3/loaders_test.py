"""Tests for loaders.py - load_available_preaggs function."""

from unittest.mock import MagicMock, AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.loaders import load_available_preaggs
from datajunction_server.construction.build_v3.types import BuildContext
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.preaggregation import (
    PreAggregation,
)
from datajunction_server.database.user import User
from datajunction_server.models.decompose import (
    MetricComponent,
    AggregationRule,
    Aggregability,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.user import OAuthProvider
from datajunction_server.sql.parsing.types import IntegerType


def make_measure(
    name: str,
    expression: str,
    aggregation: str = "SUM",
) -> MetricComponent:
    """Helper to create a MetricComponent for testing."""
    return MetricComponent(
        name=name,
        expression=expression,
        aggregation=aggregation,
        rule=AggregationRule(type=Aggregability.FULL),
    )


@pytest_asyncio.fixture
async def minimal_node_revision(clean_session: AsyncSession):
    """Create a minimal node revision for testing PreAggregation."""
    # Create user
    user = User(
        username="test_loader_user",
        email="test_loader@test.com",
        oauth_provider=OAuthProvider.BASIC,
    )
    clean_session.add(user)
    await clean_session.flush()

    # Create node
    node = Node(
        name="test.loader.source_node",
        type=NodeType.SOURCE,
        created_by_id=user.id,
    )
    clean_session.add(node)
    await clean_session.flush()

    # Create node revision
    revision = NodeRevision(
        name=node.name,
        node_id=node.id,
        type=NodeType.SOURCE,
        version="1",
        columns=[Column(name="col1", type=IntegerType(), order=0)],
        created_by_id=user.id,
    )
    clean_session.add(revision)
    await clean_session.flush()

    return revision


class TestLoadAvailablePreaggs:
    """Tests for load_available_preaggs function."""

    @pytest.mark.asyncio
    async def test_skips_when_use_materialized_false(self):
        """When use_materialized=False, should return early without querying."""
        mock_session = MagicMock(spec=AsyncSession)
        mock_session.execute = AsyncMock()

        ctx = BuildContext(
            session=mock_session,
            metrics=["test.metric"],
            dimensions=[],
            use_materialized=False,
        )
        ctx._parent_revision_ids = {1, 2, 3}

        await load_available_preaggs(ctx)

        # Should not execute any queries
        mock_session.execute.assert_not_called()
        assert ctx.available_preaggs == {}

    @pytest.mark.asyncio
    async def test_skips_when_no_parent_revision_ids(self):
        """When _parent_revision_ids is empty, should return early."""
        mock_session = MagicMock(spec=AsyncSession)
        mock_session.execute = AsyncMock()

        ctx = BuildContext(
            session=mock_session,
            metrics=["test.metric"],
            dimensions=[],
            use_materialized=True,
        )
        # Empty parent_revision_ids
        ctx._parent_revision_ids = set()

        await load_available_preaggs(ctx)

        mock_session.execute.assert_not_called()
        assert ctx.available_preaggs == {}

    @pytest.mark.asyncio
    async def test_loads_preaggs_with_valid_availability(
        self,
        clean_session: AsyncSession,
        minimal_node_revision: NodeRevision,
    ):
        """Pre-aggs with valid availability should be loaded."""
        # Create availability
        availability = AvailabilityState(
            catalog="analytics",
            schema_="materialized",
            table="preagg_test",
            valid_through_ts=9999999999,
        )
        clean_session.add(availability)
        await clean_session.flush()

        # Create pre-agg with availability
        preagg = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=["test.dim"],
            measures=[make_measure("sum_x", "x")],
            columns=[],
            sql="SELECT x FROM t",
            grain_group_hash="hash123",
            preagg_hash="load_t01",
            availability_id=availability.id,
        )
        clean_session.add(preagg)
        await clean_session.flush()

        ctx = BuildContext(
            session=clean_session,
            metrics=["test.metric"],
            dimensions=[],
            use_materialized=True,
        )
        ctx._parent_revision_ids = {minimal_node_revision.id}

        await load_available_preaggs(ctx)

        assert minimal_node_revision.id in ctx.available_preaggs
        assert len(ctx.available_preaggs[minimal_node_revision.id]) == 1
        assert ctx.available_preaggs[minimal_node_revision.id][0].id == preagg.id

    @pytest.mark.asyncio
    async def test_ignores_preaggs_without_availability(
        self,
        clean_session: AsyncSession,
        minimal_node_revision: NodeRevision,
    ):
        """Pre-aggs without availability_id should not be loaded."""
        preagg = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=["test.dim"],
            measures=[make_measure("sum_x", "x")],
            columns=[],
            sql="SELECT x FROM t",
            grain_group_hash="hash_no_avail",
            preagg_hash="load_t02",
            # No availability_id
        )
        clean_session.add(preagg)
        await clean_session.flush()

        ctx = BuildContext(
            session=clean_session,
            metrics=["test.metric"],
            dimensions=[],
            use_materialized=True,
        )
        ctx._parent_revision_ids = {minimal_node_revision.id}

        await load_available_preaggs(ctx)

        # Should be empty - pre-agg filtered out by SQL query
        assert ctx.available_preaggs == {}

    @pytest.mark.asyncio
    async def test_ignores_preaggs_with_unavailable_status(
        self,
        clean_session: AsyncSession,
        minimal_node_revision: NodeRevision,
    ):
        """Pre-aggs where is_available() returns False should not be loaded."""
        availability = AvailabilityState(
            catalog="analytics",
            schema_="materialized",
            table="preagg_unavail",
            valid_through_ts=9999999999,
        )
        clean_session.add(availability)
        await clean_session.flush()

        preagg = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=["test.dim"],
            measures=[make_measure("sum_x", "x")],
            columns=[],
            sql="SELECT x FROM t",
            grain_group_hash="hash_unavail",
            preagg_hash="load_t03",
            availability_id=availability.id,
        )
        clean_session.add(preagg)
        await clean_session.flush()

        ctx = BuildContext(
            session=clean_session,
            metrics=["test.metric"],
            dimensions=[],
            use_materialized=True,
        )
        ctx._parent_revision_ids = {minimal_node_revision.id}

        # Patch is_available to return False
        with patch.object(AvailabilityState, "is_available", return_value=False):
            await load_available_preaggs(ctx)

        assert ctx.available_preaggs == {}

    @pytest.mark.asyncio
    async def test_multiple_preaggs_same_revision_id(
        self,
        clean_session: AsyncSession,
        minimal_node_revision: NodeRevision,
    ):
        """Multiple pre-aggs for same node_revision_id should all be loaded."""
        availability1 = AvailabilityState(
            catalog="analytics",
            schema_="mat",
            table="preagg1",
            valid_through_ts=9999999999,
        )
        availability2 = AvailabilityState(
            catalog="analytics",
            schema_="mat",
            table="preagg2",
            valid_through_ts=9999999999,
        )
        clean_session.add_all([availability1, availability2])
        await clean_session.flush()

        preagg1 = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=["dim1"],
            measures=[make_measure("sum_a", "a")],
            columns=[],
            sql="SELECT a",
            grain_group_hash="hash_multi_1",
            preagg_hash="load_t04",
            availability_id=availability1.id,
        )
        preagg2 = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=["dim2"],
            measures=[make_measure("sum_b", "b")],
            columns=[],
            sql="SELECT b",
            grain_group_hash="hash_multi_2",
            preagg_hash="load_t05",
            availability_id=availability2.id,
        )
        clean_session.add_all([preagg1, preagg2])
        await clean_session.flush()

        ctx = BuildContext(
            session=clean_session,
            metrics=["test.metric"],
            dimensions=[],
            use_materialized=True,
        )
        ctx._parent_revision_ids = {minimal_node_revision.id}

        await load_available_preaggs(ctx)

        assert minimal_node_revision.id in ctx.available_preaggs
        assert len(ctx.available_preaggs[minimal_node_revision.id]) == 2

    @pytest.mark.asyncio
    async def test_preaggs_indexed_by_different_revision_ids(
        self,
        clean_session: AsyncSession,
        minimal_node_revision: NodeRevision,
    ):
        """Pre-aggs for different revisions should be indexed separately."""
        # Create a second node revision
        user = await clean_session.get(User, minimal_node_revision.created_by_id)
        node2 = Node(
            name="test.loader.source_node_2",
            type=NodeType.SOURCE,
            created_by_id=user.id,
        )
        clean_session.add(node2)
        await clean_session.flush()

        revision2 = NodeRevision(
            name=node2.name,
            node_id=node2.id,
            type=NodeType.SOURCE,
            version="1",
            columns=[Column(name="col1", type=IntegerType(), order=0)],
            created_by_id=user.id,
        )
        clean_session.add(revision2)
        await clean_session.flush()

        # Create availabilities
        avail1 = AvailabilityState(
            catalog="analytics",
            schema_="mat",
            table="preagg_rev1",
            valid_through_ts=9999999999,
        )
        avail2 = AvailabilityState(
            catalog="analytics",
            schema_="mat",
            table="preagg_rev2",
            valid_through_ts=9999999999,
        )
        clean_session.add_all([avail1, avail2])
        await clean_session.flush()

        # Create pre-aggs for different revisions
        preagg1 = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=["dim1"],
            measures=[make_measure("sum_a", "a")],
            columns=[],
            sql="SELECT a",
            grain_group_hash="hash_rev1",
            preagg_hash="load_t06",
            availability_id=avail1.id,
        )
        preagg2 = PreAggregation(
            node_revision_id=revision2.id,
            grain_columns=["dim2"],
            measures=[make_measure("sum_b", "b")],
            columns=[],
            sql="SELECT b",
            grain_group_hash="hash_rev2",
            preagg_hash="load_t07",
            availability_id=avail2.id,
        )
        clean_session.add_all([preagg1, preagg2])
        await clean_session.flush()

        ctx = BuildContext(
            session=clean_session,
            metrics=["test.metric"],
            dimensions=[],
            use_materialized=True,
        )
        ctx._parent_revision_ids = {minimal_node_revision.id, revision2.id}

        await load_available_preaggs(ctx)

        # Should have entries for both revision IDs
        assert minimal_node_revision.id in ctx.available_preaggs
        assert revision2.id in ctx.available_preaggs
        assert len(ctx.available_preaggs[minimal_node_revision.id]) == 1
        assert len(ctx.available_preaggs[revision2.id]) == 1

    @pytest.mark.asyncio
    async def test_no_matching_preaggs_in_db(
        self,
        clean_session: AsyncSession,
        minimal_node_revision: NodeRevision,
    ):
        """When no pre-aggs match the revision IDs, available_preaggs should be empty."""
        ctx = BuildContext(
            session=clean_session,
            metrics=["test.metric"],
            dimensions=[],
            use_materialized=True,
        )
        # Use a revision ID that doesn't exist in the database
        ctx._parent_revision_ids = {999999}

        await load_available_preaggs(ctx)

        assert ctx.available_preaggs == {}

    @pytest.mark.asyncio
    async def test_only_loads_preaggs_for_requested_revision_ids(
        self,
        clean_session: AsyncSession,
        minimal_node_revision: NodeRevision,
    ):
        """Should only load pre-aggs for revision IDs in _parent_revision_ids."""
        # Create a second revision that we won't request
        user = await clean_session.get(User, minimal_node_revision.created_by_id)
        node2 = Node(
            name="test.loader.other_node",
            type=NodeType.SOURCE,
            created_by_id=user.id,
        )
        clean_session.add(node2)
        await clean_session.flush()

        other_revision = NodeRevision(
            name=node2.name,
            node_id=node2.id,
            type=NodeType.SOURCE,
            version="1",
            columns=[Column(name="col1", type=IntegerType(), order=0)],
            created_by_id=user.id,
        )
        clean_session.add(other_revision)
        await clean_session.flush()

        # Create availabilities
        avail1 = AvailabilityState(
            catalog="analytics",
            schema_="mat",
            table="preagg_wanted",
            valid_through_ts=9999999999,
        )
        avail2 = AvailabilityState(
            catalog="analytics",
            schema_="mat",
            table="preagg_unwanted",
            valid_through_ts=9999999999,
        )
        clean_session.add_all([avail1, avail2])
        await clean_session.flush()

        # Create pre-aggs - one for each revision
        wanted_preagg = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=["dim1"],
            measures=[make_measure("sum_a", "a")],
            columns=[],
            sql="SELECT a",
            grain_group_hash="hash_wanted",
            preagg_hash="load_t08",
            availability_id=avail1.id,
        )
        unwanted_preagg = PreAggregation(
            node_revision_id=other_revision.id,
            grain_columns=["dim2"],
            measures=[make_measure("sum_b", "b")],
            columns=[],
            sql="SELECT b",
            grain_group_hash="hash_unwanted",
            preagg_hash="load_t09",
            availability_id=avail2.id,
        )
        clean_session.add_all([wanted_preagg, unwanted_preagg])
        await clean_session.flush()

        ctx = BuildContext(
            session=clean_session,
            metrics=["test.metric"],
            dimensions=[],
            use_materialized=True,
        )
        # Only request the first revision
        ctx._parent_revision_ids = {minimal_node_revision.id}

        await load_available_preaggs(ctx)

        # Should only have the wanted pre-agg
        assert minimal_node_revision.id in ctx.available_preaggs
        assert other_revision.id not in ctx.available_preaggs
        assert len(ctx.available_preaggs[minimal_node_revision.id]) == 1


class TestLoadMissingUpstreamNodes:
    """
    Option B: _load_missing_upstream_nodes provides a self-healing fallback
    that scans loaded nodes' SQL for references absent from ctx.nodes and
    loads them directly from the DB, guarding against stale NodeRelationship data.
    """

    @pytest.mark.asyncio
    async def test_loads_source_missing_from_node_relationship(
        self,
        clean_session: AsyncSession,
    ):
        """
        If a source node is referenced in a transform's SQL but absent from
        ctx.nodes (because NodeRelationship is stale), _load_missing_upstream_nodes
        should detect the reference and load the source into ctx.nodes.
        """
        from datajunction_server.construction.build_v3.loaders import (
            _load_missing_upstream_nodes,
            _node_load_options,
        )
        from datajunction_server.construction.build_v3.types import BuildContext
        from datajunction_server.database.catalog import Catalog
        from datajunction_server.database.node import Node, NodeRevision
        from datajunction_server.database.column import Column
        from datajunction_server.database.user import User
        from datajunction_server.models.node_type import NodeType
        from datajunction_server.models.user import OAuthProvider
        from datajunction_server.sql.parsing.types import IntegerType

        user = User(
            username="test_heal_user",
            email="test_heal@test.com",
            oauth_provider=OAuthProvider.BASIC,
        )
        clean_session.add(user)
        await clean_session.flush()

        catalog = Catalog(name="default")
        clean_session.add(catalog)
        await clean_session.flush()

        # Source node — exists in the DB but will NOT be in ctx.nodes initially
        src_node = Node(
            name="heal.src_orders",
            type=NodeType.SOURCE,
            created_by_id=user.id,
        )
        clean_session.add(src_node)
        await clean_session.flush()
        src_rev = NodeRevision(
            name=src_node.name,
            node_id=src_node.id,
            type=NodeType.SOURCE,
            version="v1.0",
            catalog_id=catalog.id,
            schema_="shop",
            table="orders",
            columns=[Column(name="order_id", type=IntegerType(), order=0)],
            created_by_id=user.id,
        )
        clean_session.add(src_rev)
        src_node.current_version = "v1.0"
        await clean_session.flush()

        # Transform node — its SQL references heal.src_orders
        tfm_node = Node(
            name="heal.order_summary",
            type=NodeType.TRANSFORM,
            created_by_id=user.id,
        )
        clean_session.add(tfm_node)
        await clean_session.flush()
        tfm_rev = NodeRevision(
            name=tfm_node.name,
            node_id=tfm_node.id,
            type=NodeType.TRANSFORM,
            version="v1.0",
            query="SELECT order_id FROM heal.src_orders",
            columns=[Column(name="order_id", type=IntegerType(), order=0)],
            created_by_id=user.id,
        )
        clean_session.add(tfm_rev)
        tfm_node.current_version = "v1.0"
        await clean_session.flush()
        await clean_session.commit()

        # Re-fetch transform with full eager loading (simulates how load_nodes populates ctx.nodes)
        from sqlalchemy import select as sa_select

        tfm_loaded = (
            await clean_session.execute(
                sa_select(Node)
                .where(Node.name == "heal.order_summary")
                .options(*_node_load_options()),
            )
        ).scalar_one()

        # Simulate stale NodeRelationship: transform is in ctx.nodes but source is not
        ctx = BuildContext(
            session=clean_session,
            metrics=[],
            dimensions=[],
        )
        ctx.nodes["heal.order_summary"] = tfm_loaded
        # heal.src_orders is intentionally absent from ctx.nodes

        await _load_missing_upstream_nodes(ctx)

        assert "heal.src_orders" in ctx.nodes, (
            "self-healing should have loaded the source node referenced in the transform SQL"
        )

    @pytest.mark.asyncio
    async def test_no_op_when_all_nodes_present(self, clean_session: AsyncSession):
        """
        When all referenced nodes are already in ctx.nodes, _load_missing_upstream_nodes
        should make no DB queries and leave ctx.nodes unchanged.
        """
        from datajunction_server.construction.build_v3.loaders import (
            _load_missing_upstream_nodes,
        )
        from datajunction_server.construction.build_v3.types import BuildContext
        from unittest.mock import AsyncMock, patch

        ctx = BuildContext(session=clean_session, metrics=[], dimensions=[])

        # Populate ctx.nodes with a mock transform whose SQL only references itself (CTE)
        mock_node = MagicMock()
        mock_node.type = NodeType.TRANSFORM
        mock_node.current.query = "WITH base AS (SELECT 1 AS x) SELECT x FROM base"
        ctx.nodes["my.transform"] = mock_node

        original_nodes = dict(ctx.nodes)

        with patch(
            "datajunction_server.construction.build_v3.loaders.find_upstream_node_names",
            new_callable=AsyncMock,
        ) as mock_find:
            await _load_missing_upstream_nodes(ctx)
            mock_find.assert_not_called()

        assert ctx.nodes == original_nodes
