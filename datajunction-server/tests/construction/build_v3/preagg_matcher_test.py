"""Tests for preagg_matcher.py - Pre-aggregation matching logic."""

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.construction.build_v3.preagg_matcher import (
    find_matching_preagg,
    get_preagg_measure_column,
    get_required_measure_hashes,
    get_temporal_partitions,
)
from datajunction_server.construction.build_v3.types import BuildContext, GrainGroup
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_expression_hash,
)
from datajunction_server.database.user import User
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
    PreAggMeasure,
)
from datajunction_server.models.dimensionlink import JoinType
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import Granularity, PartitionType
from datajunction_server.models.user import OAuthProvider
from datajunction_server.sql.parsing.types import IntegerType, StringType


def make_component(
    name: str,
    expression: str,
    aggregation: str = "SUM",
    merge: str = "SUM",
) -> MetricComponent:
    """Helper to create a MetricComponent for testing."""
    return MetricComponent(
        name=name,
        expression=expression,
        aggregation=aggregation,
        merge=merge,
        rule=AggregationRule(type=Aggregability.FULL),
    )


def make_preagg_measure(
    name: str,
    expression: str,
    aggregation: str = "SUM",
    merge: str = "SUM",
) -> PreAggMeasure:
    """Helper to create a PreAggMeasure with expr_hash for testing."""
    return PreAggMeasure(
        name=name,
        expression=expression,
        aggregation=aggregation,
        merge=merge,
        rule=AggregationRule(type=Aggregability.FULL),
        expr_hash=compute_expression_hash(expression),
    )


@pytest_asyncio.fixture
async def test_user(session: AsyncSession) -> User:
    """Create a test user."""
    user = User(
        username="test_preagg_matcher_user",
        email="test_preagg_matcher@test.com",
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(user)
    await session.flush()
    return user


@pytest_asyncio.fixture
async def parent_node(session: AsyncSession, test_user: User) -> Node:
    """Create a parent node with revision for testing."""
    node = Node(
        name="test.preagg.parent_node",
        type=NodeType.TRANSFORM,
        created_by_id=test_user.id,
    )
    session.add(node)
    await session.flush()

    revision = NodeRevision(
        name=node.name,
        node_id=node.id,
        type=NodeType.TRANSFORM,
        version="1",
        columns=[Column(name="col1", type=IntegerType(), order=0)],
        created_by_id=test_user.id,
    )
    session.add(revision)
    await session.flush()

    # Link current revision
    node.current_version = "1"
    node.current = revision
    await session.flush()

    return node


@pytest_asyncio.fixture
async def metric_node(session: AsyncSession, test_user: User) -> Node:
    """Create a metric node for testing."""
    node = Node(
        name="test.preagg.metric",
        type=NodeType.METRIC,
        created_by_id=test_user.id,
    )
    session.add(node)
    await session.flush()

    revision = NodeRevision(
        name=node.name,
        node_id=node.id,
        type=NodeType.METRIC,
        version="1",
        columns=[Column(name="value", type=IntegerType(), order=0)],
        created_by_id=test_user.id,
    )
    session.add(revision)
    await session.flush()

    node.current_version = "1"
    node.current = revision
    await session.flush()

    return node


def make_grain_group(
    parent_node: Node,
    components: list[tuple[Node, MetricComponent]],
) -> GrainGroup:
    """Create a GrainGroup for testing."""
    return GrainGroup(
        parent_node=parent_node,
        aggregability=Aggregability.FULL,
        grain_columns=[],
        components=components,
    )


class TestGetRequiredMeasureHashes:
    """Tests for get_required_measure_hashes function."""

    @pytest.mark.asyncio
    async def test_returns_hashes_for_all_components(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Should return a hash for each component in the grain group."""
        components = [
            (metric_node, make_component("sum_revenue", "price * quantity")),
            (metric_node, make_component("sum_quantity", "quantity")),
        ]
        grain_group = make_grain_group(parent_node, components)

        hashes = get_required_measure_hashes(grain_group)

        assert len(hashes) == 2
        assert compute_expression_hash("price * quantity") in hashes
        assert compute_expression_hash("quantity") in hashes

    @pytest.mark.asyncio
    async def test_empty_components_returns_empty_set(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Should return empty set when grain group has no components."""
        grain_group = make_grain_group(parent_node, [])

        hashes = get_required_measure_hashes(grain_group)

        assert hashes == set()

    @pytest.mark.asyncio
    async def test_deduplicates_same_expression(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Components with same expression should result in single hash."""
        # Same expression, different component names
        components = [
            (metric_node, make_component("sum_rev_1", "price * quantity")),
            (metric_node, make_component("sum_rev_2", "price * quantity")),
        ]
        grain_group = make_grain_group(parent_node, components)

        hashes = get_required_measure_hashes(grain_group)

        assert len(hashes) == 1


class TestFindMatchingPreagg:
    """Tests for find_matching_preagg function."""

    @pytest.mark.asyncio
    async def test_returns_none_when_use_materialized_false(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Should return None when use_materialized is disabled."""
        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=False,
        )
        grain_group = make_grain_group(parent_node, [])

        result = find_matching_preagg(ctx, parent_node, ["dim1"], grain_group)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_parent_has_no_current_revision(
        self,
        session: AsyncSession,
        test_user: User,
    ):
        """Should return None when parent_node.current is None."""
        # Create node without a current revision
        node = Node(
            name="test.preagg.no_revision",
            type=NodeType.TRANSFORM,
            created_by_id=test_user.id,
        )
        session.add(node)
        await session.flush()

        # Explicitly set current to None (node has no revision)
        node.current = None

        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
        )
        grain_group = make_grain_group(node, [])

        result = find_matching_preagg(ctx, node, ["dim1"], grain_group)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_available_preaggs(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Should return None when no pre-aggs available for the node revision."""
        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
            available_preaggs={},  # Empty
        )

        components = [(metric_node, make_component("sum_x", "x"))]
        grain_group = make_grain_group(parent_node, components)

        result = find_matching_preagg(ctx, parent_node, ["dim1"], grain_group)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_required_measures(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Should return None when grain group has no components."""
        # Create a preagg
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1"],
            measures=[make_preagg_measure("sum_x", "x")],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_01",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
            available_preaggs={parent_node.current.id: [preagg]},
        )

        grain_group = make_grain_group(parent_node, [])  # No components

        result = find_matching_preagg(ctx, parent_node, ["dim1"], grain_group)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_grain_not_covered(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Should return None when preagg grain doesn't cover requested grain."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        # Pre-agg only has dim1, but we need dim1 and dim2
        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1"],
            measures=[make_preagg_measure("sum_x", "x")],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_02",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
            available_preaggs={parent_node.current.id: [preagg]},
        )

        components = [(metric_node, make_component("sum_x", "x"))]
        grain_group = make_grain_group(parent_node, components)

        result = find_matching_preagg(
            ctx,
            parent_node,
            ["dim1", "dim2"],  # Requested grain requires dim2
            grain_group,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_measures_not_covered(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Should return None when preagg doesn't have required measures."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        # Pre-agg has sum_x, but we need sum_y
        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1"],
            measures=[make_preagg_measure("sum_x", "x")],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_03",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
            available_preaggs={parent_node.current.id: [preagg]},
        )

        components = [(metric_node, make_component("sum_y", "y"))]  # Different expr
        grain_group = make_grain_group(parent_node, components)

        result = find_matching_preagg(ctx, parent_node, ["dim1"], grain_group)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_matching_preagg(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Should return preagg when grain and measures match."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1", "dim2"],
            measures=[
                make_preagg_measure("sum_x", "x"),
                make_preagg_measure("sum_y", "y"),
            ],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_04",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
            available_preaggs={parent_node.current.id: [preagg]},
        )

        components = [(metric_node, make_component("sum_x", "x"))]
        grain_group = make_grain_group(parent_node, components)

        result = find_matching_preagg(ctx, parent_node, ["dim1"], grain_group)

        assert result is not None
        assert result.id == preagg.id

    @pytest.mark.asyncio
    async def test_prefers_smaller_grain(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Should prefer preagg with smaller grain (closer to requested)."""
        avail1 = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg1",
            valid_through_ts=9999999999,
        )
        avail2 = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg2",
            valid_through_ts=9999999999,
        )
        session.add_all([avail1, avail2])
        await session.flush()

        # Coarse grain (3 columns)
        preagg_coarse = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1", "dim2", "dim3"],
            measures=[make_preagg_measure("sum_x", "x")],
            sql="SELECT ...",
            grain_group_hash="hash_coarse",
            preagg_hash="match_05",
            availability_id=avail1.id,
        )
        # Fine grain (2 columns) - should be preferred
        preagg_fine = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1", "dim2"],
            measures=[make_preagg_measure("sum_x", "x")],
            sql="SELECT ...",
            grain_group_hash="hash_fine",
            preagg_hash="match_06",
            availability_id=avail2.id,
        )
        session.add_all([preagg_coarse, preagg_fine])
        await session.flush()

        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
            available_preaggs={parent_node.current.id: [preagg_coarse, preagg_fine]},
        )

        components = [(metric_node, make_component("sum_x", "x"))]
        grain_group = make_grain_group(parent_node, components)

        result = find_matching_preagg(ctx, parent_node, ["dim1"], grain_group)

        assert result is not None
        assert result.id == preagg_fine.id  # Fine grain preferred

    @pytest.mark.asyncio
    async def test_exact_grain_match(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Should match when requested grain exactly equals preagg grain."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1", "dim2"],
            measures=[make_preagg_measure("sum_x", "x")],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_07",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
            available_preaggs={parent_node.current.id: [preagg]},
        )

        components = [(metric_node, make_component("sum_x", "x"))]
        grain_group = make_grain_group(parent_node, components)

        result = find_matching_preagg(ctx, parent_node, ["dim1", "dim2"], grain_group)

        assert result is not None
        assert result.id == preagg.id

    @pytest.mark.asyncio
    async def test_superset_grain_match(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Should match when preagg grain is superset of requested grain."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        # Pre-agg has extra dimensions (superset)
        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1", "dim2", "dim3"],
            measures=[make_preagg_measure("sum_x", "x")],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_08",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
            available_preaggs={parent_node.current.id: [preagg]},
        )

        components = [(metric_node, make_component("sum_x", "x"))]
        grain_group = make_grain_group(parent_node, components)

        # Requesting only dim1 - preagg can roll up
        result = find_matching_preagg(ctx, parent_node, ["dim1"], grain_group)

        assert result is not None
        assert result.id == preagg.id


class TestGetPreaggMeasureColumn:
    """Tests for get_preagg_measure_column function."""

    @pytest.mark.asyncio
    async def test_returns_column_name_when_hash_matches(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Should return column name when component expr_hash matches."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1"],
            measures=[
                make_preagg_measure("total_revenue", "price * quantity"),
                make_preagg_measure("total_quantity", "quantity"),
            ],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_09",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        component = make_component("sum_revenue", "price * quantity")

        result = get_preagg_measure_column(preagg, component)

        assert result == "total_revenue"

    @pytest.mark.asyncio
    async def test_returns_none_when_no_match(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Should return None when no measure matches the component hash."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1"],
            measures=[make_preagg_measure("sum_x", "x")],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_10",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        component = make_component("sum_y", "y")  # Different expression

        result = get_preagg_measure_column(preagg, component)

        assert result is None

    @pytest.mark.asyncio
    async def test_matches_by_expression_not_name(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Should match by expression hash, not component name."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1"],
            measures=[make_preagg_measure("preagg_col_name", "x * y")],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_11",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        # Different name but same expression
        component = make_component("different_name", "x * y")

        result = get_preagg_measure_column(preagg, component)

        assert result == "preagg_col_name"

    @pytest.mark.asyncio
    async def test_handles_empty_measures(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Should return None when preagg has no measures."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1"],
            measures=[],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_12",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        component = make_component("sum_x", "x")

        result = get_preagg_measure_column(preagg, component)

        assert result is None

    @pytest.mark.asyncio
    async def test_handles_measure_without_expr_hash(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Should skip measures without expr_hash."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        # Create a measure without expr_hash
        measure_no_hash = PreAggMeasure(
            name="sum_x",
            expression="x",
            aggregation="SUM",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
            expr_hash=None,  # No hash
        )
        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1"],
            measures=[measure_no_hash],
            sql="SELECT ...",
            grain_group_hash="hash1",
            preagg_hash="match_13",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        component = make_component("sum_x", "x")

        result = get_preagg_measure_column(preagg, component)

        assert result is None


class TestGetTemporalPartitionsMultipleDimLinks:
    """Regression tests for get_temporal_partitions when a source column is
    referenced by multiple dimension links (e.g. one simple link and one
    multi-column link sharing the same FK column).
    """

    @pytest_asyncio.fixture
    async def multi_link_fact(
        self,
        session: AsyncSession,
        test_user: User,
    ) -> PreAggregation:
        """
        Build a fact node whose `region_date` column has two dimension links:
        - a simple link to `test.dim_date.dateint`
        - a multi-column link to `test.dim_tcd` (title_id, country_code, date_id),
          also referencing `region_date`.

        Links are inserted simple-first, multi-column-second, so in the buggy
        single-valued col_to_dim the multi-column link wins — matching the
        in-the-wild failure.

        Returns a PreAggregation attached to the fact with grain_columns set
        by the caller via `preagg.grain_columns = [...]`.
        """
        dim_date_node = Node(
            name="test.dim_date",
            type=NodeType.DIMENSION,
            created_by_id=test_user.id,
        )
        dim_tcd_node = Node(
            name="test.dim_tcd",
            type=NodeType.DIMENSION,
            created_by_id=test_user.id,
        )
        session.add_all([dim_date_node, dim_tcd_node])
        await session.flush()

        dim_date_rev = NodeRevision(
            name=dim_date_node.name,
            node_id=dim_date_node.id,
            type=NodeType.DIMENSION,
            version="1",
            columns=[Column(name="dateint", type=IntegerType(), order=0)],
            created_by_id=test_user.id,
        )
        dim_tcd_rev = NodeRevision(
            name=dim_tcd_node.name,
            node_id=dim_tcd_node.id,
            type=NodeType.DIMENSION,
            version="1",
            columns=[
                Column(name="title_id", type=IntegerType(), order=0),
                Column(name="country_code", type=StringType(), order=1),
                Column(name="date_id", type=IntegerType(), order=2),
            ],
            created_by_id=test_user.id,
        )
        session.add_all([dim_date_rev, dim_tcd_rev])
        await session.flush()
        dim_date_node.current_version = "1"
        dim_date_node.current = dim_date_rev
        dim_tcd_node.current_version = "1"
        dim_tcd_node.current = dim_tcd_rev

        fact_node = Node(
            name="test.fact",
            type=NodeType.SOURCE,
            created_by_id=test_user.id,
        )
        session.add(fact_node)
        await session.flush()

        region_date_col = Column(name="region_date", type=IntegerType(), order=0)
        fact_rev = NodeRevision(
            name=fact_node.name,
            node_id=fact_node.id,
            type=NodeType.SOURCE,
            version="1",
            columns=[
                region_date_col,
                Column(name="title_id", type=IntegerType(), order=1),
                Column(name="country_code", type=StringType(), order=2),
                Column(name="amt", type=IntegerType(), order=3),
            ],
            created_by_id=test_user.id,
        )
        session.add(fact_rev)
        await session.flush()
        fact_node.current_version = "1"
        fact_node.current = fact_rev

        # Temporal partition on region_date
        partition = Partition(
            column_id=region_date_col.id,
            type_=PartitionType.TEMPORAL,
            granularity=Granularity.DAY,
            format="yyyyMMdd",
        )
        session.add(partition)
        await session.flush()
        region_date_col.partition = partition

        # Simple link: fact.region_date = dim_date.dateint (inserted first)
        simple_link = DimensionLink(
            node_revision=fact_rev,
            dimension=dim_date_node,
            join_sql="test.fact.region_date = test.dim_date.dateint",
            join_type=JoinType.LEFT,
        )
        # Multi-column link: fact joins dim_tcd on (title_id, country_code, date_id),
        # where date_id also comes from region_date
        multi_link = DimensionLink(
            node_revision=fact_rev,
            dimension=dim_tcd_node,
            join_sql=(
                "test.fact.title_id = test.dim_tcd.title_id "
                "AND test.fact.country_code = test.dim_tcd.country_code "
                "AND test.fact.region_date = test.dim_tcd.date_id"
            ),
            join_type=JoinType.LEFT,
        )
        session.add_all([simple_link, multi_link])
        await session.flush()

        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="fact_preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        preagg = PreAggregation(
            node_revision_id=fact_rev.id,
            grain_columns=[],  # caller sets this per-test
            measures=[make_preagg_measure("sum_amt", "amt")],
            sql="SELECT ...",
            grain_group_hash="hash_tp",
            preagg_hash="tp01",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        # Re-query with eager-loaded relationships. get_temporal_partitions is
        # synchronous and walks node_revision.columns, .dimension_links, and
        # each column's .partition — all of which must already be populated
        # to avoid MissingGreenlet lazy-load errors in an async session.
        result = await session.execute(
            select(PreAggregation)
            .where(PreAggregation.id == preagg.id)
            .options(
                joinedload(PreAggregation.node_revision).options(
                    selectinload(NodeRevision.columns).joinedload(Column.partition),
                    selectinload(NodeRevision.dimension_links).joinedload(
                        DimensionLink.dimension,
                    ),
                ),
            ),
        )
        return result.unique().scalar_one()

    @pytest.mark.asyncio
    async def test_resolves_output_name_via_simple_link_when_both_links_share_fk(
        self,
        session: AsyncSession,
        multi_link_fact: PreAggregation,
    ):
        """
        When `region_date` maps to both `dim_date.dateint` (simple) and
        `dim_tcd.date_id` (multi-column), and the user selected the simple
        link's dim in the cube grain, the output column must resolve to
        `dateint` — not the source name `region_date`.

        Before the fix, col_to_dim was a single-valued dict, so the
        multi-column link overwrote the simple link and strategy 2 failed
        to match grain_columns, leaving output_name = 'region_date'.
        """
        multi_link_fact.grain_columns = ["test.dim_date.dateint"]

        partitions = get_temporal_partitions(multi_link_fact)

        assert len(partitions) == 1
        assert partitions[0].column_name == "dateint"
