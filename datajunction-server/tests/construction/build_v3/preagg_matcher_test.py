"""Tests for preagg_matcher.py - Pre-aggregation matching logic."""

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.config import Settings
from datajunction_server.construction.build_v3.preagg_matcher import (
    JoinBackCoverage,
    is_join_back_safe,
    join_back_coverage,
    match_temporal_columns_to_grain,
    temporal_output_name,
    find_matching_preagg,
    get_preagg_dimension_column,
    get_preagg_measure_column,
    get_required_measure_identities,
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
from datajunction_server.models.query import V3ColumnMetadata
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


def test_get_preagg_dimension_column():
    """Returns the mapped physical column when present, else the DJ column name."""
    preagg = PreAggregation(
        columns=[
            V3ColumnMetadata(
                name="status",
                type="string",
                semantic_name="v3.order_details.status",
                semantic_type="dimension",
                source_column="order_status",
            ),
        ],
    )
    ctx = BuildContext(session=None, metrics=[], dimensions=[])
    # Mapped dimension -> its physical source column.
    assert (
        get_preagg_dimension_column(
            ctx,
            1,
            preagg,
            "v3.order_details.status",
            "status",
        )
        == "order_status"
    )
    # A dimension not in the pre-agg's columns falls back to the DJ column name.
    assert (
        get_preagg_dimension_column(ctx, 1, preagg, "v3.order_details.other", "other")
        == "other"
    )
    # No columns at all also falls back.
    assert (
        get_preagg_dimension_column(
            ctx,
            1,
            PreAggregation(columns=None),
            "v3.order_details.status",
            "status",
        )
        == "status"
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


class TestGetRequiredMeasureIdentities:
    """Tests for get_required_measure_identities function."""

    @pytest.mark.asyncio
    async def test_returns_identities_for_all_components(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Should return an (expression hash, aggregation) pair per component."""
        components = [
            (metric_node, make_component("sum_revenue", "price * quantity")),
            (metric_node, make_component("sum_quantity", "quantity")),
        ]
        grain_group = make_grain_group(parent_node, components)

        identities = get_required_measure_identities(grain_group)

        assert identities == {
            (compute_expression_hash("price * quantity"), "SUM"),
            (compute_expression_hash("quantity"), "SUM"),
        }

    @pytest.mark.asyncio
    async def test_empty_components_returns_empty_set(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Should return empty set when grain group has no components."""
        grain_group = make_grain_group(parent_node, [])

        identities = get_required_measure_identities(grain_group)

        assert identities == set()

    @pytest.mark.asyncio
    async def test_deduplicates_same_expression_and_aggregation(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """Components with same expression AND aggregation collapse to one."""
        # Same expression, different component names
        components = [
            (metric_node, make_component("sum_rev_1", "price * quantity")),
            (metric_node, make_component("sum_rev_2", "price * quantity")),
        ]
        grain_group = make_grain_group(parent_node, components)

        identities = get_required_measure_identities(grain_group)

        assert identities == {(compute_expression_hash("price * quantity"), "SUM")}

    @pytest.mark.asyncio
    async def test_same_expression_different_aggregation_are_distinct(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """The aggregation is part of the identity: SUM(x) and MAX(x) differ.

        This is what stops a MAX metric from binding a SUM-built pre-agg column.
        """
        components = [
            (metric_node, make_component("sum_price", "unit_price")),
            (
                metric_node,
                make_component(
                    "max_price",
                    "unit_price",
                    aggregation="MAX",
                    merge="MAX",
                ),
            ),
        ]
        grain_group = make_grain_group(parent_node, components)

        identities = get_required_measure_identities(grain_group)

        assert identities == {
            (compute_expression_hash("unit_price"), "SUM"),
            (compute_expression_hash("unit_price"), "MAX"),
        }


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

    @pytest.mark.asyncio
    async def test_non_additive_measure_requires_exact_grain(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """A non-additive (LIMITED) measure must NOT be rolled up to a coarser
        grain: a finer-grain pre-agg cannot satisfy a coarser-grain query
        because re-aggregating a distinct count double-counts."""
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
                make_preagg_measure(
                    "num_users",
                    "DISTINCT user_id",
                    aggregation="COUNT",
                ),
            ],
            sql="SELECT ...",
            grain_group_hash="hash_na1",
            preagg_hash="naex1",
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

        component = MetricComponent(
            name="num_users",
            expression="DISTINCT user_id",
            aggregation="COUNT",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.LIMITED, level=["user_id"]),
        )
        grain_group = make_grain_group(parent_node, [(metric_node, component)])

        # Requesting a coarser grain than the pre-agg: must NOT match.
        result = find_matching_preagg(ctx, parent_node, ["dim1"], grain_group)

        assert result is None

    @pytest.mark.asyncio
    async def test_non_additive_measure_matches_exact_grain(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
    ):
        """A non-additive measure still matches when the pre-agg grain is exactly
        the requested grain (no roll-up needed, so the value is correct)."""
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
                make_preagg_measure(
                    "num_users",
                    "DISTINCT user_id",
                    aggregation="COUNT",
                ),
            ],
            sql="SELECT ...",
            grain_group_hash="hash_na2",
            preagg_hash="naex2",
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

        component = MetricComponent(
            name="num_users",
            expression="DISTINCT user_id",
            aggregation="COUNT",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.LIMITED, level=["user_id"]),
        )
        grain_group = make_grain_group(parent_node, [(metric_node, component)])

        # Requesting exactly the pre-agg grain: no roll-up, so this is valid.
        result = find_matching_preagg(
            ctx,
            parent_node,
            ["dim1", "dim2"],
            grain_group,
        )

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
    async def test_prefers_source_column(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Externally-registered measures resolve to their physical source_column."""
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        await session.flush()

        measure = PreAggMeasure(
            name="total_revenue",
            expression="price * quantity",
            aggregation="SUM",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
            expr_hash=compute_expression_hash("price * quantity"),
            source_column="revenue_physical",
        )
        preagg = PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=["dim1"],
            measures=[measure],
            sql="SELECT ...",
            grain_group_hash="hash_sc",
            preagg_hash="srccol1",
            availability_id=avail.id,
        )
        session.add(preagg)
        await session.flush()

        component = make_component("sum_revenue", "price * quantity")

        assert get_preagg_measure_column(preagg, component) == "revenue_physical"

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


def _temporal_column(name: str, format_: str = "yyyyMMdd") -> Column:
    """A temporal partition column, detached from any session."""
    column = Column(name=name, type=IntegerType(), order=0)
    column.partition = Partition(
        type_=PartitionType.TEMPORAL,
        granularity=Granularity.DAY,
        format=format_,
    )
    return column


def _preagg_with(columns: list[Column], grain_columns: list[str]) -> PreAggregation:
    """A pre-agg over a detached parent revision carrying the given columns."""
    preagg = PreAggregation(grain_columns=grain_columns)
    preagg.node_revision = NodeRevision(
        name="v3.order_details",
        type=NodeType.TRANSFORM,
        version="v1.0",
        columns=columns,
    )
    return preagg


class TestMatchTemporalColumnsToGrain:
    """Pairing a parent's temporal partition columns with the grain entries they reach."""

    def test_no_node_revision(self):
        assert match_temporal_columns_to_grain(PreAggregation(grain_columns=[])) == []

    def test_no_temporal_columns(self):
        preagg = _preagg_with(
            [Column(name="order_date", type=IntegerType(), order=0)],
            ["v3.order_details.order_date"],
        )
        assert match_temporal_columns_to_grain(preagg) == []

    def test_matched_via_the_columns_own_dimension_reference(self):
        """Strategy 3: the column carries a dimension reference of its own."""
        column = _temporal_column("date_int")
        column.dimension = Node(name="v3.date", type=NodeType.DIMENSION)
        preagg = _preagg_with([column], ["v3.date.week[order]"])
        assert match_temporal_columns_to_grain(preagg) == [
            (column, "v3.date.week[order]"),
        ]

    def test_dimension_reference_not_in_grain(self):
        column = _temporal_column("date_int")
        column.dimension = Node(name="v3.date", type=NodeType.DIMENSION)
        preagg = _preagg_with([column], ["v3.order_details.status"])
        assert match_temporal_columns_to_grain(preagg) == [(column, None)]

    def test_second_column_reuses_the_link_map(self):
        """The dimension-link map is built once, not per temporal column."""
        first = _temporal_column("date_int")
        second = _temporal_column("hour_int")
        preagg = _preagg_with([first, second], ["v3.order_details.status"])
        assert match_temporal_columns_to_grain(preagg) == [
            (first, None),
            (second, None),
        ]


class TestTemporalOutputName:
    """Naming the output column from the grain entry a temporal column matched."""

    def test_falls_back_to_the_source_name(self):
        assert (
            temporal_output_name(_temporal_column("order_date"), None) == "order_date"
        )

    def test_plain_grain_reference(self):
        column = _temporal_column("order_date")
        assert (
            temporal_output_name(column, "v3.order_details.order_date") == "order_date"
        )

    def test_role_qualified_grain_reference(self):
        column = _temporal_column("date_int")
        assert temporal_output_name(column, "v3.date.week[order]") == "week_order"


JOIN_BACK_SETTINGS = (
    "datajunction_server.construction.build_v3.preagg_matcher.get_settings"
)


def _dimension_node(name: str, columns: list[str], primary_key: list[str]) -> Node:
    """A dimension node whose current revision declares a primary key."""
    from datajunction_server.database.attributetype import (
        AttributeType,
        ColumnAttribute,
    )

    pk_attr = AttributeType(namespace="system", name="primary_key")
    cols = []
    for order, col_name in enumerate(columns):
        col = Column(name=col_name, type=IntegerType(), order=order)
        if col_name in primary_key:
            col.attributes = [ColumnAttribute(attribute_type=pk_attr)]
        cols.append(col)
    node = Node(name=name, type=NodeType.DIMENSION, current_version="v1.0")
    node.current = NodeRevision(
        name=name,
        type=NodeType.DIMENSION,
        version="v1.0",
        columns=cols,
    )
    return node


def _ctx_with(dimension: Node, hops: int = 1, role: str = "customer") -> BuildContext:
    """A build context knowing one dimension, reachable in ``hops`` links."""
    ctx = BuildContext(session=None, metrics=[], dimensions=[])
    ctx.nodes[dimension.name] = dimension
    ctx.join_paths[(1, dimension.name, role)] = [
        DimensionLink(join_sql="a = b") for _ in range(hops)
    ]
    return ctx


class TestJoinBackCoverage:
    """Deciding whether a requested attribute is one join off a retained key."""

    def test_attribute_of_a_dimension_whose_key_is_retained(self):
        ctx = _ctx_with(
            _dimension_node("v3.customer", ["customer_id", "name"], ["customer_id"]),
        )
        coverage = join_back_coverage(
            ctx,
            1,
            {"v3.customer.name[customer]", "v3.order_details.status"},
            {"v3.customer.customer_id[customer]", "v3.order_details.status"},
        )
        assert coverage == [
            JoinBackCoverage(
                dimension_ref="v3.customer.name[customer]",
                key_refs=("v3.customer.customer_id[customer]",),
                dimension_node="v3.customer",
                role="customer",
            ),
        ]

    def test_retained_column_is_not_the_key(self):
        """Joining on a non-key column can match many rows and multiply measures."""
        ctx = _ctx_with(
            _dimension_node(
                "v3.customer",
                ["customer_id", "name", "location_id"],
                ["customer_id"],
            ),
        )
        assert (
            join_back_coverage(
                ctx,
                1,
                {"v3.customer.name[customer]"},
                {"v3.customer.location_id[customer]"},
            )
            is None
        )

    def test_composite_key_only_partly_retained(self):
        ctx = _ctx_with(
            _dimension_node(
                "v3.customer",
                ["customer_id", "region", "name"],
                ["customer_id", "region"],
            ),
        )
        assert (
            join_back_coverage(
                ctx,
                1,
                {"v3.customer.name[customer]"},
                {"v3.customer.customer_id[customer]"},
            )
            is None
        )

    def test_composite_key_fully_retained(self):
        ctx = _ctx_with(
            _dimension_node(
                "v3.customer",
                ["customer_id", "region", "name"],
                ["customer_id", "region"],
            ),
        )
        coverage = join_back_coverage(
            ctx,
            1,
            {"v3.customer.name[customer]"},
            {"v3.customer.customer_id[customer]", "v3.customer.region[customer]"},
        )
        assert coverage is not None
        assert coverage[0].key_refs == (
            "v3.customer.customer_id[customer]",
            "v3.customer.region[customer]",
        )

    def test_role_must_match(self):
        """Same dimension at another role is a different join path."""
        ctx = _ctx_with(
            _dimension_node("v3.customer", ["customer_id", "name"], ["customer_id"]),
            role="billing",
        )
        assert (
            join_back_coverage(
                ctx,
                1,
                {"v3.customer.name[billing]"},
                {"v3.customer.customer_id[customer]"},
            )
            is None
        )

    def test_two_dimensions_each_joined_back(self):
        ctx = BuildContext(session=None, metrics=[], dimensions=[])
        for name, cols in (
            ("v3.customer", ["customer_id", "name"]),
            ("v3.product", ["product_id", "category"]),
        ):
            node = _dimension_node(name, cols, [cols[0]])
            ctx.nodes[name] = node
            ctx.join_paths[(1, name, "")] = [DimensionLink(join_sql="a = b")]
        coverage = join_back_coverage(
            ctx,
            1,
            {"v3.customer.name", "v3.product.category"},
            {"v3.customer.customer_id", "v3.product.product_id"},
        )
        assert coverage is not None
        assert [c.dimension_ref for c in coverage] == [
            "v3.customer.name",
            "v3.product.category",
        ]

    def test_dimension_not_loaded(self):
        ctx = BuildContext(session=None, metrics=[], dimensions=[])
        assert (
            join_back_coverage(
                ctx,
                1,
                {"v3.customer.name"},
                {"v3.customer.customer_id"},
            )
            is None
        )

    def test_dimension_without_a_primary_key(self):
        ctx = _ctx_with(_dimension_node("v3.customer", ["customer_id", "name"], []))
        assert (
            join_back_coverage(
                ctx,
                1,
                {"v3.customer.name[customer]"},
                {"v3.customer.customer_id[customer]"},
            )
            is None
        )

    def test_unparseable_reference(self):
        ctx = _ctx_with(
            _dimension_node("v3.customer", ["customer_id", "name"], ["customer_id"]),
        )
        assert join_back_coverage(ctx, 1, {"status"}, set()) is None

    def test_multi_hop_dimension_is_not_covered(self):
        """The seam refuses chains, so a two-link path can't be joined back."""
        ctx = _ctx_with(
            _dimension_node("v3.customer", ["customer_id", "name"], ["customer_id"]),
            hops=2,
        )
        assert (
            join_back_coverage(
                ctx,
                1,
                {"v3.customer.name[customer]"},
                {"v3.customer.customer_id[customer]"},
            )
            is None
        )

    def test_requested_column_is_itself_the_key(self):
        """A key request would have matched directly; reaching here means it's absent."""
        ctx = _ctx_with(
            _dimension_node("v3.customer", ["customer_id", "name"], ["customer_id"]),
        )
        assert (
            join_back_coverage(ctx, 1, {"v3.customer.customer_id[customer]"}, set())
            is None
        )


class TestIsJoinBackSafe:
    """The seam that #2346's declared cardinality will replace."""

    def test_single_hop_is_safe(self):
        ctx = _ctx_with(
            _dimension_node("v3.customer", ["customer_id", "name"], ["customer_id"]),
        )
        assert is_join_back_safe(ctx, 1, "v3.customer", "customer") is True

    def test_multi_hop_is_refused_for_now(self):
        ctx = _ctx_with(
            _dimension_node("v3.customer", ["customer_id", "name"], ["customer_id"]),
            hops=2,
        )
        assert is_join_back_safe(ctx, 1, "v3.customer", "customer") is False

    def test_no_path_is_not_safe(self):
        ctx = BuildContext(session=None, metrics=[], dimensions=[])
        assert is_join_back_safe(ctx, 1, "v3.customer", "customer") is False


class TestFindMatchingPreaggJoinBack:
    """find_matching_preagg with the join-back gate on."""

    def _setup(self, session, parent_node, metric_node, grain_columns, preagg_hash):
        avail = AvailabilityState(
            catalog="test",
            schema_="test",
            table="preagg",
            valid_through_ts=9999999999,
        )
        session.add(avail)
        return avail, PreAggregation(
            node_revision_id=parent_node.current.id,
            grain_columns=grain_columns,
            measures=[make_preagg_measure("sum_x", "x")],
            sql="SELECT ...",
            grain_group_hash=preagg_hash,
            preagg_hash=preagg_hash,
        )

    def _customer_ctx(self, session, parent_node, preaggs):
        ctx = BuildContext(
            session=session,
            metrics=["test.metric"],
            dimensions=["test.dim"],
            use_materialized=True,
            available_preaggs={parent_node.current.id: preaggs},
        )
        customer = _dimension_node(
            "v3.customer",
            ["customer_id", "name"],
            ["customer_id"],
        )
        ctx.nodes["v3.customer"] = customer
        ctx.join_paths[(parent_node.current.id, "v3.customer", "")] = [
            DimensionLink(join_sql="a = b"),
        ]
        return ctx

    @pytest.mark.asyncio
    async def test_matches_by_joining_back_to_a_retained_key(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
        mocker,
    ):
        """An attribute absent from the grain is reachable via the retained key."""
        mocker.patch(
            JOIN_BACK_SETTINGS,
            return_value=Settings(preagg_join_back=True),
        )
        avail, preagg = self._setup(
            session,
            parent_node,
            metric_node,
            ["v3.customer.customer_id"],
            "jb01",
        )
        await session.flush()
        preagg.availability_id = avail.id
        session.add(preagg)
        await session.flush()

        ctx = self._customer_ctx(session, parent_node, [preagg])
        grain_group = make_grain_group(
            parent_node,
            [(metric_node, make_component("sum_x", "x"))],
        )
        result = find_matching_preagg(
            ctx,
            parent_node,
            ["v3.customer.name"],
            grain_group,
        )
        assert result is not None
        assert result.id == preagg.id

    @pytest.mark.asyncio
    async def test_gate_off_does_not_match(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
        mocker,
    ):
        """Default configuration leaves routing exactly as it was."""
        mocker.patch(
            JOIN_BACK_SETTINGS,
            return_value=Settings(preagg_join_back=False),
        )
        avail, preagg = self._setup(
            session,
            parent_node,
            metric_node,
            ["v3.customer.customer_id"],
            "jb02",
        )
        await session.flush()
        preagg.availability_id = avail.id
        session.add(preagg)
        await session.flush()

        ctx = self._customer_ctx(session, parent_node, [preagg])
        grain_group = make_grain_group(
            parent_node,
            [(metric_node, make_component("sum_x", "x"))],
        )
        assert (
            find_matching_preagg(ctx, parent_node, ["v3.customer.name"], grain_group)
            is None
        )

    @pytest.mark.asyncio
    async def test_direct_match_beats_a_join_back_at_finer_grain(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
        mocker,
    ):
        """Joins rank ahead of grain size, so a direct scan always wins."""
        mocker.patch(
            JOIN_BACK_SETTINGS,
            return_value=Settings(preagg_join_back=True),
        )
        # The joining candidate has the smaller grain, so grain size alone would
        # pick it over the one that covers the request outright.
        avail_a, joining = self._setup(
            session,
            parent_node,
            metric_node,
            ["v3.customer.customer_id"],
            "jb03a",
        )
        avail_b, direct = self._setup(
            session,
            parent_node,
            metric_node,
            ["v3.customer.name", "dim2", "dim3"],
            "jb03b",
        )
        await session.flush()
        joining.availability_id = avail_a.id
        direct.availability_id = avail_b.id
        session.add_all([joining, direct])
        await session.flush()

        ctx = self._customer_ctx(session, parent_node, [joining, direct])
        grain_group = make_grain_group(
            parent_node,
            [(metric_node, make_component("sum_x", "x"))],
        )
        result = find_matching_preagg(
            ctx,
            parent_node,
            ["v3.customer.name"],
            grain_group,
        )
        assert result is not None
        assert result.id == direct.id

    @pytest.mark.asyncio
    async def test_non_additive_measure_never_joins_back(
        self,
        session: AsyncSession,
        parent_node: Node,
        metric_node: Node,
        mocker,
    ):
        """Exact-grain demands set equality, which a join back can never satisfy."""
        mocker.patch(
            JOIN_BACK_SETTINGS,
            return_value=Settings(preagg_join_back=True),
        )
        avail, preagg = self._setup(
            session,
            parent_node,
            metric_node,
            ["v3.customer.customer_id"],
            "jb04",
        )
        await session.flush()
        preagg.availability_id = avail.id
        session.add(preagg)
        await session.flush()

        ctx = self._customer_ctx(session, parent_node, [preagg])
        limited = make_component("count_distinct_x", "x")
        limited.rule = AggregationRule(type=Aggregability.LIMITED, level=["x"])
        grain_group = make_grain_group(parent_node, [(metric_node, limited)])
        assert (
            find_matching_preagg(ctx, parent_node, ["v3.customer.name"], grain_group)
            is None
        )
