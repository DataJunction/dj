"""Tests for preagg_freshness.py - freshness gating for pre-aggregations."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.construction.build_v3.preagg_freshness import (
    _dimension_ref_of,
    freshness_gating_enabled,
    preagg_is_fresh,
    query_upper_bound,
    render_partition_value,
    temporal_grain_refs,
)
from datajunction_server.construction.build_v3.types import BuildContext
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
    PreAggMeasure,
)
from datajunction_server.models.dimensionlink import JoinType
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import Granularity, PartitionType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.types import IntegerType

SETTINGS_PATH = (
    "datajunction_server.construction.build_v3.preagg_freshness.get_settings"
)


def configure(mocker, *, gating: bool, max_staleness: int | None = None) -> None:
    """Point the module's settings lookup at an explicit gating configuration."""
    mocker.patch(
        SETTINGS_PATH,
        return_value=SimpleNamespace(
            preagg_freshness_gating=gating,
            preagg_max_staleness_seconds=max_staleness,
        ),
    )


def make_context(filters: list[str]) -> BuildContext:
    """A build context carrying nothing but the query's dimension filters."""
    return BuildContext(
        session=None,
        metrics=[],
        dimensions=[],
        dimension_filters=filters,
    )


def make_preagg(
    grain_columns: list[str],
    valid_through_ts: int,
    node_revision: NodeRevision | None = None,
) -> PreAggregation:
    """A pre-agg with availability, detached from any session."""
    preagg = PreAggregation(
        id=7,
        node_revision_id=1,
        grain_columns=grain_columns,
        measures=[
            PreAggMeasure(
                name="line_total_sum",
                expression="line_total",
                aggregation="SUM",
                merge="SUM",
                rule=AggregationRule(type=Aggregability.FULL),
                expr_hash=compute_expression_hash("line_total"),
            ),
        ],
        sql="SELECT 1",
        grain_group_hash="hash",
        preagg_hash="fresh001",
    )
    preagg.availability = AvailabilityState(
        catalog="default",
        schema_="analytics",
        table="preagg",
        valid_through_ts=valid_through_ts,
    )
    if node_revision is not None:
        preagg.node_revision = node_revision
    return preagg


def make_partitioned_revision(
    column_name: str = "order_date",
    format_: str | None = "yyyyMMdd",
) -> NodeRevision:
    """A detached node revision with one temporal partition column."""
    column = Column(name=column_name, type=IntegerType(), order=0)
    column.partition = Partition(
        type_=PartitionType.TEMPORAL,
        granularity=Granularity.DAY,
        format=format_,
    )
    return NodeRevision(
        name="v3.order_details",
        type=NodeType.TRANSFORM,
        version="v1.0",
        columns=[column],
    )


class TestRenderPartitionValue:
    """Rendering a moment into a partition format's integer encoding."""

    def test_day_granularity(self):
        moment = datetime(2026, 7, 21, 5, 4, 3, tzinfo=timezone.utc)
        assert render_partition_value(moment, "yyyyMMdd") == 20260721

    def test_sub_day_granularity(self):
        moment = datetime(2026, 7, 21, 5, 4, 3, tzinfo=timezone.utc)
        assert render_partition_value(moment, "yyyyMMddHHmmss") == 20260721050403

    def test_coarse_granularity(self):
        moment = datetime(2026, 7, 21, tzinfo=timezone.utc)
        assert render_partition_value(moment, "yyyyMM") == 202607

    def test_no_format(self):
        assert render_partition_value(datetime(2026, 7, 21), None) is None

    def test_non_numeric_format(self):
        assert render_partition_value(datetime(2026, 7, 21), "yyyy-MM-dd") is None


class TestQueryUpperBound:
    """Reading the query's ceiling on a temporal dimension out of its filters."""

    def test_no_filters(self):
        assert query_upper_bound(make_context([]), 1, "v3.date.date_id") is None

    def test_less_than_or_equal(self):
        ctx = make_context(["v3.date.date_id <= 20250101"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") == 20250101

    def test_strict_less_than_lowers_the_bound(self):
        ctx = make_context(["v3.date.date_id < 20250102"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") == 20250101

    def test_equality(self):
        ctx = make_context(["v3.date.date_id = 20250101"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") == 20250101

    def test_between(self):
        ctx = make_context(["v3.date.date_id BETWEEN 20240101 AND 20250101"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") == 20250101

    def test_between_on_another_dimension(self):
        ctx = make_context(["v3.product.price BETWEEN 1 AND 20250101"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") is None

    def test_between_non_integer_literal(self):
        ctx = make_context(["v3.date.date_id BETWEEN '2024-01-01' AND '2025-01-01'"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") is None

    def test_negated_between_is_not_a_bound(self):
        ctx = make_context(["v3.date.date_id NOT BETWEEN 20240101 AND 20250101"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") is None

    def test_tightest_of_several_filters_wins(self):
        ctx = make_context(
            [
                "v3.date.date_id <= 20250601",
                "v3.date.date_id <= 20250101",
                "v3.order_details.status = 'shipped'",
            ],
        )
        assert query_upper_bound(ctx, 1, "v3.date.date_id") == 20250101

    def test_lower_bound_only(self):
        ctx = make_context(["v3.date.date_id >= 20250101"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") is None

    def test_other_dimension(self):
        ctx = make_context(["v3.product.category <= 20250101"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") is None

    def test_non_integer_literal(self):
        ctx = make_context(["v3.date.date_id <= '2025-01-01'"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") is None

    def test_disjunction_is_skipped(self):
        ctx = make_context(
            ["v3.date.date_id <= 20250101 OR v3.order_details.status = 'shipped'"],
        )
        assert query_upper_bound(ctx, 1, "v3.date.date_id") is None

    def test_expression_over_the_column_is_not_a_bound(self):
        ctx = make_context(["DATE_TRUNC('day', v3.date.date_id) <= 20250101"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id") is None

    def test_role_qualified_reference(self):
        ctx = make_context(["v3.date.date_id[order] <= 20250101"])
        assert query_upper_bound(ctx, 1, "v3.date.date_id[order]") == 20250101

    def test_subscript_over_a_non_column_is_not_a_reference(self):
        subscript = ast.Subscript(
            expr=ast.Number(1),
            index=ast.Column(ast.Name("order")),
        )
        assert _dimension_ref_of(make_context([]), 1, subscript) is None


class TestTemporalGrainRefs:
    """Locating the pre-agg's temporal axis within its grain."""

    def test_no_node_revision(self):
        assert temporal_grain_refs(make_preagg(["v3.date.date_id"], 20250101)) == []

    def test_source_column_directly_in_grain(self):
        preagg = make_preagg(
            ["v3.order_details.order_date"],
            20250101,
            make_partitioned_revision(),
        )
        assert temporal_grain_refs(preagg) == [
            ("v3.order_details.order_date", "yyyyMMdd"),
        ]

    def test_temporal_column_absent_from_grain(self):
        preagg = make_preagg(
            ["v3.order_details.status"],
            20250101,
            make_partitioned_revision(),
        )
        assert temporal_grain_refs(preagg) == []

    def test_no_temporal_partition_at_all(self):
        revision = NodeRevision(
            name="v3.order_details",
            type=NodeType.TRANSFORM,
            version="v1.0",
            columns=[Column(name="order_date", type=IntegerType(), order=0)],
        )
        preagg = make_preagg(["v3.order_details.order_date"], 20250101, revision)
        assert temporal_grain_refs(preagg) == []


class TestPreaggIsFresh:
    """The gate itself."""

    def test_gating_off_allows_anything(self, mocker):
        configure(mocker, gating=False)
        preagg = make_preagg(
            ["v3.order_details.order_date"],
            20200101,
            make_partitioned_revision(),
        )
        ctx = make_context(["v3.order_details.order_date <= 20260101"])
        assert preagg_is_fresh(ctx, preagg) is True

    def test_no_temporal_grain_is_not_judged(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(["v3.order_details.status"], 20200101)
        assert preagg_is_fresh(make_context([]), preagg) is True

    def test_query_bounded_within_the_watermark(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(
            ["v3.order_details.order_date"],
            20250101,
            make_partitioned_revision(),
        )
        ctx = make_context(["v3.order_details.order_date <= 20240301"])
        assert preagg_is_fresh(ctx, preagg) is True

    def test_query_bounded_past_the_watermark(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(
            ["v3.order_details.order_date"],
            20250101,
            make_partitioned_revision(),
        )
        ctx = make_context(["v3.order_details.order_date <= 20250601"])
        assert preagg_is_fresh(ctx, preagg) is False

    def test_open_ended_query_without_a_budget(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(
            ["v3.order_details.order_date"],
            20200101,
            make_partitioned_revision(),
        )
        assert preagg_is_fresh(make_context([]), preagg) is True

    def test_open_ended_query_within_the_budget(self, mocker):
        configure(mocker, gating=True, max_staleness=86400 * 7)
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        preagg = make_preagg(
            ["v3.order_details.order_date"],
            int(yesterday.strftime("%Y%m%d")),
            make_partitioned_revision(),
        )
        assert preagg_is_fresh(make_context([]), preagg) is True

    def test_open_ended_query_past_the_budget(self, mocker):
        configure(mocker, gating=True, max_staleness=86400)
        preagg = make_preagg(
            ["v3.order_details.order_date"],
            20200101,
            make_partitioned_revision(),
        )
        assert preagg_is_fresh(make_context([]), preagg) is False

    def test_open_ended_query_with_an_unrenderable_format(self, mocker):
        configure(mocker, gating=True, max_staleness=86400)
        preagg = make_preagg(
            ["v3.order_details.order_date"],
            20200101,
            make_partitioned_revision(format_="yyyy-MM-dd"),
        )
        assert preagg_is_fresh(make_context([]), preagg) is True

    def test_one_unbounded_axis_makes_the_query_open_ended(self, mocker):
        """A bound on one temporal axis doesn't cover a second, unfiltered one."""
        configure(mocker, gating=True)
        revision = make_partitioned_revision()
        second = Column(name="ship_date", type=IntegerType(), order=1)
        second.partition = Partition(
            type_=PartitionType.TEMPORAL,
            granularity=Granularity.DAY,
            format="yyyyMMdd",
        )
        revision.columns.append(second)
        preagg = make_preagg(
            ["v3.order_details.order_date", "v3.order_details.ship_date"],
            20200101,
            revision,
        )
        # The bound on order_date alone would reject; with ship_date unbounded the
        # gate falls through to the (unset) staleness budget and allows instead.
        ctx = make_context(["v3.order_details.order_date <= 20260101"])
        assert preagg_is_fresh(ctx, preagg) is True


def test_freshness_gating_enabled_reads_settings(mocker):
    """The loader's cheap check reads the same setting."""
    configure(mocker, gating=True)
    assert freshness_gating_enabled() is True
    configure(mocker, gating=False)
    assert freshness_gating_enabled() is False


@pytest_asyncio.fixture
async def linked_preagg(session: AsyncSession) -> PreAggregation:
    """
    A pre-agg on a fact whose temporal partition column feeds a dimension link,
    with the linked dimension attribute — not the source column — in its grain.
    """
    user = (await session.execute(select(User).limit(1))).scalars().one()

    date_node = Node(
        name="freshness.dim_date",
        type=NodeType.DIMENSION,
        created_by_id=user.id,
    )
    fact_node = Node(
        name="freshness.fact",
        type=NodeType.SOURCE,
        created_by_id=user.id,
    )
    session.add_all([date_node, fact_node])
    await session.flush()

    date_rev = NodeRevision(
        name=date_node.name,
        node_id=date_node.id,
        type=NodeType.DIMENSION,
        version="v1.0",
        columns=[Column(name="dateint", type=IntegerType(), order=0)],
        created_by_id=user.id,
    )
    order_date = Column(name="order_date", type=IntegerType(), order=0)
    fact_rev = NodeRevision(
        name=fact_node.name,
        node_id=fact_node.id,
        type=NodeType.SOURCE,
        version="v1.0",
        columns=[order_date, Column(name="amount", type=IntegerType(), order=1)],
        created_by_id=user.id,
    )
    session.add_all([date_rev, fact_rev])
    await session.flush()
    date_node.current_version = "v1.0"
    date_node.current = date_rev
    fact_node.current_version = "v1.0"
    fact_node.current = fact_rev

    partition = Partition(
        column_id=order_date.id,
        type_=PartitionType.TEMPORAL,
        granularity=Granularity.DAY,
        format="yyyyMMdd",
    )
    session.add(partition)
    await session.flush()
    order_date.partition = partition

    session.add(
        DimensionLink(
            node_revision=fact_rev,
            dimension=date_node,
            join_sql="freshness.fact.order_date = freshness.dim_date.dateint",
            join_type=JoinType.LEFT,
        ),
    )
    availability = AvailabilityState(
        catalog="default",
        schema_="analytics",
        table="fact_preagg",
        valid_through_ts=20250101,
    )
    session.add(availability)
    await session.flush()

    preagg = PreAggregation(
        node_revision_id=fact_rev.id,
        grain_columns=["freshness.dim_date.dateint"],
        measures=[
            PreAggMeasure(
                name="amount_sum",
                expression="amount",
                aggregation="SUM",
                merge="SUM",
                rule=AggregationRule(type=Aggregability.FULL),
                expr_hash=compute_expression_hash("amount"),
            ),
        ],
        sql="SELECT 1",
        grain_group_hash="freshness_hash",
        preagg_hash="fresh002",
        availability_id=availability.id,
    )
    session.add(preagg)
    await session.flush()

    # Re-query with the eager loads that load_available_preaggs applies, since
    # temporal_grain_refs walks these relationships synchronously.
    result = await session.execute(
        select(PreAggregation)
        .where(PreAggregation.id == preagg.id)
        .options(
            joinedload(PreAggregation.availability),
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
async def test_temporal_axis_resolved_through_a_dimension_link(
    linked_preagg: PreAggregation,
):
    """The grain's dimension attribute is the axis, reached via the FK mapping."""
    assert temporal_grain_refs(linked_preagg) == [
        ("freshness.dim_date.dateint", "yyyyMMdd"),
    ]


@pytest.mark.asyncio
async def test_linked_axis_absent_from_the_grain(linked_preagg: PreAggregation):
    """A linked temporal column that no grain entry reaches yields no axis."""
    linked_preagg.grain_columns = ["freshness.fact.region"]
    assert temporal_grain_refs(linked_preagg) == []


@pytest.mark.asyncio
async def test_linked_axis_gates_on_the_dimension_filter(
    linked_preagg: PreAggregation,
    mocker,
):
    """A filter written against the linked dimension is what the gate compares."""
    configure(mocker, gating=True)
    within = make_context(["freshness.dim_date.dateint <= 20241231"])
    beyond = make_context(["freshness.dim_date.dateint <= 20250102"])
    assert preagg_is_fresh(within, linked_preagg) is True
    assert preagg_is_fresh(beyond, linked_preagg) is False
