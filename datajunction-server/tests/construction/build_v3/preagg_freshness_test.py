"""Tests for preagg_freshness.py - freshness gating for pre-aggregations."""

from datetime import datetime, timezone

import pytest

from datajunction_server.config import Settings
from datajunction_server.construction.build_v3.filters import (
    dimension_ref_of_expression,
    lower_bounds_by_ref,
    upper_bounds_by_ref,
)
from datajunction_server.construction.build_v3.preagg_freshness import (
    TemporalAxis,
    preagg_is_fresh,
    temporal_grain_axis,
)
from datajunction_server.models.partition import render_partition_value
from datajunction_server.construction.build_v3.types import BuildContext
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.column import Column
from datajunction_server.database.node import NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_expression_hash,
)
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    PreAggMeasure,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import Granularity, PartitionType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.types import IntegerType

SETTINGS_PATH = (
    "datajunction_server.construction.build_v3.preagg_freshness.get_settings"
)


def configure(mocker, *, gating: bool, max_staleness: int | None = None) -> None:
    """Point the module's settings lookup at an explicit gating configuration."""
    # A real Settings instance, so renaming a setting fails here loudly rather
    # than leaving these tests green while the feature stops working.
    mocker.patch(
        SETTINGS_PATH,
        return_value=Settings(
            preagg_freshness_gating=gating,
            preagg_max_staleness_seconds=max_staleness,
        ),
    )


def make_context(
    filters: list[str],
    build_timestamp: datetime | None = None,
) -> BuildContext:
    """A build context carrying nothing but the query's dimension filters."""
    return BuildContext(
        session=None,
        metrics=[],
        dimensions=[],
        dimension_filters=filters,
        build_timestamp=build_timestamp or datetime.now(timezone.utc),
    )


def upper_bound_for(filters: list[str], ref: str = "v3.date.date_id") -> int | None:
    """Tightest upper bound the filters place on a dimension reference."""
    return upper_bounds_by_ref(make_context(filters), 1).get(ref)


def lower_bound_for(filters: list[str], ref: str = "v3.date.date_id") -> int | None:
    """Tightest lower bound the filters place on a dimension reference."""
    return lower_bounds_by_ref(make_context(filters), 1).get(ref)


def make_preagg(
    grain_columns: list[str],
    valid_through_ts: int,
    node_revision: NodeRevision | None = None,
    *,
    temporal_partitions: list[str] | None = None,
    min_temporal_partition: list[str] | None = None,
    max_temporal_partition: list[str] | None = None,
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
        temporal_partitions=temporal_partitions or [],
        min_temporal_partition=min_temporal_partition or [],
        max_temporal_partition=max_temporal_partition or [],
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


def make_two_axis_revision() -> NodeRevision:
    """A revision partitioned on both a date and an hour column."""
    revision = make_partitioned_revision()
    hour_col = Column(name="order_hour", type=IntegerType(), order=1)
    hour_col.partition = Partition(
        type_=PartitionType.TEMPORAL,
        granularity=Granularity.HOUR,
        format="HH",
    )
    revision.columns = list(revision.columns) + [hour_col]
    return revision


DATE_GRAIN = ["v3.order_details.order_date"]


class TestRenderPartitionValue:
    """Rendering a moment into an integer partition format."""

    @pytest.mark.parametrize(
        "format_, expected",
        [
            ("yyyyMMdd", 20260731),
            ("yyyyMMddHH", 2026073114),
            ("yyyyMM", 202607),
            ("yyyy", 2026),
            # Separators can't produce an integer.
            ("yyyy-MM-dd", None),
            # Out of order, so the integer's ordering isn't chronological.
            ("ddMMyyyy", None),
            ("", None),
            (None, None),
        ],
    )
    def test_render(self, format_, expected):
        moment = datetime(2026, 7, 31, 14, 5, 9, tzinfo=timezone.utc)
        assert render_partition_value(moment, format_) == expected


class TestUpperBounds:
    """Reading the query's ceiling on a dimension out of its filters."""

    @pytest.mark.parametrize(
        "filters, expected",
        [
            ([], None),
            (["v3.date.date_id <= 20250101"], 20250101),
            # A strict < is lowered by one.
            (["v3.date.date_id < 20250102"], 20250101),
            (["v3.date.date_id = 20250101"], 20250101),
            (["v3.date.date_id BETWEEN 20240101 AND 20250101"], 20250101),
            # Either operand order reads the same.
            (["20250101 >= v3.date.date_id"], 20250101),
            (["20250102 > v3.date.date_id"], 20250101),
            # Filters are ANDed, so the tightest ceiling wins.
            (["v3.date.date_id <= 20250601", "v3.date.date_id <= 20250101"], 20250101),
            # No ceiling to read.
            (["v3.date.date_id >= 20250101"], None),
            (["v3.order_details.status = 'shipped'"], None),
            (["v3.date.date_id NOT BETWEEN 20240101 AND 20250101"], None),
            (["v3.date.date_id <= '2025-01-01'"], None),
            (["v3.date.date_id BETWEEN 20240101 AND '2025-01-01'"], None),
            (["DATE_TRUNC('day', v3.date.date_id) <= 20250101"], None),
            # A disjunction can widen the range past either side's bound.
            (["v3.date.date_id <= 20250101 OR v3.date.date_id <= 20990101"], None),
        ],
    )
    def test_upper_bound(self, filters, expected):
        assert upper_bound_for(filters) == expected

    @pytest.mark.parametrize(
        "filters",
        [
            # A negated comparison asserts the opposite of what it reads as.
            ["NOT v3.date.date_id <= 20200101"],
            ["NOT (v3.date.date_id <= 20200101)"],
            # A comparison inside CASE is not asserted by the query at all.
            ["CASE WHEN v3.date.date_id < 20200101 THEN 0 ELSE 1 END = 1"],
            # Either side of a disjunction can widen the range.
            ["v3.date.date_id <= 20200101 OR v3.date.date_id <= 20990101"],
        ],
    )
    def test_bounds_are_only_read_from_asserted_predicates(self, filters):
        """Reading a bound out of a negation would invert its meaning."""
        ctx = make_context(filters)
        assert upper_bounds_by_ref(ctx, 1) == {}
        assert lower_bounds_by_ref(ctx, 1) == {}

    def test_conjuncts_beside_a_disjunction_still_count(self):
        """An AND-ed bound holds regardless of what the other conjunct says."""
        ctx = make_context(
            ["v3.date.date_id <= 20250101 AND (status = 'a' OR status = 'b')"],
        )
        assert upper_bounds_by_ref(ctx, 1) == {"v3.date.date_id": 20250101}

    def test_role_qualified_reference(self):
        assert (
            upper_bound_for(
                ["v3.date.date_id[order] <= 20250101"],
                "v3.date.date_id[order]",
            )
            == 20250101
        )

    def test_bounds_are_computed_once_per_node_revision(self):
        ctx = make_context(["v3.date.date_id <= 20250101"])
        first = upper_bounds_by_ref(ctx, 1)
        assert upper_bounds_by_ref(ctx, 1) is first

    def test_subscript_over_a_non_column_is_not_a_reference(self):
        subscript = ast.Subscript(
            expr=ast.Number(value=1),
            index=ast.Column(name=ast.Name("order")),
        )
        assert dimension_ref_of_expression(subscript) is None

    def test_non_column_expression_is_not_a_reference(self):
        assert dimension_ref_of_expression(ast.Number(value=1)) is None


class TestLowerBounds:
    """The mirror image: reading the query's floor on a dimension."""

    @pytest.mark.parametrize(
        "filters, expected",
        [
            ([], None),
            (["v3.date.date_id >= 20250101"], 20250101),
            # A strict > is raised by one.
            (["v3.date.date_id > 20250100"], 20250101),
            (["v3.date.date_id = 20250101"], 20250101),
            (["v3.date.date_id BETWEEN 20250101 AND 20260101"], 20250101),
            # Either operand order reads the same.
            (["20250101 <= v3.date.date_id"], 20250101),
            (["20250100 < v3.date.date_id"], 20250101),
            # Filters are ANDed, so the tightest floor wins.
            (["v3.date.date_id >= 20240101", "v3.date.date_id >= 20250101"], 20250101),
            # No floor to read.
            (["v3.date.date_id <= 20250101"], None),
            (["v3.order_details.status = 'shipped'"], None),
            (["v3.date.date_id NOT BETWEEN 20240101 AND 20250101"], None),
            (["v3.date.date_id >= '2025-01-01'"], None),
            (["v3.date.date_id BETWEEN '2025-01-01' AND 20260101"], None),
            (["DATE_TRUNC('day', v3.date.date_id) >= 20250101"], None),
            (["v3.date.date_id >= 20250101 OR v3.date.date_id >= 20200101"], None),
        ],
    )
    def test_lower_bound(self, filters, expected):
        assert lower_bound_for(filters) == expected

    def test_bounds_are_computed_once_per_node_revision(self):
        ctx = make_context(["v3.date.date_id >= 20250101"])
        first = lower_bounds_by_ref(ctx, 1)
        assert lower_bounds_by_ref(ctx, 1) is first

    def test_both_sides_are_cached_separately(self):
        ctx = make_context(["v3.date.date_id BETWEEN 20240101 AND 20250101"])
        assert lower_bounds_by_ref(ctx, 1) == {"v3.date.date_id": 20240101}
        assert upper_bounds_by_ref(ctx, 1) == {"v3.date.date_id": 20250101}


class TestTemporalGrainAxis:
    """Finding the single temporal axis the availability range describes."""

    def test_no_node_revision(self):
        assert temporal_grain_axis(make_preagg(["v3.order_details.status"], 1)) is None

    def test_source_column_directly_in_grain(self):
        preagg = make_preagg(
            DATE_GRAIN,
            20250101,
            node_revision=make_partitioned_revision(),
        )
        assert temporal_grain_axis(preagg) == TemporalAxis(
            "v3.order_details.order_date",
            "yyyyMMdd",
            "order_date",
        )

    def test_temporal_column_absent_from_grain(self):
        preagg = make_preagg(
            ["v3.order_details.status"],
            20250101,
            node_revision=make_partitioned_revision(),
        )
        assert temporal_grain_axis(preagg) is None

    def test_no_temporal_partition_at_all(self):
        revision = NodeRevision(
            name="v3.order_details",
            type=NodeType.TRANSFORM,
            version="v1.0",
            columns=[Column(name="order_date", type=IntegerType(), order=0)],
        )
        preagg = make_preagg(DATE_GRAIN, 20250101, node_revision=revision)
        assert temporal_grain_axis(preagg) is None

    def test_two_temporal_axes_are_not_judged(self):
        """A single covered range can't say which of two axes it belongs to."""
        preagg = make_preagg(
            ["v3.order_details.order_date", "v3.order_details.order_hour"],
            20250101,
            node_revision=make_two_axis_revision(),
        )
        assert temporal_grain_axis(preagg) is None


class TestPreaggIsFresh:
    """The gate itself."""

    def test_gating_off_allows_anything(self, mocker):
        configure(mocker, gating=False)
        preagg = make_preagg(
            DATE_GRAIN,
            20200101,
            node_revision=make_partitioned_revision(),
            max_temporal_partition=["20200101"],
        )
        ctx = make_context(["v3.order_details.order_date <= 20250101"])
        assert preagg_is_fresh(ctx, preagg) is True

    def test_no_temporal_grain_is_not_judged(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(["v3.order_details.status"], 20200101)
        assert preagg_is_fresh(make_context([]), preagg) is True

    def test_query_inside_the_covered_range(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(
            DATE_GRAIN,
            20250601,
            node_revision=make_partitioned_revision(),
            min_temporal_partition=["20240101"],
            max_temporal_partition=["20250601"],
        )
        ctx = make_context(
            ["v3.order_details.order_date BETWEEN 20250101 AND 20250201"],
        )
        assert preagg_is_fresh(ctx, preagg) is True

    def test_query_reaching_above_the_covered_range(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(
            DATE_GRAIN,
            20250601,
            node_revision=make_partitioned_revision(),
            min_temporal_partition=["20240101"],
            max_temporal_partition=["20250601"],
        )
        ctx = make_context(["v3.order_details.order_date <= 20251231"])
        assert preagg_is_fresh(ctx, preagg) is False

    def test_query_reaching_below_the_covered_range(self, mocker):
        """A table backfilled only to 2024 can't answer a query about 2020."""
        configure(mocker, gating=True)
        preagg = make_preagg(
            DATE_GRAIN,
            20250601,
            node_revision=make_partitioned_revision(),
            min_temporal_partition=["20240101"],
            max_temporal_partition=["20250601"],
        )
        ctx = make_context(["v3.order_details.order_date >= 20200101"])
        assert preagg_is_fresh(ctx, preagg) is False

    def test_lower_bound_in_another_encoding_is_not_judged(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(
            DATE_GRAIN,
            20250601,
            node_revision=make_partitioned_revision(),
            min_temporal_partition=["1704067200"],
            max_temporal_partition=["20250601"],
        )
        ctx = make_context(["v3.order_details.order_date >= 20240101"])
        assert preagg_is_fresh(ctx, preagg) is True

    def test_no_recorded_lower_bound_is_not_judged(self, mocker):
        """Without a min there is nothing to reject a historical query against."""
        configure(mocker, gating=True)
        preagg = make_preagg(
            DATE_GRAIN,
            20250601,
            node_revision=make_partitioned_revision(),
            max_temporal_partition=["20250601"],
        )
        ctx = make_context(["v3.order_details.order_date >= 20200101"])
        assert preagg_is_fresh(ctx, preagg) is True

    def test_unparseable_partition_value_falls_back_to_the_watermark(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(
            DATE_GRAIN,
            20250601,
            node_revision=make_partitioned_revision(),
            max_temporal_partition=["2025-06-01"],
        )
        ctx = make_context(["v3.order_details.order_date <= 20251231"])
        assert preagg_is_fresh(ctx, preagg) is False

    def test_watermark_stands_in_for_a_missing_max(self, mocker):
        """Externally registered pre-aggs report only the scalar."""
        configure(mocker, gating=True)
        preagg = make_preagg(
            DATE_GRAIN,
            20250101,
            node_revision=make_partitioned_revision(),
        )
        ctx = make_context(["v3.order_details.order_date <= 20250601"])
        assert preagg_is_fresh(ctx, preagg) is False

    def test_watermark_in_another_encoding_is_not_judged(self, mocker):
        """An epoch watermark can't be compared to a partition-format bound."""
        configure(mocker, gating=True)
        preagg = make_preagg(
            DATE_GRAIN,
            1767225600000,
            node_revision=make_partitioned_revision(),
        )
        ctx = make_context(["v3.order_details.order_date <= 20250601"])
        assert preagg_is_fresh(ctx, preagg) is True

    def test_open_ended_query_without_a_budget(self, mocker):
        configure(mocker, gating=True)
        preagg = make_preagg(
            DATE_GRAIN,
            20200101,
            node_revision=make_partitioned_revision(),
            max_temporal_partition=["20200101"],
        )
        assert preagg_is_fresh(make_context([]), preagg) is True

    def test_open_ended_query_within_the_budget(self, mocker):
        configure(mocker, gating=True, max_staleness=86400 * 7)
        now = datetime(2025, 1, 10, tzinfo=timezone.utc)
        preagg = make_preagg(
            DATE_GRAIN,
            20250108,
            node_revision=make_partitioned_revision(),
            max_temporal_partition=["20250108"],
        )
        assert preagg_is_fresh(make_context([], build_timestamp=now), preagg) is True

    def test_open_ended_query_past_the_budget(self, mocker):
        configure(mocker, gating=True, max_staleness=86400)
        now = datetime(2025, 1, 10, tzinfo=timezone.utc)
        preagg = make_preagg(
            DATE_GRAIN,
            20240101,
            node_revision=make_partitioned_revision(),
            max_temporal_partition=["20240101"],
        )
        assert preagg_is_fresh(make_context([], build_timestamp=now), preagg) is False

    def test_open_ended_query_with_an_unrenderable_format(self, mocker):
        configure(mocker, gating=True, max_staleness=86400)
        preagg = make_preagg(
            DATE_GRAIN,
            20200101,
            node_revision=make_partitioned_revision(format_="yyyy-MM-dd"),
            max_temporal_partition=["20200101"],
        )
        assert preagg_is_fresh(make_context([]), preagg) is True

    def test_open_ended_watermark_in_another_encoding(self, mocker):
        configure(mocker, gating=True, max_staleness=86400)
        now = datetime(2025, 1, 10, tzinfo=timezone.utc)
        preagg = make_preagg(
            DATE_GRAIN,
            1767225600000,
            node_revision=make_partitioned_revision(),
        )
        assert preagg_is_fresh(make_context([], build_timestamp=now), preagg) is True

    def test_the_whole_build_is_judged_against_one_instant(self, mocker):
        """build_timestamp is fixed per request, not read per pre-agg."""
        configure(mocker, gating=True, max_staleness=86400)
        ctx = make_context([])
        assert ctx.build_timestamp is ctx.build_timestamp


class TestMultiColumnTemporalPartitions:
    """
    Placing the axis's entry in a multi-column min/max list.

    ``min_temporal_partition`` runs parallel to ``temporal_partitions``, so a
    table partitioned by date and hour reports ``["20250101", "00"]``.
    """

    def _preagg(self, mocker, **availability):
        configure(mocker, gating=True)
        # Only order_date is in the grain, so it is the single axis; order_hour
        # is a partition column of the parent that the grain doesn't carry.
        return make_preagg(
            DATE_GRAIN,
            20250601,
            node_revision=make_two_axis_revision(),
            **availability,
        )

    def test_aligned_by_column_name(self, mocker):
        preagg = self._preagg(
            mocker,
            temporal_partitions=["order_date", "order_hour"],
            max_temporal_partition=["20250601", "23"],
        )
        ctx = make_context(["v3.order_details.order_date <= 20251231"])
        assert preagg_is_fresh(ctx, preagg) is False

    def test_axis_absent_from_the_declared_columns(self, mocker):
        """Nothing places the values, so the max is declined and the scalar used."""
        preagg = self._preagg(
            mocker,
            temporal_partitions=["dt", "hr"],
            max_temporal_partition=["20991231", "23"],
        )
        ctx = make_context(["v3.order_details.order_date <= 20251231"])
        assert preagg_is_fresh(ctx, preagg) is False

    def test_list_length_contradicts_the_declared_columns(self, mocker):
        preagg = self._preagg(
            mocker,
            temporal_partitions=["order_date", "order_hour"],
            max_temporal_partition=["20991231"],
        )
        ctx = make_context(["v3.order_details.order_date <= 20251231"])
        assert preagg_is_fresh(ctx, preagg) is False

    def test_a_lone_value_needs_no_placing(self, mocker):
        preagg = self._preagg(
            mocker,
            temporal_partitions=["order_date"],
            max_temporal_partition=["20991231"],
        )
        ctx = make_context(["v3.order_details.order_date <= 20251231"])
        assert preagg_is_fresh(ctx, preagg) is True
