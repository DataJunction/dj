"""
Freshness gating for pre-aggregations.

A pre-aggregation's availability records the range its table actually covers:
``min_temporal_partition`` and ``max_temporal_partition``, the lowest and highest
partition values written, alongside the self-reported ``valid_through_ts``
scalar. Nothing consulted any of it when routing queries, so a pre-agg that had
never been backfilled past 2026, or whose pipeline had stopped running, kept
serving silently truncated results.

The gate is a two-sided range check: a pre-agg is rejected when the query asks
for data outside what the table holds, below the covered range as readily as
above it. Both ends matter -- a backfill gap at the low end is at least as common
as a dead pipeline at the high end -- and both are compared against the range the
query's own filters describe, not against wall clock. A query bounded at last
March is served correctly by a table that stopped updating yesterday, and
rejecting it would be wrong.

Every comparison stays inside the partition format, which is what makes it safe:
a partition value is never interpreted as a point in time, only compared against
a filter literal written in the same encoding. ``valid_through_ts`` stands in for
a missing ``max_temporal_partition`` -- externally registered pre-aggs report
nothing else -- but its encoding is contradicted across the repo, so it is used
only where its magnitude is consistent with the value it's compared against.

Pre-aggregations only. Cubes and node-level materializations have their own
availability checks and are not gated here. Off unless
``preagg_freshness_gating`` is set.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, NamedTuple

from datajunction_server.construction.build_v3.filters import (
    lower_bounds_by_ref,
    upper_bounds_by_ref,
)
from datajunction_server.construction.build_v3.preagg_matcher import (
    canonical_dimension_ref,
    match_temporal_columns_to_grain,
    temporal_output_name,
)
from datajunction_server.models.partition import render_partition_value
from datajunction_server.utils import get_settings

if TYPE_CHECKING:
    from datajunction_server.config import Settings
    from datajunction_server.construction.build_v3.types import BuildContext
    from datajunction_server.database.availabilitystate import AvailabilityState
    from datajunction_server.database.preaggregation import PreAggregation

logger = logging.getLogger(__name__)


class TemporalAxis(NamedTuple):
    """The single temporal axis a pre-aggregation can be judged on."""

    # Canonical grain entry, e.g. ``v3.date.date_id[order]``.
    grain_ref: str
    # Partition format of the underlying column, e.g. ``yyyyMMdd``.
    partition_format: str | None
    # Name the axis carries in the pre-agg's output table.
    output_name: str


def temporal_grain_axis(preagg: "PreAggregation") -> TemporalAxis | None:
    """
    The single temporal axis a pre-agg's availability range describes.

    A pre-agg can only be judged when exactly one of its temporal partition
    columns is part of the grain. Zero means there is no temporal axis to judge;
    more than one means bounds read off different axes may not even share an
    encoding. Both decline rather than guess -- the same collapse to a single
    axis that combiner construction already makes.
    """
    axes = [
        TemporalAxis(
            grain_ref,
            temporal_col.partition.format,
            temporal_output_name(temporal_col, grain_ref),
        )
        for temporal_col, grain_ref in match_temporal_columns_to_grain(preagg)
        if grain_ref
    ]
    return axes[0] if len(axes) == 1 else None


def _is_comparable(covered: int, requested: int) -> bool:
    """
    Whether two values plausibly share an encoding.

    Partition formats, epoch seconds and epoch milliseconds all appear in the
    wild as ``valid_through_ts``, while a filter literal is only ever in the
    partition format. Comparing across encodings is meaningless in a way that
    silently passes everything, so an order-of-magnitude mismatch is treated as
    "cannot judge" and reported.
    """
    return len(str(abs(covered))) == len(str(abs(requested)))


def _as_int(value: str) -> int | None:
    """A partition value as the integer its format renders to, if it is one."""
    try:
        return int(value)
    except ValueError:
        return None


def _axis_value(
    availability: "AvailabilityState",
    values: list[str] | None,
    axis: TemporalAxis,
) -> int | None:
    """
    The bound on ``axis`` read out of a min/max temporal partition list.

    The list runs parallel to ``temporal_partitions`` -- one value per temporal
    partition column, ``["20260721", "13"]`` against ``["date", "hour"]`` -- so
    the axis's entry is the one at its output column's position. A lone value
    needs no placing. Anything else, including a list whose length contradicts
    the declared columns, is declined rather than guessed at.

    Values are read as integers rather than compared as strings: the filter side
    is already an integer literal, so string comparison would mean rendering it
    back out at a width nothing declares, whereas a partition value in an integer
    format parses exactly.
    """
    values = values or []
    names = availability.temporal_partitions or []
    if names and len(names) != len(values):
        return None
    if len(values) == 1:
        return _as_int(values[0])
    if axis.output_name in names:
        return _as_int(values[names.index(axis.output_name)])
    return None


def _report_incomparable(
    preagg: "PreAggregation",
    covered: int,
    requested: int,
) -> None:
    """Note that a bound can't be judged, and why."""
    logger.warning(
        "[BuildV3] Pre-agg %s reports coverage %s, which is not in the same "
        "encoding as the requested bound %s -- not gating on freshness. Report "
        "temporal coverage in the table's partition format.",
        preagg.id,
        covered,
        requested,
    )


def _within_staleness_budget(
    ctx: "BuildContext",
    preagg: "PreAggregation",
    axis: TemporalAxis,
    covered_through: int,
    settings: "Settings",
) -> bool:
    """
    Whether an open-ended query's implicit "through the present" is satisfied.

    A query with no readable ceiling asks for data up to now, and no partition
    comparison settles that on its own, so this is the one place a wall-clock
    budget applies. It renders "now minus the budget" into the partition format
    and requires the covered range to reach it, keeping the comparison inside the
    one encoding even though the question is about wall clock.
    """
    if settings.preagg_max_staleness_seconds is None:
        return True

    cutoff = render_partition_value(
        ctx.build_timestamp - timedelta(seconds=settings.preagg_max_staleness_seconds),
        axis.partition_format,
    )
    if cutoff is None or not _is_comparable(covered_through, cutoff):
        return True
    if covered_through < cutoff:
        logger.info(
            "[BuildV3] Pre-agg %s covers through %s, past the staleness budget "
            "cutoff of %s for an open-ended query",
            preagg.id,
            covered_through,
            cutoff,
        )
        return False
    return True


def preagg_is_fresh(ctx: "BuildContext", preagg: "PreAggregation") -> bool:
    """
    Whether the range a pre-agg covers contains the range the query asks for.

    Always True unless ``preagg_freshness_gating`` is enabled, and True whenever
    the pre-agg offers no single temporal axis to judge it on.
    """
    settings = get_settings()
    if not settings.preagg_freshness_gating:
        return True

    axis = temporal_grain_axis(preagg)
    if axis is None:
        return True

    availability = preagg.availability
    covered_from = _axis_value(availability, availability.min_temporal_partition, axis)
    covered_through = _axis_value(
        availability,
        availability.max_temporal_partition,
        axis,
    )
    if covered_through is None:
        # Externally registered pre-aggs report only the scalar watermark.
        covered_through = availability.valid_through_ts

    # Filter bounds are keyed canonically, so the grain entry must be too.
    ref = canonical_dimension_ref(ctx, preagg.node_revision_id, axis.grain_ref)
    requested_from = lower_bounds_by_ref(ctx, preagg.node_revision_id).get(ref)
    requested_through = upper_bounds_by_ref(ctx, preagg.node_revision_id).get(ref)

    if requested_from is not None and covered_from is not None:
        if not _is_comparable(covered_from, requested_from):
            _report_incomparable(preagg, covered_from, requested_from)
        elif requested_from < covered_from:
            logger.info(
                "[BuildV3] Pre-agg %s covers from %s but the query asks for "
                "data from %s",
                preagg.id,
                covered_from,
                requested_from,
            )
            return False

    if requested_through is None:
        return _within_staleness_budget(ctx, preagg, axis, covered_through, settings)

    if not _is_comparable(covered_through, requested_through):
        _report_incomparable(preagg, covered_through, requested_through)
        return True
    if requested_through > covered_through:
        logger.info(
            "[BuildV3] Pre-agg %s covers through %s but the query asks for "
            "data through %s",
            preagg.id,
            covered_through,
            requested_through,
        )
        return False
    return True
