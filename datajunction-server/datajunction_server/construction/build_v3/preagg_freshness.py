"""
Freshness gating for pre-aggregations.

A pre-aggregation reports how far its data goes with ``valid_through_ts``, an
integer in the backing table's partition format (``20260721`` for ``yyyyMMdd``).
Nothing consulted it when routing queries, so a pre-agg whose pipeline stopped
running kept serving silently truncated results.

The gate compares that watermark against the time range the query actually asks
for, rather than against wall clock: a query bounded at last March is served
correctly by a table that stopped updating yesterday, and rejecting it would be
wrong. Only when the query has no discoverable upper bound -- so it implicitly
asks for data through the present -- does a wall-clock staleness budget apply.

Both comparisons stay in the partition format, which is what makes them safe:
``valid_through_ts`` is never interpreted as a point in time, only compared
against a filter literal written in the same encoding, or against "now" rendered
into that encoding. Where the watermark plainly isn't in that encoding the gate
says so and declines, rather than comparing incommensurable integers.

Pre-aggregations only. Cubes and node-level materializations have their own
availability checks and are not gated here. Off unless
``preagg_freshness_gating`` is set.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from datajunction_server.construction.build_v3.filters import upper_bounds_by_ref
from datajunction_server.construction.build_v3.preagg_matcher import (
    match_temporal_columns_to_grain,
)
from datajunction_server.models.partition import render_partition_value
from datajunction_server.utils import get_settings

if TYPE_CHECKING:
    from datajunction_server.construction.build_v3.types import BuildContext
    from datajunction_server.database.preaggregation import PreAggregation

logger = logging.getLogger(__name__)


def freshness_gating_enabled() -> bool:
    """Whether freshness gating is turned on for this deployment."""
    return get_settings().preagg_freshness_gating


def temporal_grain_axis(preagg: "PreAggregation") -> tuple[str, str | None] | None:
    """
    The single temporal axis ``valid_through_ts`` describes, as (grain ref, format).

    A pre-agg reports one watermark, so it can only be judged when exactly one of
    its temporal partition columns is part of the grain. Zero means there is no
    temporal axis to judge; more than one means the watermark doesn't say which it
    belongs to, and bounds read off different axes may not even share an encoding.
    Both decline rather than guess -- the same collapse to a single axis that
    combiner construction already makes.
    """
    axes = [
        (grain_ref, temporal_col.partition.format)
        for temporal_col, grain_ref in match_temporal_columns_to_grain(preagg)
        if grain_ref
    ]
    return axes[0] if len(axes) == 1 else None


def _watermark_is_comparable(valid_through_ts: int, reference: int) -> bool:
    """
    Whether a watermark plausibly shares an encoding with a partition-format value.

    ``valid_through_ts`` has no agreed encoding across DJ -- partition format,
    epoch seconds and epoch milliseconds all appear. Comparing across encodings is
    meaningless in a way that silently passes everything, so an order-of-magnitude
    mismatch is treated as "cannot judge" and reported.
    """
    return len(str(abs(valid_through_ts))) == len(str(abs(reference)))


def preagg_is_fresh(ctx: "BuildContext", preagg: "PreAggregation") -> bool:
    """
    Whether a pre-agg's reported freshness covers what the query asks for.

    Always True unless ``preagg_freshness_gating`` is enabled, and True whenever
    the pre-agg offers no single temporal axis to judge it on.
    """
    settings = get_settings()
    if not settings.preagg_freshness_gating:
        return True

    axis = temporal_grain_axis(preagg)
    if axis is None:
        return True
    ref, partition_format = axis

    valid_through_ts = preagg.availability.valid_through_ts
    requested_through = upper_bounds_by_ref(ctx, preagg.node_revision_id).get(ref)

    if requested_through is not None:
        if not _watermark_is_comparable(valid_through_ts, requested_through):
            logger.warning(
                "[BuildV3] Pre-agg %s reports valid_through_ts %s, which is not in "
                "the same encoding as the requested bound %s -- not gating on "
                "freshness. Report valid_through_ts in the table's partition format.",
                preagg.id,
                valid_through_ts,
                requested_through,
            )
            return True
        if requested_through > valid_through_ts:
            logger.info(
                "[BuildV3] Pre-agg %s is valid through %s but the query asks "
                "for data through %s",
                preagg.id,
                valid_through_ts,
                requested_through,
            )
            return False
        return True

    # Open-ended request: the query implicitly asks for data through the present,
    # so fall back to the wall-clock staleness budget if one is configured.
    if settings.preagg_max_staleness_seconds is None:
        return True

    cutoff = render_partition_value(
        ctx.build_timestamp - timedelta(seconds=settings.preagg_max_staleness_seconds),
        partition_format,
    )
    if cutoff is None or not _watermark_is_comparable(valid_through_ts, cutoff):
        return True
    if valid_through_ts < cutoff:
        logger.info(
            "[BuildV3] Pre-agg %s is valid through %s, past the staleness "
            "budget cutoff of %s for an open-ended query",
            preagg.id,
            valid_through_ts,
            cutoff,
        )
        return False
    return True
