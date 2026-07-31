"""
Freshness gating for pre-aggregations.

A pre-aggregation reports how far its data goes with ``valid_through_ts``, an
integer in the backing table's partition format (``20260721`` for ``yyyyMMdd``).
Nothing consulted it when routing queries, so a pre-agg whose pipeline stopped
running kept serving silently truncated results.

The gate here compares that watermark against the time range the query actually
asks for, rather than against wall clock: a query bounded at last March is served
correctly by a table that stopped updating yesterday, and rejecting it would be
wrong. Only when the query has no discoverable upper bound -- so it implicitly
asks for data through the present -- does a wall-clock staleness budget apply.

Both comparisons stay in the partition format, which is what makes them safe:
``valid_through_ts`` is never interpreted as a point in time, it is only compared
against a filter literal written in the same encoding, or against "now" rendered
into that encoding.

Everything here is off unless ``preagg_freshness_gating`` is set.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable

from datajunction_server.construction.build_v3.filters import (
    _extract_full_column_ref,
    extract_subscript_role,
    parse_filter,
)
from datajunction_server.construction.build_v3.preagg_matcher import (
    canonical_dimension_ref,
)
from datajunction_server.naming import SEPARATOR
from datajunction_server.sql.parsing import ast
from datajunction_server.utils import get_settings

if TYPE_CHECKING:
    from datajunction_server.construction.build_v3.types import BuildContext
    from datajunction_server.database.preaggregation import PreAggregation

logger = logging.getLogger(__name__)

# Comparisons that put a ceiling on a temporal column.
UPPER_BOUND_OPS = {
    ast.BinaryOpKind.Lt,
    ast.BinaryOpKind.LtEq,
    ast.BinaryOpKind.Eq,
}

# A disjunction can widen the requested range beyond any bound we read off one
# of its sides, so filters containing one are skipped rather than trusted.
DISJUNCTION_OPS = {ast.BinaryOpKind.Or, ast.BinaryOpKind.LogicalOr}

# Java-style temporal format tokens, rendered as the zero-padded value.
FORMAT_TOKENS: dict[str, Callable[[datetime], str]] = {
    "yyyy": lambda dt: f"{dt.year:04d}",
    "MM": lambda dt: f"{dt.month:02d}",
    "dd": lambda dt: f"{dt.day:02d}",
    "HH": lambda dt: f"{dt.hour:02d}",
    "mm": lambda dt: f"{dt.minute:02d}",
    "ss": lambda dt: f"{dt.second:02d}",
}
TOKEN_PATTERN = re.compile("|".join(FORMAT_TOKENS))


def freshness_gating_enabled() -> bool:
    """Whether freshness gating is turned on for this deployment."""
    return get_settings().preagg_freshness_gating


def render_partition_value(moment: datetime, format_: str | None) -> int | None:
    """
    Render a point in time as an integer in a partition format.

    Only all-token formats render: anything with separators or unknown text
    (``yyyy-MM-dd``) can't produce an integer comparable to ``valid_through_ts``,
    and yields None so the caller declines to judge.
    """
    if not format_ or TOKEN_PATTERN.sub("", format_) != "":
        return None
    return int(
        TOKEN_PATTERN.sub(lambda match: FORMAT_TOKENS[match.group()](moment), format_),
    )


def temporal_grain_refs(preagg: "PreAggregation") -> list[tuple[str, str | None]]:
    """
    The pre-agg's temporal partition columns as (grain dimension ref, format).

    A pre-agg's freshness is only meaningful along a temporal column that is part
    of its grain, since that is the axis ``valid_through_ts`` describes. The
    parent's temporal partition column is matched to a grain entry either
    directly, or through the dimension link that column feeds -- the same two
    strategies ``get_temporal_partitions`` uses to name the output column.
    """
    node_revision = preagg.node_revision
    if not node_revision:
        return []

    grain_columns = preagg.grain_columns or []

    # A source column can feed several dimension links, so this is multi-valued.
    col_to_dims: dict[str, list[str]] = {}
    for dim_attr, source_col in node_revision.dimensions_to_columns_map().items():
        source_name = source_col.identifier().split(SEPARATOR)[-1]
        col_to_dims.setdefault(source_name, []).append(dim_attr)

    refs: list[tuple[str, str | None]] = []
    for temporal_col in node_revision.temporal_partition_columns():
        format_ = temporal_col.partition.format
        direct_ref = f"{node_revision.name}{SEPARATOR}{temporal_col.name}"
        if direct_ref in grain_columns:
            refs.append((direct_ref, format_))
            continue
        for dim_attr in col_to_dims.get(temporal_col.name, []):
            dim_node = dim_attr.rsplit(SEPARATOR, 1)[0]
            matched = next(
                (
                    grain_col
                    for grain_col in grain_columns
                    if grain_col == dim_attr
                    or grain_col.startswith(dim_node + SEPARATOR)
                ),
                None,
            )
            if matched:
                refs.append((matched, format_))
                break
    return refs


def _dimension_ref_of(
    ctx: "BuildContext",
    node_rev_id: int,
    expression: ast.Expression,
) -> str | None:
    """
    Canonical dimension reference an expression names, if it names one directly.
    """
    if isinstance(expression, ast.Subscript):
        role = extract_subscript_role(expression)
        if not isinstance(expression.expr, ast.Column) or not role:
            return None
        ref = f"{_extract_full_column_ref(expression.expr)}[{role}]"
        return canonical_dimension_ref(ctx, node_rev_id, ref)
    if isinstance(expression, ast.Column):
        return canonical_dimension_ref(
            ctx,
            node_rev_id,
            _extract_full_column_ref(expression),
        )
    return None


def _int_literal(expression: ast.Expression) -> int | None:
    """The integer a literal expression holds, or None if it isn't one."""
    if isinstance(expression, ast.Number) and isinstance(expression.value, int):
        return expression.value
    return None


def query_upper_bound(
    ctx: "BuildContext",
    node_rev_id: int,
    ref: str,
) -> int | None:
    """
    Tightest upper bound the query's filters place on a dimension reference.

    Reads ``<``, ``<=``, ``=`` and ``BETWEEN`` against an integer literal, which
    is the shape a partition-format temporal filter takes. A strict ``<`` is
    lowered by one, which is exact for a contiguous partition ordinal and merely
    conservative otherwise. Returns None when no bound can be read, which the
    caller treats as an open-ended request rather than as permission.
    """
    bounds: list[int] = []
    for filter_str in ctx.dimension_filters:
        filter_ast = parse_filter(filter_str)
        if any(op.op in DISJUNCTION_OPS for op in filter_ast.find_all(ast.BinaryOp)):
            continue
        for comparison in filter_ast.find_all(ast.BinaryOp):
            if comparison.op not in UPPER_BOUND_OPS:
                continue
            if _dimension_ref_of(ctx, node_rev_id, comparison.left) != ref:
                continue
            value = _int_literal(comparison.right)
            if value is None:
                continue
            bounds.append(
                value - 1 if comparison.op == ast.BinaryOpKind.Lt else value,
            )
        for between in filter_ast.find_all(ast.Between):
            if between.negated:
                continue
            if _dimension_ref_of(ctx, node_rev_id, between.expr) != ref:
                continue
            value = _int_literal(between.high)
            if value is not None:
                bounds.append(value)
    return min(bounds) if bounds else None


def preagg_is_fresh(ctx: "BuildContext", preagg: "PreAggregation") -> bool:
    """
    Whether a pre-agg's reported freshness covers what the query asks for.

    Always True unless ``preagg_freshness_gating`` is enabled, and True whenever
    the pre-agg carries no temporal grain to judge it on.
    """
    if not freshness_gating_enabled():
        return True

    refs = temporal_grain_refs(preagg)
    if not refs:
        return True

    valid_through_ts = preagg.availability.valid_through_ts
    node_rev_id = preagg.node_revision_id

    bounds: list[int] = []
    for ref, _ in refs:
        bound = query_upper_bound(
            ctx,
            node_rev_id,
            canonical_dimension_ref(ctx, node_rev_id, ref),
        )
        if bound is None:
            bounds = []
            break
        bounds.append(bound)

    if bounds:
        requested_through = max(bounds)
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
    max_staleness = get_settings().preagg_max_staleness_seconds
    if max_staleness is None:
        return True

    cutoff = render_partition_value(
        datetime.now(timezone.utc) - timedelta(seconds=max_staleness),
        refs[0][1],
    )
    if cutoff is None:
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
