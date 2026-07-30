"""
Pre-aggregation matching logic for SQL generation.

This module provides functions to find pre-aggregations that can satisfy
a grain group's requirements, enabling SQL substitution when materialized
tables are available.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from datajunction_server.errors import DJInvalidInputException
from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_expression_hash,
)
from datajunction_server.models.decompose import Aggregability, MetricComponent
from datajunction_server.models.preaggregation import TemporalPartitionColumn
from datajunction_server.naming import SEPARATOR
from datajunction_server.construction.build_v3.dimensions import (
    parse_dimension_ref,
    roles_reaching_dimension,
)

if TYPE_CHECKING:
    from datajunction_server.construction.build_v3.types import BuildContext, GrainGroup
    from datajunction_server.database.node import Node

logger = logging.getLogger(__name__)


def get_required_measure_identities(
    grain_group: "GrainGroup",
) -> set[tuple[str, str]]:
    """
    Identity of each measure a grain group needs: (expression hash, Phase-1
    aggregation).

    Matching is name-independent, so ``expr_hash`` stands in for identity -- but
    it covers the expression alone, making ``SUM(x)`` and ``MAX(x)`` hash alike
    and letting a MAX metric read a SUM column. Pairing it with the aggregation
    restores the distinction: a partial is reusable only by a metric that
    accumulates the same way.

    Phase-1 ``aggregation`` rather than ``merge``, because merge is not unique --
    COUNT accumulates with COUNT but merges with SUM.
    """
    return {
        (
            compute_expression_hash(component.expression),
            component.normalized_aggregation,
        )
        for _, component in grain_group.components
    }


def canonical_dimension_ref(
    ctx: "BuildContext",
    node_rev_id: int,
    ref: str,
) -> str:
    """
    Canonical form of a dimension reference, so that a pre-agg registered with one
    spelling still matches a query written with the other.

    A bare reference is qualified only when exactly one named role reaches the
    dimension and no role-free link does -- mirroring how the reference resolves,
    which prefers the role-free link. Anything else is returned unchanged.
    """
    try:
        dim_ref = parse_dimension_ref(ref)
    except DJInvalidInputException:
        # Not a node.column reference, so there is no dimension node to look up
        # roles for. Compared as-is rather than failing the whole build.
        return ref
    if dim_ref.role:
        return ref
    roles = roles_reaching_dimension(ctx, node_rev_id, dim_ref.node_name)
    if "" in roles or len(roles) != 1:
        return ref
    return f"{ref}[{roles.pop()}]"


def find_matching_preagg(
    ctx: "BuildContext",
    parent_node: "Node",
    requested_grain: list[str],
    grain_group: "GrainGroup",
) -> PreAggregation | None:
    """
    Find a pre-aggregation that can satisfy the grain group requirements.

    Matching rules:
    1. Pre-agg must be for the same parent node (same node_revision_id)
    2. Pre-agg grain must be a SUPERSET of requested grain (can roll up but not drill down)
    3. Pre-agg measures must be a SUPERSET of required measures (by expr_hash)
    4. Pre-agg must have availability (already checked in load_available_preaggs)

    Args:
        ctx: Build context with available_preaggs cache
        parent_node: The parent node for this grain group
        requested_grain: List of dimension references requested
        grain_group: The grain group with required components

    Returns:
        Matching PreAggregation if found, None otherwise
    """
    if not ctx.use_materialized:
        return None

    if not parent_node.current:
        return None

    node_rev_id = parent_node.current.id

    # Get available pre-aggs for this parent node
    available = ctx.available_preaggs.get(node_rev_id, [])
    if not available:
        return None

    required_measures = get_required_measure_identities(grain_group)
    if not required_measures:
        return None

    requested_grain_set = {
        canonical_dimension_ref(ctx, node_rev_id, ref) for ref in requested_grain
    }

    # Non-additive measures (e.g. COUNT(DISTINCT ...), raw AVG components) cannot
    # be rolled up to a coarser grain without producing wrong results. When any
    # required component is not fully additive, a pre-agg can only satisfy the
    # query if its grain EXACTLY matches the requested grain (no roll-up). Fully
    # additive measures may still be rolled up via a superset (subset) match.
    requires_exact_grain = any(
        component.rule.type != Aggregability.FULL
        for _, component in grain_group.components
    )

    # Find a matching pre-agg
    best_match: PreAggregation | None = None
    best_grain_size = float("inf")

    for preagg in available:
        preagg_grain_set = {
            canonical_dimension_ref(ctx, node_rev_id, ref)
            for ref in (preagg.grain_columns or [])
        }

        # Check grain compatibility. For additive measures the pre-agg may be at
        # the same or a finer grain (roll-up allowed → subset match). For
        # non-additive measures the pre-agg grain must exactly match the request.
        if requires_exact_grain:
            if requested_grain_set != preagg_grain_set:
                logger.debug(
                    f"[BuildV3] Pre-agg {preagg.id} grain {preagg_grain_set} "
                    f"must exactly match requested grain {requested_grain_set} "
                    f"because the grain group has non-additive measures",
                )
                continue
        elif not requested_grain_set.issubset(preagg_grain_set):
            logger.debug(
                f"[BuildV3] Pre-agg {preagg.id} grain {preagg_grain_set} "
                f"doesn't cover requested grain {requested_grain_set}",
            )
            continue

        # Coverage check on (expression hash, Phase-1 aggregation) -- see
        # get_required_measure_identities for why the aggregation is part of it.
        preagg_measures = {
            (measure.expr_hash, measure.normalized_aggregation)
            for measure in preagg.measures
            if measure.expr_hash
        }
        if not required_measures.issubset(preagg_measures):
            logger.debug(
                f"[BuildV3] Pre-agg {preagg.id} measures {preagg_measures} "
                f"don't cover required measures {required_measures}",
            )
            continue

        # Found a compatible pre-agg - prefer the one with smallest grain
        # (closest to requested grain = less roll-up work)
        if len(preagg_grain_set) < best_grain_size:  # pragma: no branch
            best_match = preagg
            best_grain_size = len(preagg_grain_set)
            logger.debug(
                f"[BuildV3] Found matching pre-agg {preagg.id} "
                f"(grain={preagg_grain_set}, measures={len(preagg_measures)})",
            )

    if best_match:
        logger.info(
            f"[BuildV3] Using pre-agg {best_match.id} for parent={parent_node.name} "
            f"grain={requested_grain_set}",
        )

    return best_match


def get_preagg_measure_column(
    preagg: PreAggregation,
    component: MetricComponent,
) -> str | None:
    """
    Find the column name in the pre-agg that corresponds to a metric component.

    Matches on the full identity -- expression hash and Phase-1 aggregation -- so
    the right column is found even when names differ. One pre-agg can hold several
    measures over the same expression (SUM(x) and MAX(x)), where matching on the
    hash alone would return whichever came first.

    Args:
        preagg: The pre-aggregation to search
        component: The metric component to find

    Returns:
        Column name in the pre-agg, or None if not found
    """
    target = (
        compute_expression_hash(component.expression),
        component.normalized_aggregation,
    )

    for measure in preagg.measures:
        if (measure.expr_hash, measure.normalized_aggregation) == target:
            # Externally-registered pre-aggs bind the measure to a physical
            # column name that may differ from the DJ component name.
            return measure.source_column or measure.name

    return None


def get_preagg_dimension_column(
    ctx: "BuildContext",
    node_rev_id: int,
    preagg: PreAggregation,
    dimension_ref: str,
    default_column: str,
) -> str:
    """
    Physical column in the pre-agg backing a resolved grain dimension.

    External pre-aggs may store a dimension under a different column name (kept as
    source_column). Matched by canonical dimension reference, so a bare and a
    role-qualified spelling of the same dimension still match, falling back to the
    DJ column name.
    """
    target = canonical_dimension_ref(ctx, node_rev_id, dimension_ref)
    for col in preagg.columns or []:
        if col.semantic_type == "dimension" and target == canonical_dimension_ref(
            ctx,
            node_rev_id,
            col.semantic_name,
        ):
            return col.source_column or col.name
    return default_column


def get_temporal_partitions(preagg: PreAggregation) -> list[TemporalPartitionColumn]:
    """
    Get temporal partition columns for a pre-aggregation.
    """
    col_type_map: dict[str, str] = {}
    if preagg.node_revision and preagg.node_revision.columns:  # pragma: no branch
        for col in preagg.node_revision.columns:
            col_type_map[col.name] = str(col.type)

    temporal_partitions: list[TemporalPartitionColumn] = []
    if preagg.node_revision:  # pragma: no branch
        # Build reverse mapping: source column name -> list of dimension attributes.
        # dimensions_to_columns_map returns {dim_attr (AST Column): source_col (AST Column)}.
        # A single source column may appear in multiple dimension links (e.g., one
        # simple link plus one multi-column link), so this must be multi-valued —
        # otherwise only one mapping survives and we may miss the dim_attr that
        # the user actually selected in the cube grain.
        col_to_dims: dict[str, list[str]] = {}
        dim_to_col = preagg.node_revision.dimensions_to_columns_map()
        for dim_attr, source_col in dim_to_col.items():
            source_col_name = source_col.identifier().split(SEPARATOR)[-1]
            col_to_dims.setdefault(source_col_name, []).append(dim_attr)

        for temporal_col in preagg.node_revision.temporal_partition_columns():
            source_name = temporal_col.name
            source_type = str(temporal_col.type) if temporal_col.type else "int"
            output_name = source_name  # default

            # Strategy 1: Source name directly in grain
            full_source_col = f"{preagg.node_revision.name}{SEPARATOR}{source_name}"
            if full_source_col in preagg.grain_columns:
                output_name = source_name  # pragma: no cover

            # Strategy 2: Check dimension links via dimensions_to_columns_map.
            # Try every dim_attr that maps back to this source column — the first
            # one whose attribute (or parent node) appears in grain_columns wins.
            elif source_name in col_to_dims:
                for dim_attr in col_to_dims[source_name]:
                    dim_node = dim_attr.rsplit(SEPARATOR, 1)[0]
                    matched = False
                    for gc in preagg.grain_columns:
                        if gc == dim_attr or gc.startswith(dim_node + SEPARATOR):
                            # Parse the dimension ref to handle role syntax properly
                            # e.g., "v3.date.week[order]" -> column_name="week", role="order"
                            # -> output_name="week_order"
                            parsed = parse_dimension_ref(gc)
                            output_name = parsed.column_name
                            if parsed.role:  # pragma: no branch
                                output_name = f"{output_name}_{parsed.role}"
                            logger.info(
                                "Temporal column %s links to dimension %s -> output %s",
                                source_name,
                                dim_attr,
                                output_name,
                            )
                            matched = True
                            break
                    if matched:
                        break

            # Strategy 3: Check column.dimension reference link
            elif temporal_col.dimension:  # pragma: no cover
                dim_name = temporal_col.dimension.name
                for gc in preagg.grain_columns:
                    if gc.startswith(dim_name + SEPARATOR):
                        # Parse the dimension ref to handle role syntax properly
                        parsed = parse_dimension_ref(gc)
                        output_name = parsed.column_name
                        if parsed.role:
                            output_name = f"{output_name}_{parsed.role}"
                        break

            # Map output column to source type (for DDL generation)
            if output_name != source_name:
                col_type_map[output_name] = source_type  # pragma: no cover

            logger.info(
                "Temporal partition: source=%s -> output=%s (type=%s)",
                source_name,
                output_name,
                source_type,
            )

            temporal_partitions.append(
                TemporalPartitionColumn(
                    column_name=output_name,
                    column_type=source_type,
                    format=temporal_col.partition.format
                    if temporal_col.partition
                    else None,
                    granularity=(
                        str(temporal_col.partition.granularity.value)
                        if temporal_col.partition and temporal_col.partition.granularity
                        else None
                    ),
                    expression=(
                        str(temporal_col.partition.temporal_expression())
                        if temporal_col.partition
                        else None
                    ),
                ),
            )
    return temporal_partitions
