"""
Pre-aggregation matching logic for SQL generation.

This module provides functions to find pre-aggregations that can satisfy
a grain group's requirements, enabling SQL substitution when materialized
tables are available.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from datajunction_server.database.preaggregation import (
    PreAggregation,
    get_measure_expr_hashes,
    compute_expression_hash,
)
from datajunction_server.models.decompose import MetricComponent
from datajunction_server.models.preaggregation import TemporalPartitionColumn
from datajunction_server.naming import SEPARATOR
from datajunction_server.construction.build_v3.dimensions import parse_dimension_ref

if TYPE_CHECKING:
    from datajunction_server.construction.build_v3.types import BuildContext, GrainGroup
    from datajunction_server.database.node import Node

logger = logging.getLogger(__name__)


def get_required_measure_hashes(grain_group: "GrainGroup") -> set[str]:
    """
    Get the set of expression hashes for all measures required by a grain group.

    Args:
        grain_group: The grain group to analyze

    Returns:
        Set of expression hashes (MD5 hashes of normalized expressions)
    """
    hashes = set()
    for _, component in grain_group.components:
        expr_hash = compute_expression_hash(component.expression)
        hashes.add(expr_hash)
    return hashes


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

    # Get required measure hashes
    required_measures = get_required_measure_hashes(grain_group)
    if not required_measures:
        return None

    # Normalize requested grain to a set for comparison
    requested_grain_set = set(requested_grain)

    # Find a matching pre-agg
    best_match: PreAggregation | None = None
    best_grain_size = float("inf")

    for preagg in available:
        preagg_grain_set = set(preagg.grain_columns or [])

        # Check grain compatibility: requested grain must be a subset of pre-agg grain
        # This means pre-agg is at same or finer grain (can roll up)
        if not requested_grain_set.issubset(preagg_grain_set):
            logger.debug(
                f"[BuildV3] Pre-agg {preagg.id} grain {preagg_grain_set} "
                f"doesn't cover requested grain {requested_grain_set}",
            )
            continue

        # Check measure coverage: required measures must be subset of pre-agg measures
        preagg_measure_hashes = get_measure_expr_hashes(preagg.measures)
        if not required_measures.issubset(preagg_measure_hashes):
            logger.debug(
                f"[BuildV3] Pre-agg {preagg.id} measures {preagg_measure_hashes} "
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
                f"(grain={preagg_grain_set}, measures={len(preagg_measure_hashes)})",
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

    Matches by expression hash to ensure we're getting the right column
    even if names differ.

    Args:
        preagg: The pre-aggregation to search
        component: The metric component to find

    Returns:
        Column name in the pre-agg, or None if not found
    """
    target_hash = compute_expression_hash(component.expression)

    for measure in preagg.measures:
        if measure.expr_hash == target_hash:
            return measure.name

    return None


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
        # Build reverse mapping: source column name -> dimension attribute
        # dimensions_to_columns_map returns {dim_attr (AST Column): source_col (AST Column)}
        col_to_dim: dict[str, str] = {}
        dim_to_col = preagg.node_revision.dimensions_to_columns_map()
        for dim_attr, source_col in dim_to_col.items():
            # dim_attr is like "dimensions.date.dateint"
            # source_col is an AST Column, get its name (e.g., "utc_date")
            source_col_name = source_col.identifier().split(SEPARATOR)[-1]
            col_to_dim[source_col_name] = dim_attr

        for temporal_col in preagg.node_revision.temporal_partition_columns():
            source_name = temporal_col.name
            source_type = str(temporal_col.type) if temporal_col.type else "int"
            output_name = source_name  # default

            # Strategy 1: Source name directly in grain
            full_source_col = f"{preagg.node_revision.name}{SEPARATOR}{source_name}"
            if full_source_col in preagg.grain_columns:
                output_name = source_name  # pragma: no cover

            # Strategy 2: Check dimension links via dimensions_to_columns_map
            # If temporal column maps to a dimension attribute, find that in grain
            elif source_name in col_to_dim:
                dim_attr = col_to_dim[source_name]
                dim_node = dim_attr.rsplit(SEPARATOR, 1)[0]
                # Check if this dimension attribute or its parent node is in grain_columns
                for gc in preagg.grain_columns:
                    if gc == dim_attr or gc.startswith(dim_node + SEPARATOR):
                        # Parse the dimension ref to handle role syntax properly
                        # e.g., "v3.date.week[order]" -> column_name="week", role="order"
                        # -> output_name="week_order"
                        parsed = parse_dimension_ref(gc)
                        output_name = parsed.column_name
                        if parsed.role:
                            output_name = f"{output_name}_{parsed.role}"
                        logger.info(
                            "Temporal column %s links to dimension %s -> output %s",
                            source_name,
                            dim_attr,
                            output_name,
                        )
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
