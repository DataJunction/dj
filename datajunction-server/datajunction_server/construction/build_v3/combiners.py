"""
Combiners Module for V3 SQL Builder.

This module provides functions for combining multiple grain groups into a single
SQL query using FULL OUTER JOIN on shared dimensions. This is used for:

1. GET /sql/measures/v3/combined - Returns SQL that combines grain groups
2. POST /cubes/{name}/materialize - Generates combiner SQL for Druid ingestion

The combiner SQL:
- Uses FULL OUTER JOIN to combine grain groups on shared dimensions
- COALESCEs shared dimension columns to handle NULLs from outer joins
- Includes all measures from all grain groups (without applying metric expressions)
- Does NOT apply final metric aggregations (Druid handles that)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datajunction_server.construction.build_v3.preagg_matcher import (
    get_temporal_partitions,
)
from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_grain_group_hash,
)

from datajunction_server.construction.build_v3.types import GrainGroupSQL
from datajunction_server.models.column import SemanticType
from datajunction_server.models.query import V3ColumnMetadata
from datajunction_server.sql.parsing import ast
from datajunction_server.construction.build_v3.builder import build_measures_sql
from datajunction_server.models.dialect import Dialect
from datajunction_server.utils import get_settings

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class CombinedGrainGroupResult:
    """
    Result of combining multiple grain groups.

    Contains the combined SQL query and metadata about what was combined.
    """

    query: ast.Query  # The combined SQL query AST
    columns: list[V3ColumnMetadata]  # Output column metadata
    grain_groups_combined: int  # Number of grain groups that were combined
    shared_dimensions: list[str]  # Dimension columns used in JOIN
    all_measures: list[str]  # All measure columns in output

    @property
    def sql(self) -> str:
        """Render the query AST to SQL string."""
        return str(self.query)


def build_combiner_sql(
    grain_groups: list[GrainGroupSQL],
    output_table_names: list[str] | None = None,
) -> CombinedGrainGroupResult:
    """
    Build SQL that combines multiple grain groups with FULL OUTER JOIN.

    This function generates a query like:
        SELECT
            COALESCE(gg1.dim1, gg2.dim1) AS dim1,
            COALESCE(gg1.dim2, gg2.dim2) AS dim2,
            gg1.measure_a,
            gg1.measure_b,
            gg2.measure_c,
            gg2.measure_d
        FROM grain_group_1 gg1
        FULL OUTER JOIN grain_group_2 gg2
            ON gg1.dim1 = gg2.dim1 AND gg1.dim2 = gg2.dim2

    Args:
        grain_groups: List of GrainGroupSQL objects to combine.
        output_table_names: Optional list of table names/aliases for each grain group.
            If not provided, generates aliases like gg1, gg2, etc.

    Returns:
        CombinedGrainGroupResult with the combined query and metadata.

    Raises:
        ValueError: If grain_groups is empty or grains don't match.
    """
    if not grain_groups:
        raise ValueError("At least one grain group is required")

    # Single grain group - no combination needed
    if len(grain_groups) == 1:
        return _single_grain_group_result(grain_groups[0])

    # Multiple grain groups - combine with FULL OUTER JOIN
    return _combine_multiple_grain_groups(grain_groups, output_table_names)


def _single_grain_group_result(grain_group: GrainGroupSQL) -> CombinedGrainGroupResult:
    """
    Handle the single grain group case - no JOIN needed.

    Returns the grain group's query wrapped in a result object.
    """
    # Extract dimension and measure columns
    dimension_cols = []
    measure_cols = []

    for col in grain_group.columns:
        if col.semantic_type in ("dimension", "metric_input"):
            dimension_cols.append(col.name)
        elif col.semantic_type in ("metric_component", "measure", "metric"):
            measure_cols.append(col.name)

    # Build V3ColumnMetadata from GrainGroupSQL.columns
    output_columns = [
        V3ColumnMetadata(
            name=col.name,
            type=col.type,
            semantic_entity=col.semantic_name,
            semantic_type=col.semantic_type,
        )
        for col in grain_group.columns
    ]

    return CombinedGrainGroupResult(
        query=grain_group.query,
        columns=output_columns,
        grain_groups_combined=1,
        shared_dimensions=grain_group.grain,
        all_measures=measure_cols,
    )


def _combine_multiple_grain_groups(
    grain_groups: list[GrainGroupSQL],
    output_table_names: list[str] | None = None,
) -> CombinedGrainGroupResult:
    """
    Combine multiple grain groups using FULL OUTER JOIN.

    Uses CTEs to define each grain group, then joins them in the final SELECT.
    This follows the same pattern as metrics.py for cross-fact metrics.
    """
    # Generate CTE aliases if not provided
    if output_table_names is None:
        output_table_names = [f"gg{i + 1}" for i in range(len(grain_groups))]

    # Validate all grain groups have the same grain (dimensions)
    # Preserve order from the first grain group
    first_grain = grain_groups[0].grain
    reference_grain_set = set(first_grain)

    for i, gg in enumerate(grain_groups[1:], 2):
        if set(gg.grain) != reference_grain_set:
            logger.warning(
                "Grain groups have different grains: %s vs %s. "
                "Using intersection for JOIN.",
                reference_grain_set,
                set(gg.grain),
            )
            reference_grain_set = reference_grain_set.intersection(set(gg.grain))

    # Preserve order from first grain group, filtering to only shared columns
    shared_grain = [g for g in first_grain if g in reference_grain_set]

    # Create CTEs for each grain group
    ctes = _create_ctes(grain_groups, output_table_names)

    # Create table references for each CTE (used in projections)
    table_refs = {name: ast.Table(name=ast.Name(name)) for name in output_table_names}

    # Build COALESCE expressions for shared dimensions
    dimension_projections = _build_coalesced_dimensions(
        grain_groups,
        output_table_names,
        table_refs,
        shared_grain,
    )

    # Build measure projections (each measure from its source grain group)
    measure_projections, all_measures = _build_measure_projections(
        grain_groups,
        output_table_names,
        table_refs,
    )

    # Build FROM clause with FULL OUTER JOINs (referencing CTEs by name)
    from_clause = _build_join_from_clause(
        output_table_names,
        table_refs,
        shared_grain,
    )

    # Combine all projections
    all_projections = dimension_projections + measure_projections  # type: ignore

    # Build the final query with CTEs
    combined_query = ast.Query(
        select=ast.Select(
            projection=all_projections,  # type: ignore
            from_=from_clause,
        ),
        ctes=ctes,
    )

    # Build output column metadata
    output_columns = _build_output_columns(
        grain_groups,
        shared_grain,
        all_measures,
    )

    return CombinedGrainGroupResult(
        query=combined_query,
        columns=output_columns,
        grain_groups_combined=len(grain_groups),
        shared_dimensions=shared_grain,
        all_measures=all_measures,
    )


def _create_ctes(
    grain_groups: list[GrainGroupSQL],
    cte_names: list[str],
) -> list[ast.Query]:
    """
    Create CTEs (Common Table Expressions) for each grain group.

    Each grain group's query becomes a CTE with the given alias.
    Uses the Query.to_cte() method to properly format the CTE with
    parentheses and AS keyword.
    """
    from copy import deepcopy

    # First, collect all nested CTEs from all grain groups (deduplicated)
    nested_ctes: list[ast.Query] = []
    seen_cte_names: set[str] = set()

    for gg in grain_groups:
        if gg.query.ctes:
            for nested_cte in gg.query.ctes:
                # CTE name is stored in the alias attribute (set by to_cte method)
                cte_name = nested_cte.alias.name if nested_cte.alias else None
                if cte_name and cte_name not in seen_cte_names:
                    seen_cte_names.add(cte_name)
                    nested_ctes.append(deepcopy(nested_cte))

    # Then create the grain group CTEs
    grain_group_ctes = []
    for gg, name in zip(grain_groups, cte_names):
        # Deep copy the query to avoid mutating the original
        cte_query = deepcopy(gg.query)
        # Clear nested CTEs from this query (they're now at top level)
        cte_query.ctes = []
        # Convert to CTE format (adds parentheses, AS keyword, etc.)
        cte_query.to_cte(ast.Name(name))
        grain_group_ctes.append(cte_query)

    # Return nested CTEs first, then grain group CTEs
    return nested_ctes + grain_group_ctes


def _build_coalesced_dimensions(
    grain_groups: list[GrainGroupSQL],
    table_names: list[str],
    table_refs: dict[str, ast.Table],
    shared_grain: list[str],
) -> list[ast.Aliasable]:
    """
    Build COALESCE expressions for shared dimensions.

    Example output:
        COALESCE(gg1.date_id, gg2.date_id, gg3.date_id) AS date_id
    """
    projections = []

    for grain_col in shared_grain:
        coalesce_args: list[ast.Expression] = [
            ast.Column(
                name=ast.Name(grain_col),
                _table=table_refs.get(name),
            )
            for name in table_names
        ]

        coalesce_expr = ast.Function(
            name=ast.Name("COALESCE"),
            args=coalesce_args,
        ).set_alias(alias=ast.Name(grain_col))

        projections.append(coalesce_expr)

    return projections  # type: ignore


def _build_measure_projections(
    grain_groups: list[GrainGroupSQL],
    table_names: list[str],
    table_refs: dict[str, ast.Table],
) -> tuple[list[ast.Column], list[str]]:
    """
    Build projections for all measures from all grain groups.

    Each measure is qualified with its source table alias.
    Returns (projections, list of measure names).
    """
    projections = []
    all_measures = []
    seen_measures = set()

    for gg, name in zip(grain_groups, table_names):
        for col in gg.columns:
            # Only include measure/component columns, not dimensions
            if col.semantic_type not in ("metric_component", "measure", "metric"):
                continue

            # Track the measure name for output metadata
            if col.name not in seen_measures:
                all_measures.append(col.name)
                seen_measures.add(col.name)

                # Create qualified column reference
                measure_col = ast.Column(
                    name=ast.Name(col.name),
                    _table=table_refs.get(name),
                    semantic_type=SemanticType.MEASURE,
                )

                projections.append(measure_col)

    return projections, all_measures


def _build_join_from_clause(
    cte_names: list[str],
    table_refs: dict[str, ast.Table],
    shared_grain: list[str],
) -> ast.From:
    """
    Build FROM clause with FULL OUTER JOINs on CTEs.

    Example output (CTEs are defined in the WITH clause):
        FROM gg1
        FULL OUTER JOIN gg2 ON gg1.dim1 = gg2.dim1 AND gg1.dim2 = gg2.dim2
        FULL OUTER JOIN gg3 ON gg1.dim1 = gg3.dim1 AND gg1.dim2 = gg3.dim2
    """
    first_name = cte_names[0]

    # Build JOIN extensions for remaining CTEs
    join_extensions = []
    for name in cte_names[1:]:
        # Build JOIN criteria on shared grain columns
        join_criteria = _build_join_criteria(
            table_refs[first_name],
            table_refs[name],
            shared_grain,
        )

        join_extension = ast.Join(
            join_type="FULL OUTER",
            right=ast.Table(name=ast.Name(name)),
            criteria=ast.JoinCriteria(on=join_criteria),
        )

        join_extensions.append(join_extension)

    # Build the FROM clause - primary is first CTE, extensions are JOINs
    from_relation = ast.Relation(
        primary=ast.Table(name=ast.Name(first_name)),
        extensions=join_extensions,
    )

    return ast.From(relations=[from_relation])


def _build_join_criteria(
    left_table: ast.Table,
    right_table: ast.Table,
    grain_columns: list[str],
) -> ast.Expression:
    """
    Build JOIN ON condition for grain columns.

    Example output:
        left.dim1 = right.dim1 AND left.dim2 = right.dim2
    """
    if not grain_columns:
        # No grain columns - use TRUE (cartesian join)
        return ast.Boolean(True)  # type: ignore

    conditions = [
        ast.BinaryOp.Eq(
            ast.Column(name=ast.Name(col), _table=left_table),
            ast.Column(name=ast.Name(col), _table=right_table),
        )
        for col in grain_columns
    ]

    if len(conditions) == 1:
        return conditions[0]

    return ast.BinaryOp.And(*conditions)


def _build_output_columns(
    grain_groups: list[GrainGroupSQL],
    shared_grain: list[str],
    all_measures: list[str],
) -> list[V3ColumnMetadata]:
    """
    Build output column metadata for the combined query.
    """
    columns = []
    seen_columns = set()

    # Add dimension columns (from shared grain)
    # Use the first grain group's metadata for types
    first_gg = grain_groups[0]
    col_metadata_lookup = {col.name: col for col in first_gg.columns}

    for grain_col in shared_grain:
        if grain_col in seen_columns:
            continue  # pragma: no cover

        col_meta = col_metadata_lookup.get(grain_col)
        if col_meta:
            columns.append(  # pragma: no cover
                V3ColumnMetadata(
                    name=grain_col,
                    type=col_meta.type,
                    semantic_entity=col_meta.semantic_name,
                    semantic_type="dimension",
                ),
            )
            seen_columns.add(grain_col)

    # Add measure columns
    # Look up metadata from whichever grain group has the measure
    all_col_metadata = {}
    for gg in grain_groups:
        for col in gg.columns:
            if col.name not in all_col_metadata:
                all_col_metadata[col.name] = col

    for measure_name in all_measures:
        if measure_name in seen_columns:
            continue  # pragma: no cover

        col_meta = all_col_metadata.get(measure_name)
        if col_meta:  # pragma: no branch
            columns.append(
                V3ColumnMetadata(
                    name=measure_name,
                    type=col_meta.type,
                    semantic_entity=col_meta.semantic_name,
                    semantic_type="metric_component",
                ),
            )
            seen_columns.add(measure_name)

    return columns


def validate_grain_groups_compatible(
    grain_groups: list[GrainGroupSQL],
) -> tuple[bool, str | None]:
    """
    Validate that grain groups can be combined.

    Grain groups are compatible if they share the same set of grain columns
    (the dimensions they're aggregated to).

    Args:
        grain_groups: List of grain groups to validate.

    Returns:
        (is_valid, error_message) tuple.
    """
    if not grain_groups:
        return False, "No grain groups provided"

    if len(grain_groups) == 1:
        return True, None

    reference_grain = set(grain_groups[0].grain)
    for i, gg in enumerate(grain_groups[1:], 2):
        current_grain = set(gg.grain)
        if current_grain != reference_grain:
            return (
                False,
                f"Grain group {i} has different grain {current_grain} "
                f"than grain group 1 {reference_grain}",
            )

    return True, None


# =============================================================================
# Pre-Agg Table Reference Functions
# =============================================================================


def _compute_preagg_table_name(parent_name: str, grain_group_hash: str) -> str:
    """
    Compute the deterministic pre-agg table name.

    Format: {node_short}_preagg_{hash[:8]}
    """
    node_short = parent_name.replace(".", "_")
    return f"{node_short}_preagg_{grain_group_hash[:8]}"


@dataclass
class TemporalPartitionInfo:
    """Temporal partition info extracted from pre-agg source nodes."""

    column_name: str  # Output column name (e.g., "dateint")
    format: str | None  # Date format (e.g., "yyyyMMdd")
    granularity: str | None  # Granularity (e.g., "day")


async def build_combiner_sql_from_preaggs(
    session,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None = None,
    dialect=None,
) -> tuple[CombinedGrainGroupResult, list[str], TemporalPartitionInfo | None]:
    """
    Build combined SQL that reads from pre-aggregation tables.

    Instead of computing measures from source tables, this generates SQL
    that reads from the deterministically-named pre-agg tables. The table
    names are computed from settings.preagg_catalog/schema + the grain group hash.

    This is used when source=preagg_tables in the combined endpoint, enabling
    Druid cube workflows to wait on pre-agg table VTTS before starting.

    Args:
        session: Database session
        metrics: List of metric names
        dimensions: List of dimension references
        filters: Optional filters
        dialect: SQL dialect

    Returns:
        Tuple of:
        - CombinedGrainGroupResult
        - list of pre-agg table references
        - TemporalPartitionInfo (auto-detected from source nodes, or None if not found)
    """
    settings = get_settings()

    # Build measures SQL to get grain groups and their metadata
    result = await build_measures_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=dialect or Dialect.SPARK,
        use_materialized=False,  # We'll manually reference pre-agg tables
    )

    if not result.grain_groups:  # pragma: no cover
        raise ValueError("No grain groups generated")

    ctx = result.ctx
    preagg_table_refs = []
    preagg_grain_groups = []
    temporal_partitions_found: list[TemporalPartitionInfo] = []

    # Use requested_dimensions (fully qualified) for hash lookup, not gg.grain (aliases)
    grain_columns_for_hash = list(result.requested_dimensions)

    for gg in result.grain_groups:
        # Get the parent node and its revision ID
        parent_node = ctx.nodes.get(gg.parent_name)
        if not parent_node or not parent_node.current:  # pragma: no cover
            raise ValueError(f"Parent node {gg.parent_name} not found")

        node_revision_id = parent_node.current.id

        # Look up the PreAggregation record to get temporal partition info
        # Use fully qualified dimensions (requested_dimensions) not aliases (gg.grain)
        grain_group_hash = compute_grain_group_hash(
            node_revision_id,
            grain_columns_for_hash,
        )
        preaggs = await PreAggregation.get_by_grain_group_hash(
            session,
            grain_group_hash,
        )
        if preaggs:
            # Use get_temporal_partitions from preagg_matcher (reuse existing logic)
            for preagg in preaggs:  # pragma: no branch
                for tp in get_temporal_partitions(preagg):
                    temporal_partitions_found.append(
                        TemporalPartitionInfo(
                            column_name=tp.column_name,
                            format=tp.format,
                            granularity=tp.granularity,
                        ),
                    )
                break  # Only need one preagg per grain group for temporal info

        # Use the same grain_group_hash for the pre-agg table name
        # (must match the hash used when the pre-agg was created)
        table_name = _compute_preagg_table_name(gg.parent_name, grain_group_hash)
        print(f"table_name: {table_name}")

        # Build full table reference
        full_table_ref = (
            f"{settings.preagg_catalog}.{settings.preagg_schema}.{table_name}"
        )
        preagg_table_refs.append(full_table_ref)

        # Create a modified grain group that reads from the pre-agg table
        # We need to build a simple SELECT from the pre-agg table with re-aggregation
        preagg_gg = _build_grain_group_from_preagg_table(
            gg,
            full_table_ref,
        )
        preagg_grain_groups.append(preagg_gg)

    # Combine the pre-agg grain groups
    combined_result = build_combiner_sql(
        preagg_grain_groups,
        output_table_names=[f"gg{i + 1}" for i in range(len(preagg_grain_groups))],
    )

    # Determine temporal partition info
    # If all grain groups agree on temporal partition, use it; otherwise None
    temporal_partition_info: TemporalPartitionInfo | None = None
    if temporal_partitions_found:
        # Check if all found partitions match (same column name)
        first = temporal_partitions_found[0]
        if all(  # pragma: no branch
            tp.column_name == first.column_name for tp in temporal_partitions_found
        ):
            temporal_partition_info = first

    return combined_result, preagg_table_refs, temporal_partition_info


def _build_grain_group_from_preagg_table(
    original_gg: GrainGroupSQL,
    preagg_table_ref: str,
) -> GrainGroupSQL:
    """
    Build a GrainGroupSQL that reads from a pre-agg table.

    The generated SQL is:
        SELECT dim1, dim2, SUM(measure1) AS measure1, ...
        FROM preagg_table
        GROUP BY dim1, dim2

    Args:
        original_gg: The original grain group (for metadata)
        preagg_table_ref: Full pre-agg table reference (catalog.schema.table)

    Returns:
        GrainGroupSQL with query reading from pre-agg table
    """
    from datajunction_server.construction.build_v3.types import ColumnMetadata

    # Build SELECT columns
    select_items: list[ast.Aliasable | ast.Expression | ast.Column] = []
    group_by_cols: list[str] = []

    # Add dimension columns
    for grain_col in original_gg.grain:
        col_ref = ast.Column(name=ast.Name(grain_col))
        select_items.append(col_ref)
        group_by_cols.append(grain_col)

    # Add measure columns with re-aggregation
    for col in original_gg.columns:
        if col.semantic_type in ("metric_component", "measure", "metric"):
            col_ref = ast.Column(name=ast.Name(col.name))

            # Find the component to get the merge function
            merge_func = None
            for comp in original_gg.components:
                if (  # pragma: no branch
                    comp.name == col.name
                    or original_gg.component_aliases.get(comp.name) == col.name
                ):
                    merge_func = comp.merge
                    break

            if merge_func:
                # Apply re-aggregation
                agg_expr = ast.Function(
                    name=ast.Name(merge_func),
                    args=[col_ref],
                )
                aliased = ast.Alias(child=agg_expr, alias=ast.Name(col.name))
                select_items.append(aliased)
            else:
                # No merge function - just select the column
                select_items.append(col_ref)

    # Build GROUP BY
    group_by: list[ast.Expression] = [
        ast.Column(name=ast.Name(col)) for col in group_by_cols
    ]

    # Build FROM clause
    from_clause = ast.From.Table(preagg_table_ref)

    # Build SELECT statement
    select = ast.Select(
        projection=select_items,
        from_=from_clause,
        group_by=group_by if group_by else [],
    )

    # Build the query
    query = ast.Query(select=select)

    # Create new ColumnMetadata with correct types
    new_columns = [
        ColumnMetadata(
            name=col.name,
            semantic_name=col.semantic_name,
            type=col.type,
            semantic_type=col.semantic_type,
        )
        for col in original_gg.columns
    ]

    return GrainGroupSQL(
        query=query,
        columns=new_columns,
        grain=original_gg.grain,
        aggregability=original_gg.aggregability,
        metrics=original_gg.metrics,
        parent_name=original_gg.parent_name,
        component_aliases=original_gg.component_aliases,
        is_merged=original_gg.is_merged,
        component_aggregabilities=original_gg.component_aggregabilities,
        components=original_gg.components,
    )
