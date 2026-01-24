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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datajunction_server.models.decompose import MetricComponent
from datajunction_server.construction.build_v3.preagg_matcher import (
    get_temporal_partitions,
)
from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_grain_group_hash,
)

from datajunction_server.construction.build_v3.cte import (
    process_metric_combiner_expression,
)
from datajunction_server.construction.build_v3.types import GrainGroupSQL
from datajunction_server.construction.build_v3.utils import build_join_from_clause
from datajunction_server.models.column import SemanticType
from datajunction_server.models.query import V3ColumnMetadata
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import render_for_dialect, to_sql
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
    # Metric components with aggregation info (for materialization)
    measure_components: list["MetricComponent"] = field(default_factory=list)
    # Mapping from component name to output column alias
    # e.g., {"account_id_hll_e7b21ce4": "approx_unique_accounts_rating"}
    component_aliases: dict[str, str] = field(default_factory=dict)
    # Mapping from metric name to combiner expression SQL
    # e.g., {"v3.avg_rating": "SUM(rating_sum_abc123) / SUM(rating_count_def456)"}
    metric_combiners: dict[str, str] = field(default_factory=dict)
    # Dialect for rendering SQL (used for dialect-specific function names)
    dialect: Dialect = Dialect.SPARK

    @property
    def sql(self) -> str:
        """Render the query AST to SQL string for the target dialect."""
        return to_sql(self.query, self.dialect)


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
            semantic_name=col.semantic_name,
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
        measure_components=grain_group.components,
        component_aliases=grain_group.component_aliases,
        dialect=grain_group.dialect,
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
    from_clause = build_join_from_clause(
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

    # Collect all components and aliases from all grain groups
    all_components = []
    all_component_aliases: dict[str, str] = {}
    for gg in grain_groups:
        all_components.extend(gg.components)
        all_component_aliases.update(gg.component_aliases)

    # Use dialect from the first grain group (all should have same dialect)
    dialect = grain_groups[0].dialect if grain_groups else Dialect.SPARK

    return CombinedGrainGroupResult(
        query=combined_query,
        columns=output_columns,
        grain_groups_combined=len(grain_groups),
        shared_dimensions=shared_grain,
        all_measures=all_measures,
        measure_components=all_components,
        component_aliases=all_component_aliases,
        dialect=dialect,
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
                    semantic_name=col_meta.semantic_name,
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
                    semantic_name=col_meta.semantic_name,
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

    # Populate metric_combiners from decomposed_metrics
    # These are the expressions that combine pre-aggregated components into final metric values
    # Use the same expression processing as generate_metrics_sql to ensure consistency
    # Render in Druid dialect since these are used by viz tools that query Druid directly
    #
    # Build dimension refs in tuple format: {name: (cte_alias, column_name)}
    # For cube queries, cte_alias is empty string (no CTEs, direct column access)
    # Note: We don't replace component refs because combiner_ast already has the correct
    # component hash names which ARE the column names in the Druid cube output
    dimension_refs = {
        dim_ref: ("", col_alias)
        for dim_ref, col_alias in ctx.alias_registry.all_mappings().items()
    }
    with render_for_dialect(Dialect.DRUID):
        for metric_name, decomposed in result.decomposed_metrics.items():
            # Process the expression using the shared helper function
            # This applies the same transformations as generate_metrics_sql:
            # - Replace dimension refs (e.g., "v3.date.dateint" -> "dateint")
            # - Inject PARTITION BY for window functions
            processed_expr = process_metric_combiner_expression(
                combiner_ast=decomposed.combiner_ast,
                dimension_refs=dimension_refs,
                partition_dimensions=combined_result.shared_dimensions,
            )
            combined_result.metric_combiners[metric_name] = str(processed_expr)

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

    # Reorder columns so partition column is last
    # This is required for Hive/Spark INSERT OVERWRITE ... PARTITION (col) syntax
    if temporal_partition_info:
        combined_result = _reorder_partition_column_last(
            combined_result,
            temporal_partition_info.column_name,
        )

    return combined_result, preagg_table_refs, temporal_partition_info


def _reorder_partition_column_last(
    result: CombinedGrainGroupResult,
    partition_column: str,
) -> CombinedGrainGroupResult:
    """
    Reorder columns so:
    1. Column metadata order matches the actual SQL projection order
    2. The partition column is last (required for Hive/Spark INSERT OVERWRITE PARTITION)

    For Hive/Spark INSERT OVERWRITE ... PARTITION (col) syntax:
    - Non-partition columns must match the target table's column order (by position)
    - Partition column(s) must be last

    This function:
    1. First synchronizes column metadata to match projection order
    2. Then moves the partition column to the end of both

    Args:
        result: The combined grain group result to reorder
        partition_column: Name of the partition column to move to the end

    Returns:
        A CombinedGrainGroupResult with reordered columns (mutates the input)
    """
    # Note: We mutate the input result directly since the caller immediately
    # reassigns to the return value and doesn't use the original afterward.
    # This avoids expensive deepcopy of AST objects.

    def _get_projection_name(proj: ast.Node) -> str | None:
        """Extract the output column name from a projection element."""
        # Aliasable types (Column, Alias) have alias_or_name which prefers alias over name
        if isinstance(proj, ast.Aliasable):
            return proj.alias_or_name.name
        # Named types (Function) have name directly
        if isinstance(proj, ast.Named):
            return proj.name.name
        return None

    # Step 1: Build a mapping from column name to metadata
    col_meta_by_name = {col.name: col for col in result.columns}

    # Step 2: Extract projection names in order (this is the canonical order)
    projection_names = [
        name
        for proj in result.query.select.projection
        if (name := _get_projection_name(proj))
    ]

    # Step 3: Reorder column metadata to match projection order
    reordered_columns = [
        col_meta_by_name[name] for name in projection_names if name in col_meta_by_name
    ]

    result.columns = reordered_columns

    # Step 4: Move partition column to end of both projections and columns
    projections = result.query.select.projection
    partition_proj = None
    other_projs = []

    for proj in projections:
        proj_name = _get_projection_name(proj)
        if proj_name == partition_column:
            partition_proj = proj
        else:
            other_projs.append(proj)

    if partition_proj is not None:
        result.query.select.projection = other_projs + [partition_proj]

    # Reorder column metadata to match (partition last)
    partition_col_meta = None
    other_cols = []
    for col in result.columns:
        if col.name == partition_column:
            partition_col_meta = col
        else:
            other_cols.append(col)

    if partition_col_meta is not None:
        result.columns = other_cols + [partition_col_meta]

    # Also reorder shared_dimensions to put partition column last
    if partition_column in result.shared_dimensions:
        new_dims = [d for d in result.shared_dimensions if d != partition_column]
        new_dims.append(partition_column)
        result = CombinedGrainGroupResult(
            query=result.query,
            columns=result.columns,
            grain_groups_combined=result.grain_groups_combined,
            shared_dimensions=new_dims,
            all_measures=result.all_measures,
            measure_components=result.measure_components,
            component_aliases=result.component_aliases,
            metric_combiners=result.metric_combiners,
            dialect=result.dialect,
        )

    return result


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
        dialect=original_gg.dialect,
    )
