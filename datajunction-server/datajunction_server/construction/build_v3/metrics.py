"""
Metrics SQL Generation

This module handles generating the final metrics SQL query,
combining grain groups and applying combiner expressions.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Optional

from datajunction_server.construction.build_v3.cte import (
    build_alias_to_dimension_node,
    build_dimension_node_to_aliases,
    get_window_order_by_columns,
    has_window_function,
    inject_partition_by_into_windows,
    replace_component_refs_in_ast,
    replace_dimension_refs_in_ast,
    replace_metric_refs_in_ast,
)
from datajunction_server.construction.build_v3.filters import (
    parse_and_resolve_filters,
)
from datajunction_server.construction.build_v3.utils import (
    build_join_from_clause,
    get_short_name,
    make_column_ref,
)
from datajunction_server.construction.build_v3.types import (
    BaseMetricsResult,
    BuildContext,
    ColumnMetadata,
    ColumnRef,
    ColumnResolver,
    ColumnType,
    DecomposedMetricInfo,
    GeneratedMeasuresSQL,
    GeneratedSQL,
    GrainGroupSQL,
    MetricExprInfo,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import Aggregability
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast

logger = logging.getLogger(__name__)


def build_base_metric_expression(
    decomposed: DecomposedMetricInfo,
    cte_alias: str,
    gg: GrainGroupSQL,
) -> tuple[ast.Expression, dict[str, tuple[str, str]]]:
    """
    Build an expression AST for a base metric from a grain group.

    Always applies re-aggregation in the final SELECT using the component's
    merge function (e.g., SUM for sums/counts, MIN for min, hll_union for HLL).

    This is correct whether the CTE is at the exact requested grain or finer:
    - If CTE is at requested grain: re-aggregation is a no-op (SUM of one = that one)
    - If CTE is at finer grain: re-aggregation does actual work

    Args:
        decomposed: Decomposed metric info with components and combiner
        cte_alias: Alias of the grain group CTE
        gg: The grain group SQL containing component metadata

    Returns:
        Tuple of (expression AST, component column mappings)
        The component mappings are {component_name: (cte_alias, column_name)}
    """
    comp_mappings: dict[str, tuple[str, str]] = {}

    # Build component -> column mappings
    # For merged: use component_aggregabilities to determine column source
    # For non-merged: use decomposed.aggregability
    for comp in decomposed.components:
        if gg.is_merged:
            orig_agg = gg.component_aggregabilities.get(comp.name, Aggregability.FULL)
        else:
            orig_agg = decomposed.aggregability

        if orig_agg == Aggregability.LIMITED:
            # LIMITED: use component alias if available (cube case), else grain column
            # For cubes, the component is pre-aggregated and we need to use its column name
            actual_col = gg.component_aliases.get(comp.name)
            if actual_col:
                comp_mappings[comp.name] = (cte_alias, actual_col)
            else:  # pragma: no cover
                # Not from cube - use grain column for COUNT DISTINCT
                grain_col = comp.rule.level[0] if comp.rule.level else comp.expression
                comp_mappings[comp.name] = (cte_alias, grain_col)
        else:
            # FULL/NONE: use pre-aggregated column
            actual_col = gg.component_aliases.get(comp.name, comp.name)
            comp_mappings[comp.name] = (cte_alias, actual_col)  # type: ignore

    # Build the aggregation expression
    expr_ast = _build_metric_aggregation(decomposed, cte_alias, gg, comp_mappings)
    return expr_ast, comp_mappings


def _build_metric_aggregation(
    decomposed: DecomposedMetricInfo,
    cte_alias: str,
    gg: GrainGroupSQL,
    comp_mappings: dict[str, tuple[str, str]],
) -> ast.Expression:
    """
    Build aggregation expression for a metric.

    Always uses combiner_ast from decomposition to ensure correct handling of
    complex combiners like HLL (which needs hll_sketch_estimate wrapped around
    hll_union_agg), not just the bare merge function.

    Special case: LIMITED aggregability (COUNT DISTINCT) is handled separately
    since it can't be pre-aggregated.

    This works whether the CTE is at exact grain or finer grain:
    - Exact grain: re-aggregation is a no-op (SUM of one value = that value)
    - Finer grain: re-aggregation does actual combining

    Args:
        decomposed: Decomposed metric info
        cte_alias: CTE alias for column references
        gg: Grain group SQL with aggregability info
        comp_mappings: Pre-computed component -> (alias, column) mappings
    """

    # Determine aggregability for each component
    def get_comp_aggregability(comp_name: str) -> Aggregability:
        if gg.is_merged:
            return gg.component_aggregabilities.get(comp_name, Aggregability.FULL)
        return decomposed.aggregability

    # Handle LIMITED aggregability (COUNT DISTINCT) specially
    # This can't be pre-aggregated, so we need COUNT(DISTINCT grain_col)
    if len(decomposed.components) == 1:
        comp = decomposed.components[0]
        orig_agg = get_comp_aggregability(comp.name)

        if orig_agg == Aggregability.LIMITED:
            _, col_name = comp_mappings[comp.name]
            distinct_col = make_column_ref(col_name, cte_alias)
            agg_name = comp.aggregation or "COUNT"
            return ast.Function(
                ast.Name(agg_name),
                args=[distinct_col],
                quantifier=ast.SetQuantifier.Distinct,
            )

    # For all other cases (single-component like HLL, or multi-component),
    # use combiner_ast to get the full expression structure.
    # This ensures complex combiners like hll_sketch_estimate(hll_union_agg(...))
    # are handled correctly, not just the bare merge function.
    expr_ast = deepcopy(decomposed.combiner_ast)
    replace_component_refs_in_ast(expr_ast, comp_mappings)
    return expr_ast


def collect_and_build_ctes(
    grain_groups: list[GrainGroupSQL],
) -> tuple[list[ast.Query], list[str]]:
    """
    Collect shared CTEs and convert grain groups to CTEs.
    Returns (all_cte_asts, cte_aliases).
    """
    # Collect all inner CTEs, dedupe by original name
    # CTEs with the same name are shared (e.g., v3_product dimension used by multiple grain groups)
    shared_ctes: dict[str, ast.Query] = {}  # original_name -> CTE AST

    for gg in grain_groups:
        gg_query = gg.query
        if gg_query.ctes:  # pragma: no branch
            for inner_cte in gg_query.ctes:
                cte_name = str(inner_cte.alias) if inner_cte.alias else "unnamed_cte"
                if cte_name not in shared_ctes:
                    # First time seeing this CTE - add it
                    shared_ctes[cte_name] = deepcopy(inner_cte)

    # Build grain group aliases and CTEs
    all_cte_asts: list[ast.Query] = []
    cte_aliases: list[str] = []

    # Add shared CTEs first (no prefix - they keep original names)
    for cte_ast in shared_ctes.values():
        all_cte_asts.append(cte_ast)

    # Track index per parent for naming: {parent}_{index}
    parent_index_counter: dict[str, int] = {}

    for gg in grain_groups:
        # Get short parent name (last part after separator)
        parent_short = get_short_name(gg.parent_name)
        # Get next index for this parent
        idx = parent_index_counter.get(parent_short, 0)
        parent_index_counter[parent_short] = idx + 1
        alias = f"{parent_short}_{idx}"
        cte_aliases.append(alias)

        # gg.query is already an AST - no need to parse!
        gg_query = gg.query

        # Build the grain group CTE (main SELECT, no inner CTEs)
        # Table references already use original CTE names (which are now shared)
        gg_main = deepcopy(gg_query)
        gg_main.ctes = []  # Clear inner CTEs - they're now in shared layer

        # Convert to CTE with the grain group alias
        gg_main.to_cte(ast.Name(alias), None)
        all_cte_asts.append(gg_main)

    return all_cte_asts, cte_aliases


def get_dimension_types(
    grain_groups: list[GrainGroupSQL],
) -> dict[str, str]:
    """
    Extract dimension types from grain group columns.

    Returns mapping of semantic_name -> type.
    """
    dim_types: dict[str, str] = {}
    for gg in grain_groups:
        for col in gg.columns:
            if col.semantic_type == "dimension" and col.semantic_name not in dim_types:
                dim_types[col.semantic_name] = col.type
    return dim_types


def parse_dimension_refs(
    ctx: BuildContext,
    dimensions: list[str],
) -> list[tuple[str, str]]:
    """
    Parse dimension references and register aliases.

    Returns list of (original_dim_ref, col_alias) tuples.
    """
    dim_info: list[tuple[str, str]] = []
    for dim in dimensions:
        # Generate a consistent alias for this dimension
        # Using register() ensures we get a proper alias (with role suffix if applicable)
        col_name = ctx.alias_registry.register(dim)
        dim_info.append((dim, col_name))
    return dim_info


def build_dimension_alias_map(
    dim_info: list[tuple[str, str]],
) -> dict[str, str]:
    """
    Build mapping from dimension refs to column aliases.

    Maps both the full reference (with role) and the base reference (without role)
    to the alias. For example:
    - "v3.date.month[order]" -> "month_order"
    - "v3.date.month" -> "month_order" (if not already mapped)
    """
    dimension_aliases: dict[str, str] = {}
    for original_dim_ref, col_alias in dim_info:
        dimension_aliases[original_dim_ref] = col_alias
        # Also map the base dimension (without role) if different
        if "[" in original_dim_ref:
            base_ref = original_dim_ref.split("[")[0]
            # Only add base ref if not already mapped (first role wins)
            if base_ref not in dimension_aliases:  # pragma: no branch
                dimension_aliases[base_ref] = col_alias
    return dimension_aliases


def qualify_dimension_refs(
    dimension_aliases: dict[str, str],
    cte_alias: str,
) -> dict[str, tuple[str, str]]:
    """
    Convert dimension aliases to CTE-qualified refs.

    Args:
        dimension_aliases: Mapping from dimension refs to column aliases
        cte_alias: The CTE alias to qualify with

    Returns:
        Mapping from dimension refs to (cte_alias, column_name) tuples
    """
    return {
        dim_ref: (cte_alias, col_alias)
        for dim_ref, col_alias in dimension_aliases.items()
    }


def build_dimension_projection(
    dim_info: list[tuple[str, str]],
    cte_aliases: list[str],
    dim_types: dict[str, str],
) -> tuple[list[Any], list[ColumnMetadata]]:
    """
    Build COALESCE projection for dimensions across grain groups.

    Returns (projection, columns_metadata) where projection contains
    COALESCE(gg0.dim, gg1.dim, ...) AS dim expressions.
    """
    projection: list[Any] = []
    columns_metadata: list[ColumnMetadata] = []

    for original_dim_ref, dim_col in dim_info:
        # Build COALESCE(gg0.col, gg1.col, ...) AS col
        coalesce_args: list[ast.Expression] = [
            make_column_ref(dim_col, alias) for alias in cte_aliases
        ]
        coalesce_func = ast.Function(ast.Name("COALESCE"), args=coalesce_args)
        aliased_coalesce = coalesce_func.set_alias(ast.Name(dim_col))
        aliased_coalesce.set_as(True)  # Include "AS" in output
        projection.append(aliased_coalesce)

        # Get actual type from grain groups, fall back to string if not found
        col_type = dim_types.get(original_dim_ref, "string")

        columns_metadata.append(
            ColumnMetadata(
                name=dim_col,
                semantic_name=original_dim_ref,  # Preserve original dimension reference
                type=col_type,
                semantic_type="dimension",
            ),
        )

    return projection, columns_metadata


def process_base_metrics(
    grain_groups: list[GrainGroupSQL],
    cte_aliases: list[str],
    decomposed_metrics: dict[str, DecomposedMetricInfo],
    dimension_aliases: dict[str, str],
) -> BaseMetricsResult:
    """
    Process base metrics from grain groups to build expression mappings.

    For each metric in each grain group:
    - Non-decomposable metrics (like MAX_BY): use original expression with CTE refs
    - Decomposable metrics: build aggregation expression from components

    Args:
        grain_groups: List of grain group SQLs
        cte_aliases: CTE aliases corresponding to each grain group
        decomposed_metrics: Decomposed metric info by metric name
        dimension_aliases: Dimension ref -> column alias mapping

    Returns:
        BaseMetricsResult with all_metrics, metric_exprs, and component_refs
    """
    component_refs: dict[str, ColumnRef] = {}
    metric_exprs: dict[str, MetricExprInfo] = {}

    # Collect all metrics in grain groups
    all_metrics: set[str] = set()
    for gg in grain_groups:
        all_metrics.update(gg.metrics)

    # Process base metrics from each grain group
    for i, gg in enumerate(grain_groups):
        alias = cte_aliases[i]
        for metric_name in gg.metrics:
            decomposed = decomposed_metrics.get(metric_name)
            short_name = get_short_name(metric_name)

            if not decomposed:  # pragma: no cover
                # No decomposition info at all - skip this metric
                continue

            if not decomposed.components:
                # Non-decomposable metric (like MAX_BY) - use original expression
                # with column references rewritten to point to grain group CTE
                expr_ast: ast.Expression = deepcopy(decomposed.combiner_ast)

                # Rewrite column references in the expression to use the CTE alias
                # _table must be an ast.Table (not ast.Name) for proper stringification
                cte_table = ast.Table(name=ast.Name(alias))
                for col in expr_ast.find_all(ast.Column):  # type: ignore
                    if col.name and not col._table:  # type: ignore  # pragma: no branch
                        col._table = cte_table  # type: ignore

                metric_exprs[metric_name] = MetricExprInfo(
                    expr_ast=expr_ast,
                    short_name=short_name,
                    cte_alias=alias,
                )
                continue

            # Build expression using unified function (handles merged + non-merged)
            expr_ast, comp_mappings = build_base_metric_expression(
                decomposed,
                alias,
                gg,
            )
            # Convert component mappings to ColumnRef objects
            for comp_name, (cte_alias, col_name) in comp_mappings.items():
                component_refs[comp_name] = ColumnRef(
                    cte_alias=cte_alias,
                    column_name=col_name,
                )

            # Qualify dimension refs with this grain group's CTE alias
            qualified_dim_refs = qualify_dimension_refs(dimension_aliases, alias)
            replace_dimension_refs_in_ast(expr_ast, qualified_dim_refs)
            metric_exprs[metric_name] = MetricExprInfo(
                expr_ast=expr_ast,
                short_name=short_name,
                cte_alias=alias,
            )

    return BaseMetricsResult(
        all_metrics=all_metrics,
        metric_exprs=metric_exprs,
        component_refs=component_refs,
    )


def find_window_metrics(
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
    all_grain_group_metrics: set[str],
) -> set[str]:
    """
    Identify derived metrics that use window functions.

    These need special handling - base metrics must be pre-computed in a CTE
    before the window functions can reference them.

    Args:
        ctx: Build context with metrics list
        decomposed_metrics: Decomposed metric info
        all_grain_group_metrics: Set of base metrics in grain groups

    Returns:
        Set of metric names that use window functions
    """
    window_metrics: set[str] = set()
    for metric_name in ctx.metrics:
        if metric_name in all_grain_group_metrics:
            continue
        decomposed = decomposed_metrics.get(metric_name)
        if decomposed and has_window_function(decomposed.combiner_ast):
            window_metrics.add(metric_name)
    return window_metrics


def find_intermediate_derived_metrics(
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
    window_metrics: set[str],
    all_grain_group_metrics: set[str],
) -> set[str]:
    """
    Find intermediate derived metrics referenced by window function metrics.

    For nested derived metrics like:
        thumbs_up_rate_wow_change = thumbs_up_rate - LAG(thumbs_up_rate) OVER (...)
    where thumbs_up_rate is itself a derived metric.

    We need to identify these intermediate metrics so they can be pre-computed
    in the base_metrics CTE before the window function is applied.

    Args:
        ctx: Build context with nodes and parent_map
        decomposed_metrics: Decomposed metric info
        window_metrics: Set of window function metric names
        all_grain_group_metrics: Set of base metrics in grain groups

    Returns:
        Set of intermediate derived metric names that need pre-computation
    """
    intermediate_metrics: set[str] = set()

    for metric_name in window_metrics:
        # Get the original query AST for this window metric
        # We need to find metric references in the original query, not the decomposed one
        metric_node = ctx.nodes.get(metric_name)
        if not metric_node:
            continue

        original_query = ctx.get_parsed_query(metric_node)
        expr_ast = original_query.select.projection[0]

        # Find all column references that might be metric references
        for col in expr_ast.find_all(ast.Column):
            col_name = col.identifier()
            # Check if this is a metric reference (not in grain groups but is a known metric)
            if col_name in ctx.nodes:
                ref_node = ctx.nodes[col_name]
                if ref_node.type == NodeType.METRIC:
                    # This is a metric reference
                    if col_name not in all_grain_group_metrics:
                        # It's not a base metric in grain groups, so it's an intermediate
                        intermediate_metrics.add(col_name)

    return intermediate_metrics


def build_intermediate_metric_expr(
    ctx: BuildContext,
    metric_name: str,
    base_metric_exprs: dict[str, MetricExprInfo],
) -> ast.Expression | None:
    """
    Build expression for an intermediate derived metric.

    Intermediate derived metrics (like avg_order_value) reference base metrics
    (like total_revenue, order_count). In the base_metrics CTE, we need to
    compute these by inlining the actual expressions for each referenced metric.

    For example, if avg_order_value = total_revenue / order_count:
    - total_revenue expr: SUM(order_details_0.line_total_sum_e1f61696)
    - order_count expr: COUNT(DISTINCT order_details_0.order_id)
    - avg_order_value becomes: SUM(...) / NULLIF(COUNT(...), 0)

    We can't just reference column aliases from the same SELECT statement,
    so we must inline the full expressions.

    Args:
        ctx: Build context with nodes and query cache
        metric_name: Name of the intermediate derived metric
        base_metric_exprs: Expressions for base metrics (already computed in base_metrics CTE)

    Returns:
        Expression AST for the intermediate metric, or None if cannot be built
    """
    from datajunction_server.construction.build_v3.cte import get_column_full_name

    metric_node = ctx.nodes.get(metric_name)
    if not metric_node:
        return None

    # Get the original query for the intermediate metric
    original_query = ctx.get_parsed_query(metric_node)
    expr_ast = deepcopy(original_query.select.projection[0])

    # Unwrap if it's an alias
    if isinstance(expr_ast, ast.Alias):
        expr_ast = expr_ast.child

    # Build a map of metric names to their expression ASTs
    # We need to inline the actual expressions, not just column names
    metric_exprs: dict[str, ast.Expression] = {
        name: deepcopy(info.expr_ast) for name, info in base_metric_exprs.items()
    }

    # Replace metric references with their full expressions
    # We must walk the AST and replace Column nodes with their expressions
    for col in list(expr_ast.find_all(ast.Column)):
        full_name = get_column_full_name(col)
        if full_name in metric_exprs:
            # Replace this column with the metric's expression
            replacement_expr = deepcopy(metric_exprs[full_name])
            if col.parent:
                col.parent.replace(from_=col, to=replacement_expr)

    return expr_ast  # type: ignore


def build_base_metrics_cte(
    dim_info: list[tuple[str, str]],
    cte_aliases: list[str],
    all_grain_group_metrics: set[str],
    metric_expr_asts: dict[str, MetricExprInfo],
    intermediate_metrics: set[str] | None = None,
    intermediate_exprs: dict[str, ast.Expression] | None = None,
) -> ast.Query:
    """
    Build an intermediate CTE that pre-computes all base metrics.

    This CTE is used when there are window function metrics that need to
    reference base metric values as columns.

    The CTE structure:
        SELECT dim1, dim2, ..., metric1 AS metric1, metric2 AS metric2, ...
        FROM gg0 FULL OUTER JOIN gg1 ON ...
        GROUP BY dim1, dim2, ...

    Args:
        dim_info: List of (original_dim_ref, col_alias) tuples
        cte_aliases: CTE aliases for grain groups
        all_grain_group_metrics: Set of base metric names
        metric_expr_asts: Metric expressions from process_base_metrics
        intermediate_metrics: Optional set of intermediate derived metric names
        intermediate_exprs: Optional dict of intermediate metric expressions

    Returns:
        AST Query for the base_metrics CTE
    """
    base_metrics_projection: list[Any] = []

    # Add dimension columns (COALESCE across grain groups)
    for _, dim_col in dim_info:
        coalesce_args: list[ast.Expression] = [
            make_column_ref(dim_col, alias) for alias in cte_aliases
        ]
        coalesce_func = ast.Function(ast.Name("COALESCE"), args=coalesce_args)
        aliased = coalesce_func.set_alias(ast.Name(dim_col))
        aliased.set_as(True)
        base_metrics_projection.append(aliased)

    # Add base metric expressions (sorted for deterministic ordering)
    for base_metric_name in sorted(all_grain_group_metrics):
        if base_metric_name not in metric_expr_asts:
            continue  # pragma: no cover
        info = metric_expr_asts[base_metric_name]
        aliased_expr = deepcopy(info.expr_ast).set_alias(ast.Name(info.short_name))
        aliased_expr.set_as(True)
        base_metrics_projection.append(aliased_expr)

    # Add intermediate derived metrics (for nested derived metrics)
    if intermediate_metrics and intermediate_exprs:
        for metric_name in sorted(intermediate_metrics):
            if metric_name not in intermediate_exprs:
                continue  # pragma: no cover
            expr_ast = intermediate_exprs[metric_name]
            short_name = get_short_name(metric_name)
            aliased_expr = deepcopy(expr_ast).set_alias(ast.Name(short_name))
            aliased_expr.set_as(True)
            base_metrics_projection.append(aliased_expr)

    # Build FROM clause with FULL OUTER JOINs
    dim_cols = [dim_col for _, dim_col in dim_info]
    table_refs = {name: ast.Table(ast.Name(name)) for name in cte_aliases}
    base_metrics_from = build_join_from_clause(cte_aliases, table_refs, dim_cols)

    # Build GROUP BY on dimensions
    group_by: list[ast.Expression] = []
    for _, dim_col in dim_info:
        group_by.append(make_column_ref(dim_col, cte_aliases[0]))

    return ast.Query(
        select=ast.Select(
            projection=base_metrics_projection,
            from_=base_metrics_from,
            group_by=group_by if group_by else [],
        ),
    )


def rebuild_projection_for_window_metrics(
    dim_info: list[tuple[str, str]],
    dim_types: dict[str, str],
    window_metrics_cte_alias: str,
) -> tuple[list[Any], list[ColumnMetadata]]:
    """
    Rebuild dimension projection to reference base_metrics CTE directly.

    When using a base_metrics CTE for window functions, the final SELECT
    should reference columns from that CTE instead of COALESCE across
    grain group CTEs.

    Args:
        dim_info: List of (original_dim_ref, col_alias) tuples
        dim_types: Dimension types from grain groups
        window_metrics_cte_alias: Alias of the base_metrics CTE

    Returns:
        Tuple of (projection, columns_metadata)
    """
    projection: list[Any] = []
    columns_metadata: list[ColumnMetadata] = []

    for original_dim_ref, dim_col in dim_info:
        col_ref = make_column_ref(dim_col, window_metrics_cte_alias)
        aliased = col_ref.set_alias(ast.Name(dim_col))
        aliased.set_as(True)
        projection.append(aliased)

        col_type = dim_types.get(original_dim_ref, "string")
        columns_metadata.append(
            ColumnMetadata(
                name=dim_col,
                semantic_name=original_dim_ref,
                type=col_type,
                semantic_type="dimension",
            ),
        )

    return projection, columns_metadata


def build_from_clause(
    dim_col_aliases: list[str],
    cte_aliases: list[str],
    window_metrics_cte_alias: Optional[str],
    grain_groups: list[GrainGroupSQL],
) -> tuple[ast.From, list[ast.Expression]]:
    """
    Build FROM clause and GROUP BY for the final SELECT.

    For window function metrics, references base_metrics CTE directly without GROUP BY.
    For standard metrics, builds FULL OUTER JOINs between grain group CTEs with GROUP BY.

    Args:
        dim_col_aliases: List of dimension column aliases for joins
        cte_aliases: List of grain group CTE aliases
        window_metrics_cte_alias: Alias of base_metrics CTE (None if no window metrics)
        grain_groups: Grain groups (for validation error message)

    Returns:
        Tuple of (from_clause, group_by)

    Raises:
        DJInvalidInputException: If cross-fact metrics have no shared dimensions
    """
    # Validate: cross-fact metrics require at least one shared dimension to join on
    # Without shared dimensions, the join would be a CROSS JOIN which produces
    # semantically meaningless results (dividing unrelated populations)
    if len(cte_aliases) > 1 and not dim_col_aliases:
        parent_names = [gg.parent_name for gg in grain_groups]
        raise DJInvalidInputException(
            f"Cross-fact metrics from different parent nodes ({', '.join(parent_names)}) "
            f"require at least one shared dimension to join on. ",
        )

    # For window function metrics, the final SELECT references the base_metrics CTE
    # (which has pre-computed base metrics) without GROUP BY
    if window_metrics_cte_alias:
        from_clause = ast.From(
            relations=[
                ast.Relation(
                    primary=ast.Table(ast.Name(window_metrics_cte_alias)),
                ),
            ],
        )
        # No GROUP BY for window function queries - they need all rows
        return from_clause, []

    # Build FROM clause with FULL OUTER JOINs between grain group CTEs
    table_refs = {name: ast.Table(ast.Name(name)) for name in cte_aliases}
    from_clause = build_join_from_clause(cte_aliases, table_refs, dim_col_aliases)

    # Add GROUP BY on requested dimensions
    # Metrics SQL re-aggregates components to produce final metric values:
    # - If CTE is at requested grain: re-aggregation is a no-op (SUM of one = that one)
    # - If CTE is at finer grain: re-aggregation does actual work
    group_by: list[ast.Expression] = []
    if dim_col_aliases:  # pragma: no branch
        group_by.extend(
            [make_column_ref(dim_col, cte_aliases[0]) for dim_col in dim_col_aliases],
        )

    return from_clause, group_by


def build_metric_projection(
    ctx: BuildContext,
    metric_expr_asts: dict[str, MetricExprInfo],
) -> tuple[list[Any], list[ColumnMetadata]]:
    """
    Build metric projection items in requested order.

    Args:
        ctx: Build context with metrics list and nodes
        metric_expr_asts: Dict of metric name -> MetricExprInfo

    Returns:
        Tuple of (projection_items, columns_metadata) for metrics
    """
    projection_items: list[Any] = []
    columns_metadata: list[ColumnMetadata] = []

    for metric_name in ctx.metrics:
        if metric_name not in metric_expr_asts:  # pragma: no cover
            continue

        info = metric_expr_asts[metric_name]
        aliased_expr = info.expr_ast.set_alias(ast.Name(info.short_name))  # type: ignore
        aliased_expr.set_as(True)
        projection_items.append(aliased_expr)

        # Get metric output type (metrics have exactly one output column)
        metric_node = ctx.nodes[metric_name]
        metric_type = str(metric_node.current.columns[0].type)
        columns_metadata.append(
            ColumnMetadata(
                name=info.short_name,
                semantic_name=metric_name,
                type=metric_type,
                semantic_type="metric",
            ),
        )

    return projection_items, columns_metadata


def build_window_metric_expr(
    ctx: BuildContext,
    metric_name: str,
    base_metric_names: set[str],
    resolver: ColumnResolver,
    partition_columns: list[str],
    window_cte_alias: str,
    intermediate_metric_names: set[str] | None = None,
    alias_to_dimension_node: dict[str, str] | None = None,
) -> ast.Expression:
    """
    Build expression AST for a window function metric.

    Window function metrics (LAG, LEAD, etc.) need special handling:
    - Use the ORIGINAL metric query AST (not decomposed combiner_ast)
    - Reference base_metrics CTE columns for metric values
    - Inject PARTITION BY clauses

    Args:
        ctx: Build context with nodes
        metric_name: Name of the window metric
        base_metric_names: Set of base metric names (for building refs)
        resolver: ColumnResolver (for dimension refs)
        partition_columns: Column names for PARTITION BY injection
        window_cte_alias: Alias of base_metrics CTE
        intermediate_metric_names: Optional set of intermediate derived metric names

    Returns:
        Expression AST with refs resolved and PARTITION BY injected
    """
    # Use the ORIGINAL metric query AST (not decomposed)
    # The decomposed combiner_ast has metric refs expanded to component expressions,
    # but we need the metric refs preserved so we can reference base_metrics columns
    metric_node = ctx.nodes[metric_name]
    original_query = ctx.get_parsed_query(metric_node)
    expr_ast = deepcopy(original_query.select.projection[0])
    if isinstance(expr_ast, ast.Alias):
        expr_ast = expr_ast.child  # pragma: no cover

    # Build refs pointing to window CTE for all base metrics
    window_metric_refs: dict[str, tuple[str, str]] = {
        name: (window_cte_alias, get_short_name(name)) for name in base_metric_names
    }

    # Also include intermediate derived metrics (for nested derived metrics)
    if intermediate_metric_names:
        for name in intermediate_metric_names:
            window_metric_refs[name] = (window_cte_alias, get_short_name(name))
    replace_metric_refs_in_ast(expr_ast, window_metric_refs)

    # Dimension refs also point to window CTE
    window_dim_refs: dict[str, tuple[str, str]] = {
        name: (window_cte_alias, ref.column_name)
        for name, ref in resolver.get_by_type(ColumnType.DIMENSION).items()
    }
    replace_dimension_refs_in_ast(expr_ast, window_dim_refs)

    # Inject PARTITION BY for window functions
    inject_partition_by_into_windows(
        expr_ast,
        partition_columns,
        alias_to_dimension_node,
    )

    return expr_ast  # type: ignore


def build_derived_metric_expr(
    decomposed: DecomposedMetricInfo,
    resolver: ColumnResolver,
    partition_columns: list[str],
    alias_to_dimension_node: dict[str, str] | None = None,
) -> ast.Expression:
    """
    Build expression AST for a non-window derived metric.

    Non-window derived metrics use the decomposed combiner_ast with
    refs resolved via the ColumnResolver.

    Args:
        decomposed: Decomposed metric info with combiner_ast
        resolver: ColumnResolver with metric, component, and dimension refs
        partition_columns: Column names for PARTITION BY injection

    Returns:
        Expression AST with refs resolved and PARTITION BY injected
    """
    expr_ast = deepcopy(decomposed.combiner_ast)

    # Replace refs using resolver
    replace_metric_refs_in_ast(expr_ast, resolver.metric_refs())
    replace_component_refs_in_ast(expr_ast, resolver.component_refs())
    replace_dimension_refs_in_ast(expr_ast, resolver.dimension_refs())

    # Inject PARTITION BY for any window functions in the expression
    inject_partition_by_into_windows(
        expr_ast,
        partition_columns,
        alias_to_dimension_node,
    )

    return expr_ast


def process_derived_metrics(
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
    base_metric_names: set[str],
    resolver: ColumnResolver,
    partition_columns: list[str],
    window_cte_alias: str | None,
    intermediate_metric_names: set[str] | None = None,
    alias_to_dimension_node: dict[str, str] | None = None,
) -> dict[str, MetricExprInfo]:
    """
    Process derived metrics (metrics not in any grain group).

    Derived metrics are computed from base metrics and may include:
    - Window function metrics (LAG, LEAD, etc.) that reference base_metrics CTE
    - Non-window derived metrics that reference other metric columns

    Args:
        ctx: Build context with metrics list and nodes
        decomposed_metrics: Decomposed metric info
        base_metric_names: Set of base metric names (in grain groups)
        resolver: ColumnResolver with metric, component, and dimension refs
        partition_columns: Column names for PARTITION BY injection
        window_cte_alias: Alias of base_metrics CTE ("base_metrics" or None)
        intermediate_metric_names: Optional set of intermediate derived metric names

    Returns:
        Dict of derived metric expressions
    """
    result: dict[str, MetricExprInfo] = {}

    # Identify which derived metrics use window functions
    window_metrics: set[str] = set()
    for metric_name in ctx.metrics:
        if metric_name in base_metric_names:
            continue
        decomposed = decomposed_metrics.get(metric_name)
        if decomposed and has_window_function(decomposed.combiner_ast):
            window_metrics.add(metric_name)

    # Get default CTE alias from resolver (for non-window path)
    metric_refs = resolver.metric_refs()
    default_cte_alias = next(iter(metric_refs.values()))[0] if metric_refs else ""

    for metric_name in ctx.metrics:
        if metric_name in base_metric_names:
            continue

        decomposed = decomposed_metrics.get(metric_name)
        if not decomposed:  # pragma: no cover
            continue

        short_name = get_short_name(metric_name)

        # Handle window function metrics specially
        if metric_name in window_metrics and window_cte_alias:
            expr_ast = build_window_metric_expr(
                ctx,
                metric_name,
                base_metric_names,
                resolver,
                partition_columns,
                window_cte_alias,
                intermediate_metric_names,
                alias_to_dimension_node,
            )
            derived_cte_alias = window_cte_alias
        else:
            expr_ast = build_derived_metric_expr(
                decomposed,
                resolver,
                partition_columns,
                alias_to_dimension_node,
            )
            derived_cte_alias = default_cte_alias

        result[metric_name] = MetricExprInfo(
            expr_ast=expr_ast,
            short_name=short_name,
            cte_alias=derived_cte_alias,
        )

    return result


@dataclass
class GrainLevelInfo:
    """Information about a grain level for window function processing."""

    dimension_node: str  # The dimension node (e.g., "common.dimensions.time.date")
    order_by_alias: str  # The ORDER BY column alias (e.g., "week_code")
    cte_alias: str  # The CTE alias (e.g., "weekly_metrics")
    group_by_dims: list[str]  # Dimensions to GROUP BY at this grain
    join_dims: list[str]  # Dimensions to JOIN on back to base_metrics
    window_metrics: set[str]  # Window metrics that use this grain


def group_window_metrics_by_grain(
    ctx: BuildContext,
    window_metrics: set[str],
    decomposed_metrics: dict[str, DecomposedMetricInfo],
    alias_to_dimension_node: dict[str, str],
    dim_info: list[tuple[str, str]],
) -> dict[str, GrainLevelInfo]:
    """
    Group window metrics by their ORDER BY dimension node.

    For period-over-period metrics, different window functions may operate at
    different grains (e.g., WoW at weekly, MoM at monthly). This function
    groups them so we can build separate CTEs for each grain level.

    Args:
        ctx: Build context with nodes
        window_metrics: Set of window metric names
        decomposed_metrics: Decomposed metric info
        alias_to_dimension_node: Mapping from alias to dimension node
        dim_info: List of (original_dim_ref, col_alias) tuples

    Returns:
        Dict mapping dimension node -> GrainLevelInfo
    """
    # Build reverse mapping: dimension_node -> set of aliases
    node_to_aliases = build_dimension_node_to_aliases(alias_to_dimension_node)

    # Build mapping from full dim ref to alias
    dim_ref_to_alias = {dim_ref: alias for dim_ref, alias in dim_info}

    # Group metrics by their ORDER BY dimension node
    grain_levels: dict[str, GrainLevelInfo] = {}

    for metric_name in window_metrics:
        decomposed = decomposed_metrics.get(metric_name)
        if not decomposed:
            continue

        # Get the ORDER BY columns from the metric's original expression
        metric_node = ctx.nodes.get(metric_name)
        if not metric_node:
            continue

        original_query = ctx.get_parsed_query(metric_node)
        order_by_cols = get_window_order_by_columns(original_query)

        for order_by_col in order_by_cols:
            # Extract the dimension node from the full column reference
            # e.g., "common.dimensions.time.date.week_code" -> "common.dimensions.time.date"
            dim_node = ".".join(order_by_col.rsplit(".", 1)[:-1]) if "." in order_by_col else ""

            if not dim_node:
                continue

            # Get the alias for this ORDER BY column
            order_by_alias = dim_ref_to_alias.get(order_by_col, "")
            if not order_by_alias:
                # Try to find it by the column name part only
                col_name = order_by_col.rsplit(".", 1)[-1]
                for ref, alias in dim_ref_to_alias.items():
                    if ref.endswith("." + col_name):
                        order_by_alias = alias
                        dim_node = ".".join(ref.rsplit(".", 1)[:-1])
                        break

            if not order_by_alias:
                continue

            if dim_node not in grain_levels:
                # Determine which dimensions to GROUP BY at this grain level
                # Exclude finer-grained dimensions from the same dimension node
                excluded_aliases = node_to_aliases.get(dim_node, set())
                group_by_dims = [
                    alias for _, alias in dim_info if alias not in excluded_aliases
                ]
                # Add the ORDER BY dimension (it's the grain we're aggregating to)
                if order_by_alias not in group_by_dims:
                    group_by_dims.append(order_by_alias)

                # Join dimensions are the GROUP BY dimensions
                join_dims = group_by_dims.copy()

                grain_levels[dim_node] = GrainLevelInfo(
                    dimension_node=dim_node,
                    order_by_alias=order_by_alias,
                    cte_alias=f"{order_by_alias}_metrics",
                    group_by_dims=group_by_dims,
                    join_dims=join_dims,
                    window_metrics=set(),
                )

            grain_levels[dim_node].window_metrics.add(metric_name)

    return grain_levels


def build_grain_level_cte(
    grain_info: GrainLevelInfo,
    base_metrics_alias: str,
    all_grain_group_metrics: set[str],
    metric_expr_asts: dict[str, MetricExprInfo],
) -> ast.Query:
    """
    Build a CTE that aggregates to a specific grain level for window functions.

    This CTE aggregates from the base_metrics CTE to a coarser grain
    (e.g., weekly or monthly) so window functions can operate correctly.

    Args:
        grain_info: Information about the grain level
        base_metrics_alias: Alias of the base_metrics CTE
        all_grain_group_metrics: Set of base metric names
        metric_expr_asts: Metric expressions (already simple column refs to base_metrics)

    Returns:
        AST Query for the grain-level CTE
    """
    projection: list[Any] = []

    # Add dimension columns
    for dim_alias in grain_info.group_by_dims:
        col_ref = make_column_ref(dim_alias, base_metrics_alias)
        aliased = col_ref.set_alias(ast.Name(dim_alias))
        aliased.set_as(True)
        projection.append(aliased)

    # Add metric aggregations (SUM the pre-computed metrics from base_metrics)
    for metric_name in sorted(all_grain_group_metrics):
        if metric_name not in metric_expr_asts:
            continue
        info = metric_expr_asts[metric_name]
        # Aggregate the metric from base_metrics
        col_ref = make_column_ref(info.short_name, base_metrics_alias)
        sum_func = ast.Function(ast.Name("SUM"), args=[col_ref])
        aliased = sum_func.set_alias(ast.Name(info.short_name))
        aliased.set_as(True)
        projection.append(aliased)

    # Build FROM clause
    from_clause = ast.From(
        relations=[
            ast.Relation(primary=ast.Table(ast.Name(base_metrics_alias))),
        ],
    )

    # Build GROUP BY
    group_by = [
        make_column_ref(dim_alias, base_metrics_alias)
        for dim_alias in grain_info.group_by_dims
    ]

    return ast.Query(
        select=ast.Select(
            projection=projection,
            from_=from_clause,
            group_by=group_by,
        ),
    )


def build_grain_level_window_cte(
    grain_info: GrainLevelInfo,
    window_metrics: set[str],
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
    all_grain_group_metrics: set[str],
    alias_to_dimension_node: dict[str, str],
) -> ast.Query:
    """
    Build a CTE that applies window functions at a specific grain level.

    Args:
        grain_info: Information about the grain level
        window_metrics: Window metrics to compute at this grain
        ctx: Build context
        decomposed_metrics: Decomposed metric info
        all_grain_group_metrics: Base metric names
        alias_to_dimension_node: Mapping for PARTITION BY logic

    Returns:
        AST Query that selects from grain-level CTE and applies window functions
    """
    agg_cte_alias = grain_info.cte_alias + "_agg"
    projection: list[Any] = []

    # Add all dimension columns
    for dim_alias in grain_info.group_by_dims:
        col_ref = make_column_ref(dim_alias, agg_cte_alias)
        aliased = col_ref.set_alias(ast.Name(dim_alias))
        aliased.set_as(True)
        projection.append(aliased)

    # Add window function expressions
    # Build partition columns (exclude ORDER BY dimension and same-node dimensions)
    partition_cols = [
        dim for dim in grain_info.group_by_dims if dim != grain_info.order_by_alias
    ]

    for metric_name in sorted(window_metrics):
        decomposed = decomposed_metrics.get(metric_name)
        if not decomposed:
            continue

        # Get the original window expression
        metric_node = ctx.nodes.get(metric_name)
        if not metric_node:
            continue

        original_query = ctx.get_parsed_query(metric_node)
        expr_ast = deepcopy(original_query.select.projection[0])
        if isinstance(expr_ast, ast.Alias):
            expr_ast = expr_ast.child

        # Replace metric references with column refs to the agg CTE
        for col in expr_ast.find_all(ast.Column):
            col_full_name = col.identifier() if hasattr(col, 'identifier') else ""
            if col_full_name in ctx.nodes and ctx.nodes[col_full_name].type == NodeType.METRIC:
                # This is a metric reference - replace with CTE column
                short_name = get_short_name(col_full_name)
                col.name = ast.Name(short_name)
                col._table = ast.Table(ast.Name(agg_cte_alias))

        # Replace dimension references
        for col in expr_ast.find_all(ast.Column):
            col_full_name = col.identifier() if hasattr(col, 'identifier') else ""
            # Check if this is a dimension reference
            for dim_ref, dim_alias in [(dr, da) for dr, da in ctx.alias_registry._registry.items()]:
                if col_full_name == dim_ref or col_full_name.endswith("." + dim_alias):
                    col.name = ast.Name(dim_alias)
                    col._table = ast.Table(ast.Name(agg_cte_alias))
                    break

        # Inject PARTITION BY (already handles dimension node exclusion)
        inject_partition_by_into_windows(expr_ast, partition_cols, alias_to_dimension_node)

        short_name = get_short_name(metric_name)
        aliased = expr_ast.set_alias(ast.Name(short_name))
        aliased.set_as(True)
        projection.append(aliased)

    # Build FROM clause
    from_clause = ast.From(
        relations=[
            ast.Relation(primary=ast.Table(ast.Name(agg_cte_alias))),
        ],
    )

    return ast.Query(
        select=ast.Select(
            projection=projection,
            from_=from_clause,
        ),
    )


def build_from_clause_with_grain_joins(
    dim_col_aliases: list[str],
    cte_aliases: list[str],
    window_metrics_cte_alias: str | None,
    grain_groups: list[GrainGroupSQL],
    grain_levels: dict[str, GrainLevelInfo],
) -> tuple[ast.From, list[ast.Expression]]:
    """
    Build FROM clause with JOINs for grain-level window CTEs.

    When window metrics operate at different grains than the requested dimensions,
    we need to JOIN the grain-level window CTEs back to the base_metrics CTE.

    For example, if requesting daily grain with WoW metrics:
    - base_metrics is at daily grain
    - weekly_metrics has the WoW calculations at weekly grain
    - Final SELECT joins them: base_metrics LEFT JOIN weekly_metrics ON (join_dims)

    Args:
        dim_col_aliases: List of dimension column aliases
        cte_aliases: List of grain group CTE aliases
        window_metrics_cte_alias: Alias of base_metrics CTE (or None)
        grain_groups: Grain groups (for validation)
        grain_levels: Grain level info for window metrics

    Returns:
        Tuple of (from_clause, group_by)
    """
    # Validate: cross-fact metrics require shared dimensions
    if len(cte_aliases) > 1 and not dim_col_aliases:
        parent_names = [gg.parent_name for gg in grain_groups]
        raise DJInvalidInputException(
            f"Cross-fact metrics from different parent nodes ({', '.join(parent_names)}) "
            f"require at least one shared dimension to join on. ",
        )

    # If no window metrics, use the standard FROM clause
    if not window_metrics_cte_alias or not grain_levels:
        # Build FROM clause with FULL OUTER JOINs between grain group CTEs
        table_refs = {name: ast.Table(ast.Name(name)) for name in cte_aliases}
        from_clause = build_join_from_clause(cte_aliases, table_refs, dim_col_aliases)

        # Add GROUP BY on requested dimensions
        group_by: list[ast.Expression] = []
        if dim_col_aliases:
            group_by.extend(
                [make_column_ref(dim_col, cte_aliases[0]) for dim_col in dim_col_aliases],
            )
        return from_clause, group_by

    # Build FROM clause starting with base_metrics
    base_table = ast.Table(ast.Name(window_metrics_cte_alias))
    relations: list[ast.Relation] = [ast.Relation(primary=base_table)]

    # Add LEFT JOINs for each grain-level window CTE
    for dim_node, grain_info in grain_levels.items():
        window_cte_table = ast.Table(ast.Name(grain_info.cte_alias))

        # Build JOIN condition on the join dimensions
        join_conditions: list[ast.Expression] = []
        for dim_alias in grain_info.join_dims:
            left_col = make_column_ref(dim_alias, window_metrics_cte_alias)
            right_col = make_column_ref(dim_alias, grain_info.cte_alias)
            condition = ast.BinaryOp(
                left=left_col,
                right=right_col,
                op=ast.BinaryOpKind.Eq,
            )
            join_conditions.append(condition)

        # Combine conditions with AND
        if join_conditions:
            join_condition = join_conditions[0]
            for cond in join_conditions[1:]:
                join_condition = ast.BinaryOp(
                    left=join_condition,
                    right=cond,
                    op=ast.BinaryOpKind.And,
                )

            # Add the JOIN
            join = ast.Join(
                join_type=ast.JoinType.LeftOuter,
                right=window_cte_table,
                on=join_condition,
            )
            relations[0].extensions.append(join)

    from_clause = ast.From(relations=relations)

    # No GROUP BY for window function queries - the data is already at the right grain
    return from_clause, []


def generate_metrics_sql(
    ctx: BuildContext,
    measures_result: GeneratedMeasuresSQL,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
) -> GeneratedSQL:
    """
    Generate the final metrics SQL query.

    This combines grain groups from measures SQL and applies
    combiner expressions for each metric. Works for both single
    and multiple grain groups (FULL OUTER JOINs them together).

    Works entirely with AST objects - no string parsing needed.
    Returns a GeneratedSQL with the query as an AST.
    """
    grain_groups = measures_result.grain_groups
    dimensions = measures_result.requested_dimensions

    # Convert grain groups to the minimal set of CTEs
    all_cte_asts, cte_aliases = collect_and_build_ctes(grain_groups)

    # Build dimension info and projection
    dim_types = get_dimension_types(grain_groups)
    dim_info = parse_dimension_refs(ctx, dimensions)
    dimension_aliases = build_dimension_alias_map(dim_info)
    # Build mapping from alias to dimension node for window function PARTITION BY logic
    # This ensures we exclude all columns from the same dimension node as the ORDER BY
    alias_to_dimension_node = build_alias_to_dimension_node(dim_info)
    projection, columns_metadata = build_dimension_projection(
        dim_info,
        cte_aliases,
        dim_types,
    )

    # Process base metrics from grain groups
    base_metrics_result = process_base_metrics(
        grain_groups,
        cte_aliases,
        decomposed_metrics,
        dimension_aliases,
    )

    # Use cleaner names for the result fields
    all_grain_group_metrics = base_metrics_result.all_metrics
    metric_expr_asts = base_metrics_result.metric_exprs

    # Process derived metrics (not base metrics in any grain group)
    # Get unique dimension aliases for PARTITION BY injection
    # Use unique values to avoid duplicates when multiple refs map to same alias
    all_dim_aliases = list(dict.fromkeys(dimension_aliases.values()))

    # Identify derived metrics with window functions
    window_metrics = find_window_metrics(
        ctx,
        decomposed_metrics,
        all_grain_group_metrics,
    )

    # Find intermediate derived metrics (for nested derived metrics)
    # These are derived metrics referenced by window function metrics
    intermediate_derived_metrics = find_intermediate_derived_metrics(
        ctx,
        decomposed_metrics,
        window_metrics,
        all_grain_group_metrics,
    )

    # Build expressions for intermediate derived metrics
    intermediate_exprs: dict[str, ast.Expression] = {}
    if intermediate_derived_metrics:
        for metric_name in intermediate_derived_metrics:
            expr = build_intermediate_metric_expr(
                ctx,
                metric_name,
                metric_expr_asts,
            )
            if expr:
                intermediate_exprs[metric_name] = expr

    # If there are window function metrics, create an intermediate CTE
    # that pre-computes all base metrics as actual columns
    window_metrics_cte_alias: str | None = None
    grain_levels: dict[str, GrainLevelInfo] = {}
    grain_window_ctes: dict[str, str] = {}  # metric_name -> window CTE alias
    if window_metrics:
        window_metrics_cte_alias = "base_metrics"

        # Build and add the base_metrics CTE (including intermediate derived metrics)
        base_metrics_query = build_base_metrics_cte(
            dim_info,
            cte_aliases,
            all_grain_group_metrics,
            metric_expr_asts,
            intermediate_derived_metrics,
            intermediate_exprs,
        )
        base_metrics_query.to_cte(ast.Name(window_metrics_cte_alias), None)
        all_cte_asts.append(base_metrics_query)

        # Rewrite base metric expressions to be simple column references to base_metrics
        # The aggregation already happened in the base_metrics CTE, so the final SELECT
        # should just reference those columns, not re-aggregate
        for metric_name in all_grain_group_metrics:
            if metric_name in metric_expr_asts:
                info = metric_expr_asts[metric_name]
                # Replace aggregation expression with simple column reference
                simple_ref = make_column_ref(info.short_name, window_metrics_cte_alias)
                metric_expr_asts[metric_name] = MetricExprInfo(
                    expr_ast=simple_ref,
                    short_name=info.short_name,
                    cte_alias=window_metrics_cte_alias,
                )

        # Rebuild projection and columns_metadata for window metrics path
        projection, columns_metadata = rebuild_projection_for_window_metrics(
            dim_info,
            dim_types,
            window_metrics_cte_alias,
        )

        # Group window metrics by their ORDER BY grain and build grain-level CTEs
        grain_levels = group_window_metrics_by_grain(
            ctx,
            window_metrics,
            decomposed_metrics,
            alias_to_dimension_node,
            dim_info,
        )

        # Build CTEs for each grain level
        grain_window_ctes: dict[str, str] = {}  # metric_name -> window CTE alias
        for dim_node, grain_info in grain_levels.items():
            # Build aggregation CTE (aggregates base_metrics to this grain)
            agg_cte_alias = grain_info.cte_alias + "_agg"
            agg_cte = build_grain_level_cte(
                grain_info,
                window_metrics_cte_alias,
                all_grain_group_metrics,
                metric_expr_asts,
            )
            agg_cte.to_cte(ast.Name(agg_cte_alias), None)
            all_cte_asts.append(agg_cte)

            # Build window CTE (applies window functions at this grain)
            window_cte = build_grain_level_window_cte(
                grain_info,
                grain_info.window_metrics,
                ctx,
                decomposed_metrics,
                all_grain_group_metrics,
                alias_to_dimension_node,
            )
            window_cte.to_cte(ast.Name(grain_info.cte_alias), None)
            all_cte_asts.append(window_cte)

            # Track which window metrics come from which CTE
            for metric_name in grain_info.window_metrics:
                grain_window_ctes[metric_name] = grain_info.cte_alias

    # Build ColumnResolver for derived metrics
    # For window metrics, dimensions come from base_metrics CTE
    # For standard path, dimensions come from the first grain group CTE
    dim_cte_alias = (
        window_metrics_cte_alias if window_metrics_cte_alias else cte_aliases[0]
    )
    resolver = ColumnResolver.from_base_metrics(
        base_metrics_result,
        dimension_aliases,
        dim_cte_alias,
    )

    # Process derived metrics (not base metrics in any grain group)
    derived_exprs = process_derived_metrics(
        ctx,
        decomposed_metrics,
        all_grain_group_metrics,
        resolver,
        all_dim_aliases,
        window_metrics_cte_alias,
        intermediate_derived_metrics,
        alias_to_dimension_node,
    )

    # Merge derived metrics into the main metric_expr_asts dict
    metric_expr_asts.update(derived_exprs)

    # For window metrics processed at grain level, create simple column references
    # to the grain-level window CTEs (they've already computed the window functions)
    for metric_name, window_cte_alias in grain_window_ctes.items():
        short_name = get_short_name(metric_name)
        simple_ref = make_column_ref(short_name, window_cte_alias)
        metric_expr_asts[metric_name] = MetricExprInfo(
            expr_ast=simple_ref,
            short_name=short_name,
            cte_alias=window_cte_alias,
        )

    # Build metric projection in requested order
    metric_projection, metric_columns = build_metric_projection(ctx, metric_expr_asts)
    projection.extend(metric_projection)
    columns_metadata.extend(metric_columns)

    # Build FROM clause and GROUP BY
    dim_col_aliases = [col_alias for _, col_alias in dim_info]
    from_clause, group_by = build_from_clause_with_grain_joins(
        dim_col_aliases,
        cte_aliases,
        window_metrics_cte_alias,
        grain_groups,
        grain_levels,
    )

    # Build WHERE clause from filters
    # For metrics SQL, filters reference dimension columns which are now in the CTEs
    where_clause: Optional[ast.Expression] = None
    if ctx.filters:
        # Resolve filters using dimension aliases
        # Use base_metrics CTE for window function queries, otherwise first grain group CTE
        filter_cte = (
            window_metrics_cte_alias if window_metrics_cte_alias else cte_aliases[0]
        )
        where_clause = parse_and_resolve_filters(
            ctx.filters,
            dimension_aliases,
            cte_alias=filter_cte,
        )

    # Build the final SELECT
    select_ast = ast.Select(
        projection=projection,
        from_=from_clause,
        where=where_clause,
        group_by=group_by,
    )

    # Build the final Query with all CTEs
    final_query = ast.Query(select=select_ast, ctes=all_cte_asts)

    return GeneratedSQL(
        query=final_query,
        columns=columns_metadata,
        dialect=measures_result.dialect,
    )
