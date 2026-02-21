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
    extract_dim_info_from_grain_groups,
    get_column_full_name,
    has_window_function,
    inject_partition_by_into_windows,
    process_metric_combiner_expression,
    replace_component_refs_in_ast,
    replace_dimension_refs_in_ast,
    replace_metric_refs_in_ast,
)
from datajunction_server.construction.build_v3.filters import (
    parse_and_resolve_filters,
    parse_filter,
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
        # Build column reference(s) for this dimension
        col_refs: list[ast.Expression] = [
            make_column_ref(dim_col, alias) for alias in cte_aliases
        ]

        # Only use COALESCE when there are multiple CTEs (Trino requires >= 2 args)
        if len(col_refs) == 1:
            col_expr = col_refs[0]
        else:
            col_expr = ast.Function(ast.Name("COALESCE"), args=col_refs)

        aliased_col = col_expr.set_alias(ast.Name(dim_col))
        aliased_col.set_as(True)  # Include "AS" in output
        projection.append(aliased_col)

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
    metric_node = ctx.nodes.get(metric_name)
    if not metric_node:
        return None  # pragma: no cover

    # Get the original query for the intermediate metric
    original_query = ctx.get_parsed_query(metric_node)
    expr_ast = deepcopy(original_query.select.projection[0])

    # Unwrap if it's an alias
    if isinstance(expr_ast, ast.Alias):
        expr_ast = expr_ast.child  # pragma: no cover

    # Build a map of metric names to their expression ASTs
    # We need to inline the actual expressions, not just column names
    metric_exprs: dict[str, ast.Expression] = {
        name: deepcopy(info.expr_ast) for name, info in base_metric_exprs.items()
    }

    # Replace metric references with their full expressions
    # We must walk the AST and replace Column nodes with their expressions
    # If any metric reference can't be resolved, return None to defer building
    for col in list(expr_ast.find_all(ast.Column)):
        full_name = get_column_full_name(col)
        if full_name in metric_exprs:
            # Replace this column with the metric's expression
            replacement_expr = deepcopy(metric_exprs[full_name])
            if col.parent:  # pragma: no branch
                col.parent.replace(from_=col, to=replacement_expr)
        else:
            # Check if this is a metric reference that we haven't built yet
            node = ctx.nodes.get(full_name)  # pragma: no cover
            if node and node.type == NodeType.METRIC:  # pragma: no cover
                # This is a metric reference but it's not in metric_exprs yet
                # The dependency hasn't been built, so defer this metric
                return None  # pragma: no cover

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

    # Add dimension columns (COALESCE only when multiple CTEs for Trino compatibility)
    for _, dim_col in dim_info:
        col_refs: list[ast.Expression] = [
            make_column_ref(dim_col, alias) for alias in cte_aliases
        ]
        # Only use COALESCE when there are multiple CTEs (Trino requires >= 2 args)
        if len(col_refs) == 1:
            col_expr = col_refs[0]
        else:
            col_expr = ast.Function(ast.Name("COALESCE"), args=col_refs)
        aliased = col_expr.set_alias(ast.Name(dim_col))
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
    # Use positional references (1, 2, 3, ...) when there are multiple CTEs joined
    # This is necessary for FULL OUTER JOIN where one-sided refs may be NULL
    group_by: list[ast.Expression] = []
    if len(cte_aliases) > 1:
        # Use positional references for cross-fact JOINs
        for pos in range(1, len(dim_info) + 1):
            group_by.append(ast.Number(pos))
    else:
        # Single CTE - can use qualified column refs
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


# def build_from_clause(
#     dim_col_aliases: list[str],
#     cte_aliases: list[str],
#     window_metrics_cte_alias: Optional[str],
#     grain_groups: list[GrainGroupSQL],
# ) -> tuple[ast.From, list[ast.Expression]]:
#     """
#     Build FROM clause and GROUP BY for the final SELECT.

#     For window function metrics, references base_metrics CTE directly without GROUP BY.
#     For standard metrics, builds FULL OUTER JOINs between grain group CTEs with GROUP BY.

#     Args:
#         dim_col_aliases: List of dimension column aliases for joins
#         cte_aliases: List of grain group CTE aliases
#         window_metrics_cte_alias: Alias of base_metrics CTE (None if no window metrics)
#         grain_groups: Grain groups (for validation error message)

#     Returns:
#         Tuple of (from_clause, group_by)

#     Raises:
#         DJInvalidInputException: If cross-fact metrics have no shared dimensions
#     """
#     # Validate: cross-fact metrics require at least one shared dimension to join on
#     # Without shared dimensions, the join would be a CROSS JOIN which produces
#     # semantically meaningless results (dividing unrelated populations)
#     if (
#         len(cte_aliases) > 1 and not dim_col_aliases
#     ):  # pragma: no cover  # TODO: add coverage
#         parent_names = [gg.parent_name for gg in grain_groups]
#         raise DJInvalidInputException(
#             f"Cross-fact metrics from different parent nodes ({', '.join(parent_names)}) "
#             f"require at least one shared dimension to join on. ",
#         )

#     # For window function metrics, the final SELECT references the base_metrics CTE
#     # (which has pre-computed base metrics) without GROUP BY
#     if window_metrics_cte_alias:
#         from_clause = ast.From(
#             relations=[
#                 ast.Relation(
#                     primary=ast.Table(ast.Name(window_metrics_cte_alias)),
#                 ),
#             ],
#         )
#         # No GROUP BY for window function queries - they need all rows
#         return from_clause, []

#     # Build FROM clause with FULL OUTER JOINs between grain group CTEs
#     table_refs = {name: ast.Table(ast.Name(name)) for name in cte_aliases}
#     from_clause = build_join_from_clause(cte_aliases, table_refs, dim_col_aliases)

#     # Add GROUP BY on requested dimensions
#     # Metrics SQL re-aggregates components to produce final metric values:
#     # - If CTE is at requested grain: re-aggregation is a no-op (SUM of one = that one)
#     # - If CTE is at finer grain: re-aggregation does actual work
#     group_by: list[ast.Expression] = []
#     if dim_col_aliases:  # pragma: no branch
#         group_by.extend(
#             [make_column_ref(dim_col, cte_aliases[0]) for dim_col in dim_col_aliases],
#         )

#     return from_clause, group_by


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
        for name in intermediate_metric_names:  # pragma: no cover
            window_metric_refs[name] = (window_cte_alias, get_short_name(name))
    replace_metric_refs_in_ast(expr_ast, window_metric_refs)

    # Dimension refs also point to window CTE
    window_dim_refs: dict[str, tuple[str, str]] = {
        name: (window_cte_alias, ref.column_name)
        for name, ref in resolver.get_by_type(ColumnType.DIMENSION).items()
    }
    replace_dimension_refs_in_ast(expr_ast, window_dim_refs)

    # Inject PARTITION BY for window functions
    # Qualify with CTE alias to avoid ambiguity when there are JOINs
    inject_partition_by_into_windows(
        expr_ast,
        partition_columns,
        alias_to_dimension_node,
        partition_cte_alias=window_cte_alias,
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
    # Use the shared helper to ensure consistency with cube materialization
    return process_metric_combiner_expression(
        combiner_ast=decomposed.combiner_ast,
        dimension_refs=resolver.dimension_refs(),
        component_refs=resolver.component_refs(),
        metric_refs=resolver.metric_refs(),
        partition_dimensions=partition_columns,
        alias_to_dimension_node=alias_to_dimension_node,
    )


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


# def strip_role_suffix(ref: str) -> str:
#     """
#     Strip role suffix like [order], [filter] from a dimension reference.

#     For example:
#         "v3.date.week[order]" -> "v3.date.week"
#         "v3.date.month" -> "v3.date.month"

#     Role suffixes are DJ-specific syntax, not part of actual column names.
#     """
#     if "[" in ref:  # pragma: no cover
#         return ref.split("[")[0]
#     return ref


@dataclass
class GrainLevelInfo:
    """Information about a grain level for window function processing."""

    dimension_node: str  # The dimension node (e.g., "common.dimensions.time.date")
    order_by_alias: str  # The ORDER BY column alias (e.g., "week_code")
    order_by_ref: (
        str  # Full dimension ref (e.g., "common.dimensions.time.date.week_code")
    )
    cte_alias: str  # The CTE alias (e.g., "weekly_metrics")
    group_by_dims: list[str]  # Dimensions to GROUP BY at this grain
    join_dims: list[str]  # Dimensions to JOIN on back to base_metrics
    window_metrics: set[str]  # Window metrics that use this grain


def build_window_agg_cte_from_grain_group(
    window_grain_group: GrainGroupSQL,
    base_grain_group: GrainGroupSQL,
    source_cte_alias: str,
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
) -> ast.Query:
    """
    Build an aggregation CTE that computes base metrics from a grain group.

    This reaggregates from a base grain group (e.g., daily) to the coarser
    window metric grain (e.g., weekly). Handles both:
    - Base metrics: applies combiner expression with component references
    - Derived metrics: expands metric references to underlying component expressions

    Args:
        window_grain_group: A grain group marked as is_window_grain_group=True
        base_grain_group: The base grain group (source of component columns)
        source_cte_alias: Alias of the base grain group CTE to SELECT from
        ctx: Build context
        decomposed_metrics: Decomposed metric info

    Returns:
        AST Query that aggregates components into metrics
    """
    projection: list[Any] = []
    group_by: list[ast.Expression] = []

    # Collect dimension columns from the grain group (these are GROUP BY columns)
    dim_columns: list[str] = []
    for col in window_grain_group.columns:
        if col.semantic_type == "dimension":
            dim_columns.append(col.name)
            col_ref = make_column_ref(col.name, source_cte_alias)
            aliased = col_ref.set_alias(ast.Name(col.name))
            aliased.set_as(True)
            projection.append(aliased)
            group_by.append(make_column_ref(col.name, source_cte_alias))

    # Find the base metrics that the window metrics reference
    base_metrics_needed: set[str] = set()
    for window_metric_name in window_grain_group.window_metrics_served:
        parent_names = ctx.parent_map.get(window_metric_name, [])
        for parent_name in parent_names:
            parent_node = ctx.nodes.get(parent_name)
            if parent_node and parent_node.type == NodeType.METRIC:  # pragma: no branch
                base_metrics_needed.add(parent_name)

    def is_derived_metric(metric_name: str) -> bool:
        """Check if a metric is derived (has metric parents, no direct components)."""
        decomposed = decomposed_metrics.get(metric_name)
        if not decomposed or not isinstance(decomposed, DecomposedMetricInfo):
            return False  # pragma: no cover
        # Derived metrics have parent metrics but no direct components in grain groups
        parent_metrics = ctx.parent_map.get(metric_name, [])
        has_metric_parents = any(
            ctx.nodes.get(p) and ctx.nodes.get(p).type == NodeType.METRIC  # type: ignore
            for p in parent_metrics
        )
        return has_metric_parents and not decomposed.components

    def get_metric_aggregation_expr(
        metric_name: str,
        visited: set[str],
    ) -> Optional[ast.Expression]:
        """
        Build aggregation expression for a metric that can be computed from
        component columns in the source CTE.

        For base metrics: returns combiner expression with component refs replaced
        For derived metrics: recursively expands metric references
        """
        if metric_name in visited:
            return None  # pragma: no cover
        visited.add(metric_name)

        decomposed = decomposed_metrics.get(metric_name)
        if not decomposed:
            return None  # pragma: no cover

        combiner_ast = deepcopy(decomposed.combiner_ast)
        if isinstance(combiner_ast, ast.Alias):
            combiner_ast = combiner_ast.child  # pragma: no cover

        if is_derived_metric(metric_name):  # pragma: no cover
            # Derived metric: need to expand metric references
            # Find column nodes that reference other metrics
            for col_node in list(combiner_ast.find_all(ast.Column)):
                col_name = (
                    col_node.identifier() if hasattr(col_node, "identifier") else ""
                )
                # Check if this column references a metric
                short_col_name = col_name.split(".")[-1] if col_name else ""

                # Try to find matching metric
                parent_metric_name = None
                for parent_name in ctx.parent_map.get(metric_name, []):
                    parent_short = get_short_name(parent_name)
                    if parent_short == short_col_name or col_name.endswith(parent_name):
                        parent_metric_name = parent_name
                        break

                if parent_metric_name:
                    # Replace with parent metric's aggregation expression
                    parent_expr = get_metric_aggregation_expr(
                        parent_metric_name,
                        visited.copy(),
                    )
                    if parent_expr:
                        # Swap the column node with the parent's expression
                        col_node.swap(parent_expr)
        else:
            # Base metric: replace component references with CTE column refs
            # Use base_grain_group's component_aliases since that's the source CTE
            for col_node in combiner_ast.find_all(ast.Column):
                col_full_name = (
                    col_node.identifier() if hasattr(col_node, "identifier") else ""
                )
                # Check if this matches a component alias
                for (
                    comp_name,
                    comp_alias,
                ) in base_grain_group.component_aliases.items():  # pragma: no branch
                    if col_full_name == comp_name or col_full_name.endswith(comp_alias):
                        col_node.name = ast.Name(comp_alias)
                        col_node._table = ast.Table(ast.Name(source_cte_alias))
                        break

        return combiner_ast

    # Build aggregation expressions for each base metric
    for base_metric_name in sorted(base_metrics_needed):
        decomposed = decomposed_metrics.get(base_metric_name)
        if not decomposed:
            continue  # pragma: no cover

        # Get aggregation expression (handles both base and derived metrics)
        combiner_ast = get_metric_aggregation_expr(base_metric_name, set())
        if not combiner_ast:
            continue  # pragma: no cover

        short_name = get_short_name(base_metric_name)
        aliased = combiner_ast.set_alias(ast.Name(short_name))  # type: ignore
        aliased.set_as(True)
        projection.append(aliased)

    # Build FROM clause
    from_clause = ast.From(
        relations=[
            ast.Relation(primary=ast.Table(ast.Name(source_cte_alias))),
        ],
    )

    return ast.Query(
        select=ast.Select(
            projection=projection,
            from_=from_clause,
            group_by=group_by if group_by else [],
        ),
    )


def build_window_agg_cte_from_base_metrics(
    window_grain_group: GrainGroupSQL,
    base_metrics_cte_alias: str,
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
) -> ast.Query:
    """
    Build an aggregation CTE from the base_metrics CTE for cross-fact window metrics.

    For cross-fact window metrics (metrics that reference base metrics from multiple
    parent facts), we need to reaggregate from base_metrics (which already has the
    FULL OUTER JOIN done) rather than from individual grain group CTEs.

    The key difference from build_window_agg_cte_from_grain_group:
    - Source is base_metrics CTE, not a specific grain group CTE
    - Metrics are referenced by their column names (e.g., total_thumb_ups),
      not by component aliases
    - Dimension columns come from base_metrics directly

    Args:
        window_grain_group: Window grain group with metadata (dimensions, metrics served)
        base_metrics_cte_alias: Alias of the base_metrics CTE (typically "base_metrics")
        ctx: Build context
        decomposed_metrics: Decomposed metric info

    Returns:
        AST Query that reaggregates from base_metrics to the window grain
    """
    projection: list[Any] = []
    group_by: list[ast.Expression] = []

    # Collect dimension columns from the grain group (these are GROUP BY columns)
    # Use base_metrics as the source
    for col in window_grain_group.columns:
        if col.semantic_type == "dimension":  # pragma: no branch
            col_ref = make_column_ref(col.name, base_metrics_cte_alias)
            aliased = col_ref.set_alias(ast.Name(col.name))
            aliased.set_as(True)
            projection.append(aliased)
            group_by.append(make_column_ref(col.name, base_metrics_cte_alias))

    # Find the base metrics that the window metrics reference
    base_metrics_needed: set[str] = set()
    for window_metric_name in window_grain_group.window_metrics_served:
        parent_names = ctx.parent_map.get(window_metric_name, [])
        for parent_name in parent_names:
            parent_node = ctx.nodes.get(parent_name)
            if parent_node and parent_node.type == NodeType.METRIC:  # pragma: no branch
                base_metrics_needed.add(parent_name)

    # Build reaggregation expressions for each base metric
    # Since we're selecting from base_metrics, we reference the metric columns directly
    for base_metric_name in sorted(base_metrics_needed):
        decomposed = decomposed_metrics.get(base_metric_name)
        if not decomposed:
            continue  # pragma: no cover

        short_name = get_short_name(base_metric_name)

        # For non-additive metrics (COUNT DISTINCT), we need COUNT(DISTINCT grain_col)
        # For additive metrics, we can SUM/AVG the pre-computed column
        if decomposed.aggregability == Aggregability.LIMITED:
            # LIMITED: needs COUNT DISTINCT at this grain
            # Find the grain column for COUNT DISTINCT
            if decomposed.components and decomposed.components[0].rule.level:
                grain_col = decomposed.components[0].rule.level[0]
                # The grain column is in base_metrics as a dimension column
                # Actually, for COUNT DISTINCT, the raw grain column should be in
                # the base grain group, not base_metrics. We need to reference
                # the original grain column from base_metrics.
                # For now, use the metric column - this works because base_metrics
                # computes COUNT DISTINCT at the fine grain, and we re-compute at coarser grain
                col_ref = make_column_ref(grain_col, base_metrics_cte_alias)
                agg_expr = ast.Function(
                    ast.Name("COUNT"),
                    args=[col_ref],
                    quantifier=ast.SetQuantifier.Distinct,
                )
            else:  # pragma: no cover
                # Fallback: SUM the metric column
                col_ref = make_column_ref(short_name, base_metrics_cte_alias)
                agg_expr = ast.Function(ast.Name("SUM"), args=[col_ref])
        elif decomposed.aggregability == Aggregability.FULL:  # pragma: no cover
            # FULL: additive metric, use SUM
            col_ref = make_column_ref(short_name, base_metrics_cte_alias)
            agg_expr = ast.Function(ast.Name("SUM"), args=[col_ref])
        elif decomposed.aggregability == Aggregability.NONE:  # pragma: no cover
            # NONE: non-additive (like AVG), need to recompute
            # For AVG, we need the raw sum and count, but those are in components
            # For now, just use the column directly (window function will handle it)
            col_ref = make_column_ref(short_name, base_metrics_cte_alias)
            agg_expr = col_ref  # type: ignore
        else:  # pragma: no cover
            # Default: SUM
            col_ref = make_column_ref(short_name, base_metrics_cte_alias)
            agg_expr = ast.Function(ast.Name("SUM"), args=[col_ref])

        aliased = agg_expr.set_alias(ast.Name(short_name))  # type: ignore
        aliased.set_as(True)
        projection.append(aliased)

    # Build FROM clause - just base_metrics
    from_clause = ast.From(
        relations=[
            ast.Relation(primary=ast.Table(ast.Name(base_metrics_cte_alias))),
        ],
    )

    return ast.Query(
        select=ast.Select(
            projection=projection,
            from_=from_clause,
            group_by=group_by if group_by else [],
        ),
    )


def build_window_cte_from_grain_group(
    window_grain_group: GrainGroupSQL,
    source_cte_alias: str,
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
    alias_to_dimension_node: dict[str, str],
) -> ast.Query:
    """
    Build a CTE that applies window functions from a window grain group.

    The source CTE should have aggregated metrics (from build_window_agg_cte_from_grain_group).
    This function creates a CTE that:
    1. SELECTs all dimension columns from the source
    2. Applies window function expressions for each window metric
    3. No GROUP BY (data is already at correct grain)

    Args:
        window_grain_group: A grain group marked as is_window_grain_group=True
        source_cte_alias: Alias of the aggregation CTE to SELECT from
        ctx: Build context
        decomposed_metrics: Decomposed metric info
        alias_to_dimension_node: Mapping for PARTITION BY logic

    Returns:
        AST Query that applies window functions
    """
    projection: list[Any] = []

    # Collect dimension columns from the grain group
    # The source CTE (agg CTE) has the same dimension columns
    dim_columns: list[str] = []
    for col in window_grain_group.columns:
        if col.semantic_type == "dimension":
            dim_columns.append(col.name)
            col_ref = make_column_ref(col.name, source_cte_alias)
            aliased = col_ref.set_alias(ast.Name(col.name))
            aliased.set_as(True)
            projection.append(aliased)

    # Note: We don't pass through base metric columns here since the window
    # expressions will reference them directly. The agg CTE has the aggregated
    # metrics (order_count, total_revenue) which window functions will use.

    # Build partition columns for PARTITION BY injection
    # Exclude the ORDER BY dimension from PARTITION BY
    order_by_dim = window_grain_group.window_order_by_dim
    order_by_alias = None
    if order_by_dim:  # pragma: no branch
        # Extract just the column alias from the full dimension ref
        order_by_alias = get_short_name(order_by_dim.split("[")[0])  # Strip role suffix

    # Get the dimension node for the ORDER BY alias (used for filtering)
    order_by_dim_node = (
        alias_to_dimension_node.get(order_by_alias) if order_by_alias else None
    )
    partition_cols = [
        dim
        for dim in dim_columns
        if dim != order_by_alias
        and alias_to_dimension_node.get(dim) != order_by_dim_node
    ]

    # Apply window function expressions for each window metric
    for window_metric_name in window_grain_group.window_metrics_served:
        decomposed = decomposed_metrics.get(window_metric_name)
        if not decomposed:
            continue  # pragma: no cover

        metric_node = ctx.nodes.get(window_metric_name)
        if not metric_node:
            continue  # pragma: no cover

        # Get the original window expression from the metric definition
        original_query = ctx.get_parsed_query(metric_node)
        expr_ast = deepcopy(original_query.select.projection[0])
        if isinstance(expr_ast, ast.Alias):
            expr_ast = expr_ast.child  # pragma: no cover

        # Replace metric references with column refs to the source CTE
        for col_node in expr_ast.find_all(ast.Column):
            col_full_name = (
                col_node.identifier() if hasattr(col_node, "identifier") else ""
            )
            if (
                col_full_name in ctx.nodes
                and ctx.nodes[col_full_name].type == NodeType.METRIC
            ):
                short_name = get_short_name(col_full_name)
                col_node.name = ast.Name(short_name)
                col_node._table = ast.Table(ast.Name(source_cte_alias))

        # Replace dimension references
        for col_node in expr_ast.find_all(ast.Column):
            col_full_name = (
                col_node.identifier() if hasattr(col_node, "identifier") else ""
            )
            for gg_col in window_grain_group.columns:
                if gg_col.semantic_type == "dimension":
                    if col_full_name == gg_col.semantic_name or col_full_name.endswith(
                        "." + gg_col.name,
                    ):
                        col_node.name = ast.Name(gg_col.name)
                        col_node._table = ast.Table(ast.Name(source_cte_alias))
                        break

        # Unwrap Subscript expressions in ORDER BY clauses (role suffix handling)
        for func in expr_ast.find_all(ast.Function):
            if func.over and func.over.order_by:
                for sort_item in func.over.order_by:
                    if isinstance(sort_item.expr, ast.Subscript):
                        sort_item.expr = sort_item.expr.expr

        # Inject PARTITION BY
        # Qualify with source CTE alias to avoid ambiguity
        inject_partition_by_into_windows(
            expr_ast,
            partition_cols,
            alias_to_dimension_node,
            partition_cte_alias=source_cte_alias,
        )

        short_name = get_short_name(window_metric_name)
        aliased = expr_ast.set_alias(ast.Name(short_name))  # type: ignore
        aliased.set_as(True)
        projection.append(aliased)

    # Build FROM clause
    from_clause = ast.From(
        relations=[
            ast.Relation(primary=ast.Table(ast.Name(source_cte_alias))),
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

    # If no window metrics, use the standard FROM clause with grain group CTEs
    if not window_metrics_cte_alias:
        # Build FROM clause with FULL OUTER JOINs between grain group CTEs
        table_refs = {name: ast.Table(ast.Name(name)) for name in cte_aliases}
        from_clause = build_join_from_clause(cte_aliases, table_refs, dim_col_aliases)

        # Add GROUP BY on requested dimensions
        group_by: list[ast.Expression] = []
        if dim_col_aliases:  # pragma: no branch
            group_by.extend(
                [
                    make_column_ref(dim_col, cte_aliases[0])
                    for dim_col in dim_col_aliases
                ],
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
        if join_conditions:  # pragma: no branch
            join_condition = join_conditions[0]
            for cond in join_conditions[1:]:
                join_condition = ast.BinaryOp(
                    left=join_condition,
                    right=cond,
                    op=ast.BinaryOpKind.And,
                )

            # Add the JOIN
            join = ast.Join(
                join_type="LEFT OUTER",
                right=window_cte_table,
                criteria=ast.JoinCriteria(on=join_condition),
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

    The takes grain groups (pre-aggregated data at specific grains) and
    combines them with metric combiner expressions to produce final SQL.

    Works entirely with AST objects - no string parsing needed.
    Returns a GeneratedSQL with the query as an AST.
    """
    grain_groups = measures_result.grain_groups
    dimensions = measures_result.requested_dimensions

    # Split grain groups into base (user-requested grain) vs window (coarser grains)
    # Window grain groups were created by build_window_metric_grain_groups() in measures phase
    base_grain_groups = [gg for gg in grain_groups if not gg.is_window_grain_group]
    window_grain_groups = [gg for gg in grain_groups if gg.is_window_grain_group]

    # Convert base grain groups to CTEs (window grain groups handled separately)
    all_cte_asts, cte_aliases = collect_and_build_ctes(base_grain_groups)

    # Build dimension info and projection
    # Filter out filter-only dimensions (they're needed for WHERE but not output)
    output_dimensions = [d for d in dimensions if d not in ctx.filter_dimensions]
    dim_types = get_dimension_types(grain_groups)
    dim_info = parse_dimension_refs(ctx, output_dimensions)
    dimension_aliases = build_dimension_alias_map(dim_info)
    # Build mapping from alias to dimension node for window function PARTITION BY logic
    # Use ALL dimensions from grain groups (not just user-requested dim_info) to ensure
    # we correctly group columns like (date_id, week, month) under the same dimension node.
    # This is critical for period-over-period metrics where e.g., WoW ordering by week
    # needs to exclude other columns from v3.date (like month, date_id) from PARTITION BY.
    all_grain_group_dim_info = extract_dim_info_from_grain_groups(grain_groups)
    alias_to_dimension_node = build_alias_to_dimension_node(all_grain_group_dim_info)
    projection, columns_metadata = build_dimension_projection(
        dim_info,
        cte_aliases,
        dim_types,
    )

    # Process base metrics from base grain groups only
    # Window grain groups are handled separately after the base_metrics CTE
    base_metrics_result = process_base_metrics(
        base_grain_groups,
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

    # Track window metrics and their CTEs
    window_metrics_cte_alias: str | None = None
    grain_window_ctes: dict[str, str] = {}  # metric_name -> window CTE alias
    all_derived_for_base_metrics: set[str] = set()
    grain_levels: dict[str, GrainLevelInfo] = {}  # For FROM clause building

    # Collect ALL window metrics - both from grain groups AND reaggregation metrics AND same-grain metrics
    # - Window grain groups: created for non-subset cases
    # - Same-grain metrics: when dimensions exactly match, tracked in window_metric_grains
    # - Aggregate window metrics: metrics with SUM/AVG/etc OVER (like trailing metrics)
    window_metrics: set[str] = set()
    for wgg in window_grain_groups:
        window_metrics.update(wgg.window_metrics_served)
    # Also include same-grain window metrics (exact match case)
    window_metrics.update(measures_result.window_metric_grains.keys())
    # Also include aggregate window metrics (like trailing metrics with SUM OVER ORDER BY)
    # These aren't LAG/LEAD so they weren't detected in measures phase, but they DO need
    # the base_metrics CTE to resolve their metric references
    for metric_name in ctx.metrics:
        if metric_name in all_grain_group_metrics:
            continue  # Base metric
        if metric_name in window_metrics:
            continue  # Already detected
        decomposed = decomposed_metrics.get(metric_name)
        if decomposed and has_window_function(decomposed.combiner_ast):
            window_metrics.add(metric_name)

    # If there are ANY window metrics, build the base_metrics CTE
    # (either for joining with window grain groups, or for same-grain window functions)
    if window_metrics:
        window_metrics_cte_alias = "base_metrics"

        # Find non-window derived metrics to pre-compute in base_metrics CTE
        for metric_name in ctx.metrics:
            if metric_name in all_grain_group_metrics:
                continue  # Base metric
            if metric_name in window_metrics:
                continue  # Window metric (handled separately)
            all_derived_for_base_metrics.add(metric_name)  # pragma: no cover

        # Also include parent derived metrics that window metrics depend on
        # For example, if wow_aov_change depends on avg_order_value, we need to
        # compute avg_order_value in base_metrics before applying the window function
        #
        # IMPORTANT: We must recursively collect ALL derived metric dependencies.
        # For example, if efficiency_ratio = avg_order_value / pages_per_session,
        # we need to collect avg_order_value and pages_per_session too.
        def collect_derived_dependencies(metric_name: str, visited: set[str]) -> None:
            """Recursively collect all derived metric dependencies."""
            if metric_name in visited:
                return  # pragma: no cover
            visited.add(metric_name)

            parent_names = ctx.parent_map.get(metric_name, [])
            for parent_name in parent_names:
                # Skip if it's a base metric (already in grain groups)
                if parent_name in all_grain_group_metrics:
                    continue
                # Skip if it's another window metric
                if parent_name in window_metrics:
                    continue  # pragma: no cover
                # Check if it's a derived metric
                parent_node = ctx.nodes.get(parent_name)
                if (
                    parent_node and parent_node.type == NodeType.METRIC
                ):  # pragma: no branch
                    all_derived_for_base_metrics.add(parent_name)
                    # Recursively collect this metric's dependencies
                    collect_derived_dependencies(parent_name, visited)

        for window_metric_name in window_metrics:
            collect_derived_dependencies(window_metric_name, set())

        # Build expressions for derived metrics in dependency order
        # We need to process metrics that depend only on base metrics first,
        # then metrics that depend on those, etc.
        all_derived_exprs: dict[str, ast.Expression] = {}

        # Create a combined dict of base metric expressions + already-built derived exprs
        # This gets updated as we build each level
        available_exprs: dict[str, MetricExprInfo] = dict(metric_expr_asts)

        # Keep building until all derived metrics are processed
        remaining = set(all_derived_for_base_metrics)
        max_iterations = len(remaining) + 1  # Prevent infinite loops
        iteration = 0

        while remaining and iteration < max_iterations:
            iteration += 1
            built_this_round: set[str] = set()

            for metric_name in list(remaining):
                expr = build_intermediate_metric_expr(ctx, metric_name, available_exprs)
                if expr:  # pragma: no branch
                    all_derived_exprs[metric_name] = expr
                    # Add to available_exprs for next level of derived metrics
                    short_name = get_short_name(metric_name)
                    available_exprs[metric_name] = MetricExprInfo(
                        expr_ast=expr,
                        short_name=short_name,
                        cte_alias="",  # Not used for intermediate building
                    )
                    built_this_round.add(metric_name)

            remaining -= built_this_round
            if not built_this_round:
                # No progress made - remaining metrics have unresolvable dependencies
                break  # pragma: no cover

        # Build and add the base_metrics CTE
        base_metrics_query = build_base_metrics_cte(
            dim_info,
            cte_aliases,
            all_grain_group_metrics,
            metric_expr_asts,
            all_derived_for_base_metrics,
            all_derived_exprs,
        )
        base_metrics_query.to_cte(ast.Name(window_metrics_cte_alias), None)
        all_cte_asts.append(base_metrics_query)

        # Rewrite base metric expressions to reference base_metrics CTE
        for metric_name in all_grain_group_metrics:
            if metric_name in metric_expr_asts:  # pragma: no branch
                info = metric_expr_asts[metric_name]
                simple_ref = make_column_ref(info.short_name, window_metrics_cte_alias)
                metric_expr_asts[metric_name] = MetricExprInfo(
                    expr_ast=simple_ref,
                    short_name=info.short_name,
                    cte_alias=window_metrics_cte_alias,
                )

        # Rewrite non-window derived metrics to reference base_metrics CTE
        for metric_name in all_derived_for_base_metrics:
            short_name = get_short_name(metric_name)
            simple_ref = make_column_ref(short_name, window_metrics_cte_alias)
            metric_expr_asts[metric_name] = MetricExprInfo(
                expr_ast=simple_ref,
                short_name=short_name,
                cte_alias=window_metrics_cte_alias,
            )

        # Rebuild projection for window metrics path
        projection, columns_metadata = rebuild_projection_for_window_metrics(
            dim_info,
            dim_types,
            window_metrics_cte_alias,
        )

        # Process each window grain group from measures phase
        # Window grain groups define the coarser grain for LAG/LEAD metrics (e.g., weekly for WoW)
        # We REAGGREGATE from the base grain group CTE, not from source tables
        # This is more efficient and enables pre-agg matching at the base grain
        for idx, wgg in enumerate(window_grain_groups):
            # Get the ORDER BY dimension for naming (e.g., "week" from "v3.date.week")
            order_by_dim = wgg.window_order_by_dim or ""
            order_by_col = (
                order_by_dim.rsplit(".", 1)[-1].split("[")[0]
                if order_by_dim
                else f"window_{idx}"
            )
            parent_short_name = get_short_name(wgg.parent_name)

            # Use the is_cross_fact_window flag set during measures phase
            # Cross-fact window groups need base_metrics CTE as source
            is_cross_fact = wgg.is_cross_fact_window

            # Find the base grain group CTE alias for this window grain group
            # For single-fact: use the matching base grain group CTE
            # For cross-fact: use base_metrics CTE
            base_grain_group: Optional[GrainGroupSQL] = None
            if is_cross_fact:
                # Cross-fact: use base_metrics CTE (already has FULL OUTER JOIN)
                source_cte_alias = window_metrics_cte_alias  # pragma: no cover
            else:
                # Single-fact: find the matching base grain group CTE
                source_cte_alias = None
                for i, gg in enumerate(base_grain_groups):
                    if gg.parent_name == wgg.parent_name:  # pragma: no branch
                        source_cte_alias = cte_aliases[i]
                        base_grain_group = gg
                        break
                if not source_cte_alias:  # pragma: no cover
                    # Fallback to base_metrics if parent not found
                    source_cte_alias = window_metrics_cte_alias
                    is_cross_fact = True  # Treat as cross-fact for building

            # Step 1: Build aggregation CTE that reaggregates to the coarser window grain
            agg_cte_alias = f"{parent_short_name}_{order_by_col}_agg"
            # source_cte_alias is guaranteed to be set by the branches above
            assert source_cte_alias is not None
            if is_cross_fact:
                # Cross-fact: use base_metrics as source, reference metric columns
                agg_cte = build_window_agg_cte_from_base_metrics(  # pragma: no cover
                    wgg,
                    source_cte_alias,  # base_metrics CTE
                    ctx,
                    decomposed_metrics,
                )
            else:
                # Single-fact: use base grain group as source, reference components
                assert base_grain_group is not None  # Guaranteed by loop above
                agg_cte = build_window_agg_cte_from_grain_group(
                    wgg,
                    base_grain_group,
                    source_cte_alias,  # Base grain group CTE
                    ctx,
                    decomposed_metrics,
                )
            agg_cte.to_cte(ast.Name(agg_cte_alias), None)
            all_cte_asts.append(agg_cte)

            # Step 2: Build window CTE that applies LAG/LEAD functions
            # This reads from the agg CTE (which has aggregated metrics at coarser grain)
            window_cte_alias = f"{parent_short_name}_{order_by_col}"
            window_cte = build_window_cte_from_grain_group(
                wgg,
                agg_cte_alias,  # Source is the agg CTE
                ctx,
                decomposed_metrics,
                alias_to_dimension_node,
            )
            window_cte.to_cte(ast.Name(window_cte_alias), None)
            all_cte_asts.append(window_cte)

            # Track which window metrics come from this CTE
            for metric_name in wgg.window_metrics_served:
                grain_window_ctes[metric_name] = window_cte_alias

            # Extract dimension column names from the window grain group for joining
            wgg_dim_aliases = [
                col.name for col in wgg.columns if col.semantic_type == "dimension"
            ]

            # Create GrainLevelInfo for FROM clause building
            grain_levels[f"window_{idx}"] = GrainLevelInfo(
                dimension_node=order_by_dim,
                order_by_alias=order_by_col,
                order_by_ref=order_by_dim,
                cte_alias=window_cte_alias,
                group_by_dims=wgg_dim_aliases,
                join_dims=wgg_dim_aliases,  # Join on all dimensions in the grain group
                window_metrics=set(wgg.window_metrics_served),
            )

    # Build ColumnResolver for derived metrics
    dim_cte_alias = (
        window_metrics_cte_alias if window_metrics_cte_alias else cte_aliases[0]
    )
    resolver = ColumnResolver.from_base_metrics(
        base_metrics_result,
        dimension_aliases,
        dim_cte_alias,
    )

    # Process remaining derived metrics (not base metrics or window metrics)
    metrics_to_skip = all_grain_group_metrics | all_derived_for_base_metrics
    derived_exprs = process_derived_metrics(
        ctx,
        decomposed_metrics,
        metrics_to_skip,
        resolver,
        all_dim_aliases,
        window_metrics_cte_alias,
        set(),  # No intermediate derived metrics in new architecture
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
        base_grain_groups,  # Only base grain groups, window handled via grain_levels
        grain_levels,
    )

    # Build WHERE clause from filters
    # Skip filters that reference filter-only dimensions (not available in final SELECT)
    where_clause: Optional[ast.Expression] = None
    if ctx.filters:
        # Use base_metrics CTE for window function queries, otherwise first grain group CTE
        filter_cte = (
            window_metrics_cte_alias if window_metrics_cte_alias else cte_aliases[0]
        )

        # Filter out filters that reference filter-only dimensions
        # Those filters are already applied in the grain group CTEs
        applicable_filters = []
        for f in ctx.filters:
            filter_ast = parse_filter(f)
            # Check if any column ref in this filter is a filter-only dimension
            refs_filter_only = False
            for col in filter_ast.find_all(ast.Column):
                full_name = get_column_full_name(col)
                if full_name and full_name in ctx.filter_dimensions:
                    refs_filter_only = True
                    break
            if not refs_filter_only:
                applicable_filters.append(f)

        if applicable_filters:
            where_clause = parse_and_resolve_filters(
                applicable_filters,
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

    # Aggregate scan estimates from all grain groups
    scan_estimate = None
    grain_groups_with_scans = [
        gg for gg in measures_result.grain_groups if gg.scan_estimate is not None
    ]
    if grain_groups_with_scans:
        from datajunction_server.models.sql import ScanEstimate, SourceScanInfo

        # Aggregate by source name to avoid double-counting if same source appears in multiple grain groups
        sources_by_name: dict[str, SourceScanInfo] = {}
        for gg in grain_groups_with_scans:
            if gg.scan_estimate:  # pragma: no branch
                for source in gg.scan_estimate.sources:
                    if source.source_name not in sources_by_name:
                        sources_by_name[source.source_name] = source

        # Sum total_bytes, skipping sources with None (no size data)
        total_bytes = sum(
            s.total_bytes for s in sources_by_name.values() if s.total_bytes is not None
        )
        # Set to None if no sources have size data
        total_bytes_result = total_bytes if total_bytes > 0 else None

        scan_estimate = ScanEstimate(
            total_bytes=total_bytes_result,
            sources=list(sources_by_name.values()),
        )

    return GeneratedSQL(
        query=final_query,
        columns=columns_metadata,
        dialect=measures_result.dialect,
        scan_estimate=scan_estimate,
    )
