"""
Metrics SQL Generation

This module handles generating the final metrics SQL query,
combining grain groups and applying combiner expressions.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from functools import reduce
from typing import Any, Optional

from datajunction_server.construction.build_v3.cte import (
    inject_partition_by_into_windows,
    replace_component_refs_in_ast,
    replace_dimension_refs_in_ast,
    replace_metric_refs_in_ast,
)
from datajunction_server.construction.build_v3.filters import (
    parse_and_resolve_filters,
)
from datajunction_server.construction.build_v3.utils import (
    get_short_name,
    make_column_ref,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    DecomposedMetricInfo,
    GeneratedMeasuresSQL,
    GeneratedSQL,
    GrainGroupSQL,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import Aggregability
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.utils import SEPARATOR

logger = logging.getLogger(__name__)


def compute_metric_layers(
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
) -> list[list[str]]:
    """
    Compute the order in which metrics should be evaluated.

    Returns a list of layers, where each layer contains metric names
    that can be computed in parallel (no dependencies on each other).

    Layer 0: Base metrics (no metric dependencies)
    Layer 1+: Derived metrics (depend on metrics in previous layers)
    """
    # Build dependency graph
    # A metric depends on another if its parent node is that metric
    dependencies: dict[str, set[str]] = {name: set() for name in decomposed_metrics}

    for metric_name in decomposed_metrics:
        # Use parent_map from context instead of accessing lazy-loaded relationships
        parent_names = ctx.parent_map.get(metric_name, [])
        for parent_name in parent_names:
            parent_node = ctx.nodes.get(parent_name)
            if (
                parent_node
                and parent_node.type == NodeType.METRIC
                and parent_name in decomposed_metrics
            ):
                dependencies[metric_name].add(parent_name)

    # Topological sort into layers
    layers: list[list[str]] = []
    computed: set[str] = set()

    while len(computed) < len(decomposed_metrics):
        # Find metrics whose dependencies are all computed
        layer = [
            name
            for name, deps in dependencies.items()
            if name not in computed and deps <= computed
        ]

        if not layer:
            # Circular dependency - shouldn't happen
            remaining = set(decomposed_metrics.keys()) - computed
            raise DJInvalidInputException(
                f"Circular dependency detected in metrics: {remaining}",
            )

        layers.append(sorted(layer))  # Sort for deterministic output
        computed.update(layer)

    return layers


def build_base_metric_expression(
    decomposed: DecomposedMetricInfo,
    metric_name: str,
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
        metric_name: Full metric name (for fallback column lookup)
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
            else:
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


def generate_metrics_sql(
    ctx: BuildContext,
    measures_result: GeneratedMeasuresSQL,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
    metric_layers: list[list[str]],
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

    # For cross-fact or cross-aggregability queries, we need to:
    # 1. Collect shared CTEs (dedupe by original name across all grain groups)
    # 2. Create a final CTE for each grain group's main SELECT
    # 3. FULL OUTER JOIN them on the common dimensions
    # 4. Apply combiner expressions in the final SELECT

    # Phase 1: Collect all inner CTEs, dedupe by original name
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

    # Phase 2: Build grain group aliases and CTEs
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

    # Build projection as AST expressions
    # Use Any type since projection accepts Union[Aliasable, Expression, Column]
    projection: list[Any] = []
    columns_metadata: list[ColumnMetadata] = []

    # Build a map of dimension types from grain groups (they have proper type lookup)
    # Map semantic_name -> type from any grain group's columns
    dim_types: dict[str, str] = {}
    for gg in grain_groups:
        for col in gg.columns:
            if col.semantic_type == "dimension" and col.semantic_name not in dim_types:
                dim_types[col.semantic_name] = col.type

    # Parse dimensions to get column names and preserve mapping to original refs
    dim_info: list[tuple[str, str]] = []  # (original_dim_ref, col_alias)
    for dim in dimensions:
        # Generate a consistent alias for this dimension
        # Using register() ensures we get a proper alias (with role suffix if applicable)
        col_name = ctx.alias_registry.register(dim)
        dim_info.append((dim, col_name))

    # Add dimension columns using COALESCE across all grain groups
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

    # Build maps for resolving references in metric expressions:
    # 1. component_columns: component_name -> (gg_alias, actual_col_name)
    # 2. metric_expr_asts: metric_name -> (expr_ast, short_name) for building projection
    # 3. dimension_aliases: dimension_ref -> col_alias (for resolving dims in expressions)
    component_columns: dict[str, tuple[str, str]] = {}
    metric_expr_asts: dict[str, tuple[ast.Expression, str]] = {}

    # Build dimension alias mapping for resolving dimension references in metric expressions
    # Dimension refs can appear anywhere: window functions, CASE expressions, arithmetic, etc.
    # Maps both the full reference (with role) and the base reference (without role) to the alias
    dimension_aliases: dict[str, str] = {}
    for original_dim_ref, col_alias in dim_info:
        dimension_aliases[original_dim_ref] = col_alias
        # Also map the base dimension (without role) if different
        # e.g., "v3.date.month[order]" -> also map "v3.date.month"
        if "[" in original_dim_ref:
            base_ref = original_dim_ref.split("[")[0]
            # Only add base ref if not already mapped (first role wins)
            if base_ref not in dimension_aliases:  # pragma: no branch
                dimension_aliases[base_ref] = col_alias

    # Collect all metrics in grain groups
    all_grain_group_metrics = set()
    for gg in grain_groups:
        all_grain_group_metrics.update(gg.metrics)

    # Build expressions for all metrics
    # Also track metric -> (cte_alias, column_name) for derived metric resolution
    metric_column_refs: dict[str, tuple[str, str]] = {}

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

                metric_expr_asts[metric_name] = (expr_ast, short_name)
                metric_column_refs[metric_name] = (alias, short_name)
                continue

            # Build expression using unified function (handles merged + non-merged)
            expr_ast, comp_mappings = build_base_metric_expression(
                decomposed,
                metric_name,
                alias,
                gg,
            )
            component_columns.update(comp_mappings)
            replace_dimension_refs_in_ast(expr_ast, dimension_aliases)
            metric_expr_asts[metric_name] = (expr_ast, short_name)
            # Track the metric's output column for derived metric resolution
            metric_column_refs[metric_name] = (alias, short_name)

    # Process derived metrics (not in any grain group)
    # Get unique dimension aliases for PARTITION BY injection
    # Use unique values to avoid duplicates when multiple refs map to same alias
    all_dim_aliases = list(dict.fromkeys(dimension_aliases.values()))

    for metric_name in ctx.metrics:
        if metric_name in all_grain_group_metrics:
            continue

        decomposed = decomposed_metrics.get(metric_name)
        if not decomposed:  # pragma: no cover
            continue

        short_name = metric_name.split(SEPARATOR)[-1]
        expr_ast = deepcopy(decomposed.combiner_ast)
        # Replace metric name references (e.g., v3.total_revenue -> cte.total_revenue)
        replace_metric_refs_in_ast(expr_ast, metric_column_refs)
        # Also try component refs (for backward compatibility)
        replace_component_refs_in_ast(expr_ast, component_columns)
        replace_dimension_refs_in_ast(expr_ast, dimension_aliases)
        # Inject PARTITION BY for window functions (e.g., LAG in WoW metrics)
        # This ensures window calculations are partitioned by all non-ORDER-BY dimensions
        inject_partition_by_into_windows(expr_ast, all_dim_aliases)
        metric_expr_asts[metric_name] = (expr_ast, short_name)

    # Build projection in requested order
    for metric_name in ctx.metrics:
        if metric_name not in metric_expr_asts:  # pragma: no cover
            continue

        expr_ast, short_name = metric_expr_asts[metric_name]
        aliased_expr = expr_ast.set_alias(ast.Name(short_name))  # type: ignore
        aliased_expr.set_as(True)
        projection.append(aliased_expr)

        # Get metric output type (metrics have exactly one output column)
        metric_node = ctx.nodes[metric_name]
        metric_type = str(metric_node.current.columns[0].type)
        columns_metadata.append(
            ColumnMetadata(
                name=short_name,
                semantic_name=metric_name,
                type=metric_type,
                semantic_type="metric",
            ),
        )

    # Build FROM clause with JOINs as AST
    dim_col_aliases = [col_alias for _, col_alias in dim_info]

    # Validate: cross-fact metrics require at least one shared dimension to join on
    # Without shared dimensions, the join would be a CROSS JOIN which produces
    # semantically meaningless results (dividing unrelated populations)
    if len(cte_aliases) > 1 and not dim_col_aliases:
        parent_names = [gg.parent_name for gg in grain_groups]
        raise DJInvalidInputException(
            f"Cross-fact metrics from different parent nodes ({', '.join(parent_names)}) "
            f"require at least one shared dimension to join on. ",
        )

    # Build JOIN extensions for the Relation
    join_extensions: list[ast.Join] = []
    for i in range(1, len(cte_aliases)):
        # Build join condition: gg0.dim1 = ggN.dim1 AND gg0.dim2 = ggN.dim2 ...
        conditions: list[ast.Expression] = []
        for dim_col in dim_col_aliases:
            left_col = make_column_ref(dim_col, cte_aliases[0])
            right_col = make_column_ref(dim_col, cte_aliases[i])
            conditions.append(ast.BinaryOp.Eq(left_col, right_col))

        # Combine conditions with AND
        if len(conditions) == 1:
            on_clause = conditions[0]
        else:
            on_clause = reduce(lambda a, b: ast.BinaryOp.And(a, b), conditions)

        # Build the JOIN with criteria
        join_extensions.append(
            ast.Join(
                right=ast.Table(ast.Name(cte_aliases[i])),
                criteria=ast.JoinCriteria(on=on_clause),
                join_type="FULL OUTER",
            ),
        )

    # Build the FROM clause as a Relation with primary table and join extensions
    from_clause = ast.From(
        relations=[
            ast.Relation(
                primary=ast.Table(ast.Name(cte_aliases[0])),
                extensions=join_extensions,
            ),
        ],
    )

    # Always add GROUP BY on requested dimensions
    # Metrics SQL always re-aggregates components to produce final metric values:
    # - If CTE is at requested grain: re-aggregation is a no-op (SUM of one = that one)
    # - If CTE is at finer grain: re-aggregation does actual work
    group_by: list[ast.Expression] = []
    if dim_col_aliases:  # pragma: no branch
        group_by.extend(
            [make_column_ref(dim_col, cte_aliases[0]) for dim_col in dim_col_aliases],
        )

    # Build WHERE clause from filters
    # For metrics SQL, filters reference dimension columns which are now in the CTEs
    where_clause: Optional[ast.Expression] = None
    if ctx.filters:
        # Resolve filters using dimension aliases
        where_clause = parse_and_resolve_filters(
            ctx.filters,
            dimension_aliases,
            cte_alias=cte_aliases[0],  # Reference the first CTE for dimensions
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
