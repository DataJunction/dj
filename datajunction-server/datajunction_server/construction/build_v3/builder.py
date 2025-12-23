"""
Build V3: SQL Generation

It generates both measures SQL (pre-aggregated to dimensional grain) and
metrics SQL (with final metric expressions applied).
"""

from __future__ import annotations

import logging
from copy import deepcopy
from functools import reduce
from typing import Optional, cast, Any

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.alias_registry import AliasRegistry
from datajunction_server.database.node import Node
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import MetricComponent, Aggregability
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.utils import SEPARATOR
from datajunction_server.construction.build_v3.loaders import load_nodes
from datajunction_server.construction.build_v3.helpers import (
    extract_columns_from_expression,
    decompose_metric,
    build_component_expression,
    resolve_dimensions,
    get_parent_node,
    get_metric_node,
    build_join_clause,
    is_derived_metric,
    get_base_metrics_for_derived,
    get_column_type,
    collect_node_ctes,
    get_table_reference_parts_with_materialization,
    make_name,
    parse_dimension_ref,
    analyze_grain_groups,
    merge_grain_groups,
    replace_dimension_refs_in_ast,
    replace_component_refs_in_ast,
    get_short_name,
    extract_join_columns_for_node,
    get_dimension_table_alias,
    make_column_ref,
    parse_and_resolve_filters,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    GrainGroupSQL,
    GeneratedMeasuresSQL,
    GeneratedSQL,
    ResolvedDimension,
    DecomposedMetricInfo,
    GrainGroup,
    MetricGroup,
)

logger = logging.getLogger(__name__)


# =============================================================================
# AST Construction
# =============================================================================


def _add_table_prefixes_to_filter(
    filter_ast: ast.Expression,
    resolved_dimensions: list[ResolvedDimension],
    main_alias: str,
    dim_aliases: dict[tuple[str, Optional[str]], str],
) -> None:
    """
    Add table prefixes to column references in a filter AST.

    This mutates the filter_ast in place, adding the appropriate table alias
    (main_alias or dim_alias) to each column reference based on which table
    the column comes from.

    Args:
        filter_ast: The filter AST to mutate
        resolved_dimensions: List of resolved dimensions with join info
        main_alias: Alias for the main (fact) table
        dim_aliases: Map from (dim_name, role) to table alias
    """
    # Build a map from column alias to table alias
    col_to_table: dict[str, str] = {}

    for resolved_dim in resolved_dimensions:
        col_alias = resolved_dim.column_name
        if resolved_dim.is_local:
            col_to_table[col_alias] = main_alias
        elif resolved_dim.join_path:
            # Get the dimension's table alias
            for link in resolved_dim.join_path.links:
                dim_key = (link.dimension.name, resolved_dim.role)
                if dim_key in dim_aliases:
                    col_to_table[col_alias] = dim_aliases[dim_key]
                    break

    def add_prefixes(node: ast.Expression) -> None:
        """Recursively add table prefixes to columns."""
        if isinstance(node, ast.Column):
            if node.name and not (node.name.namespace and node.name.namespace.name):
                # Unqualified column - check if we know its table
                col_name = node.name.name
                if col_name in col_to_table:
                    node.name = ast.Name(
                        col_name,
                        namespace=ast.Name(col_to_table[col_name]),
                    )
                else:
                    # Default to main table for unknown columns (local fact columns)
                    node.name = ast.Name(col_name, namespace=ast.Name(main_alias))

        # Recursively process children
        if hasattr(node, "children"):
            for child in node.children:
                if child and isinstance(child, ast.Expression):
                    add_prefixes(child)

    add_prefixes(filter_ast)


def build_select_ast(
    ctx: BuildContext,
    metric_expressions: list[tuple[str, ast.Expression]],
    resolved_dimensions: list[ResolvedDimension],
    parent_node: Node,
    grain_columns: list[str] | None = None,
    filters: list[str] | None = None,
) -> ast.Query:
    """
    Build a SELECT AST for measures SQL with JOIN support.

    Args:
        ctx: Build context
        metric_expressions: List of (alias, expression AST) tuples
        resolved_dimensions: List of resolved dimension objects
        parent_node: The parent node (fact/transform)
        grain_columns: Optional list of columns required in GROUP BY for LIMITED
                       aggregability (e.g., ["customer_id"] for COUNT DISTINCT).
                       These are added to the output grain to enable re-aggregation.
        filters: Optional list of filter strings to apply as WHERE clause.
                 Filter strings can reference dimensions (e.g., "v3.product.category = 'Electronics'")
                 or local columns (e.g., "status = 'active'").

    Returns:
        AST Query node
    """
    # Build projection (SELECT clause)
    # Use Any type to satisfy ast.Select.projection which accepts Union[Aliasable, Expression, Column]
    projection: list[Any] = []
    grain_columns = grain_columns or []

    # Generate alias for the main table
    main_alias = ctx.next_table_alias(parent_node.name)

    # Track which dimension nodes need joins and their aliases
    # Key by (node_name, role) to support multiple joins to same dimension with different roles
    dim_aliases: dict[tuple[str, Optional[str]], str] = {}  # (node_name, role) -> alias
    joins: list[ast.Join] = []

    # Process dimensions to build joins
    for resolved_dim in resolved_dimensions:
        if not resolved_dim.is_local and resolved_dim.join_path:
            # Need to add join(s) for this dimension
            current_left_alias = main_alias

            for link in resolved_dim.join_path.links:
                dim_node_name = link.dimension.name
                dim_key = (dim_node_name, resolved_dim.role)

                # Generate alias for dimension table if not already created
                # Key includes role to allow multiple joins to same dimension with different roles
                if dim_key not in dim_aliases:
                    # Use role as part of alias if present to distinguish multiple joins to same dim
                    if resolved_dim.role:
                        alias_base = resolved_dim.role.replace("->", "_")
                    else:
                        alias_base = get_short_name(dim_node_name)
                    dim_alias = ctx.next_table_alias(alias_base)
                    dim_aliases[dim_key] = dim_alias

                    # Build join clause
                    join = build_join_clause(ctx, link, current_left_alias, dim_alias)
                    joins.append(join)

                # For multi-hop, the next join's left is this dimension
                current_left_alias = dim_aliases[dim_key]

    # Add dimension columns to projection
    for resolved_dim in resolved_dimensions:
        table_alias = get_dimension_table_alias(resolved_dim, main_alias, dim_aliases)

        # Build column reference with table alias
        col_ref = make_column_ref(resolved_dim.column_name, table_alias)

        # Register and apply clean alias
        clean_alias = ctx.alias_registry.register(resolved_dim.original_ref)
        if clean_alias != resolved_dim.column_name:
            col_ref.alias = ast.Name(clean_alias)

        projection.append(col_ref)

    # Add grain columns for LIMITED aggregability (e.g., customer_id for COUNT DISTINCT)
    # These are added to the output so the result can be re-aggregated
    grain_col_refs: list[ast.Column] = []
    for grain_col in grain_columns:
        col_ref = make_column_ref(grain_col, main_alias)
        grain_col_refs.append(col_ref)
        projection.append(col_ref)

    # Add metric expressions
    for alias_name, expr in metric_expressions:
        clean_alias = ctx.alias_registry.register(alias_name)

        # Rewrite column references in expression to use main table alias
        def add_table_prefix(e):
            if isinstance(e, ast.Column):
                if e.name and not (e.name.namespace and e.name.namespace.name):
                    # Add table alias to unqualified columns
                    e.name = ast.Name(e.name.name, namespace=ast.Name(main_alias))
            for child in e.children if hasattr(e, "children") else []:
                if child:
                    add_table_prefix(child)

        add_table_prefix(expr)

        # Clone expression and add alias
        aliased_expr = ast.Alias(
            alias=ast.Name(clean_alias),
            child=expr,
        )
        projection.append(aliased_expr)

    # Build GROUP BY (use same column references as projection, without aliases)
    group_by: list[ast.Expression] = []
    for resolved_dim in resolved_dimensions:
        table_alias = get_dimension_table_alias(resolved_dim, main_alias, dim_aliases)
        group_by.append(make_column_ref(resolved_dim.column_name, table_alias))

    # Add grain columns to GROUP BY for LIMITED aggregability
    for grain_col in grain_columns:
        group_by.append(make_column_ref(grain_col, main_alias))

    # Collect all nodes that need CTEs and their needed columns
    nodes_for_ctes: list[Node] = []
    needed_columns_by_node: dict[str, set[str]] = {}

    # Collect columns needed from parent node
    parent_needed_cols: set[str] = set()

    # Add local dimension columns
    for resolved_dim in resolved_dimensions:
        if resolved_dim.is_local:
            parent_needed_cols.add(resolved_dim.column_name)

    # Add grain columns for LIMITED aggregability
    parent_needed_cols.update(grain_columns)

    # Add columns from metric expressions
    for _, expr in metric_expressions:
        parent_needed_cols.update(extract_columns_from_expression(expr))

    # Add join key columns (from the left side of joins)
    for resolved_dim in resolved_dimensions:
        if resolved_dim.join_path:
            for link in resolved_dim.join_path.links:
                if link.join_sql:
                    parent_needed_cols.update(
                        extract_join_columns_for_node(link.join_sql, parent_node.name),
                    )

    # Parent node needs CTE if it's not a source
    if parent_node.type != NodeType.SOURCE:
        nodes_for_ctes.append(parent_node)
        needed_columns_by_node[parent_node.name] = parent_needed_cols

    # Dimension nodes from joins need CTEs
    for resolved_dim in resolved_dimensions:
        if resolved_dim.join_path:
            for link in resolved_dim.join_path.links:
                # Look up full node from ctx.nodes to avoid lazy loading
                dim_node = ctx.nodes.get(link.dimension.name, link.dimension)
                if dim_node and dim_node.type != NodeType.SOURCE:
                    if dim_node not in nodes_for_ctes:
                        nodes_for_ctes.append(dim_node)

                    # Collect needed columns for this dimension
                    dim_cols: set[str] = set()

                    # Add the dimension column being selected
                    if resolved_dim.join_path.target_node_name == dim_node.name:
                        dim_cols.add(resolved_dim.column_name)

                    # Add join key columns from this dimension
                    if link.join_sql:
                        dim_cols.update(
                            extract_join_columns_for_node(link.join_sql, dim_node.name),
                        )

                    # Merge with existing if any
                    if dim_node.name in needed_columns_by_node:
                        needed_columns_by_node[dim_node.name].update(dim_cols)
                    else:
                        needed_columns_by_node[dim_node.name] = dim_cols

    # Build CTEs for all non-source nodes with column filtering
    ctes = collect_node_ctes(ctx, nodes_for_ctes, needed_columns_by_node)

    # Build FROM clause with main table (use materialized table if available)
    table_parts, _ = get_table_reference_parts_with_materialization(ctx, parent_node)
    table_name = make_name(SEPARATOR.join(table_parts))

    # Create relation with joins
    primary_expr: ast.Expression = cast(
        ast.Expression,
        ast.Alias(
            child=ast.Table(name=table_name),
            alias=ast.Name(main_alias),
        ),
    )
    relation = ast.Relation(
        primary=primary_expr,
        extensions=joins,
    )

    from_clause = ast.From(relations=[relation])

    # Build WHERE clause from filters
    where_clause: Optional[ast.Expression] = None
    if filters:
        # Build column alias mapping for filter resolution
        # Maps dimension refs to their table-qualified column names
        filter_column_aliases: dict[str, str] = {}

        # Add dimension columns with their table aliases
        for resolved_dim in resolved_dimensions:
            table_alias = get_dimension_table_alias(
                resolved_dim,
                main_alias,
                dim_aliases,
            )
            # Map the original ref (e.g., "v3.product.category") to "category"
            # The table alias is handled by resolve_filter_references
            col_alias = ctx.alias_registry.get_alias(resolved_dim.original_ref)
            if col_alias:
                filter_column_aliases[resolved_dim.original_ref] = col_alias
            else:
                filter_column_aliases[resolved_dim.original_ref] = (
                    resolved_dim.column_name
                )

        # Add local columns from the parent node (for simple column refs like "status")
        if parent_node.current and parent_node.current.columns:
            for col in parent_node.current.columns:
                if col.name not in filter_column_aliases:
                    filter_column_aliases[col.name] = col.name

        # Parse and resolve filters
        # Note: We don't pass a cte_alias because the column references are already
        # qualified with their table aliases during dimension resolution
        where_clause = parse_and_resolve_filters(
            filters,
            filter_column_aliases,
            cte_alias=None,  # Don't add table prefix - we'll handle it per column
        )

        # Now resolve table prefixes for filter columns based on where they come from
        if where_clause:
            _add_table_prefixes_to_filter(
                where_clause,
                resolved_dimensions,
                main_alias,
                dim_aliases,
            )

    # Build SELECT
    select = ast.Select(
        projection=projection,
        from_=from_clause,
        where=where_clause,
        group_by=group_by if group_by else [],
    )

    # Build Query with CTEs
    query = ast.Query(select=select)

    # Add CTEs to the query
    if ctes:
        cte_list = []
        for cte_name, cte_query in ctes:
            # Convert the query to a CTE using to_cte method
            cte_query.to_cte(ast.Name(cte_name), query)
            cte_list.append(cte_query)
        query.ctes = cte_list

    return query


# =============================================================================
# Metric Grouping
# =============================================================================


async def decompose_and_group_metrics(
    ctx: BuildContext,
) -> tuple[list[MetricGroup], dict[str, DecomposedMetricInfo]]:
    """
    Decompose metrics and group them by parent node (fact/transform).

    For base metrics: groups by direct parent
    For derived metrics: decomposes into base metrics and groups by their parents

    This enables cross-fact derived metrics by producing separate grain groups
    for each underlying fact.

    Returns:
        Tuple of:
        - List of MetricGroup, one per unique parent node, with decomposed metrics.
        - Dict of metric_name -> DecomposedMetricInfo for reuse by callers.
    """
    # Map parent node name -> list of DecomposedMetricInfo
    parent_groups: dict[str, list[DecomposedMetricInfo]] = {}
    parent_nodes: dict[str, Node] = {}
    # Cache of all decomposed metrics to avoid redundant work
    all_decomposed: dict[str, DecomposedMetricInfo] = {}

    for metric_name in ctx.metrics:
        metric_node = get_metric_node(ctx, metric_name)

        if is_derived_metric(ctx, metric_node):
            # Derived metric - get base metrics and decompose each
            base_metric_nodes = get_base_metrics_for_derived(ctx, metric_node)

            for base_metric in base_metric_nodes:
                # Get the fact/transform parent of the base metric
                parent_node = get_parent_node(ctx, base_metric)
                parent_name = parent_node.name

                # Check if already decomposed (e.g., shared base metric)
                if base_metric.name in all_decomposed:
                    decomposed = all_decomposed[base_metric.name]
                else:
                    # Decompose the BASE metric (not the derived one) - use cache!
                    decomposed = await decompose_metric(
                        ctx.session,
                        base_metric,
                        nodes_cache=ctx.nodes,
                        parent_map=ctx.parent_map,
                    )
                    all_decomposed[base_metric.name] = decomposed

                # Always ensure parent group exists and add if not already present
                if parent_name not in parent_groups:
                    parent_groups[parent_name] = []
                    parent_nodes[parent_name] = parent_node

                # Only add to parent group if not already there
                if decomposed not in parent_groups[parent_name]:
                    parent_groups[parent_name].append(decomposed)

            # Also decompose the derived metric itself and cache it
            if metric_name not in all_decomposed:
                derived_decomposed = await decompose_metric(
                    ctx.session,
                    metric_node,
                    nodes_cache=ctx.nodes,
                    parent_map=ctx.parent_map,
                )
                all_decomposed[metric_name] = derived_decomposed
        else:
            # Base metric - use direct parent
            parent_node = get_parent_node(ctx, metric_node)
            parent_name = parent_node.name

            if metric_name in all_decomposed:
                # Already decomposed - just ensure it's in the parent group
                decomposed = all_decomposed[metric_name]
                if parent_name not in parent_groups:
                    parent_groups[parent_name] = []
                    parent_nodes[parent_name] = parent_node
                if decomposed not in parent_groups[parent_name]:
                    parent_groups[parent_name].append(decomposed)
                continue

            # Use cache to avoid DB queries!
            decomposed = await decompose_metric(
                ctx.session,
                metric_node,
                nodes_cache=ctx.nodes,
                parent_map=ctx.parent_map,
            )
            all_decomposed[metric_node.name] = decomposed

            if parent_name not in parent_groups:
                parent_groups[parent_name] = []
                parent_nodes[parent_name] = parent_node

            parent_groups[parent_name].append(decomposed)

    # Build MetricGroup objects
    metric_groups = [
        MetricGroup(parent_node=parent_nodes[name], decomposed_metrics=metrics)
        for name, metrics in parent_groups.items()
    ]
    return metric_groups, all_decomposed


# =============================================================================
# Main Entry Points
# =============================================================================


def build_grain_group_sql(
    ctx: BuildContext,
    grain_group: GrainGroup,
    resolved_dimensions: list[ResolvedDimension],
    components_per_metric: dict[str, int],
) -> GrainGroupSQL:
    """
    Build SQL for a single grain group.

    Args:
        ctx: Build context
        grain_group: The grain group to generate SQL for
        resolved_dimensions: Pre-resolved dimensions with join paths
        components_per_metric: Metric name -> component count mapping

    Returns:
        GrainGroupSQL with SQL and metadata for this grain group
    """
    parent_node = grain_group.parent_node

    # Build list of component expressions with their aliases
    component_expressions: list[tuple[str, ast.Expression]] = []
    component_metadata: list[tuple[str, MetricComponent, Node, bool]] = []

    # Track which metrics are covered by this grain group
    metrics_covered: set[str] = set()

    # Track which components we've already added (deduplicate by component name)
    seen_components: set[str] = set()

    # Collect unique MetricComponent objects for the API response
    unique_components: list[MetricComponent] = []

    # Track mapping from component name to actual SQL alias
    # This is needed for metrics SQL to correctly reference component columns
    component_aliases: dict[str, str] = {}

    for metric_node, component in grain_group.components:
        metrics_covered.add(metric_node.name)

        # Deduplicate components - same component may appear for multiple derived metrics
        if component.name in seen_components:
            continue
        seen_components.add(component.name)

        # Collect unique components for API response
        unique_components.append(component)

        # For NONE aggregability, output raw columns (no aggregation possible)
        if grain_group.aggregability == Aggregability.NONE:
            if component.expression:
                col_ast = make_column_ref(component.expression)
                component_alias = component.expression
                component_expressions.append((component_alias, col_ast))
                component_metadata.append(
                    (component_alias, component, metric_node, False),
                )
                component_aliases[component.name] = component_alias
            continue

        # For merged grain groups, handle based on original component aggregability
        if grain_group.is_merged:
            orig_agg = grain_group.component_aggregabilities.get(
                component.name,
                Aggregability.FULL,
            )
            if orig_agg == Aggregability.LIMITED:
                # LIMITED: grain column is already in GROUP BY, no output needed
                # The grain column (e.g., order_id) will be used for COUNT DISTINCT
                # in the final SELECT
                continue
            else:
                # FULL: apply aggregation at finest grain, will be re-aggregated in final SELECT
                num_components = components_per_metric.get(metric_node.name, 1)
                is_simple = num_components == 1
                if is_simple:
                    component_alias = get_short_name(metric_node.name)
                else:
                    component_alias = component.name
                expr_ast = build_component_expression(component)
                component_expressions.append((component_alias, expr_ast))
                component_metadata.append(
                    (component_alias, component, metric_node, is_simple),
                )
                component_aliases[component.name] = component_alias
            continue

        # Skip LIMITED aggregability components with no aggregation
        # These are represented by grain columns instead
        if component.rule.type == Aggregability.LIMITED and not component.aggregation:
            continue

        num_components = components_per_metric.get(metric_node.name, 1)
        is_simple = num_components == 1

        if is_simple:
            component_alias = metric_node.name.split(SEPARATOR)[-1]
        else:
            component_alias = component.name

        expr_ast = build_component_expression(component)
        component_expressions.append((component_alias, expr_ast))
        component_metadata.append((component_alias, component, metric_node, is_simple))

        # Track the mapping from component name to actual SQL alias
        # This is needed for metrics SQL to correctly reference component columns
        component_aliases[component.name] = component_alias

    # Determine grain columns for this group
    if grain_group.is_merged:
        # Merged: use finest grain (all grain columns from merged groups)
        effective_grain_columns = grain_group.grain_columns
    elif grain_group.aggregability == Aggregability.NONE:
        # NONE: use native grain (PK columns)
        effective_grain_columns = grain_group.grain_columns
    elif grain_group.aggregability == Aggregability.LIMITED:
        # LIMITED: use level columns from components
        effective_grain_columns = grain_group.grain_columns
    else:
        # FULL: no additional grain columns
        effective_grain_columns = []

    # Build AST
    query_ast = build_select_ast(
        ctx,
        metric_expressions=component_expressions,
        resolved_dimensions=resolved_dimensions,
        parent_node=parent_node,
        grain_columns=effective_grain_columns,
        filters=ctx.filters,
    )

    # Build column metadata
    columns_metadata = []

    # Add dimension columns
    for resolved_dim in resolved_dimensions:
        alias = (
            ctx.alias_registry.get_alias(resolved_dim.original_ref)
            or resolved_dim.column_name
        )
        if resolved_dim.is_local:
            col_type = get_column_type(parent_node, resolved_dim.column_name)
        else:
            dim_node = ctx.nodes.get(resolved_dim.node_name)
            col_type = (
                get_column_type(dim_node, resolved_dim.column_name)
                if dim_node
                else "string"
            )
        columns_metadata.append(
            ColumnMetadata(
                name=alias,
                semantic_name=resolved_dim.original_ref,
                type=col_type,
                semantic_type="dimension",
            ),
        )

    # Add grain columns (for LIMITED and NONE)
    for grain_col in effective_grain_columns:
        col_type = get_column_type(parent_node, grain_col)
        columns_metadata.append(
            ColumnMetadata(
                name=grain_col,
                semantic_name=f"{parent_node.name}{SEPARATOR}{grain_col}",
                type=col_type,
                semantic_type="dimension",  # Added for aggregability (e.g., customer_id for COUNT DISTINCT)
            ),
        )

    # Add metric component columns
    for comp_alias, component, metric_node, is_simple in component_metadata:
        if grain_group.aggregability == Aggregability.NONE:
            # NONE: raw column, will be aggregated in metrics SQL
            columns_metadata.append(
                ColumnMetadata(
                    name=comp_alias,
                    semantic_name=f"{metric_node.name}:{component.expression}",
                    type="number",
                    semantic_type="metric_input",  # Raw input for non-aggregatable metric
                ),
            )
        elif is_simple:
            columns_metadata.append(
                ColumnMetadata(
                    name=ctx.alias_registry.get_alias(comp_alias) or comp_alias,
                    semantic_name=metric_node.name,
                    type="number",
                    semantic_type="metric",
                ),
            )
        else:
            columns_metadata.append(
                ColumnMetadata(
                    name=ctx.alias_registry.get_alias(comp_alias) or comp_alias,
                    semantic_name=f"{metric_node.name}:{component.name}",
                    type="number",
                    semantic_type="metric_component",
                ),
            )

    # Build the full grain list (GROUP BY columns)
    # Start with dimension column aliases
    full_grain = []
    for resolved_dim in resolved_dimensions:
        alias = (
            ctx.alias_registry.get_alias(resolved_dim.original_ref)
            or resolved_dim.column_name
        )
        full_grain.append(alias)

    # Add any additional grain columns (from LIMITED/NONE aggregability)
    for grain_col in effective_grain_columns:
        if grain_col not in full_grain:
            full_grain.append(grain_col)

    # Sort for deterministic output
    full_grain.sort()

    return GrainGroupSQL(
        query=query_ast,
        columns=columns_metadata,
        grain=full_grain,
        aggregability=grain_group.aggregability,
        metrics=list(metrics_covered),
        parent_name=grain_group.parent_node.name,
        component_aliases=component_aliases,
        is_merged=grain_group.is_merged,
        component_aggregabilities=grain_group.component_aggregabilities,
        components=unique_components,
    )


async def build_measures_sql(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None = None,
    dialect: Dialect = Dialect.SPARK,
    use_materialized: bool = True,
) -> GeneratedMeasuresSQL:
    """
    Build measures SQL for a set of metrics and dimensions.

    This is the main entry point for V3 measures SQL generation.

    Measures SQL aggregates metric components to the requested dimensional
    grain, producing one SQL query per grain group. Different aggregability
    levels (FULL, LIMITED, NONE) result in different grain groups.

    Use cases:
    - Materialization: Each grain group can be materialized separately
    - Live queries: Pass to build_metrics_sql() to get a single combined query

    Args:
        session: Database session
        metrics: List of metric node names
        dimensions: List of dimension names (format: "node.column" or "node.column[role]")
        filters: Optional list of filter expressions
        dialect: SQL dialect for output
        use_materialized: If True (default), use materialized tables when available.
            Set to False when generating SQL for materialization refresh to avoid
            circular references.

    Returns:
        GeneratedMeasuresSQL with one GrainGroupSQL per aggregation level,
        plus context and decomposed metrics for efficient reuse by build_metrics_sql
    """
    ctx = BuildContext(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters or [],
        dialect=dialect,
        use_materialized=use_materialized,
    )

    # Load all required nodes (single DB round trip)
    await load_nodes(ctx)

    # Validate we have at least one metric
    if not ctx.metrics:
        raise DJInvalidInputException("At least one metric is required")

    # Decompose metrics and group by parent node
    # Also returns the decomposed metrics to avoid redundant work
    metric_groups, decomposed_metrics = await decompose_and_group_metrics(ctx)

    # Process each metric group into grain group SQLs
    # Cross-fact metrics produce separate grain groups (one per parent node)
    all_grain_group_sqls: list[GrainGroupSQL] = []
    for metric_group in metric_groups:
        grain_group_sqls = process_metric_group(ctx, metric_group)
        all_grain_group_sqls.extend(grain_group_sqls)

    # Decompose any requested metrics not already decomposed (e.g., derived metrics)
    # Uses cache to avoid DB queries
    for metric_name in metrics:
        if metric_name not in decomposed_metrics:
            metric_node = ctx.nodes.get(metric_name)
            if metric_node:
                decomposed = await decompose_metric(
                    session,
                    metric_node,
                    nodes_cache=ctx.nodes,
                    parent_map=ctx.parent_map,
                )
                decomposed_metrics[metric_name] = decomposed

    # Also decompose any base metrics from grain groups not yet decomposed
    all_grain_group_metrics = set()
    for gg in all_grain_group_sqls:
        all_grain_group_metrics.update(gg.metrics)

    for metric_name in all_grain_group_metrics:
        if metric_name not in decomposed_metrics:
            metric_node = ctx.nodes.get(metric_name)
            if metric_node:
                decomposed = await decompose_metric(
                    session,
                    metric_node,
                    nodes_cache=ctx.nodes,
                    parent_map=ctx.parent_map,
                )
                decomposed_metrics[metric_name] = decomposed

    return GeneratedMeasuresSQL(
        grain_groups=all_grain_group_sqls,
        dialect=dialect,
        requested_dimensions=dimensions,
        ctx=ctx,
        decomposed_metrics=decomposed_metrics,
    )


def process_metric_group(
    ctx: BuildContext,
    metric_group: MetricGroup,
) -> list[GrainGroupSQL]:
    """
    Process a single MetricGroup into one or more GrainGroupSQLs.

    This handles:
    1. Counting components per metric for naming strategy
    2. Analyzing grain groups by aggregability
    3. Resolving dimension join paths
    4. Building SQL for each grain group

    Args:
        ctx: Build context
        metric_group: The metric group to process

    Returns:
        List of GrainGroupSQL, one per aggregability level
    """
    parent_node = metric_group.parent_node

    # Count components per metric to determine naming strategy
    components_per_metric: dict[str, int] = {}
    for decomposed in metric_group.decomposed_metrics:
        components_per_metric[decomposed.metric_node.name] = len(decomposed.components)

    # Analyze grain groups - split by aggregability
    # Extract just the column names from dimensions for grain analysis
    dim_column_names = [parse_dimension_ref(d).column_name for d in ctx.dimensions]
    grain_groups = analyze_grain_groups(metric_group, dim_column_names)

    # Merge compatible grain groups from same parent into single CTEs
    # This optimization reduces duplicate JOINs by outputting raw values
    # at finest grain, with aggregations applied in final SELECT
    grain_groups = merge_grain_groups(grain_groups)

    # Resolve dimensions (find join paths) - shared across grain groups
    resolved_dimensions = resolve_dimensions(ctx, parent_node)

    # Build SQL for each grain group
    grain_group_sqls: list[GrainGroupSQL] = []
    for grain_group in grain_groups:
        # Reset alias registry for each grain group to avoid conflicts
        ctx.alias_registry = AliasRegistry()
        ctx._table_alias_counter = 0

        grain_group_sql = build_grain_group_sql(
            ctx,
            grain_group,
            resolved_dimensions,
            components_per_metric,
        )
        grain_group_sqls.append(grain_group_sql)
    return grain_group_sqls


async def build_metrics_sql(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None = None,
    dialect: Dialect = Dialect.SPARK,
) -> GeneratedSQL:
    """
    Build metrics SQL for a set of metrics and dimensions.

    This is the main entry point for V3 metrics SQL generation.

    Metrics SQL applies final metric expressions on top of measures,
    including handling derived metrics. It produces a single executable
    query that:
    1. Uses measures SQL output as CTEs (one per grain group)
    2. JOINs grain groups if metrics come from different facts/aggregabilities
    3. Applies combiner expressions for multi-component metrics
    4. Computes derived metrics that reference other metrics

    Architecture:
    - Layer 0 (Measures): Grain group CTEs from build_measures_sql()
    - Layer 1 (Base Metrics): Combiner expressions applied
    - Layer 2+ (Derived Metrics): Metrics referencing other metrics
    """
    # Step 1: Get measures SQL with grain groups
    # This also returns context and decomposed metrics to avoid redundant work
    measures_result = await build_measures_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=dialect,
    )

    if not measures_result.grain_groups:
        raise DJInvalidInputException("No grain groups produced from measures SQL")

    # Step 2: Reuse context and decomposed metrics from measures SQL
    # This avoids redundant database queries and metric decomposition
    ctx = measures_result.ctx
    if ctx is None:
        raise DJInvalidInputException("Measures result missing context")

    decomposed_metrics = measures_result.decomposed_metrics

    # Step 3: Build metric dependency DAG and compute layers
    metric_layers = compute_metric_layers(ctx, decomposed_metrics)

    # Step 4: Generate the combined SQL (returns GeneratedSQL with AST query)
    return generate_metrics_sql(
        ctx,
        measures_result,
        decomposed_metrics,
        metric_layers,
    )


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
            # LIMITED: use grain column (for COUNT DISTINCT)
            grain_col = comp.rule.level[0] if comp.rule.level else comp.expression
            comp_mappings[comp.name] = (cte_alias, grain_col)
        else:
            # FULL/NONE: use pre-aggregated column
            actual_col = gg.component_aliases.get(comp.name, comp.name)
            comp_mappings[comp.name] = (cte_alias, actual_col)

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

    Always applies re-aggregation using each component's merge function:
    - FULL: Uses comp.merge (SUM for counts/sums, MIN for min, hll_union for HLL, etc.)
    - LIMITED: COUNT(DISTINCT grain_col)

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

    if len(decomposed.components) == 1:
        comp = decomposed.components[0]
        orig_agg = get_comp_aggregability(comp.name)
        _, col_name = comp_mappings[comp.name]

        if orig_agg == Aggregability.LIMITED:
            # COUNT DISTINCT on grain column
            distinct_col = make_column_ref(col_name, cte_alias)
            agg_name = comp.aggregation or "COUNT"
            return ast.Function(
                ast.Name(agg_name),
                args=[distinct_col],
                quantifier=ast.SetQuantifier.Distinct,
            )
        else:
            # Re-aggregate with merge function
            col_ref = make_column_ref(col_name, cte_alias)
            merge_func = comp.merge or "SUM"
            return ast.Function(ast.Name(merge_func), args=[col_ref])

    # Multi-component: build combiner with component refs
    # The combiner AST already has the aggregation structure
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
        if gg_query.ctes:
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

        columns_metadata.append(
            ColumnMetadata(
                name=dim_col,
                semantic_name=original_dim_ref,  # Preserve original dimension reference
                type="string",
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
            if base_ref not in dimension_aliases:
                dimension_aliases[base_ref] = col_alias

    # Collect all metrics in grain groups
    all_grain_group_metrics = set()
    for gg in grain_groups:
        all_grain_group_metrics.update(gg.metrics)

    # =========================================================================
    # PHASE 1: Build expressions for all metrics (don't add to projection yet)
    # =========================================================================

    # Process base metrics from each grain group
    for i, gg in enumerate(grain_groups):
        alias = cte_aliases[i]
        for metric_name in gg.metrics:
            decomposed = decomposed_metrics.get(metric_name)
            short_name = get_short_name(metric_name)

            if not decomposed or not decomposed.components:
                # No decomposition info - use column from metadata
                col_name = next(
                    (c.name for c in gg.columns if c.semantic_name == metric_name),
                    short_name,
                )
                expr_ast: ast.Expression = make_column_ref(col_name, alias)
                metric_expr_asts[metric_name] = (expr_ast, short_name)
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

    # Process derived metrics (not in any grain group)
    for metric_name in ctx.metrics:
        if metric_name in all_grain_group_metrics:
            continue

        decomposed = decomposed_metrics.get(metric_name)
        if not decomposed:
            continue

        short_name = metric_name.split(SEPARATOR)[-1]
        expr_ast = deepcopy(decomposed.combiner_ast)
        replace_component_refs_in_ast(expr_ast, component_columns)
        replace_dimension_refs_in_ast(expr_ast, dimension_aliases)
        metric_expr_asts[metric_name] = (expr_ast, short_name)

    # =========================================================================
    # PHASE 2: Build projection in requested order
    # =========================================================================

    for metric_name in ctx.metrics:
        if metric_name not in metric_expr_asts:
            continue

        expr_ast, short_name = metric_expr_asts[metric_name]
        aliased_expr = expr_ast.set_alias(ast.Name(short_name))  # type: ignore
        aliased_expr.set_as(True)
        projection.append(aliased_expr)
        columns_metadata.append(
            ColumnMetadata(
                name=short_name,
                semantic_name=metric_name,
                type="number",
                semantic_type="metric",
            ),
        )

    # Build FROM clause with JOINs as AST
    dim_col_aliases = [col_alias for _, col_alias in dim_info]

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
    if dim_col_aliases:
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
