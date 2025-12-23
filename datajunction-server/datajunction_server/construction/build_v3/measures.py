"""
Measures SQL Generation

This module handles the generation of pre-aggregated "measures" SQL,
which aggregates metric components to the requested dimensional grain.
"""

from __future__ import annotations

from typing import Any, Optional, cast

from datajunction_server.construction.build_v3.cte import (
    collect_node_ctes,
)
from datajunction_server.construction.build_v3.decomposition import (
    build_component_expression,
)
from datajunction_server.construction.build_v3.dimensions import (
    build_join_clause,
)
from datajunction_server.construction.build_v3.filters import (
    parse_and_resolve_filters,
)
from datajunction_server.construction.build_v3.utils import (
    get_column_type,
    get_short_name,
    make_column_ref,
    make_name,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.construction.build_v3.utils import (
    extract_columns_from_expression,
)
from datajunction_server.construction.build_v3.materialization import (
    get_table_reference_parts_with_materialization,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    GrainGroup,
    GrainGroupSQL,
    ResolvedDimension,
)
from datajunction_server.database.node import Node
from datajunction_server.models.decompose import Aggregability, MetricComponent
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.utils import SEPARATOR
from datajunction_server.construction.build_v3.alias_registry import AliasRegistry
from datajunction_server.construction.build_v3.decomposition import (
    analyze_grain_groups,
    merge_grain_groups,
)
from datajunction_server.construction.build_v3.dimensions import (
    parse_dimension_ref,
    resolve_dimensions,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    GrainGroupSQL,
    MetricGroup,
)


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
        elif resolved_dim.join_path:  # pragma: no branch
            # Get the dimension's table alias
            for link in resolved_dim.join_path.links:  # pragma: no branch
                dim_key = (link.dimension.name, resolved_dim.role)
                if dim_key in dim_aliases:  # pragma: no branch
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
        for child in node.children:
            if child and isinstance(child, ast.Expression):
                add_prefixes(child)

    add_prefixes(filter_ast)


def extract_join_columns_for_node(join_sql: str, node_name: str) -> set[str]:
    """
    Extract column names from join SQL that belong to a specific node.

    Parses the join_sql (e.g., "v3.order_details.customer_id = v3.customer.customer_id")
    and returns the short column names for columns belonging to the given node.

    Args:
        join_sql: The join condition SQL string
        node_name: The fully qualified node name to filter by

    Returns:
        Set of short column names (e.g., {"customer_id"})

    Examples:
        extract_join_columns_for_node(
            "v3.order_details.customer_id = v3.customer.customer_id",
            "v3.order_details"
        ) -> {"customer_id"}
    """
    result: set[str] = set()
    join_expr = parse(f"SELECT 1 WHERE {join_sql}").select.where
    if join_expr:  # pragma: no branch
        prefix = node_name + SEPARATOR
        for col in join_expr.find_all(ast.Column):
            col_id = col.identifier()
            if col_id.startswith(prefix):
                result.add(get_short_name(col_id))
    return result


def get_dimension_table_alias(
    resolved_dim: ResolvedDimension,
    main_alias: str,
    dim_aliases: dict[tuple[str, Optional[str]], str],
) -> str:
    """
    Get the table alias for a resolved dimension's column.

    Args:
        resolved_dim: The resolved dimension
        main_alias: The alias for the main/parent table
        dim_aliases: Map of (node_name, role) -> table_alias for dimension joins

    Returns:
        The appropriate table alias to use for this dimension's column
    """
    if resolved_dim.is_local:
        return main_alias
    elif resolved_dim.join_path:  # pragma: no branch
        final_dim_name = resolved_dim.join_path.target_node_name
        dim_key = (final_dim_name, resolved_dim.role)
        return dim_aliases.get(dim_key, main_alias)
    return main_alias  # pragma: no cover


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
                if dim_key not in dim_aliases:  # pragma: no branch
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
                if e.name and not (  # pragma: no branch
                    e.name.namespace and e.name.namespace.name
                ):
                    # Add table alias to unqualified columns
                    e.name = ast.Name(e.name.name, namespace=ast.Name(main_alias))
            for child in e.children if hasattr(e, "children") else []:
                if child:  # pragma: no branch
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
                if link.join_sql:  # pragma: no branch
                    parent_needed_cols.update(
                        extract_join_columns_for_node(link.join_sql, parent_node.name),
                    )

    # Parent node needs CTE if it's not a source
    if parent_node.type != NodeType.SOURCE:  # pragma: no branch
        nodes_for_ctes.append(parent_node)
        needed_columns_by_node[parent_node.name] = parent_needed_cols

    # Dimension nodes from joins need CTEs
    for resolved_dim in resolved_dimensions:
        if resolved_dim.join_path:
            for link in resolved_dim.join_path.links:
                # Look up full node from ctx.nodes to avoid lazy loading
                dim_node = ctx.nodes.get(link.dimension.name, link.dimension)
                if dim_node and dim_node.type != NodeType.SOURCE:  # pragma: no branch
                    if dim_node not in nodes_for_ctes:
                        nodes_for_ctes.append(dim_node)

                    # Collect needed columns for this dimension
                    dim_cols: set[str] = set()

                    # Add the dimension column being selected
                    if resolved_dim.join_path.target_node_name == dim_node.name:
                        dim_cols.add(resolved_dim.column_name)

                    # Add join key columns from this dimension
                    if link.join_sql:  # pragma: no branch
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
            if col_alias:  # pragma: no branch
                filter_column_aliases[resolved_dim.original_ref] = col_alias
            else:
                # Defensive: dimensions should always be registered
                filter_column_aliases[resolved_dim.original_ref] = (  # pragma: no cover
                    resolved_dim.column_name
                )

        # Add local columns from the parent node (for simple column refs like "status")
        if parent_node.current and parent_node.current.columns:  # pragma: no branch
            for col in parent_node.current.columns:
                if col.name not in filter_column_aliases:  # pragma: no branch
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
        if where_clause:  # pragma: no branch
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
        # Note: This path is only hit for BASE metrics with NONE aggregability
        # (e.g., metrics with RANK() directly). Derived metrics with window functions
        # don't go through this path - they're computed in generate_metrics_sql.
        if grain_group.aggregability == Aggregability.NONE:  # pragma: no cover
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
                    component_alias = component.name  # pragma: no cover
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
        effective_grain_columns = grain_group.grain_columns  # pragma: no cover
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
        # Get metric output type (metrics have exactly one output column)
        metric_type = str(metric_node.current.columns[0].type)
        if grain_group.aggregability == Aggregability.NONE:
            # NONE: raw column, will be aggregated in metrics SQL
            columns_metadata.append(  # pragma: no cover
                ColumnMetadata(
                    name=comp_alias,
                    semantic_name=f"{metric_node.name}:{component.expression}",
                    type=metric_type,
                    semantic_type="metric_input",  # Raw input for non-aggregatable metric
                ),
            )
        elif is_simple:
            columns_metadata.append(
                ColumnMetadata(
                    name=ctx.alias_registry.get_alias(comp_alias) or comp_alias,
                    semantic_name=metric_node.name,
                    type=metric_type,
                    semantic_type="metric",
                ),
            )
        else:
            columns_metadata.append(
                ColumnMetadata(
                    name=ctx.alias_registry.get_alias(comp_alias) or comp_alias,
                    semantic_name=f"{metric_node.name}:{component.name}",
                    type=metric_type,
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
        if grain_col not in full_grain:  # pragma: no branch
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
