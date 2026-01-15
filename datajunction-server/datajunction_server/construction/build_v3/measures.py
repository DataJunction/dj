"""
Measures SQL Generation

This module handles the generation of pre-aggregated "measures" SQL,
which aggregates metric components to the requested dimensional grain.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from datajunction_server.database.preaggregation import PreAggregation

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
from datajunction_server.construction.build_v3.preagg_matcher import (
    find_matching_preagg,
    get_preagg_measure_column,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    GrainGroupSQL,
    MetricGroup,
)
from datajunction_server.sql.functions import function_registry
from datajunction_server.sql.parsing import types as ct
import re


# Mapping from type string to ColumnType instance
# Used to convert stored type strings back to type objects for function inference
_TYPE_STRING_MAP: dict[str, ct.ColumnType] = {
    "int": ct.IntegerType(),
    "integer": ct.IntegerType(),
    "tinyint": ct.TinyIntType(),
    "smallint": ct.SmallIntType(),
    "bigint": ct.BigIntType(),
    "long": ct.LongType(),
    "float": ct.FloatType(),
    "double": ct.DoubleType(),
    "string": ct.StringType(),
    "boolean": ct.BooleanType(),
    "date": ct.DateType(),
    "timestamp": ct.TimestampType(),
    "binary": ct.BinaryType(),
}


def _parse_type_string(type_str: str | None) -> ct.ColumnType | None:
    """
    Convert a type string to a ColumnType instance.

    Args:
        type_str: Type string like "double", "int", "bigint"

    Returns:
        ColumnType instance or None if unrecognized
    """
    if not type_str:
        return None  # pragma: no cover
    # Normalize to lowercase for lookup
    normalized = type_str.lower().strip()
    return _TYPE_STRING_MAP.get(normalized)


def infer_component_type(
    component: MetricComponent,
    metric_type: str,
    parent_node: Node | None = None,
) -> str:
    """
    Infer the SQL type of a metric component based on its aggregation function.

    Uses the function registry to look up the aggregation function and infer
    its output type based on the input expression type. Falls back to the
    metric_type if inference fails.

    Args:
        component: The metric component
        metric_type: The final metric's output type (fallback)
        parent_node: Optional parent node to look up column types from

    Returns:
        The inferred SQL type string
    """
    if not component.aggregation:
        return metric_type  # pragma: no cover

    # Extract the outermost function name from the aggregation
    # e.g., "SUM" from "SUM", "SUM" from "SUM(POWER({}, 2))", "hll_sketch_agg" from "hll_sketch_agg"
    agg_str = component.aggregation.strip()
    match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)", agg_str)
    if not match:
        return metric_type  # pragma: no cover

    func_name = match.group(1).upper()

    # Look up the function in the registry
    try:
        func_class = function_registry[func_name]
    except KeyError:  # pragma: no cover
        return metric_type

    # Get input type from parent node's columns by looking up the expression
    input_type = None
    if parent_node and component.expression:  # pragma: no branch
        col_type_str = get_column_type(parent_node, component.expression)
        input_type = _parse_type_string(col_type_str)

    try:
        if input_type:
            result_type = func_class.infer_type(input_type)
        else:  # pragma: no cover
            # Fallback: try with a generic ColumnType
            result_type = func_class.infer_type(ct.ColumnType("unknown", "unknown"))
        return str(result_type)
    except (TypeError, NotImplementedError, AttributeError):
        # Function may require more specific types - fall back to metric type
        return metric_type


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
    skip_aggregation: bool = False,
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
        skip_aggregation: If True, skip adding GROUP BY clause. Used for non-decomposable
                          metrics where raw rows need to be passed through.

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

    # Add temporal partition column if temporal filtering is enabled
    # This ensures the column is available in the CTE for the WHERE clause
    if ctx.include_temporal_filters and parent_node.current:
        temporal_cols = parent_node.current.temporal_partition_columns()
        if temporal_cols:  # pragma: no branch
            parent_needed_cols.add(temporal_cols[0].name)

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

    # Inject temporal partition filters for incremental materialization
    # This ensures partition pruning at the source level
    all_filters = list(filters or [])
    temporal_filter_ast = build_temporal_filter(ctx, parent_node, main_alias)

    # Build WHERE clause from filters
    where_clause: Optional[ast.Expression] = None
    if all_filters:
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
            all_filters,
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

    # Combine user filters with temporal filter
    if temporal_filter_ast:
        if where_clause:
            where_clause = ast.BinaryOp.And(where_clause, temporal_filter_ast)
        else:
            where_clause = temporal_filter_ast

    # Build SELECT
    # For non-decomposable metrics, skip GROUP BY to pass through raw rows
    effective_group_by = [] if skip_aggregation else (group_by if group_by else [])
    select = ast.Select(
        projection=projection,
        from_=from_clause,
        where=where_clause,
        group_by=effective_group_by,
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


def build_temporal_filter(
    ctx: BuildContext,
    parent_node: Node,
    table_alias: str,
) -> Optional[ast.Expression]:
    """
    Build temporal filter expression based on partition metadata.

    Returns:
        - BinaryOp (col = expr) for exact partition match
        - Between (col BETWEEN start AND end) for lookback window
        - None if temporal filtering not enabled or no partition configured
    """
    if not ctx.include_temporal_filters or not parent_node.current:
        return None

    temporal_cols = parent_node.current.temporal_partition_columns()
    if not temporal_cols:
        return None  # pragma: no cover

    # Use the first temporal partition column
    temporal_col = temporal_cols[0]
    if not temporal_col.partition:
        return None  # pragma: no cover

    # Generate the temporal expression using partition metadata
    # For exact partition: dateint = CAST(DATE_FORMAT(...), 'yyyyMMdd') AS INT)
    # For lookback: dateint BETWEEN start_expr AND end_expr
    col_ref = make_column_ref(temporal_col.name, table_alias)

    # Get the end expression (current logical timestamp)
    end_expr = temporal_col.partition.temporal_expression(interval=None)

    if ctx.lookback_window and end_expr:
        # For lookback, generate BETWEEN filter
        # Start = DJ_LOGICAL_TIMESTAMP() - interval
        if start_expr := temporal_col.partition.temporal_expression(
            interval=ctx.lookback_window,
        ):
            return ast.Between(
                expr=col_ref,
                low=start_expr,
                high=end_expr,
            )
    elif end_expr:
        # No lookback - exact partition match
        return ast.BinaryOp(
            left=col_ref,
            right=end_expr,
            op=ast.BinaryOpKind.Eq,
        )

    return None  # pragma: no cover


# TODO: Remove this once we have a way to test pre-aggregations
def build_grain_group_from_preagg(  # pragma: no cover
    ctx: BuildContext,
    grain_group: GrainGroup,
    preagg: "PreAggregation",
    resolved_dimensions: list[ResolvedDimension],
    components_per_metric: dict[str, int],
) -> GrainGroupSQL:
    """
    Build SQL for a grain group using a pre-aggregation table.

    Instead of computing from source, generates SQL that reads from the
    pre-aggregation's materialized table and re-aggregates to the requested grain.

    The generated SQL looks like:
        SELECT dim1, dim2, SUM(measure1), SUM(measure2), ...
        FROM catalog.schema.preagg_table
        GROUP BY dim1, dim2

    Args:
        ctx: Build context
        grain_group: The grain group to generate SQL for
        preagg: The pre-aggregation to use
        resolved_dimensions: Pre-resolved dimensions with join paths
        components_per_metric: Metric name -> component count mapping

    Returns:
        GrainGroupSQL with SQL and metadata for this grain group
    """
    parent_node = grain_group.parent_node
    avail = preagg.availability

    if not avail:  # pragma: no cover
        raise ValueError(f"Pre-agg {preagg.id} has no availability")

    # Build table reference
    table_parts = [p for p in [avail.catalog, avail.schema_, avail.table] if p]

    # Build SELECT columns
    select_items: list[ast.Aliasable | ast.Expression | ast.Column] = []
    columns: list[ColumnMetadata] = []
    component_aliases: dict[str, str] = {}
    metrics_covered: set[str] = set()
    unique_components: list[MetricComponent] = []
    seen_components: set[str] = set()

    # Add dimension columns (grain columns)
    grain_col_names: list[str] = []
    for dim in resolved_dimensions:
        col_name = dim.column_name
        grain_col_names.append(col_name)

        col_ref = ast.Column(name=ast.Name(col_name))
        select_items.append(col_ref)

        # Get type from pre-agg columns if available
        col_type = preagg.get_column_type(col_name, default="string")
        columns.append(
            ColumnMetadata(
                name=col_name,
                semantic_name=dim.original_ref,
                type=col_type,
                semantic_type="dimension",
            ),
        )

    # Add measure columns with re-aggregation (or grain columns if no merge func)
    for metric_node, component in grain_group.components:
        metrics_covered.add(metric_node.name)

        # Deduplicate components
        if component.name in seen_components:
            continue
        seen_components.add(component.name)
        unique_components.append(component)

        # Find the measure column name in the pre-agg
        measure_col = get_preagg_measure_column(preagg, component)
        if not measure_col:  # pragma: no cover
            raise ValueError(
                f"Component {component.name} not found in pre-agg {preagg.id}",
            )

        # Always use the measure column name (component hash) as the output alias
        # This ensures consistency with the non-preagg path
        output_alias = measure_col

        component_aliases[component.name] = output_alias

        col_ref = ast.Column(name=ast.Name(measure_col))

        # If no merge function, output column directly (e.g., grain column for LIMITED)
        # Otherwise, apply the merge function for re-aggregation
        if component.merge:
            agg_expr = ast.Function(
                name=ast.Name(component.merge),
                args=[col_ref],
            )
            aliased = ast.Alias(child=agg_expr, alias=ast.Name(output_alias))
            select_items.append(aliased)
        else:
            # No merge - output grain column directly, add to GROUP BY
            select_items.append(col_ref)
            grain_col_names.append(measure_col)
            output_alias = measure_col
            component_aliases[component.name] = output_alias

        # Get type from pre-agg columns
        col_type = preagg.get_column_type(measure_col, default="double")
        columns.append(
            ColumnMetadata(
                name=output_alias,
                semantic_name=metric_node.name,
                type=col_type,
                semantic_type="metric" if component.merge else "dimension",
            ),
        )

    # Build GROUP BY clause (list of column references)
    group_by: list[ast.Expression] = []
    if grain_col_names:
        group_by = [ast.Column(name=ast.Name(col)) for col in grain_col_names]

    # Build FROM clause using the helper method
    from_clause = ast.From.Table(SEPARATOR.join(table_parts))

    # Build SELECT statement
    select = ast.Select(
        projection=select_items,
        from_=from_clause,
        group_by=group_by,
    )

    # Build the query
    query = ast.Query(select=select)

    return GrainGroupSQL(
        query=query,
        columns=columns,
        grain=grain_col_names,
        aggregability=grain_group.aggregability,
        metrics=list(metrics_covered),
        parent_name=parent_node.name,
        component_aliases=component_aliases,
        is_merged=grain_group.is_merged,
        component_aggregabilities=grain_group.component_aggregabilities,
        components=unique_components,
        dialect=ctx.dialect,
    )


def build_grain_group_sql(
    ctx: BuildContext,
    grain_group: GrainGroup,
    resolved_dimensions: list[ResolvedDimension],
    components_per_metric: dict[str, int],
) -> GrainGroupSQL:
    """
    Build SQL for a single grain group.

    First checks if a matching pre-aggregation is available. If so, uses the
    pre-agg table. Otherwise, computes from source tables.

    Args:
        ctx: Build context
        grain_group: The grain group to generate SQL for
        resolved_dimensions: Pre-resolved dimensions with join paths
        components_per_metric: Metric name -> component count mapping

    Returns:
        GrainGroupSQL with SQL and metadata for this grain group
    """
    parent_node = grain_group.parent_node

    # Check for matching pre-aggregation
    # TODO: Remove this once we have a way to test pre-aggregations
    if ctx.use_materialized and ctx.available_preaggs:  # pragma: no cover
        requested_grain = [dim.original_ref for dim in resolved_dimensions]
        matching_preagg = find_matching_preagg(
            ctx,
            parent_node,
            requested_grain,
            grain_group,
        )
        if matching_preagg:
            return build_grain_group_from_preagg(
                ctx,
                grain_group,
                matching_preagg,
                resolved_dimensions,
                components_per_metric,
            )

    # Build list of component expressions with their aliases
    component_expressions: list[tuple[str, ast.Expression]] = []
    component_metadata: list[tuple[str, MetricComponent, Node]] = []

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
                    (component_alias, component, metric_node),
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
                # Still set alias to the grain column name for pre-agg creation
                grain_col = (
                    component.rule.level[0]
                    if component.rule.level
                    else component.expression
                )
                component_aliases[component.name] = grain_col
                continue
            else:
                # FULL: apply aggregation at finest grain, will be re-aggregated in final SELECT
                # Always use component.name for consistency - no special case for single-component
                component_alias = component.name
                expr_ast = build_component_expression(component)
                component_expressions.append((component_alias, expr_ast))
                component_metadata.append(
                    (component_alias, component, metric_node),
                )
                component_aliases[component.name] = component_alias
            continue

        # Skip LIMITED aggregability components with no aggregation
        # These are represented by grain columns instead
        if component.rule.type == Aggregability.LIMITED and not component.aggregation:
            # Still set alias to the grain column name for pre-agg creation
            grain_col = (
                component.rule.level[0]
                if component.rule.level
                else component.expression
            )
            component_aliases[component.name] = grain_col
            continue

        # Always use component.name for consistency - no special case for single-component
        component_alias = component.name

        expr_ast = build_component_expression(component)
        component_expressions.append((component_alias, expr_ast))
        component_metadata.append((component_alias, component, metric_node))

        # Track the mapping from component name to actual SQL alias
        # This is needed for metrics SQL to correctly reference component columns
        component_aliases[component.name] = component_alias

    # Handle non-decomposable metrics (like MAX_BY)
    # Extract column references from the metric expression and pass them through
    non_decomposable_columns: list[tuple[str, ast.Expression]] = []
    for decomposed in grain_group.non_decomposable_metrics:
        metrics_covered.add(decomposed.metric_node.name)

        # Extract column references from the metric's derived AST
        # These are the columns needed for the aggregation function
        for col in decomposed.derived_ast.find_all(ast.Column):
            col_name = col.name.name if col.name else None
            if col_name and col_name not in seen_components:  # pragma: no branch
                seen_components.add(col_name)
                col_ast = make_column_ref(col_name)
                non_decomposable_columns.append((col_name, col_ast))

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
    # For non-decomposable metrics (NONE aggregability with no components),
    # we pass through raw rows without aggregation
    if grain_group.non_decomposable_metrics and not component_expressions:
        # Pure non-decomposable case: pass through raw rows (no GROUP BY)
        # Add non-decomposable columns to grain_columns so they appear as plain columns
        # (not aliased expressions) since we're just selecting them for pass-through
        pass_through_columns = effective_grain_columns + [
            col_name for col_name, _ in non_decomposable_columns
        ]
        query_ast = build_select_ast(
            ctx,
            metric_expressions=[],  # No aggregated expressions
            resolved_dimensions=resolved_dimensions,
            parent_node=parent_node,
            grain_columns=pass_through_columns,
            filters=ctx.filters,
            skip_aggregation=True,  # Don't add GROUP BY
        )
    else:
        # Normal case: combine component expressions with non-decomposable columns
        all_metric_expressions = component_expressions + non_decomposable_columns
        query_ast = build_select_ast(
            ctx,
            metric_expressions=all_metric_expressions,
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
    # All decomposed metrics are now treated as components - no special case for single-component
    for comp_alias, component, metric_node in component_metadata:
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
        else:
            columns_metadata.append(
                ColumnMetadata(
                    name=ctx.alias_registry.get_alias(comp_alias) or comp_alias,
                    semantic_name=f"{metric_node.name}:{component.name}",
                    type=infer_component_type(component, metric_type, parent_node),
                    semantic_type="metric_component",
                ),
            )

    # Add columns for non-decomposable metrics (raw columns passed through)
    for col_name, _ in non_decomposable_columns:
        col_type = get_column_type(parent_node, col_name)
        columns_metadata.append(
            ColumnMetadata(
                name=col_name,
                semantic_name=f"{parent_node.name}{SEPARATOR}{col_name}",
                type=col_type,
                semantic_type="dimension",  # Treated as dimension (raw value for aggregation)
            ),
        )

    # Build the full grain list (GROUP BY columns or unique row identity)
    # For NONE aggregability, grain is just the native grain (no dimensions)
    # because we're passing through raw rows without grouping
    full_grain = []
    if grain_group.aggregability != Aggregability.NONE:
        # FULL/LIMITED: dimensions are part of the grain
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
        dialect=ctx.dialect,
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
