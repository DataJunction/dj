"""
Build V3: Helper Functions

This module contains utility functions for SQL building, organized by domain:
- Node/Column utilities
- AST/CTE manipulation
- Dimension resolution
- Metric analysis

All functions here are either pure functions or take a BuildContext for data access.
Database operations are in loaders.py.
"""

from __future__ import annotations
from copy import deepcopy

import logging
from typing import Optional, cast

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import MetricComponent, Aggregability
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import SEPARATOR

from datajunction_server.construction.build_v3.types import (
    BuildContext,
    JoinPath,
    ResolvedDimension,
    DecomposedMetricInfo,
    GrainGroup,
    DimensionRef,
    MetricGroup,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Node/Column Utilities
# =============================================================================


def get_short_name(full_name: str) -> str:
    """
    Get the last segment of a dot-separated name.

    Examples:
        get_short_name("v3.order_details") -> "order_details"
        get_short_name("v3.product.category") -> "category"
        get_short_name("simple_name") -> "simple_name"
    """
    return full_name.split(SEPARATOR)[-1]


def make_column_ref(col_name: str, table_alias: str | None = None) -> ast.Column:
    """
    Build a column reference AST node with optional table alias.

    Args:
        col_name: The column name
        table_alias: Optional table/CTE alias to qualify the column

    Returns:
        ast.Column node that renders to SQL

    Generated SQL examples:
        make_column_ref("status")           -> status
        make_column_ref("status", "t1")     -> t1.status
        make_column_ref("category", "gg0")  -> gg0.category
    """
    if table_alias:
        return ast.Column(
            name=ast.Name(col_name),
            _table=ast.Table(ast.Name(table_alias)),
        )
    return ast.Column(name=ast.Name(col_name))


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
    if join_expr:
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
    elif resolved_dim.join_path:
        final_dim_name = resolved_dim.join_path.target_node_name
        dim_key = (final_dim_name, resolved_dim.role)
        return dim_aliases.get(dim_key, main_alias)
    return main_alias


def get_column_type(node: Node, column_name: str) -> str:
    """
    Look up the column type from a node's columns.

    Returns the string representation of the column type, or "string" as fallback.
    """
    if node.current and node.current.columns:
        for col in node.current.columns:
            if col.name == column_name:
                return str(col.type) if col.type else "string"
    return "string"


def get_metric_node(ctx: BuildContext, metric_name: str) -> Node:
    """Get a metric node by name, raising if not found or not a metric."""
    node = ctx.nodes.get(metric_name)
    if not node:
        raise DJInvalidInputException(f"Metric not found: {metric_name}")
    if node.type != NodeType.METRIC:
        raise DJInvalidInputException(f"Not a metric node: {metric_name}")
    return node


def get_parent_node(ctx: BuildContext, metric_node: Node) -> Node:
    """Get the parent node of a metric (the node it's defined on)."""
    # Use parent_map from Query 1 instead of the parents relationship
    parent_names = ctx.parent_map.get(metric_node.name, [])
    if not parent_names:
        raise DJInvalidInputException(f"Metric {metric_node.name} has no parent node")

    # Metrics typically have one parent (the node they SELECT FROM)
    parent_name = parent_names[0]
    parent = ctx.nodes.get(parent_name)
    if not parent:
        raise DJInvalidInputException(f"Parent node not found: {parent_name}")
    return parent


def get_native_grain(node: Node) -> list[str]:
    """
    Get the native grain (primary key columns) of a node.

    For transforms/dimensions, this is their primary key columns.
    If no PK is defined, returns empty list (meaning row-level grain).
    """
    if not node.current:
        return []

    pk_columns = []
    for col in node.current.columns:
        # Check if this column is part of the primary key
        if col.has_primary_key_attribute:
            pk_columns.append(col.name)

    return pk_columns


def get_physical_table_name(node: Node) -> Optional[str]:
    """
    Get the physical table name for a source node.

    For source nodes: Returns catalog.schema.table
    For other nodes: Returns None (they need CTEs)
    """
    rev = node.current
    if not rev:
        return None

    if node.type == NodeType.SOURCE:
        parts = []
        if rev.catalog:
            parts.append(rev.catalog.name)
        if rev.schema_:
            parts.append(rev.schema_)
        if rev.table:
            parts.append(rev.table)
        else:
            parts.append(node.name)
        return SEPARATOR.join(parts)

    return None


def get_table_reference(ctx: BuildContext, node: Node) -> tuple[str, bool]:
    """
    Get the table reference for a node.

    Returns:
        (reference_name, is_cte): The name to use and whether it's a CTE

    For source nodes: Returns (catalog.schema.table, False)
    For transform/dimension: Returns (cte_name, True) - must add CTE
    """
    # Check for physical table first
    physical = get_physical_table_name(node)
    if physical:
        return (physical, False)

    # For transforms/dimensions, use CTE name
    cte_name = amenable_name(node.name)
    return (cte_name, True)


def get_table_reference_parts(node: Node) -> list[str]:
    """
    Get the parts of the fully qualified table reference for a source/transform/dimension node.

    For source nodes: [catalog, schema, table]
    For other nodes: [amenable_name] (CTE reference)

    Returns:
        List of name parts
    """
    rev = node.current
    if not rev:
        raise DJInvalidInputException(f"Node {node.name} has no current revision")

    # For source nodes, build catalog.schema.table
    if node.type == NodeType.SOURCE:
        parts = []
        if rev.catalog:
            parts.append(rev.catalog.name)
        if rev.schema_:
            parts.append(rev.schema_)
        if rev.table:
            parts.append(rev.table)
        else:
            # Fall back to node name
            parts.append(node.name)
        return parts

    # For transform/dimension nodes, use amenable name (CTE reference)
    return [amenable_name(node.name)]


# =============================================================================
# Materialization Utilities
# =============================================================================


def has_available_materialization(node: Node) -> bool:
    """
    Check if a node has an available materialized table.

    A materialized table is available if:
    1. The node has an availability state attached
    2. The availability state indicates the data is available

    Args:
        node: The node to check

    Returns:
        True if a materialized table is available, False otherwise
    """
    if not node.current:
        return False

    availability = node.current.availability
    if not availability:
        return False

    # The is_available method on AvailabilityState checks criteria
    # For now, we just check that availability exists
    # TODO: Add criteria checking when BuildCriteria is integrated
    return availability.is_available()


def get_materialized_table_parts(node: Node) -> list[str] | None:
    """
    Get the table reference parts for a materialized table.

    Returns [catalog, schema, table] if the node has materialization,
    or None if it doesn't.

    Args:
        node: The node to get materialization for

    Returns:
        List of table name parts, or None if no materialization
    """
    if not node.current or not node.current.availability:
        return None

    availability = node.current.availability
    parts = []

    # Note: availability.catalog is always present
    # availability.schema_ and table are required
    if availability.catalog:
        parts.append(availability.catalog)
    if availability.schema_:
        parts.append(availability.schema_)
    if availability.table:
        parts.append(availability.table)

    return parts if parts else None


def should_use_materialized_table(
    ctx: "BuildContext",  # Forward reference
    node: Node,
) -> bool:
    """
    Determine if we should use a materialized table for a node.

    Considerations:
    1. ctx.use_materialized must be True (default)
    2. The node must have an available materialization
    3. Source nodes always use their physical table (not considered "materialization")

    Args:
        ctx: Build context
        node: Node to check

    Returns:
        True if materialization should be used, False otherwise
    """
    # Source nodes always use physical tables (not materialization)
    if node.type == NodeType.SOURCE:
        return False

    # Check if materialization is enabled in context
    if not ctx.use_materialized:
        return False

    # Check if node has available materialization
    return has_available_materialization(node)


def get_table_reference_parts_with_materialization(
    ctx: "BuildContext",  # Forward reference
    node: Node,
) -> tuple[list[str], bool]:
    """
    Get table reference parts, considering materialization.

    For source nodes: Always returns physical table [catalog, schema, table]
    For other nodes:
      - If materialized and use_materialized=True: Returns [catalog, schema, table]
      - Otherwise: Returns [cte_name] for CTE reference

    Args:
        ctx: Build context
        node: Node to get reference for

    Returns:
        Tuple of (table_parts, is_materialized)
        - table_parts: List of name parts
        - is_materialized: True if using materialized table (not CTE)
    """
    rev = node.current
    if not rev:
        raise DJInvalidInputException(f"Node {node.name} has no current revision")

    # For source nodes, always use physical table
    if node.type == NodeType.SOURCE:
        parts = []
        if rev.catalog:
            parts.append(rev.catalog.name)
        if rev.schema_:
            parts.append(rev.schema_)
        if rev.table:
            parts.append(rev.table)
        else:
            parts.append(node.name)
        return (parts, True)  # Sources are considered "materialized" (physical)

    # For non-source nodes, check for materialization
    if should_use_materialized_table(ctx, node):
        mat_parts = get_materialized_table_parts(node)
        if mat_parts:
            logger.debug(
                f"[BuildV3] Using materialized table for {node.name}: "
                f"{'.'.join(mat_parts)}",
            )
            return (mat_parts, True)

    # Fall back to CTE reference
    return ([amenable_name(node.name)], False)


# =============================================================================
# AST/CTE Utilities
# =============================================================================


def get_cte_name(node_name: str) -> str:
    """
    Generate a CTE-safe name from a node name.

    Replaces dots with underscores to create valid SQL identifiers.
    Uses the same logic as amenable_name for consistency.
    """
    return node_name.replace(SEPARATOR, "_").replace("-", "_")


def amenable_name(name: str) -> str:
    """Convert a node name to a SQL-safe identifier (for CTEs)."""
    return name.replace(SEPARATOR, "_").replace("-", "_")


def make_name(dotted_name: str) -> ast.Name:
    """
    Create an AST Name from a dotted string like 'catalog.schema.table'.

    The Name class uses nested namespace attributes:
    'a.b.c' becomes Name('c', namespace=Name('b', namespace=Name('a')))
    """
    parts = dotted_name.split(SEPARATOR)
    if not parts:
        return ast.Name("")

    # Build from left to right, each becoming the namespace of the next
    result = ast.Name(parts[0])
    for part in parts[1:]:
        result = ast.Name(part, namespace=result)

    return result


def get_table_references_from_ast(query_ast: ast.Query) -> set[str]:
    """
    Extract all table references from a query AST.

    Returns set of table names (as dotted strings like 'v3.src_orders').
    """
    table_names: set[str] = set()
    for table in query_ast.find_all(ast.Table):
        # Get the full table name including namespace
        table_name = str(table.name)
        if table_name:
            table_names.add(table_name)
    return table_names


def extract_columns_from_expression(expr: ast.Expression) -> set[str]:
    """
    Extract all column names referenced in an expression.
    """
    columns: set[str] = set()
    for col in expr.find_all(ast.Column):
        # Get the column name (last part of the identifier)
        if col.name:
            columns.add(col.name.name)
    return columns


def get_column_full_name(col: ast.Column) -> str:
    """
    Get the full dotted name of a column, including its table/namespace.

    For example, a column like v3.date.month returns "v3.date.month".
    """
    parts: list[str] = []

    # Get table prefix if present
    if col.table and hasattr(col.table, "alias_or_name"):
        table_name = col.table.alias_or_name
        if table_name:
            if isinstance(table_name, str):
                parts.append(table_name)
            else:
                # It's an ast.Name with possible namespace
                parts.append(table_name.identifier(quotes=False))

    # Get column name with its namespace chain
    if col.name:
        parts.append(col.name.identifier(quotes=False))

    return ".".join(parts) if parts else ""


def replace_component_refs_in_ast(
    expr_ast: ast.Node,
    component_aliases: dict[str, tuple[str, str]],
) -> None:
    """
    Replace component name references in an AST with qualified column references.

    Modifies the AST in place. For each Column node in the AST, checks if its
    name matches a component name (hash-based like "unit_price_sum_abc123").
    If so, replaces it with a qualified reference like "gg0.actual_col".

    Args:
        expr_ast: The AST expression to modify (mutated in place)
        component_aliases: Mapping from component name to (table_alias, column_name)
            e.g., {"unit_price_sum_abc123": ("gg0", "sum_unit_price")}
    """
    for col in expr_ast.find_all(ast.Column):
        # Get the column name (might be in name.name or just name)
        col_name = col.name.name if col.name else None
        if not col_name:
            continue

        # Check if this column name matches a component
        if col_name in component_aliases:
            table_alias, actual_col = component_aliases[col_name]
            # Replace with qualified column reference
            col.name = ast.Name(actual_col)
            col._table = ast.Table(ast.Name(table_alias))


def replace_dimension_refs_in_ast(
    expr_ast: ast.Node,
    dimension_aliases: dict[str, str],
) -> None:
    """
    Replace dimension references in an AST with their resolved column aliases.

    Modifies the AST in place. For each Column node in the AST, checks if its
    full identifier (e.g., "v3.date.month") matches a dimension reference.
    If so, replaces it with a simple column reference using the resolved alias.

    Args:
        expr_ast: The AST expression to modify (mutated in place)
        dimension_aliases: Mapping from dimension refs to column aliases
            e.g., {"v3.date.month": "month_order", "v3.date.month[order]": "month_order"}
    """
    for col in expr_ast.find_all(ast.Column):
        full_name = get_column_full_name(col)
        if not full_name:
            continue

        # Check for exact match first (handles roles like "v3.date.month[order]")
        if full_name in dimension_aliases:
            alias = dimension_aliases[full_name]
            # Replace with simple column reference
            col.name = ast.Name(alias)
            col._table = None
            continue

        # Check without role suffix (for base dimension refs)
        # The column AST might be just "v3.date.month" but we have "v3.date.month[order]"
        for dim_ref, alias in dimension_aliases.items():
            # Match if the dim_ref starts with our full_name and has a role suffix
            if dim_ref.startswith(full_name) and (
                dim_ref == full_name or dim_ref[len(full_name)] == "["
            ):
                col.name = ast.Name(alias)
                col._table = None
                break


def topological_sort_nodes(ctx: BuildContext, node_names: set[str]) -> list[Node]:
    """
    Sort nodes in topological order (dependencies first).

    Uses the query AST to find table references and determine dependencies.
    Source nodes have no dependencies and come first.
    Transform/dimension nodes depend on what they reference in their queries.

    Returns:
        List of nodes sorted so dependencies come before dependents.
    """
    # Build dependency graph
    dependencies: dict[str, set[str]] = {}
    node_map: dict[str, Node] = {}

    for name in node_names:
        node = ctx.nodes.get(name)
        if not node:
            continue
        node_map[name] = node

        if node.type == NodeType.SOURCE:
            # Sources have no dependencies
            dependencies[name] = set()
        elif node.type == NodeType.METRIC:
            # Metrics depend on their parent node (handled separately, skip)
            continue
        elif node.current and node.current.query:
            # Transform/dimension - parse query to find references (using cache)
            try:
                query_ast = ctx.get_parsed_query(node)
                refs = get_table_references_from_ast(query_ast)
                # Only keep references that are in our node set
                dependencies[name] = {r for r in refs if r in node_names}
            except Exception:
                # If we can't parse, assume no dependencies
                dependencies[name] = set()
        else:
            dependencies[name] = set()

    # Kahn's algorithm for topological sort
    in_degree = {name: 0 for name in dependencies}
    for deps in dependencies.values():
        for dep in deps:
            if dep in in_degree:
                in_degree[dep] = in_degree.get(dep, 0) + 1

    # Wait, that's backwards. Let me fix the in-degree calculation.
    # in_degree[X] = number of nodes that X depends on
    in_degree = {name: len(deps) for name, deps in dependencies.items()}

    # Build reverse mapping: which nodes depend on this node?
    dependents: dict[str, list[str]] = {name: [] for name in dependencies}
    for name, deps in dependencies.items():
        for dep in deps:
            if dep in dependents:
                dependents[dep].append(name)

    # Start with nodes that have no dependencies (in_degree == 0)
    # Sort to ensure deterministic output order
    queue = sorted([name for name, degree in in_degree.items() if degree == 0])
    sorted_names: list[str] = []

    while queue:
        current = queue.pop(0)
        sorted_names.append(current)
        # Reduce in-degree for all dependents
        # Collect new zero-degree nodes and sort for determinism
        new_ready = []
        for dependent in dependents.get(current, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                new_ready.append(dependent)
        queue.extend(sorted(new_ready))

    # Return sorted nodes (excluding any we couldn't sort due to cycles)
    return [node_map[name] for name in sorted_names if name in node_map]


def rewrite_table_references(
    query_ast: ast.Query,
    ctx: BuildContext,
    cte_names: dict[str, str],
    inner_cte_renames: Optional[dict[str, str]] = None,
) -> ast.Query:
    """
    Rewrite table references in a query AST.

    - Source nodes -> physical table names (catalog.schema.table)
    - Materialized nodes -> physical materialized table names
    - Transform/dimension nodes -> CTE names
    - Inner CTE names -> prefixed CTE names with alias to original name
      e.g., `FROM base` -> `FROM prefix_base base` (keeps column refs like base.col working)

    Args:
        query_ast: The query AST to rewrite (modified in place)
        ctx: Build context with loaded nodes
        cte_names: Mapping of node names to their CTE names
        inner_cte_renames: Optional mapping of inner CTE old names to prefixed names

    Returns:
        The modified query AST
    """
    inner_cte_renames = inner_cte_renames or {}

    for table in query_ast.find_all(ast.Table):
        table_name = str(table.name)

        # First check if it's an inner CTE reference that needs renaming
        if table_name in inner_cte_renames:
            # Use the prefixed name but alias it to the original name
            # So `FROM base` becomes `FROM prefix_base base`
            # This keeps column references like `base.col` working
            new_name = inner_cte_renames[table_name]
            table.name = ast.Name(new_name)
            # Only set alias if not already set (preserve existing aliases)
            if not table.alias:
                table.alias = ast.Name(table_name)
            continue

        # Then check if it's a node reference
        ref_node = ctx.nodes.get(table_name)
        if ref_node:
            # Use the unified function that handles source, materialized, and CTE cases
            table_parts, is_physical = get_table_reference_parts_with_materialization(
                ctx,
                ref_node,
            )
            if is_physical:
                # Source or materialized - use physical table name
                table.name = ast.Name(SEPARATOR.join(table_parts))
            elif table_name in cte_names:
                # Replace with CTE name
                table.name = ast.Name(cte_names[table_name])

    return query_ast


def filter_cte_projection(
    query_ast: ast.Query,
    columns_to_select: set[str],
) -> ast.Query:
    """
    Filter a query's projection to only include specified columns.

    This modifies the SELECT clause to only project columns that are
    actually needed downstream.

    Args:
        query_ast: The query AST to modify
        columns_to_select: Set of column names to keep

    Returns:
        Modified query AST with filtered projection
    """
    if not query_ast.select.projection:
        return query_ast

    new_projection = []
    for expr in query_ast.select.projection:
        # Get the name this column will be known by
        if isinstance(expr, ast.Alias):
            col_name = str(expr.alias.name) if expr.alias else None
            if not col_name and isinstance(expr.child, ast.Column):
                col_name = str(expr.child.name.name)
        elif isinstance(expr, ast.Column):
            col_name = str(expr.alias.name) if expr.alias else str(expr.name.name)
        else:
            # Keep expressions we can't analyze
            new_projection.append(expr)
            continue

        # Keep if it's in our needed set
        if col_name and col_name in columns_to_select:
            new_projection.append(expr)

    # If we filtered everything, keep original (shouldn't happen)
    if new_projection:
        query_ast.select.projection = new_projection

    return query_ast


def flatten_inner_ctes(
    query_ast: ast.Query,
    outer_cte_name: str,
) -> tuple[list[tuple[str, ast.Query]], dict[str, str]]:
    """
    Extract inner CTEs from a query and rename them to avoid collisions.

    If a transform has:
        WITH temp AS (SELECT ...) SELECT * FROM temp

    We extract 'temp' as 'v3_transform__temp' and return the rename mapping.
    The caller is responsible for rewriting references using the returned mapping.

    Args:
        query_ast: The parsed query that may contain inner CTEs
        outer_cte_name: The name of the outer CTE (e.g., 'v3_order_details')

    Returns:
        Tuple of:
        - List of (prefixed_cte_name, cte_query) tuples for the extracted CTEs
        - Dict mapping old CTE names to new prefixed names (for reference rewriting)
    """
    if not query_ast.ctes:
        return [], {}

    extracted_ctes: list[tuple[str, ast.Query]] = []

    # Build mapping of old CTE name -> new prefixed name
    inner_cte_renames: dict[str, str] = {}
    for inner_cte in query_ast.ctes:
        if inner_cte.alias:
            old_name = (
                inner_cte.alias.name
                if hasattr(inner_cte.alias, "name")
                else str(inner_cte.alias)
            )
            new_name = f"{outer_cte_name}__{old_name}"
            inner_cte_renames[old_name] = new_name

    # Extract each inner CTE with renamed name
    for inner_cte in query_ast.ctes:
        if inner_cte.alias:
            old_name = (
                inner_cte.alias.name
                if hasattr(inner_cte.alias, "name")
                else str(inner_cte.alias)
            )
            new_name = inner_cte_renames[old_name]

            # Create a new Query for the CTE content
            cte_query = ast.Query(select=inner_cte.select)
            if inner_cte.ctes:
                # Recursively flatten if this CTE also has CTEs
                nested_ctes, nested_renames = flatten_inner_ctes(cte_query, new_name)
                extracted_ctes.extend(nested_ctes)
                inner_cte_renames.update(nested_renames)

            extracted_ctes.append((new_name, cte_query))

    # Clear inner CTEs from the original query
    query_ast.ctes = []

    return extracted_ctes, inner_cte_renames


def collect_node_ctes(
    ctx: BuildContext,
    nodes_to_include: list[Node],
    needed_columns_by_node: Optional[dict[str, set[str]]] = None,
) -> list[tuple[str, ast.Query]]:
    """
    Collect CTEs for all non-source nodes, recursively expanding table references.

    This handles the full dependency chain:
    - Source nodes -> replaced with physical table names (catalog.schema.table)
    - Materialized nodes -> replaced with materialized table names (no CTE)
    - Transform/dimension nodes -> recursive CTEs with dependencies resolved
    - Inner CTEs within transforms -> flattened and prefixed to avoid collisions

    Args:
        ctx: Build context
        nodes_to_include: List of nodes to create CTEs for
        needed_columns_by_node: Optional dict of node_name -> set of column names
            If provided, CTEs will only select the needed columns.

    Returns list of (cte_name, query_ast) tuples in dependency order.
    """
    # Collect all node names that need CTEs (including transitive dependencies)
    all_node_names: set[str] = set()
    mat_check_time = 0.0
    parse_check_time = 0.0
    ref_extract_time = 0.0
    call_count = 0

    def collect_refs(node: Node, visited: set[str]) -> None:
        nonlocal mat_check_time, parse_check_time, ref_extract_time, call_count
        call_count += 1

        if node.name in visited:
            return
        visited.add(node.name)

        if node.type == NodeType.SOURCE:
            return  # Sources don't become CTEs

        # Skip materialized nodes - they use physical tables, not CTEs
        is_mat = should_use_materialized_table(ctx, node)
        if is_mat:
            return

        all_node_names.add(node.name)

        if node.current and node.current.query:
            try:
                # Use cached parsed query for reference extraction
                query_ast = ctx.get_parsed_query(node)

                refs = get_table_references_from_ast(query_ast)

                for ref in refs:
                    ref_node = ctx.nodes.get(ref)
                    if ref_node:
                        collect_refs(ref_node, visited)
            except Exception:
                pass

    # Collect from all starting nodes with SHARED visited set
    # This prevents re-parsing nodes that are shared dependencies
    shared_visited: set[str] = set()
    for node in nodes_to_include:
        collect_refs(node, shared_visited)

    # Topologically sort all collected nodes
    sorted_nodes = topological_sort_nodes(ctx, all_node_names)

    # Build CTE name mapping
    cte_names: dict[str, str] = {}
    for node in sorted_nodes:
        cte_names[node.name] = get_cte_name(node.name)

    # Build CTEs in dependency order
    ctes: list[tuple[str, ast.Query]] = []
    for node in sorted_nodes:
        if node.type == NodeType.SOURCE:
            continue

        # Skip materialized nodes (they use physical tables directly)
        if should_use_materialized_table(ctx, node):
            continue

        if not node.current or not node.current.query:
            continue

        # Get parsed query from cache (uses deepcopy internally to avoid mutation)
        query_ast = deepcopy(ctx.get_parsed_query(node))

        cte_name = cte_names[node.name]

        # Flatten any inner CTEs to avoid nested WITH clauses
        # Returns extracted CTEs and mapping of old names -> prefixed names
        inner_ctes, inner_cte_renames = flatten_inner_ctes(query_ast, cte_name)

        # Rewrite table references in extracted inner CTEs
        # (they may reference sources or materialized nodes -> physical table names)
        for inner_cte_name, inner_cte_query in inner_ctes:
            rewrite_table_references(
                inner_cte_query,
                ctx,
                cte_names,
                inner_cte_renames,
            )

        ctes.extend(inner_ctes)

        # Rewrite table references in main query
        # (sources -> physical tables, materialized -> physical, others -> CTE names)
        rewrite_table_references(
            query_ast,
            ctx,
            cte_names,
            inner_cte_renames,
        )

        # Apply column filtering if specified
        needed_cols = None
        if needed_columns_by_node:
            needed_cols = needed_columns_by_node.get(node.name)

        if needed_cols:
            query_ast = filter_cte_projection(query_ast, needed_cols)

        ctes.append((cte_name, query_ast))

    return ctes


# =============================================================================
# Dimension Resolution
# =============================================================================


def parse_dimension_ref(dim_ref: str) -> DimensionRef:
    """
    Parse a dimension reference string.

    Formats:
    - "v3.customer.name" -> node=v3.customer, col=name, role=None
    - "v3.customer.name[order]" -> node=v3.customer, col=name, role=order
    - "v3.date.month[customer->registration]" -> node=v3.date, col=month, role=customer->registration
    """
    # Extract role if present
    role = None
    if "[" in dim_ref:
        dim_part, role_part = dim_ref.rsplit("[", 1)
        role = role_part.rstrip("]")
    else:
        dim_part = dim_ref

    # Split into node and column
    parts = dim_part.rsplit(SEPARATOR, 1)
    if len(parts) == 2:
        node_name, column_name = parts
    else:
        # Assume single part is column name on current node
        node_name = ""
        column_name = parts[0]

    return DimensionRef(node_name=node_name, column_name=column_name, role=role)


def find_join_path(
    ctx: BuildContext,
    from_node: Node,
    target_dim_name: str,
    role: Optional[str] = None,
) -> Optional[JoinPath]:
    """
    Find the join path from a node to a target dimension.

    Uses preloaded join paths from ctx.join_paths (populated by load_nodes).
    This is a pure in-memory lookup - no database queries.

    For single-hop joins:
        fact -> dimension (direct link)

    For multi-hop joins (role like "customer->home"):
        fact -> customer -> location

    If no role is specified, will find ANY path to the dimension (first match).
    This handles cases where the dimension link has a role but the user
    doesn't specify one.

    Returns None if no path found.
    """
    if not from_node.current:
        return None

    source_revision_id = from_node.current.id
    role_path = role or ""

    # Look up preloaded path with exact role match
    key = (source_revision_id, target_dim_name, role_path)
    links = ctx.join_paths.get(key)

    if links:
        # Path found in preloaded cache
        return JoinPath(
            links=links,
            target_dimension=links[-1].dimension,
            role=role,
        )

    # Fallback: if no role specified, find ANY path to this dimension
    # This handles cases where the dimension link has a role but user didn't specify one
    if not role:
        for (src_id, dim_name, stored_role), path_links in ctx.join_paths.items():
            if src_id == source_revision_id and dim_name == target_dim_name:
                logger.debug(
                    f"[BuildV3] Using path with role '{stored_role}' for "
                    f"dimension {target_dim_name} (no role specified)",
                )
                return JoinPath(
                    links=path_links,
                    target_dimension=path_links[-1].dimension,
                    role=stored_role or None,
                )

    return None


def can_skip_join_for_dimension(
    dim_ref: DimensionRef,
    join_path: Optional[JoinPath],
    parent_node: Node,
) -> tuple[bool, Optional[str]]:
    """
    Check if we can skip joining to the dimension and use a local column instead.

    This optimization applies when the requested dimension column is the join key
    itself. For example, if requesting v3.customer.customer_id and the join is:
        v3.order_details.customer_id = v3.customer.customer_id
    We can use v3.order_details.customer_id directly without joining.

    Args:
        dim_ref: The parsed dimension reference
        join_path: The join path to the dimension (if any)
        parent_node: The parent/fact node

    Returns:
        Tuple of (can_skip: bool, local_column_name: str | None)
    """
    if not join_path or not join_path.links:
        return False, None

    # Only optimize single-hop joins for now
    if len(join_path.links) > 1:
        return False, None

    link = join_path.links[0]

    # Get the dimension column being requested (fully qualified)
    dim_col_fqn = f"{dim_ref.node_name}{SEPARATOR}{dim_ref.column_name}"

    # Check if this dimension column is in the foreign keys mapping
    if parent_col := link.foreign_keys_reversed.get(dim_col_fqn):
        return True, get_short_name(parent_col)
    return False, None


def resolve_dimensions(
    ctx: BuildContext,
    parent_node: Node,
) -> list[ResolvedDimension]:
    """
    Resolve all requested dimensions to their join paths.

    Includes optimization: if the requested dimension is the join key itself,
    we skip the join and use the local column instead.

    Returns a list of ResolvedDimension objects with join path information.
    """
    resolved = []

    for dim in ctx.dimensions:
        dim_ref = parse_dimension_ref(dim)

        # Check if it's a local dimension (column on the parent node itself)
        is_local = False
        if dim_ref.node_name == parent_node.name:
            is_local = True
        elif not dim_ref.node_name:
            # No node specified, assume it's local
            is_local = True
            dim_ref.node_name = parent_node.name

        if is_local:
            resolved.append(
                ResolvedDimension(
                    original_ref=dim,
                    node_name=dim_ref.node_name,
                    column_name=dim_ref.column_name,
                    role=dim_ref.role,
                    join_path=None,
                    is_local=True,
                ),
            )
        else:
            # Need to find join path
            join_path = find_join_path(
                ctx,
                parent_node,
                dim_ref.node_name,
                dim_ref.role,
            )

            if not join_path and dim_ref.role:
                # Try finding via role path
                # For "v3.date.month[customer->registration]", the target is v3.date
                # but the role path is through customer first
                role_parts = dim_ref.role.split("->")
                if len(role_parts) > 1:
                    # Multi-hop: find path through intermediate dimensions
                    join_path = find_join_path(
                        ctx,
                        parent_node,
                        dim_ref.node_name,
                        dim_ref.role,
                    )

            # Optimization: if requesting the join key column, skip the join
            can_skip, local_col = can_skip_join_for_dimension(
                dim_ref,
                join_path,
                parent_node,
            )
            if can_skip and local_col:
                logger.info(
                    f"[BuildV3] Skipping join for {dim} - using local column {local_col}",
                )
                resolved.append(
                    ResolvedDimension(
                        original_ref=dim,
                        node_name=parent_node.name,  # Use parent node
                        column_name=local_col,  # Use local column name
                        role=dim_ref.role,
                        join_path=None,  # No join needed!
                        is_local=True,
                    ),
                )
            else:
                resolved.append(
                    ResolvedDimension(
                        original_ref=dim,
                        node_name=dim_ref.node_name,
                        column_name=dim_ref.column_name,
                        role=dim_ref.role,
                        join_path=join_path,
                        is_local=False,
                    ),
                )

    return resolved


def build_join_clause(
    ctx: BuildContext,
    link: DimensionLink,
    left_alias: str,
    right_alias: str,
) -> ast.Join:
    """
    Build a JOIN clause AST from a dimension link.

    Args:
        ctx: Build context
        link: The dimension link defining the join
        left_alias: Alias for the left (source) table
        right_alias: Alias for the right (dimension) table

    Returns:
        AST Join node
    """
    # Parse the join SQL to get the ON clause
    # link.join_sql looks like: "v3.order_details.customer_id = v3.customer.customer_id"
    join_sql = link.join_sql

    # Replace the original node names with aliases in the join condition
    left_node_name = link.node_revision.name
    right_node_name = link.dimension.name

    # Build a simple ON clause by parsing the join SQL
    # We'll create a binary comparison
    on_clause = parse(f"SELECT 1 WHERE {join_sql}").select.where

    # Now we need to rewrite column references to use our aliases
    def rewrite_column_refs(expr):
        """Recursively rewrite column references to use table aliases."""
        if isinstance(expr, ast.Column):
            if expr.name and expr.name.namespace:
                full_name = expr.identifier()
                if full_name.startswith(left_node_name + SEPARATOR):
                    col_name = full_name[len(left_node_name) + 1 :]
                    expr.name = ast.Name(col_name, namespace=ast.Name(left_alias))
                elif full_name.startswith(right_node_name + SEPARATOR):
                    col_name = full_name[len(right_node_name) + 1 :]
                    expr.name = ast.Name(col_name, namespace=ast.Name(right_alias))

        # Recurse into children
        for child in expr.children if hasattr(expr, "children") else []:
            if child:
                rewrite_column_refs(child)

    if on_clause:
        rewrite_column_refs(on_clause)

    # Determine join type (as string for ast.Join)
    from datajunction_server.models.dimensionlink import JoinType

    join_type_str = "LEFT OUTER"  # Default
    if link.join_type == JoinType.INNER:
        join_type_str = "INNER"
    elif link.join_type == JoinType.LEFT:
        join_type_str = "LEFT OUTER"
    elif link.join_type == JoinType.RIGHT:
        join_type_str = "RIGHT OUTER"
    elif link.join_type == JoinType.FULL:
        join_type_str = "FULL OUTER"

    # Build the right table reference (use materialized table if available)
    # Look up full node from ctx.nodes to avoid lazy loading
    dim_node = ctx.nodes.get(link.dimension.name, link.dimension)
    right_table_parts, _ = get_table_reference_parts_with_materialization(
        ctx,
        dim_node,
    )
    right_table_name = make_name(SEPARATOR.join(right_table_parts))

    # Create the join
    right_expr: ast.Expression = cast(
        ast.Expression,
        ast.Alias(
            child=ast.Table(name=right_table_name),
            alias=ast.Name(right_alias),
        ),
    )
    join = ast.Join(
        join_type=join_type_str,
        right=right_expr,
        criteria=ast.JoinCriteria(on=on_clause) if on_clause else None,
    )

    return join


# =============================================================================
# Metric Analysis
# =============================================================================


def get_metric_expression_ast(ctx: BuildContext, metric_node: Node) -> ast.Expression:
    """
    Extract the metric expression AST from the metric's query.

    A metric query looks like: SELECT <expression> FROM <parent>
    We want to extract just the expression as an AST node.

    Returns:
        The expression AST node (with alias removed if present)
    """
    if not metric_node.current or not metric_node.current.query:
        raise DJInvalidInputException(f"Metric {metric_node.name} has no query")

    # Use cached parsed query
    query_ast = ctx.get_parsed_query(metric_node)
    if not query_ast.select.projection:
        raise DJInvalidInputException(f"Metric {metric_node.name} has no projection")

    # Get the first projection expression (the metric expression)
    expr = query_ast.select.projection[0]

    # Remove alias if present - we want the raw expression
    if isinstance(expr, ast.Alias):
        expr = expr.child

    # Copy the expression before modifying to protect the cache
    expr = expr.copy()
    if isinstance(expr, ast.Aliasable) and expr.alias:
        expr.alias = None

    # Clear parent reference so we can attach to new query
    expr.clear_parent()

    return cast(ast.Expression, expr)


async def decompose_metric(
    session: AsyncSession,
    metric_node: Node,
    *,
    nodes_cache: dict[str, Node] | None = None,
    parent_map: dict[str, list[str]] | None = None,
) -> DecomposedMetricInfo:
    """
    Decompose a metric into its constituent components.

    Uses MetricComponentExtractor to break down aggregations like:
    - SUM(x) -> [sum_x component]
    - AVG(x) -> [sum_x component, count_x component]
    - COUNT(DISTINCT x) -> [distinct_x component with LIMITED aggregability]

    Args:
        session: Database session
        metric_node: The metric node to decompose
        nodes_cache: Optional dict of node_name -> Node. If provided along with
            parent_map, avoids database queries by using cached data.
        parent_map: Optional dict of child_name -> list of parent_names.
            Required if nodes_cache is provided.

    Returns:
        DecomposedMetricInfo with components, combiner expression, and aggregability
    """
    if not metric_node.current:
        raise DJInvalidInputException(
            f"Metric {metric_node.name} has no current revision",
        )

    # Use the MetricComponentExtractor with optional cache
    extractor = MetricComponentExtractor(metric_node.current.id)
    components, derived_ast = await extractor.extract(
        session,
        nodes_cache=nodes_cache,
        parent_map=parent_map,
        metric_node=metric_node,
    )

    # Extract combiner expression from the derived query AST
    # The first projection element is the metric expression with component references
    combiner = (
        str(derived_ast.select.projection[0]) if derived_ast.select.projection else ""
    )

    # Determine overall aggregability (worst case among components)
    if not components:
        # No decomposable aggregations found - treat as NONE
        aggregability = Aggregability.NONE
    elif any(c.rule.type == Aggregability.NONE for c in components):
        aggregability = Aggregability.NONE
    elif any(c.rule.type == Aggregability.LIMITED for c in components):
        aggregability = Aggregability.LIMITED
    else:
        aggregability = Aggregability.FULL

    return DecomposedMetricInfo(
        metric_node=metric_node,
        components=components,
        aggregability=aggregability,
        combiner=combiner,
        derived_ast=derived_ast,
    )


def build_component_expression(component: MetricComponent) -> ast.Expression:
    """
    Build the accumulate expression AST for a metric component.

    For simple aggregations like SUM, this is: SUM(expression)
    For templates like "SUM(POWER({}, 2))", expands to: SUM(POWER(expression, 2))
    """
    if not component.aggregation:
        # No aggregation - just return the expression as a column
        return ast.Column(name=ast.Name(component.expression))

    # Check if it's a template with {}
    if "{" in component.aggregation:
        # Template like "SUM(POWER({}, 2))" - expand it
        expanded = component.aggregation.replace("{}", component.expression)
        # Parse as expression
        expr_ast = parse(f"SELECT {expanded}").select.projection[0]
        if isinstance(expr_ast, ast.Alias):
            expr_ast = expr_ast.child
        expr_ast.clear_parent()
        return cast(ast.Expression, expr_ast)
    else:
        # Simple function name like "SUM" - build SUM(expression)
        arg_expr = parse(f"SELECT {component.expression}").select.projection[0]
        func = ast.Function(
            name=ast.Name(component.aggregation),
            args=[cast(ast.Expression, arg_expr)],
        )
        return func


def get_fact_parent(ctx: BuildContext, metric_node: Node) -> Node:
    """
    Get the fact/transform parent of a metric, traversing through derived metrics.

    For base metrics: returns the direct parent (a fact/transform)
    For derived metrics: returns None (caller should get base metrics first)
    """
    parent_names = ctx.parent_map.get(metric_node.name, [])
    if not parent_names:
        raise DJInvalidInputException(f"Metric {metric_node.name} has no parent node")

    # Check if first parent is a metric (derived) or fact/transform (base)
    first_parent_name = parent_names[0]
    first_parent = ctx.nodes.get(first_parent_name)

    if not first_parent:
        raise DJInvalidInputException(f"Parent node not found: {first_parent_name}")

    return first_parent


def get_base_metrics_for_derived(ctx: BuildContext, metric_node: Node) -> list[Node]:
    """
    For a derived metric, get all the base metrics it depends on.

    Returns list of base metric nodes (metrics that SELECT FROM a fact/transform, not other metrics).
    """
    base_metrics = []
    visited = set()

    def collect_bases(node: Node):
        if node.name in visited:
            return
        visited.add(node.name)

        parent_names = ctx.parent_map.get(node.name, [])
        for parent_name in parent_names:
            parent = ctx.nodes.get(parent_name)
            if not parent:
                continue

            if parent.type == NodeType.METRIC:
                # Parent is also a metric - recurse
                collect_bases(parent)
            else:
                # Parent is a fact/transform - this is a base metric
                base_metrics.append(node)
                break  # Found the base, don't check other parents

    collect_bases(metric_node)
    return base_metrics


def is_derived_metric(ctx: BuildContext, metric_node: Node) -> bool:
    """Check if a metric is derived (references other metrics) vs base (references fact/transform)."""
    parent_names = ctx.parent_map.get(metric_node.name, [])
    if not parent_names:
        return False

    first_parent = ctx.nodes.get(parent_names[0])
    return first_parent is not None and first_parent.type == NodeType.METRIC


def analyze_grain_groups(
    metric_group: MetricGroup,
    requested_dimensions: list[str],
) -> list[GrainGroup]:
    """
    Analyze a MetricGroup and split it into GrainGroups based on aggregability.

    Each GrainGroup contains components that can be computed at the same grain.

    Rules:
    - FULL aggregability: grain = requested dimensions
    - LIMITED aggregability: grain = requested dimensions + level columns
    - NONE aggregability: grain = native grain (PK of parent)

    Args:
        metric_group: MetricGroup with decomposed metrics
        requested_dimensions: Dimensions requested by user (column names only)

    Returns:
        List of GrainGroups, one per unique grain
    """
    parent_node = metric_group.parent_node

    # Group components by their effective grain
    # Key: (aggregability, tuple of additional grain columns)
    grain_buckets: dict[
        tuple[Aggregability, tuple[str, ...]],
        list[tuple[Node, MetricComponent]],
    ] = {}

    for metric_node, component in metric_group.get_all_components():
        agg_type = component.rule.type

        # Explicitly type the key to satisfy mypy
        key: tuple[Aggregability, tuple[str, ...]]
        if agg_type == Aggregability.FULL:
            # FULL: no additional grain columns needed
            key = (Aggregability.FULL, ())
        elif agg_type == Aggregability.LIMITED:
            # LIMITED: add level columns to grain
            level_cols = tuple(sorted(component.rule.level or []))
            key = (Aggregability.LIMITED, level_cols)
        else:  # NONE
            # NONE: use native grain (PK columns)
            native_grain = get_native_grain(parent_node)
            key = (Aggregability.NONE, tuple(sorted(native_grain)))

        if key not in grain_buckets:
            grain_buckets[key] = []
        grain_buckets[key].append((metric_node, component))

    # Convert buckets to GrainGroup objects
    grain_groups = []
    for (agg_type, grain_cols), components in grain_buckets.items():
        grain_groups.append(
            GrainGroup(
                parent_node=parent_node,
                aggregability=agg_type,
                grain_columns=list(grain_cols),
                components=components,
            ),
        )

    # Sort groups: FULL first, then LIMITED, then NONE (for consistent output)
    agg_order = {Aggregability.FULL: 0, Aggregability.LIMITED: 1, Aggregability.NONE: 2}
    grain_groups.sort(
        key=lambda g: (agg_order.get(g.aggregability, 3), g.grain_columns),
    )

    return grain_groups


def merge_grain_groups(grain_groups: list[GrainGroup]) -> list[GrainGroup]:
    """
    Merge compatible grain groups from the same parent into single CTEs.

    Grain groups can be merged if they share the same parent node. The merged
    group uses the finest grain (union of all grain columns) and outputs raw
    columns instead of pre-aggregated values. Aggregations are then applied
    in the final SELECT.

    This optimization reduces duplicate CTEs and JOINs when multiple metrics
    with different aggregabilities come from the same parent.

    Args:
        grain_groups: List of grain groups to potentially merge

    Returns:
        List of grain groups with compatible groups merged
    """
    from collections import defaultdict

    # Group by parent node name
    by_parent: dict[str, list[GrainGroup]] = defaultdict(list)
    for gg in grain_groups:
        by_parent[gg.parent_node.name].append(gg)

    merged_groups: list[GrainGroup] = []

    for parent_name, parent_groups in by_parent.items():
        if len(parent_groups) == 1:
            # Only one group for this parent - no merge needed
            merged_groups.append(parent_groups[0])
        else:
            # Multiple groups for same parent - merge them
            merged = _merge_parent_grain_groups(parent_groups)
            merged_groups.append(merged)

    # Sort for deterministic output
    agg_order = {Aggregability.FULL: 0, Aggregability.LIMITED: 1, Aggregability.NONE: 2}
    merged_groups.sort(
        key=lambda g: (
            g.parent_node.name,
            agg_order.get(g.aggregability, 3),
            g.grain_columns,
        ),
    )

    return merged_groups


def _merge_parent_grain_groups(groups: list[GrainGroup]) -> GrainGroup:
    """
    Merge multiple grain groups from the same parent into one.

    The merged group:
    - Uses the finest grain (union of all grain columns)
    - Has aggregability = worst case (NONE > LIMITED > FULL)
    - Contains all components from all groups
    - Has is_merged=True to signal that aggregations happen in final SELECT
    - Tracks original component aggregabilities for proper final aggregation
    """
    if not groups:
        raise ValueError("Cannot merge empty list of grain groups")

    parent_node = groups[0].parent_node

    # Collect all components and track their original aggregabilities
    all_components: list[tuple[Node, MetricComponent]] = []
    component_aggregabilities: dict[str, Aggregability] = {}

    for gg in groups:
        for metric_node, component in gg.components:
            all_components.append((metric_node, component))
            # Track original aggregability for each component
            component_aggregabilities[component.name] = gg.aggregability

    # Compute finest grain (union of all grain columns)
    finest_grain_set: set[str] = set()
    for gg in groups:
        finest_grain_set.update(gg.grain_columns)
    finest_grain = sorted(finest_grain_set)

    # Determine worst-case aggregability
    # NONE > LIMITED > FULL (NONE is worst, forces finest grain)
    agg_order = {Aggregability.FULL: 0, Aggregability.LIMITED: 1, Aggregability.NONE: 2}
    worst_agg = max(
        groups,
        key=lambda g: agg_order.get(g.aggregability, 0),
    ).aggregability

    return GrainGroup(
        parent_node=parent_node,
        aggregability=worst_agg,
        grain_columns=finest_grain,
        components=all_components,
        is_merged=True,
        component_aggregabilities=component_aggregabilities,
    )


def compute_metric_layers(
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
) -> list[list[str]]:
    """
    Compute the order in which metrics should be evaluated.

    Returns a list of layers, where each layer contains metric names
    that can be computed in parallel (no dependencies on each other).
    """
    dependencies: dict[str, set[str]] = {}

    for metric_name, decomposed in decomposed_metrics.items():
        deps: set[str] = set()

        metric_node = decomposed.metric_node
        base_metrics = get_base_metrics_for_derived(ctx, metric_node)
        for base in base_metrics:
            if base.name in decomposed_metrics:
                deps.add(base.name)

        dependencies[metric_name] = deps

    layers: list[list[str]] = []
    computed: set[str] = set()

    while len(computed) < len(decomposed_metrics):
        layer = [
            name
            for name, deps in dependencies.items()
            if name not in computed and deps <= computed
        ]

        if not layer:
            remaining = set(decomposed_metrics.keys()) - computed
            raise DJInvalidInputException(
                f"Circular dependency detected in metrics: {remaining}",
            )

        layers.append(sorted(layer))
        computed.update(layer)

    return layers


# =============================================================================
# Filter Utilities
# =============================================================================


def parse_filter(filter_str: str) -> ast.Expression:
    """
    Parse a filter string into an AST expression.

    The filter string is a SQL expression like:
    - "v3.product.category = 'Electronics'"
    - "v3.date.year[order] >= 2024"
    - "status IN ('active', 'pending')"

    Args:
        filter_str: A SQL predicate expression

    Returns:
        The parsed AST expression

    Example:
        >>> expr = parse_filter("v3.product.category = 'Electronics'")
        >>> # Returns ast.BinaryOp with comparison
    """
    # Parse as "SELECT 1 WHERE <filter>" and extract the WHERE clause
    query = parse(f"SELECT 1 WHERE {filter_str}")
    if query.select.where is None:
        raise DJInvalidInputException(f"Failed to parse filter: {filter_str}")
    return query.select.where


def resolve_filter_references(
    filter_ast: ast.Expression,
    column_aliases: dict[str, str],
    cte_alias: str | None = None,
) -> ast.Expression:
    """
    Resolve dimension/column references in a filter AST to their actual column aliases.

    This replaces references like "v3.product.category" with the appropriate
    table-qualified column reference like "t2.category".

    Args:
        filter_ast: The parsed filter expression (will be mutated!)
        column_aliases: Map from dimension ref (e.g., "v3.product.category") to alias (e.g., "category")
        cte_alias: Optional CTE alias to prefix column refs with (e.g., "order_details_0")

    Returns:
        The modified filter AST (same object, mutated in place)

    Example:
        >>> filter_ast = parse_filter("v3.product.category = 'Electronics'")
        >>> aliases = {"v3.product.category": "category"}
        >>> resolve_filter_references(filter_ast, aliases, "t2")
        >>> # Now filter_ast contains "t2.category = 'Electronics'"
    """

    def resolve_refs(node: ast.Expression) -> None:
        """Recursively resolve column references in the AST."""
        if isinstance(node, ast.Column):
            # Reconstruct the full reference from the column name
            # Column names may be namespaced (e.g., v3.product.category)
            full_ref = _extract_full_column_ref(node)

            if full_ref in column_aliases:
                col_alias = column_aliases[full_ref]
                # Replace with table-aliased reference
                if cte_alias:
                    node.name = ast.Name(col_alias, namespace=ast.Name(cte_alias))
                else:
                    node.name = ast.Name(col_alias)

        # Recursively process children
        if hasattr(node, "children"):
            for child in node.children:
                if child and isinstance(child, ast.Expression):
                    resolve_refs(child)

    resolve_refs(filter_ast)
    return filter_ast


def _extract_full_column_ref(col: ast.Column) -> str:
    """
    Extract the full reference string from a Column AST node.

    Handles both simple columns (e.g., "category") and namespaced columns
    (e.g., "v3.product.category").
    """
    parts: list[str] = []

    def collect_names(name_node: ast.Name | None) -> None:
        if name_node is None:
            return
        if name_node.namespace:
            collect_names(name_node.namespace)
        parts.append(name_node.name)

    collect_names(col.name)
    return ".".join(parts)


def combine_filters(filters: list[ast.Expression]) -> ast.Expression | None:
    """
    Combine multiple filter expressions with AND.

    Args:
        filters: List of filter AST expressions

    Returns:
        Combined expression or None if empty list

    Example:
        >>> f1 = parse_filter("status = 'active'")
        >>> f2 = parse_filter("year >= 2024")
        >>> combined = combine_filters([f1, f2])
        >>> # Returns (status = 'active') AND (year >= 2024)
    """
    if not filters:
        return None

    if len(filters) == 1:
        return filters[0]

    from functools import reduce

    return reduce(lambda a, b: ast.BinaryOp.And(a, b), filters)


def parse_and_resolve_filters(
    filter_strs: list[str],
    column_aliases: dict[str, str],
    cte_alias: str | None = None,
) -> ast.Expression | None:
    """
    Parse filter strings and resolve references, returning combined WHERE clause.

    This is a convenience function that combines parse_filter, resolve_filter_references,
    and combine_filters.

    Args:
        filter_strs: List of filter strings
        column_aliases: Map from dimension ref to alias
        cte_alias: Optional CTE alias to prefix column refs with

    Returns:
        Combined filter expression or None if no filters

    Example:
        >>> filters = ["v3.product.category = 'Electronics'", "status = 'active'"]
        >>> aliases = {"v3.product.category": "category", "status": "status"}
        >>> where_clause = parse_and_resolve_filters(filters, aliases, "t1")
    """
    if not filter_strs:
        return None

    parsed_filters = []
    for f in filter_strs:
        print("parsing filter", f)
        filter_ast = parse_filter(f)
        resolved = resolve_filter_references(
            deepcopy(filter_ast),  # Make a copy to avoid mutating cache
            column_aliases,
            cte_alias,
        )
        parsed_filters.append(resolved)

    return combine_filters(parsed_filters)
