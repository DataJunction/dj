"""
Dimension + join path resolution and building functions
"""

from __future__ import annotations

from http import HTTPStatus
import logging
from typing import Optional, cast

from datajunction_server.construction.build_v3.utils import (
    get_short_name,
    make_name,
)
from datajunction_server.errors import DJException
from datajunction_server.construction.build_v3.materialization import (
    get_table_reference_parts_with_materialization,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    DimensionRef,
    JoinPath,
    ResolvedDimension,
)
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import SEPARATOR

logger = logging.getLogger(__name__)


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
    else:  # pragma: no cover
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
    if not from_node.current:  # pragma: no cover
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

    # Fallback: if exact role not found, try to find any path to this dimension
    # This handles cases where:
    # 1. User didn't specify a role, but link has one
    # 2. User specified a role that doesn't match, so we use the actual link's role
    # We prefer empty role (null) over named roles when falling back
    fallback_paths = []
    for (src_id, dim_name, stored_role), path_links in ctx.join_paths.items():
        if src_id == source_revision_id and dim_name == target_dim_name:
            fallback_paths.append((stored_role, path_links))

    if fallback_paths:
        # Prefer paths with no role (empty string) as they're the "default" link
        # Also prefer shorter paths (fewer hops) over longer ones
        fallback_paths.sort(
            key=lambda x: (len(x[1]), x[0] != "", x[0]),
        )  # Shortest path, then empty role, then alphabetical
        stored_role, path_links = fallback_paths[0]

        if role and stored_role != role_path:
            logger.info(  # pragma: no cover
                "[BuildV3] Role mismatch: requested '%s' but using '%s' for dimension %s",
                role,
                stored_role or "null",
                target_dim_name,
            )

        return JoinPath(
            links=path_links,
            target_dimension=path_links[-1].dimension,
            role=stored_role or None,
        )

    return None  # pragma: no cover


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
    if not join_path or not join_path.links:  # pragma: no cover
        return False, None

    # Only optimize single-hop joins for now
    if len(join_path.links) > 1:
        return False, None

    link = join_path.links[0]

    # Get the dimension column being requested (fully qualified)
    dim_col_fqn = f"{dim_ref.node_name}{SEPARATOR}{dim_ref.column_name}"

    # Check if this dimension column is in the foreign keys mapping
    if parent_col := link.foreign_keys_reversed.get(dim_col_fqn):
        # Join can be skipped - the FK column on the parent matches the requested dim
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
        # Note: A dimension with a role is not local, even if the node name matches
        # (e.g., employee[manager] requires a self-join, not a local column)
        is_local = False
        if dim_ref.node_name == parent_node.name and not dim_ref.role:
            is_local = True
        elif not dim_ref.node_name:  # pragma: no cover
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

            if not join_path and dim_ref.role:  # pragma: no cover
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

            # Validate that we found a join path
            if not join_path:
                raise DJException(  # pragma: no cover
                    http_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                    message=f"Cannot find join path from {parent_node.name} to dimension {dim_ref.node_name}. "
                    f"Please create a dimension link between these nodes.",
                )

            # Optimization: if requesting the join key column, skip the join
            can_skip, local_col = can_skip_join_for_dimension(
                dim_ref,
                join_path,
                parent_node,
            )
            if can_skip and local_col:
                # Store the mapping for filter resolution
                # This allows filters referencing the dimension name to resolve to the local column
                ctx.skip_join_column_mapping[dim] = local_col
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


def _rewrite_column_refs_with_aliases(
    expr: ast.Node,
    left_node_name: str,
    right_node_name: str,
    left_alias: str,
    right_alias: str,
    is_self_join: bool,
    occurrence_count: list[int],
) -> None:
    """
    Recursively rewrite column references in a JOIN condition to use table aliases.

    This function walks an AST expression tree and replaces fully-qualified column
    references (e.g., "node.column") with aliased references (e.g., "t1.column").

    For regular joins where left and right nodes are different:
        - Columns matching left_node_name get rewritten with left_alias
        - Columns matching right_node_name get rewritten with right_alias

    For self-joins where both nodes are the same:
        - Uses occurrence order to assign aliases
        - First occurrence of the node name gets left_alias
        - Second occurrence gets right_alias
        - Example: "employee.manager_id = employee.employee_id"
          becomes "t1.manager_id = manager.employee_id"

    Args:
        expr: AST expression to rewrite (typically a JOIN ON clause)
        left_node_name: Full name of the left/source node (e.g., "default.orders")
        right_node_name: Full name of the right/dimension node (e.g., "default.customer")
        left_alias: Alias to use for left node columns (e.g., "t1")
        right_alias: Alias to use for right node columns (e.g., "t2" or role like "manager")
        is_self_join: True if joining a node to itself
        occurrence_count: Mutable counter for self-join occurrence tracking (modified in-place)
    """
    if isinstance(expr, ast.Column):
        if expr.name and expr.name.namespace:  # pragma: no branch
            full_name = expr.identifier()

            if is_self_join:
                # Self-join: both left and right node names are the same
                # Use occurrence order to determine which alias to apply
                if full_name.startswith(
                    left_node_name + SEPARATOR,
                ):  # pragma: no branch
                    col_name = full_name[len(left_node_name) + 1 :]
                    # First occurrence -> left_alias, subsequent -> right_alias
                    if occurrence_count[0] == 0:
                        expr.name = ast.Name(col_name, namespace=ast.Name(left_alias))
                        occurrence_count[0] += 1
                    else:
                        expr.name = ast.Name(
                            col_name,
                            namespace=ast.Name(right_alias),
                        )
            else:
                # Regular join: use node name matching to determine alias
                if full_name.startswith(left_node_name + SEPARATOR):
                    col_name = full_name[len(left_node_name) + 1 :]
                    expr.name = ast.Name(col_name, namespace=ast.Name(left_alias))
                elif full_name.startswith(
                    right_node_name + SEPARATOR,
                ):  # pragma: no branch
                    col_name = full_name[len(right_node_name) + 1 :]
                    expr.name = ast.Name(col_name, namespace=ast.Name(right_alias))

    # Recurse into child expressions
    for child in expr.children if hasattr(expr, "children") else []:
        if child:  # pragma: no branch
            _rewrite_column_refs_with_aliases(
                child,
                left_node_name,
                right_node_name,
                left_alias,
                right_alias,
                is_self_join,
                occurrence_count,
            )


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

    # Detect self-join: when joining a dimension to itself
    is_self_join = left_node_name == right_node_name

    # Rewrite column references to use table aliases
    # For self-joins: track occurrence order (first occurrence -> left, second -> right)
    # For regular joins: match by node name (left node -> left alias, right node -> right alias)
    occurrence_count = [0]  # Mutable counter for self-join occurrence tracking
    if on_clause:  # pragma: no branch
        _rewrite_column_refs_with_aliases(
            on_clause,
            left_node_name,
            right_node_name,
            left_alias,
            right_alias,
            is_self_join,
            occurrence_count,
        )

    # Determine join type (as string for ast.Join)
    from datajunction_server.models.dimensionlink import JoinType

    join_type_str = "LEFT OUTER"  # Default
    if link.join_type == JoinType.INNER:  # pragma: no cover
        join_type_str = "INNER"
    elif link.join_type == JoinType.LEFT:
        join_type_str = "LEFT OUTER"
    elif link.join_type == JoinType.RIGHT:  # pragma: no cover
        join_type_str = "RIGHT OUTER"
    elif link.join_type == JoinType.FULL:  # pragma: no cover
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
