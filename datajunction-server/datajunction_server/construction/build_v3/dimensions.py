"""
Dimension + join path resolution and building functions
"""

from __future__ import annotations

import difflib
from http import HTTPStatus
import logging
from typing import Optional, cast

from datajunction_server.construction.build_v3.utils import (
    get_short_name,
    make_name,
)
from datajunction_server.errors import (
    DJError,
    DJException,
    DJInvalidInputException,
    ErrorCode,
)
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

    A reference without an extractable node (e.g. a bare ``status``) is
    rejected — DJ can't route the reference to a CTE without knowing the
    owning node.
    """
    from datajunction_server.errors import DJInvalidInputException

    # Extract role if present
    role = None
    if "[" in dim_ref:
        dim_part, role_part = dim_ref.rsplit("[", 1)
        role = role_part.rstrip("]")
    else:
        dim_part = dim_ref

    # Split into node and column
    parts = dim_part.rsplit(SEPARATOR, 1)
    if len(parts) != 2:
        raise DJInvalidInputException(
            f"Reference `{dim_ref}` is not fully qualified. Use the "
            f"`node.column` form (e.g., `v3.order_details.status`) so DJ "
            f"can route the reference to the correct node.",
        )
    node_name, column_name = parts

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
) -> tuple[bool, Optional[str], int]:
    """
    Check whether the trailing hops of a join path can be elided because the
    requested column is foreign-key-aligned with a column on a closer node.

    Walks backwards through the join path, threading the requested column
    through each link's `foreign_keys_reversed` mapping. As long as the lookup
    succeeds, the corresponding join can be dropped: the column is already
    available on the previous node via FK alignment.

    Behaviors:
    - **Full elision** (single-hop or multi-hop): every hop's FK chain succeeds
      back to the parent node. The requested column resolves to a local column
      on the fact; no joins needed.
    - **Partial elision**: the chain breaks after some number of trailing hops.
      We can drop those trailing hops; the column lives on the deepest dim we
      still join to. The caller emits a reduced join path.

    Examples:
    - Single-hop full: requesting v3.customer.customer_id where the only link
      is v3.order_details.customer_id = v3.customer.customer_id. Returns
      (True, "customer_id", 1) and the caller observes 1 == len(links) → full.
    - Two-hop full: requesting v3.account.account_id where each link is on
      account_id and the fact has account_id directly. Returns (True,
      "account_id", 2) and the caller treats it as full elision.
    - Two-hop partial: fact -> allocation_day -> account, requesting
      account.account_id. The link allocation_day -> account aligns
      account.account_id with allocation_day.account_id, but the link
      fact -> allocation_day does NOT carry that column further (it's keyed on
      something else). Returns (True, "account_id", 1): drop only the last
      hop; the column lives on allocation_day's CTE.

    Args:
        dim_ref: The parsed dimension reference
        join_path: The join path to the dimension (if any)
        parent_node: The parent/fact node

    Returns:
        ``(can_skip, local_column_short_name, num_hops_elided)``. When
        ``can_skip`` is False, the other two values are ``None``/``0``.
        When ``num_hops_elided == len(join_path.links)`` the column lives on
        ``parent_node`` (full elision); otherwise it lives on the dim at
        ``join_path.links[-num_hops_elided - 1].dimension`` (partial elision).
    """
    log_prefix = (
        f"[skip-join-debug] dim_ref={dim_ref.node_name}{SEPARATOR}"
        f"{dim_ref.column_name} parent_node={parent_node.name}"
    )

    if not join_path or not join_path.links:  # pragma: no cover
        logger.info(
            "%s — skipping optimization: empty join_path (links=%s)",
            log_prefix,
            None if not join_path else join_path.links,
        )
        return False, None, 0

    # Peel off trailing hops as long as the FK alignment carries the requested
    # column to the previous node. Stop as soon as a hop's FK map doesn't
    # contain the column we currently need.
    current_col_fqn = f"{dim_ref.node_name}{SEPARATOR}{dim_ref.column_name}"
    hops_elided = 0
    for link in reversed(join_path.links):
        prev_col_fqn = link.foreign_keys_reversed.get(current_col_fqn)
        if prev_col_fqn is None:
            break
        current_col_fqn = prev_col_fqn
        hops_elided += 1

    if hops_elided == 0:
        logger.info(
            "%s — skipping optimization: column %r is not an FK target of "
            "the terminal link %s -> %s; available keys=%s",
            log_prefix,
            current_col_fqn,
            join_path.links[-1].node_revision.name,
            join_path.links[-1].dimension.name,
            sorted(join_path.links[-1].foreign_keys_reversed.keys()),
        )
        return False, None, 0

    logger.info(
        "%s — APPLYING optimization: eliding %d of %d hop(s); column lives at %r",
        log_prefix,
        hops_elided,
        len(join_path.links),
        current_col_fqn,
    )
    return True, get_short_name(current_col_fqn), hops_elided


def _format_column_validation_error(
    node: Node,
    column_name: str,
    original_ref: str,
) -> str | None:
    """
    Return an error message string when ``column_name`` is not on ``node``,
    or ``None`` when the column is valid (or the node has no loaded columns).

    This batches multiple validation errors into a single exception instead
    of raising on the first failure.
    """
    if not node.current or not node.current.columns:  # pragma: no cover
        return None

    available = [col.name for col in node.current.columns]
    if column_name in available:
        return None

    suggestions = difflib.get_close_matches(column_name, available, n=3, cutoff=0.6)
    suffix = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
    return (
        f"Column `{column_name}` does not exist on node `{node.name}` "
        f"(referenced as `{original_ref}`).{suffix}"
    )


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
    column_errors: list[str] = []

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
            err = _format_column_validation_error(
                parent_node,
                dim_ref.column_name,
                dim,
            )
            if err is not None:
                column_errors.append(err)
                continue
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

            # Validate column exists on the target dimension node before we
            # commit to a column reference that might never resolve.
            err = _format_column_validation_error(
                join_path.target_dimension,
                dim_ref.column_name,
                dim,
            )
            if err is not None:
                column_errors.append(err)
                continue

            # Optimization: when the requested column is FK-aligned with a
            # column on a closer node, drop the trailing joins.
            can_skip, local_col, hops_elided = can_skip_join_for_dimension(
                dim_ref,
                join_path,
                parent_node,
            )
            if can_skip and local_col and hops_elided == len(join_path.links):
                # Full elision: column lives on the parent fact/transform.
                # Filters referencing the dim resolve to the parent's local
                # column.
                ctx.skip_join_column_mapping[dim] = local_col
                resolved.append(
                    ResolvedDimension(
                        original_ref=dim,
                        node_name=parent_node.name,
                        column_name=local_col,
                        role=dim_ref.role,
                        join_path=None,
                        is_local=True,
                    ),
                )
            elif can_skip and local_col and hops_elided > 0:
                # Partial elision: keep only the leading links of the join
                # path; the column lives on the dim we stop at. We rewrite the
                # ResolvedDimension to point at that intermediate node.
                kept_links = join_path.links[:-hops_elided]
                intermediate_dim = kept_links[-1].dimension
                reduced_path = JoinPath(
                    links=kept_links,
                    target_dimension=intermediate_dim,
                    role=join_path.role,
                )
                resolved.append(
                    ResolvedDimension(
                        original_ref=dim,
                        node_name=intermediate_dim.name,
                        column_name=local_col,
                        role=dim_ref.role,
                        join_path=reduced_path,
                        is_local=False,
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

    if column_errors:
        raise DJInvalidInputException(
            errors=[
                DJError(code=ErrorCode.INVALID_COLUMN, message=msg)
                for msg in column_errors
            ],
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
    # CROSS JOINs have no ON condition, so join_sql may be empty/None
    on_clause = parse(f"SELECT 1 WHERE {join_sql}").select.where if join_sql else None

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
    elif link.join_type == JoinType.CROSS:  # pragma: no cover
        join_type_str = "CROSS"

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
