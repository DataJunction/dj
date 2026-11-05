"""
Dimension + join path resolution and building functions
"""

from __future__ import annotations

import difflib
import logging
from http import HTTPStatus
from typing import cast

from datajunction_server.construction.build_v3.materialization import (
    get_table_reference_parts_with_materialization,
)
from datajunction_server.construction.build_v3.decomposition import (
    get_base_metrics_for_derived,
    is_derived_metric,
)
from datajunction_server.construction.build_v3.dimension_refs import (
    split_dimension_ref,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    DimensionRef,
    GrainGroup,
    JoinPath,
    ResolvedDimension,
)
from datajunction_server.construction.build_v3.utils import (
    get_short_name,
    iter_namespaced_columns,
    make_name,
)
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node
from datajunction_server.errors import (
    DJError,
    DJException,
    DJInvalidInputException,
    DJWarning,
    ErrorCode,
)
from datajunction_server.models.dimensionlink import JoinCardinality
from datajunction_server.sql.decompose import (
    is_metric_duplication_sensitive,
    merge_inflates,
)
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

    dim_part, role = split_dimension_ref(dim_ref)

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
    role: str | None = None,
) -> JoinPath | None:
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


def roles_reaching_dimension(
    ctx: BuildContext,
    node_rev_id: int,
    dim_node_name: str,
) -> set[str]:
    """
    Role paths from a node revision to a dimension node, ``""`` for a role-free
    link. Read from the join paths preloaded for this build.
    """
    return {
        stored_role
        for (src_id, dim_name, stored_role) in ctx.join_paths
        if src_id == node_rev_id and dim_name == dim_node_name
    }


def can_skip_join_for_dimension(
    dim_ref: DimensionRef,
    join_path: JoinPath | None,
    parent_node: Node,
) -> tuple[bool, str | None, int]:
    """
    Check whether the trailing hops of a join path can be skipped because the
    requested column is foreign-key-aligned with a column on a closer node.

    Walks backwards through the join path, threading the requested column
    through each link's `foreign_keys_reversed` mapping. As long as the lookup
    succeeds, the corresponding join can be skipped: the column is already
    available on the previous node via FK alignment.

    Behaviors:
    - **Full skip** (single-hop or multi-hop): every hop's FK chain succeeds
      back to the parent node. The requested column resolves to a local column
      on the fact; no joins needed.
    - **Partial skip**: the chain breaks after some number of trailing hops.
      We can skip those trailing hops; the column lives on the deepest dim we
      still join to. The caller emits a reduced join path.

    Examples:
    - Single-hop full: requesting v3.customer.customer_id where the only link
      is v3.order_details.customer_id = v3.customer.customer_id. Returns
      (True, "customer_id", 1) and the caller observes 1 == len(links) → full.
    - Two-hop full: requesting v3.account.account_id where each link is on
      account_id and the fact has account_id directly. Returns (True,
      "account_id", 2) and the caller treats it as a full skip.
    - Two-hop partial: order_details -> customer -> location[home], requesting
      v3.location.location_id[home]. The link customer -> location[home]
      aligns v3.location.location_id with v3.customer.location_id, but the
      link order_details -> customer is keyed on customer_id and does not
      carry location_id further. Returns (True, "location_id", 1): skip only
      the last hop; the column lives on the customer CTE.

    Args:
        dim_ref: The parsed dimension reference
        join_path: The join path to the dimension (if any)
        parent_node: The parent/fact node

    Returns:
        ``(can_skip, local_column_short_name, num_hops_skipped)``. When
        ``can_skip`` is False, the other two values are ``None``/``0``.
        When ``num_hops_skipped == len(join_path.links)`` the column lives on
        ``parent_node`` (full skip); otherwise it lives on the dim at
        ``join_path.links[-num_hops_skipped - 1].dimension`` (partial skip).
    """
    if not join_path or not join_path.links:  # pragma: no cover
        return False, None, 0

    # Peel off trailing hops as long as the FK alignment carries the requested
    # column to the previous node. Stop as soon as a hop's FK map doesn't
    # contain the column we currently need.
    current_col_fqn = f"{dim_ref.node_name}{SEPARATOR}{dim_ref.column_name}"
    hops_skipped = 0
    for link in reversed(join_path.links):
        prev_col_fqn = link.foreign_keys_reversed.get(current_col_fqn)
        if prev_col_fqn is None:
            break
        current_col_fqn = prev_col_fqn
        hops_skipped += 1

    if hops_skipped == 0:
        return False, None, 0

    return True, get_short_name(current_col_fqn), hops_skipped


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


def _resolve_filter_only_dim(
    ctx: BuildContext,
    parent_node: Node,
    dim_ref: DimensionRef,
    original_ref: str,
) -> bool:
    """Discover explicit upstream bindings without modifying authored queries.

    Placement later checks the consumer path and resolves the whole predicate.
    Multiple bindings are retained so ambiguity is reported rather than guessed.
    """
    target = f"{dim_ref.node_name}{SEPARATOR}{dim_ref.column_name}"
    candidates: set[tuple[str, str]] = set()
    visited = {parent_node.name}
    pending = list(ctx.parent_map.get(parent_node.name, []))
    while pending:
        name = pending.pop()
        if name in visited:
            continue
        visited.add(name)
        node = ctx.nodes.get(name)
        if node and node.current:
            for link in node.current.dimension_links:
                if link.dimension.name != dim_ref.node_name:
                    continue
                if dim_ref.role is not None and link.role != dim_ref.role:
                    continue
                column = link.foreign_keys_reversed.get(target)
                if column:
                    candidates.add((name, get_short_name(column)))
        pending.extend(ctx.parent_map.get(name, []))
    if candidates:
        ctx.upstream_filter_bindings[parent_node.name, original_ref] = sorted(
            candidates,
        )
    return bool(candidates)


def _local_reference_dimension_column(
    ctx: BuildContext,
    parent_node: Node,
    dim_ref: DimensionRef,
) -> str | None:
    """
    Return the parent column that supplies dim_ref via a column-level dimension
    annotation (a reference / denormalized dimension), or None.

    A column tagged with a dimension node carries that dimension's attribute
    directly on the parent, so it needs no join. This mirrors the column-to-dimension
    edge that commonDimensions traverses, so discovery and the builder agree.
    Resolution uses the reliably-loaded dimension_id / dimension_column scalars plus
    the ctx id-to-name map, since the Column.dimension relationship doesn't reliably
    eager-load in this build.
    """
    from datajunction_server.construction.build_v3.cte import strip_role_suffix

    if not parent_node.current or not parent_node.current.columns:  # pragma: no cover
        return None
    for col in parent_node.current.columns:
        if col.dimension_id is None:
            continue
        if ctx.reference_dimension_names.get(col.dimension_id) != dim_ref.node_name:
            continue
        # dimension_column may carry a "[role]" suffix; fall back to the column's
        # own name when unset.
        target = strip_role_suffix(col.dimension_column or col.name)
        if target == dim_ref.column_name:
            return col.name
    return None


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
        elif (
            not dim_ref.role
            and (
                ref_col := _local_reference_dimension_column(ctx, parent_node, dim_ref)
            )
            is not None
        ):
            # Reference (denormalized) dimension: the parent carries the
            # attribute on a column tagged with the dimension node, so it
            # wins over a dimension link to the same node and needs no join.
            resolved.append(
                ResolvedDimension(
                    original_ref=dim,
                    node_name=parent_node.name,
                    column_name=ref_col,
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
                # Reference (denormalized) dimension: the parent may carry the
                # attribute directly on a column tagged with the dimension node, so
                # no join is needed. commonDimensions advertises these, so the
                # builder must serve them too.
                ref_col = _local_reference_dimension_column(ctx, parent_node, dim_ref)
                if ref_col is not None:
                    resolved.append(
                        ResolvedDimension(
                            original_ref=dim,
                            node_name=parent_node.name,
                            column_name=ref_col,
                            role=dim_ref.role,
                            join_path=None,
                            is_local=True,
                        ),
                    )
                    continue

                # An upstream link can bind a filter-only key even when the
                # fact does not project it. Placement validates its owner.
                if dim in ctx.filter_dimensions and _resolve_filter_only_dim(
                    ctx,
                    parent_node,
                    dim_ref,
                    dim,
                ):
                    continue
                raise DJException(
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
            # column on a closer node, skip the trailing joins.
            can_skip, local_col, hops_skipped = can_skip_join_for_dimension(
                dim_ref,
                join_path,
                parent_node,
            )
            if can_skip and local_col and hops_skipped == len(join_path.links):
                # Full skip: column lives on the parent fact/transform.
                # Filters referencing the dim resolve to the parent's local
                # column.
                resolved.append(
                    ResolvedDimension(
                        original_ref=dim,
                        node_name=parent_node.name,
                        column_name=local_col,
                        role=dim_ref.role,
                        join_path=None,
                        is_local=True,
                        pre_skip_join_path=join_path,
                    ),
                )
            elif can_skip and local_col and hops_skipped > 0:
                # Partial skip: keep only the leading links of the join
                # path; the column lives on the dim we stop at. We rewrite the
                # ResolvedDimension to point at that intermediate node.
                kept_links = join_path.links[:-hops_skipped]
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


def resolve_metric_expression_dimensions(
    ctx: BuildContext,
    parent_node: Node,
    expressions: list[ast.Expression],
    requested_dimensions: list[ResolvedDimension],
) -> list[ResolvedDimension]:
    """
    Resolve dimension-node column refs that appear *inside* metric expressions.

    A metric's aggregate expression may reference a column on a joined
    dimension node via the fully-qualified ``<dim_node>.<column>`` form, e.g.
    ``COUNT(DISTINCT CASE WHEN ... THEN customer.days_since ELSE NULL END)``.
    When that dimension node is *not* also part of the requested grain, no join
    is generated for it, so the reference leaks into the SQL as a raw,
    unresolved node path and the engine fails with "column does not exist".

    This resolves each such dimension node as a *join-only* dimension (one per
    referenced node): it is joined and CTE-projected, but excluded from the
    SELECT projection and GROUP BY by registering its ref in
    ``ctx.filter_dimensions``. The namespace rewrite in measures'
    ``_resolve_dim_namespace_refs`` then resolves the reference to the join
    alias.

    Dimension nodes already joined for the requested grain are skipped — their
    aliases are already available for the rewrite.

    Returns the list of newly resolved join-only ResolvedDimensions (empty when
    no metric expression references an unjoined dimension node).
    """
    # Dimension nodes already joined for the requested grain. A local /
    # skip-joined requested dim does not produce an alias, so only non-local
    # requested dims count as "already joined".
    already_joined: set[str] = {parent_node.name}
    for rdim in requested_dimensions:
        if not rdim.is_local:
            already_joined.add(parse_dimension_ref(rdim.original_ref).node_name)

    extra: list[ResolvedDimension] = []
    handled_nodes: set[str] = set()
    for expr in expressions:
        for nc in iter_namespaced_columns(expr):
            if nc.node in already_joined or nc.node in handled_nodes:
                continue
            handled_nodes.add(nc.node)

            # The referenced column itself is validated against the dimension
            # node at metric-creation time, so we only need the join path here.
            join_path = find_join_path(ctx, parent_node, nc.node, None)
            if not join_path:
                raise DJException(
                    http_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                    message=(
                        f"Cannot find join path from {parent_node.name} to "
                        f"dimension {nc.node} referenced in a metric expression. "
                        f"Please create a dimension link between these nodes."
                    ),
                )

            original_ref = f"{nc.node}{SEPARATOR}{nc.name}"
            # Join the dim, but keep it out of the projection and GROUP BY.
            ctx.filter_dimensions.add(original_ref)
            extra.append(
                ResolvedDimension(
                    original_ref=original_ref,
                    node_name=nc.node,
                    column_name=nc.name,
                    role=None,
                    join_path=join_path,
                    is_local=False,
                ),
            )

    return extra


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


# Fan-out cardinalities (default MANY_TO_ONE is safe). See check_fanout_safety.
UNSAFE_JOIN_CARDINALITIES = frozenset(
    {JoinCardinality.ONE_TO_MANY, JoinCardinality.MANY_TO_MANY},
)


def find_unsafe_cardinality_links(
    resolved_dimensions: list[ResolvedDimension],
) -> list[DimensionLink]:
    """Return the emitted join-path links whose cardinality can fan out."""
    unsafe: list[DimensionLink] = []
    for resolved_dim in resolved_dimensions:
        if resolved_dim.is_local or not resolved_dim.join_path:
            continue
        for link in resolved_dim.join_path.links:
            if link.join_cardinality in UNSAFE_JOIN_CARDINALITIES:
                unsafe.append(link)
    return unsafe


def requested_metrics_for(ctx: BuildContext, metric_names: set[str]) -> list[str]:
    """
    Map metric names found in a grain group back to the metrics the caller asked for.

    A derived metric is decomposed into its base metrics before grain grouping, so a
    grain group holds base metric nodes the caller never named. Report the requested
    metric instead. A requested base metric maps to itself, and anything that can't be
    traced back to a request is kept as-is rather than dropped.
    """
    requested: set[str] = set()
    covered: set[str] = set()
    for requested_name in ctx.metrics:
        metric_node = ctx.get_metric_node(requested_name)
        names = {requested_name}
        if is_derived_metric(ctx, metric_node):
            names.update(
                base.name for base in get_base_metrics_for_derived(ctx, metric_node)
            )
        covered.update(names)
        if names & metric_names:
            requested.add(requested_name)
    return sorted(requested | (metric_names - covered))


def check_fanout_safety(
    ctx: BuildContext,
    grain_group: GrainGroup,
    unsafe_links: list[DimensionLink],
) -> DJWarning | None:
    """
    Return a fan-out warning if this grain group aggregates a metric that row
    duplication would inflate across a fanning join.

    A ONE_TO_MANY/MANY_TO_MANY link duplicates fact rows; whether that inflates a
    given metric is decided per aggregation (see is_duplication_invariant). We warn
    rather than error: the user may know the data does not fan out.
    """
    if not unsafe_links:
        return None

    inflated_metrics: set[str] = {
        metric_node.name
        for metric_node, component in grain_group.components
        if merge_inflates(component.merge)
    }

    # Non-decomposable metrics have no component merge to read; their derived_ast is
    # the untouched metric query, so inspect its aggregations directly.
    inflated_metrics.update(
        info.metric_node.name
        for info in grain_group.non_decomposable_metrics
        if is_metric_duplication_sensitive(info.derived_ast)
    )

    if not inflated_metrics:
        return None

    base_metric_names = sorted(inflated_metrics)
    # Name what the caller asked for; the base metrics stay in debug for detail.
    metric_names = requested_metrics_for(ctx, inflated_metrics)
    unsafe_link_keys = sorted(
        {
            (
                link.node_revision.name,
                link.dimension.name,
                # Role disambiguates two links to the same dimension, which would
                # otherwise collapse into one entry here.
                link.role or "",
                JoinCardinality(link.join_cardinality).value,
            )
            for link in unsafe_links
        },
    )
    link_details = [
        {
            "node": node_name,
            "dimension": dimension_name,
            "role": role or None,
            "join_cardinality": cardinality,
        }
        for node_name, dimension_name, role, cardinality in unsafe_link_keys
    ]
    link_descriptions = [
        f"{node_name} -> {dimension_name}"
        f"{'[' + role + ']' if role else ''} "
        f"({cardinality})"
        for node_name, dimension_name, role, cardinality in unsafe_link_keys
    ]
    metrics_str = ", ".join(metric_names)
    links_str = "; ".join(link_descriptions)
    # The join isn't necessarily one the caller asked for -- a filter or the metric
    # expression itself can pull it in -- so name the link rather than blaming the
    # requested dimensions.
    message = (
        f"Possible fan-out: metric(s) {metrics_str} aggregate across a fan-out "
        f"link [{links_str}]. One {grain_group.parent_node.name} row matches many "
        f"dimension rows, so the result may be inflated by duplicated fact rows. "
        f"The join may come from a requested dimension, a filter, or the metric "
        f"expression itself. If a filter constrains the relationship to be "
        f"effectively one-to-one this result is correct; otherwise drop the "
        f"reference to the offending dimension(s), or correct the link's "
        f"cardinality if the relationship is not actually one-to-many or "
        f"many-to-many."
    )
    return DJWarning(
        code=ErrorCode.FANOUT_RISK,
        message=message,
        debug={
            # Requested metrics; base_metrics names the aggregations that actually
            # inflate, which differs from metrics for a derived metric.
            "metrics": metric_names,
            "base_metrics": base_metric_names,
            "links": link_details,
            "parent": grain_group.parent_node.name,
        },
    )
