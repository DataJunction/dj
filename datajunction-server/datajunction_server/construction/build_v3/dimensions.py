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
from datajunction_server.database.node import Node, NodeType
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


def _local_col_for_dim_via_metadata(
    node: Node,
    dim_node_name: str,
    dim_col_name: str,
) -> Optional[str]:
    """
    Return the local column on `node` that semantically equals
    `dim_node_name.dim_col_name`, using only declared metadata
    (column-level annotations + direct dim links). Returns None if neither
    layer applies — caller may then fall back to AST tracing.
    """
    if not node.current:
        return None  # pragma: no cover
    for col in node.current.columns:
        if col.dimension is None or col.dimension.name != dim_node_name:
            continue
        if (col.dimension_column or col.name) == dim_col_name:
            return col.name
    target_fqn = f"{dim_node_name}{SEPARATOR}{dim_col_name}"
    for link in node.current.dimension_links:
        if link.dimension.name != dim_node_name:
            continue
        fk_fqn = link.foreign_keys_reversed.get(target_fqn)
        if fk_fqn is not None:
            return get_short_name(fk_fqn)
    return None


def _trace_dim_col_through_query(
    ctx: BuildContext,
    node: Node,
    dim_node_name: str,
    dim_col_name: str,
    depth: int = 0,
) -> Optional[str]:
    """
    Walk `node`'s parsed query AST to find a projection whose value traces
    back to a FROM source's column that semantically corresponds to
    `dim_node_name.dim_col_name`. Returns the projection's output alias.

    Handles renames through a SELECT chain: e.g., if `cs_contact_f` has a
    dim link mapping `fact_utc_date` to the dim col, and `data_finalized`'s
    query projects `cs.fact_utc_date AS alloc_utc_date FROM cs_contact_f cs`,
    this returns `alloc_utc_date`.

    Recurses through subqueries and intermediate transforms (capped at depth
    5 to bound runtime on pathological lineages).
    """
    if depth >= 5 or not node.current or not node.current.query:
        return None
    try:
        query_ast = ctx.get_parsed_query(node)
    except Exception:  # pragma: no cover
        return None

    select = query_ast.select if hasattr(query_ast, "select") else query_ast
    if not isinstance(select, ast.Select) or select.from_ is None:
        return None  # pragma: no cover

    # Map FROM source aliases -> local col name on the source that satisfies
    # the dim semantics. Aliases are lowercased for case-insensitive matching.
    alias_to_local: dict[str, str] = {}
    for tbl in select.from_.find_all(ast.Table):
        try:
            src_name = tbl.name.identifier(quotes=False)
        except Exception:  # pragma: no cover
            src_name = str(tbl.name)
        src_alias = (
            tbl.alias.name.lower()
            if tbl.alias
            else src_name.split(SEPARATOR)[-1].lower()
        )
        src_node = ctx.nodes.get(src_name)
        if src_node is None:
            continue
        local = _local_col_for_dim_via_metadata(
            src_node,
            dim_node_name,
            dim_col_name,
        )
        if local is None:
            local = _trace_dim_col_through_query(
                ctx,
                src_node,
                dim_node_name,
                dim_col_name,
                depth + 1,
            )
        if local is not None:
            alias_to_local[src_alias] = local

    if not alias_to_local:
        return None

    # Walk projections; find one whose expression references an
    # (alias, col) pair we resolved above.
    for proj in select.projection:
        out_name: Optional[str] = None
        if isinstance(proj, ast.Aliasable) and proj.alias is not None:
            out_name = proj.alias.name
        elif isinstance(proj, ast.Column):
            out_name = proj.name.name

        if out_name is None:
            continue  # pragma: no cover

        for col_ref in proj.find_all(ast.Column):
            qualifier = col_ref.name.namespace
            qual_name = (
                qualifier.identifier(quotes=False).lower()
                if qualifier is not None
                else None
            )
            if qual_name is None:
                continue
            col_short = col_ref.name.name
            if qual_name in alias_to_local and alias_to_local[qual_name] == col_short:
                return out_name
    return None


def _find_transform_referencing_node(
    ctx: BuildContext,
    target_node_name: str,
) -> Optional[tuple[Node, str]]:
    """
    Find a transform/dimension node in ``ctx.nodes`` whose query has a direct
    Table reference to ``target_node_name``. Return (the transform node, the
    alias used). When no alias is present in the SQL, the table's short name
    is used (matching how aliases work for unaliased FROM tables).

    Used to push a dim-link FK filter into the immediate CTE that selects
    from the linked upstream node.
    """
    for node_name, node in ctx.nodes.items():
        if node_name == target_node_name:
            continue
        if node.type == NodeType.SOURCE:
            continue
        if not node.current or not node.current.query:
            continue
        try:
            query_ast = ctx.get_parsed_query(node)
        except Exception:  # pragma: no cover
            continue
        select = query_ast.select if hasattr(query_ast, "select") else None
        if select is None or select.from_ is None:
            continue  # pragma: no cover
        for tbl in select.from_.find_all(ast.Table):
            try:
                tbl_name = tbl.name.identifier(quotes=False)
            except Exception:  # pragma: no cover
                continue
            if tbl_name == target_node_name:
                alias = (
                    tbl.alias.name
                    if tbl.alias
                    else target_node_name.split(SEPARATOR)[-1]
                )
                return node, alias
    return None


def _register_upstream_pushdown(
    ctx: BuildContext,
    parent_node: Node,
    dim_ref: DimensionRef,
    original_ref: str,
) -> bool:
    """
    Identify upstreams of ``parent_node`` whose dim links to
    ``dim_ref.node_name`` carry the requested column, then push each
    user filter referencing this dim into the appropriate CTE.

    For a linked source (no CTE of its own), the filter is injected into a
    transform that directly references the source, rewritten to use the
    source's alias. For a linked transform, the filter is injected into that
    transform's CTE on its local FK column.

    Returns True if at least one filter was registered; consumed filter
    strings are added to ``ctx.pushdown_consumed_filters`` so the outer WHERE
    builder can skip them.
    """
    from datajunction_server.construction.build_v3.filters import parse_filter
    from datajunction_server.construction.build_v3.cte import get_column_full_name

    target_fqn = f"{dim_ref.node_name}{SEPARATOR}{dim_ref.column_name}"

    # Walk upstream lineage; collect (linked_node, fk_col_short).
    visited: set[str] = {parent_node.name}
    queue: list[str] = list(ctx.parent_map.get(parent_node.name, []))
    candidates: list[tuple[Node, str]] = []
    while queue:
        up_name = queue.pop(0)
        if up_name in visited:
            continue  # pragma: no cover
        visited.add(up_name)
        up_node = ctx.nodes.get(up_name)
        if up_node and up_node.current:
            for link in up_node.current.dimension_links:
                if link.dimension.name != dim_ref.node_name:
                    continue
                fk_fqn = link.foreign_keys_reversed.get(target_fqn)
                if fk_fqn is None:
                    continue
                candidates.append((up_node, get_short_name(fk_fqn)))
        queue.extend(ctx.parent_map.get(up_name, []))

    if not candidates:
        return False

    # Find the filter strings that reference this dim ref.
    matching_filters: list[str] = []
    base_ref = original_ref.split("[")[0]
    for filter_str in ctx.dimension_filters:
        if filter_str in ctx.pushdown_consumed_filters:
            continue
        try:
            f_ast = parse_filter(filter_str)
        except Exception:  # pragma: no cover
            continue
        for col in f_ast.find_all(ast.Column):
            full = get_column_full_name(col)
            if full and full.split("[")[0] == base_ref:
                matching_filters.append(filter_str)
                break

    if not matching_filters:
        return False  # pragma: no cover

    registered_any = False
    for filter_str in matching_filters:
        for linked_node, fk_col in candidates:
            # Determine the (target_node, col_qualifier) for injection.
            if linked_node.type == NodeType.SOURCE:
                found = _find_transform_referencing_node(ctx, linked_node.name)
                if found is None:
                    continue  # pragma: no cover
                target_node, alias = found
                qualifier = alias
            else:
                target_node = linked_node
                qualifier = None  # inject unaliased; CTE renders its own scope

            # Build a rewritten copy of the filter AST.
            try:
                rewritten = parse_filter(filter_str)
            except Exception:  # pragma: no cover
                continue
            for col in list(rewritten.find_all(ast.Column)):
                full = get_column_full_name(col)
                if full is None or full.split("[")[0] != base_ref:
                    continue
                new_name = (
                    ast.Name(fk_col, namespace=ast.Name(qualifier))
                    if qualifier
                    else ast.Name(fk_col)
                )
                new_col = ast.Column(name=new_name)
                if col.parent is not None:
                    col.parent.replace(col, new_col, copy=False)

            ctx.upstream_pushdown_filters.setdefault(target_node.name, []).append(
                rewritten,
            )
            registered_any = True
        if registered_any:
            ctx.pushdown_consumed_filters.add(filter_str)
    return registered_any


def _find_filter_only_local_col(
    ctx: BuildContext,
    parent_node: Node,
    dim_ref: DimensionRef,
) -> Optional[str]:
    """
    Resolve a filter-only dim ref by walking the parent's upstream lineage for
    a dimension link to the requested dim node. If an upstream link's FK
    mapping aligns the requested column with a column that's also present on
    the parent's projection, return that local column name — the filter can be
    pushed down to the parent CTE without any join.

    This is the v3 equivalent of the v2 behavior where ``filter_only=True``
    dimensions (declared via FK mappings on any upstream link) are filterable
    on a fact without requiring a join from the fact directly.
    """
    if not parent_node.current or not parent_node.current.columns:
        return None  # pragma: no cover
    parent_cols = {col.name for col in parent_node.current.columns}
    target_fqn = f"{dim_ref.node_name}{SEPARATOR}{dim_ref.column_name}"

    # Layer 1: direct metadata (annotation or own dim link) on the parent.
    local = _local_col_for_dim_via_metadata(
        parent_node,
        dim_ref.node_name,
        dim_ref.column_name,
    )
    if local is not None:
        logger.info(
            "[BuildV3] filter-only layer-1 (parent metadata): %s -> parent.%s",
            target_fqn,
            local,
        )
        return local

    # Layer 2: walk upstream lineage for any dim link to the target dim,
    # collect candidate FK cols. If any candidate FK col happens to flow up
    # to the parent's projection unchanged, use it directly.
    visited: set[str] = {parent_node.name}
    queue: list[str] = list(ctx.parent_map.get(parent_node.name, []))
    candidate_upstreams: list[tuple[str, str]] = []  # (up_name, fk_col)
    while queue:
        up_name = queue.pop(0)
        if up_name in visited:
            continue  # pragma: no cover
        visited.add(up_name)
        up_node = ctx.nodes.get(up_name)
        if up_node and up_node.current:
            for link in up_node.current.dimension_links:
                if link.dimension.name != dim_ref.node_name:
                    continue
                fk_fqn = link.foreign_keys_reversed.get(target_fqn)
                if fk_fqn is None:
                    continue
                local_col = get_short_name(fk_fqn)
                candidate_upstreams.append((up_name, local_col))
                if local_col in parent_cols:
                    logger.info(
                        "[BuildV3] filter-only layer-2 (upstream FK passthrough): "
                        "%s -> parent.%s (via upstream %s)",
                        target_fqn,
                        local_col,
                        up_name,
                    )
                    return local_col
        queue.extend(ctx.parent_map.get(up_name, []))

    # Layer 3: AST tracing through the parent's query — follows column
    # renames from an upstream's dim link / annotation through SELECT
    # projections (e.g., `cs.fact_utc_date AS alloc_utc_date`). This is
    # the fallback when the FK col gets aliased somewhere in the chain.
    traced = _trace_dim_col_through_query(
        ctx,
        parent_node,
        dim_ref.node_name,
        dim_ref.column_name,
    )
    if traced is not None:
        logger.info(
            "[BuildV3] filter-only layer-3 (AST trace): %s -> parent.%s",
            target_fqn,
            traced,
        )
        return traced

    logger.info(
        "[BuildV3] filter-only resolution FAILED: parent=%s target=%s "
        "upstream_candidates=%s parent_cols=%s",
        parent_node.name,
        target_fqn,
        candidate_upstreams,
        sorted(parent_cols),
    )
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
                # Upstream filter-only resolution: the fact may not link to
                # this dim, but an upstream of the fact might — and if the
                # upstream's FK is preserved on the parent's projection, the
                # filter can be pushed down without any join.
                if dim in ctx.filter_dimensions:
                    upstream_col = _find_filter_only_local_col(
                        ctx,
                        parent_node,
                        dim_ref,
                    )
                    if upstream_col is not None:
                        ctx.skip_join_column_mapping[dim] = upstream_col
                        resolved.append(
                            ResolvedDimension(
                                original_ref=dim,
                                node_name=parent_node.name,
                                column_name=upstream_col,
                                role=dim_ref.role,
                                join_path=None,
                                is_local=True,
                            ),
                        )
                        continue
                    # Last resort: push the filter into an upstream CTE that
                    # references the linked source. The dim ref doesn't get a
                    # ResolvedDimension; the consumed filter strings are
                    # tracked so the outer WHERE skips them.
                    if _register_upstream_pushdown(
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
                ctx.skip_join_column_mapping[dim] = local_col
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
