"""
Measures SQL Generation

This module handles the generation of pre-aggregated "measures" SQL,
which aggregates metric components to the requested dimensional grain.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from datajunction_server.database.preaggregation import PreAggregation

from datajunction_server.construction.build_v3.cte import (
    collect_node_ctes,
    extract_dimension_node,
    inject_filter_into_select,
    strip_role_suffix,
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
    extract_columns_from_expression,
    extract_columns_referenced_from_node,
    get_column_type,
    get_cte_name,
    get_short_name,
    make_column_ref,
    make_name,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.construction.build_v3.materialization import (
    get_table_reference_parts_with_materialization,
    should_use_materialized_table,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    DecomposedMetricInfo,
    GrainGroup,
    GrainGroupSQL,
    PushdownFilters,
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
from datajunction_server.internal.scan_estimation import calculate_scan_estimate
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    GrainGroupSQL,
    MetricGroup,
)
from datajunction_server.sql.functions import function_registry
from datajunction_server.sql.parsing import types as ct


_logger = logging.getLogger(__name__)


def _rewrite_col_refs(expr: Any, table_alias: str) -> None:
    """Add a table alias prefix to all unqualified column references in an expression."""
    if isinstance(expr, ast.Column):
        if expr.name and not (  # pragma: no branch
            expr.name.namespace and expr.name.namespace.name
        ):
            expr.name = ast.Name(expr.name.name, namespace=ast.Name(table_alias))
    for child in expr.children if hasattr(expr, "children") else []:
        if child:  # pragma: no branch
            _rewrite_col_refs(child, table_alias)


def _resolve_dim_namespace_refs(
    metric_expressions: list[tuple[str, ast.Expression]],
    dim_node_to_alias: dict[str, str],
) -> dict[str, set[str]]:
    """Resolve dim-namespaced column refs in metric expressions.

    A metric expression may contain a fully-qualified column like
    ``v3.customer.tier`` referring to a column on a dimension node that the
    parent fact joins to. In one walk per expression, this function:

    1. Rewrites the namespace from the dim node name to the dim's joined
       table alias (so the renderer emits ``t2.tier`` instead of the literal
       node name, which is not a valid table reference).
    2. Returns ``{dim_node_name: {col, ...}}`` so the caller can keep those
       columns in the dim's CTE projection (otherwise filter_cte_projection
       drops them as unused).
    """
    dim_cols: dict[str, set[str]] = {}
    for _, expr in metric_expressions:
        for col in expr.find_all(ast.Column):
            if not (col.name and col.name.namespace and col.name.namespace.name):
                continue
            ns = col.name.namespace.identifier(quotes=False)
            if ns not in dim_node_to_alias:
                continue
            dim_cols.setdefault(ns, set()).add(col.name.name)
            col.name = ast.Name(
                col.name.name,
                namespace=ast.Name(dim_node_to_alias[ns]),
            )
    return dim_cols


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


def _get_filter_column_name_for_dimension(
    resolved_dim: ResolvedDimension,
    parent_node: Node,
) -> str | None:
    """
    Get the column name to use for a dimension in filters.

    For local dimensions that reference other dimension nodes (e.g., v3.date.date_id
    accessed via parent's order_date column), returns the parent's FK column name.
    Otherwise returns None to indicate no rewriting needed.

    Args:
        resolved_dim: The resolved dimension
        parent_node: The parent node to look up dimension links on

    Returns:
        The FK column name if this is a local dimension reference, None otherwise
    """
    # Only rewrite for local dimensions
    if not resolved_dim.is_local:
        return None

    # Parse the original_ref to get the actual dimension node name
    parsed_ref = parse_dimension_ref(resolved_dim.original_ref)
    dim_node_name = parsed_ref.node_name

    # Check if this is a dimension reference (not a local column on parent)
    if not dim_node_name or dim_node_name == parent_node.name:
        return None

    # Look up the dimension link on the parent node to find the FK column
    if not parent_node.current or not parent_node.current.dimension_links:
        return None  # pragma: no cover

    for link in parent_node.current.dimension_links:
        if link.dimension.name == dim_node_name:
            fk_columns = link.foreign_key_column_names
            if fk_columns:  # pragma: no branch
                return next(iter(fk_columns))

    return None  # pragma: no cover


def _add_table_prefixes_to_filter(
    filter_ast: ast.Expression,
    resolved_dimensions: list[ResolvedDimension],
    main_alias: str,
    dim_aliases: dict[tuple[str, Optional[str]], str],
    parent_node: Node,
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
        parent_node: Parent node to look up dimension links
    """
    # Build a map from column alias to table alias
    col_to_table: dict[str, str] = {}

    for resolved_dim in resolved_dimensions:
        # Get the column name to use (parent's FK for local dimension refs, else dimension's column)
        col_alias = _get_filter_column_name_for_dimension(resolved_dim, parent_node)
        if not col_alias:
            col_alias = resolved_dim.column_name

        # Use get_dimension_table_alias for correctness — it uses the final target node
        # and full accumulated role. The old loop-with-break approach incorrectly used the
        # first matching intermediate node for multi-hop join paths.
        table_alias = get_dimension_table_alias(resolved_dim, main_alias, dim_aliases)
        col_to_table[col_alias] = table_alias

    def add_prefixes(node: ast.Expression) -> None:
        """Recursively add table prefixes to columns."""
        if isinstance(node, ast.Subscript):
            # A Subscript that reaches here was not resolved by resolve_filter_references.
            # The subscript index is a role marker (not a real column), so don't recurse
            # into it — only process the base expression.
            add_prefixes(node.expr)
            return
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
        dim_aliases: Map of (node_name, accumulated_role) -> table_alias for dimension joins

    Returns:
        The appropriate table alias to use for this dimension's column
    """
    if resolved_dim.is_local:
        return main_alias
    elif resolved_dim.join_path:  # pragma: no branch
        # Build accumulated role path to match how joins were created
        final_dim_name = resolved_dim.join_path.target_node_name
        accumulated_role_parts = []
        for link in resolved_dim.join_path.links:
            link_role = link.role or ""
            if link_role:
                accumulated_role_parts.append(link_role)
        accumulated_role = (
            "->".join(accumulated_role_parts) if accumulated_role_parts else ""
        )
        dim_key = (final_dim_name, accumulated_role)
        return dim_aliases.get(dim_key, main_alias)
    return main_alias  # pragma: no cover


def collect_cte_nodes_and_needed_columns(
    ctx: BuildContext,
    parent_node: Node,
    resolved_dimensions: list[ResolvedDimension],
    grain_col_specs: list[tuple[ast.Expression, str]],
    metric_expressions: list[tuple[str, ast.Expression]],
) -> tuple[list[Node], dict[str, set[str]]]:
    """
    Determine which nodes need CTEs and the minimal set of columns each must project.

    Returns a tuple of:
    - nodes_for_ctes: ordered list of non-source nodes that require CTEs
    - needed_columns_by_node: mapping of node name -> set of column names that
      must remain in that node's CTE projection after filter_cte_projection runs

    The needed columns for each node are gathered from:
    - For parent_node: local dimension columns, grain columns, metric expression
      columns, join key columns, and temporal partition columns
    - For each dimension node: the requested dimension attribute, join key columns,
      and any columns referenced from that node in parent_node's or other dimension
      nodes' SQL (including aliased references like CROSS JOIN node AS alias)
    """
    nodes_for_ctes: list[Node] = []
    needed_columns_by_node: dict[str, set[str]] = {}

    # Collect columns needed from parent node
    parent_needed_cols: set[str] = set()

    # Add local dimension columns
    for resolved_dim in resolved_dimensions:
        if resolved_dim.is_local:
            parent_needed_cols.add(resolved_dim.column_name)

    # Add grain columns for LIMITED aggregability.
    # For complex expressions, extract the actual leaf columns they reference.
    for gc_expr, _ in grain_col_specs:
        if isinstance(gc_expr, ast.Column):
            parent_needed_cols.add(gc_expr.name.name)
        else:
            parent_needed_cols.update(extract_columns_from_expression(gc_expr))

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

    # Add temporal partition columns from cube if linked to this parent
    # This ensures the columns are available in the CTE for the WHERE clause
    if ctx.temporal_partition_columns and parent_node.current:
        for partition_col_ref in ctx.temporal_partition_columns:
            dimension_ref = parse_dimension_ref(partition_col_ref)

            # Check if this parent has a dimension link to the partition column's node
            if parent_node.current.dimension_links:  # pragma: no branch
                for link in parent_node.current.dimension_links:  # pragma: no branch
                    if link.dimension.name == dimension_ref.node_name:
                        parent_needed_cols.add(dimension_ref.column_name)
                        break

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

                    # Add join key columns from this dimension (right side of this link)
                    if link.join_sql:  # pragma: no branch
                        dim_cols.update(
                            extract_join_columns_for_node(link.join_sql, dim_node.name),
                        )

                    # Case 1: parent_node's query directly selects from dim_node
                    nodes_to_scan: list[Node] = []
                    if parent_node.current and parent_node.current.query:
                        nodes_to_scan.append(parent_node)
                    # Case 2: another dimension node's query references dim_node
                    for other_rdim in resolved_dimensions:
                        if other_rdim.join_path:
                            for other_link in other_rdim.join_path.links:
                                other_dim = ctx.nodes.get(
                                    other_link.dimension.name,
                                    other_link.dimension,
                                )
                                if (
                                    other_dim
                                    and other_dim.name != dim_node.name
                                    and other_dim.type != NodeType.SOURCE
                                    and other_dim.current
                                    and other_dim.current.query
                                    and other_dim not in nodes_to_scan
                                ):
                                    nodes_to_scan.append(other_dim)
                    for referencing_node in nodes_to_scan:
                        rq = ctx.get_parsed_query(referencing_node)
                        found = extract_columns_referenced_from_node(rq, dim_node.name)
                        if found:
                            _logger.info(
                                "filter_cte_projection: %s references cols %s from %s",
                                referencing_node.name,
                                sorted(found),
                                dim_node.name,
                            )
                        dim_cols.update(found)

                    # Merge with existing if any
                    if dim_node.name in needed_columns_by_node:
                        needed_columns_by_node[dim_node.name].update(dim_cols)
                    else:
                        needed_columns_by_node[dim_node.name] = dim_cols
                    _logger.info(
                        "filter_cte_projection: keeping cols %s for %s",
                        sorted(dim_cols),
                        dim_node.name,
                    )

                # For multi-hop joins: the left side of this link is an intermediate
                # dimension node that also needs the left-side join key columns.
                # (For the first link, the left side is parent_node, already handled
                # above in parent_needed_cols.)
                left_node_name = link.node_revision.name
                if left_node_name != parent_node.name and link.join_sql:
                    left_node = ctx.nodes.get(left_node_name)
                    if (
                        left_node and left_node.type != NodeType.SOURCE
                    ):  # pragma: no branch
                        left_join_cols = extract_join_columns_for_node(
                            link.join_sql,
                            left_node_name,
                        )
                        if left_node_name in needed_columns_by_node:
                            needed_columns_by_node[left_node_name].update(
                                left_join_cols,
                            )
                        else:  # pragma: no cover
                            needed_columns_by_node[left_node_name] = left_join_cols

    return nodes_for_ctes, needed_columns_by_node


def _build_temporal_pushdown(
    ctx: BuildContext,
    parent_node: Node,
    main_alias: str,
) -> tuple[Optional[ast.Expression], dict[str, ast.Expression]]:
    """Build temporal filter and push it into the most upstream applicable CTE.

    Tries the date-spine upstream first (so the filter applies before any
    expensive join); falls back to the parent node's own CTE when no
    upstream date-spine source can be located. The filter is never applied
    to the outer query's WHERE — that would silently turn any OUTER JOIN
    inside the parent body into an INNER JOIN by dropping NULL-fill rows.

    Returns:
        (temporal_filter_ast, injected_cte_filters) where temporal_filter_ast
        is always None when the filter was successfully built (it is fully
        delegated to a CTE injection).
    """
    temporal_filter_ast, fk_col_name = build_temporal_filter(
        ctx,
        parent_node,
        main_alias,
    )
    injected_cte_filters: dict[str, ast.Expression] = {}

    if temporal_filter_ast and fk_col_name:
        upstream_node = find_upstream_temporal_source_node(
            ctx,
            parent_node,
            fk_col_name,
        )
        target_node_name = upstream_node.name if upstream_node else parent_node.name
        unaliased_filter_ast, _ = build_temporal_filter(ctx, parent_node, None)
        if unaliased_filter_ast:  # pragma: no branch
            injected_cte_filters[target_node_name] = unaliased_filter_ast
            temporal_filter_ast = None

    return temporal_filter_ast, injected_cte_filters


def build_filter_column_aliases(
    ctx: BuildContext,
    resolved_dimensions: list[ResolvedDimension],
    parent_node: Node,
) -> dict[str, str]:
    """Build a mapping from dimension refs to bare column names for filter resolution.

    Handles three cases:
    - FK remapping: ``v3.date.date_id[order]`` → ``order_date``
    - Identity: ``v3.product.category`` → ``category``
    - Skip-join override: ``dimensions.time.date.dateint`` → ``utc_date``

    Also adds bare-key fallbacks (role suffix stripped) so filters resolve
    even when the role is missing or mismatched.
    """
    aliases: dict[str, str] = {}

    for resolved_dim in resolved_dimensions:
        col_alias = _get_filter_column_name_for_dimension(resolved_dim, parent_node)
        if not col_alias:
            col_alias = resolved_dim.column_name
        aliases[resolved_dim.original_ref] = col_alias

    # Local columns from the parent node (for simple refs like "status")
    if parent_node.current and parent_node.current.columns:  # pragma: no branch
        for col in parent_node.current.columns:
            if col.name not in aliases:  # pragma: no branch
                aliases[col.name] = col.name

    # Skip-join overrides
    for dim_ref, local_col in ctx.skip_join_column_mapping.items():
        aliases[dim_ref] = local_col

    # Bare-key fallbacks (strip role suffix)
    for original_ref in list(aliases.keys()):
        if "[" in original_ref:
            base_ref = original_ref.split("[")[0]
            if base_ref not in aliases:  # pragma: no branch
                aliases[base_ref] = aliases[original_ref]

    return aliases


def build_outer_where(
    filters: list[str],
    filter_column_aliases: dict[str, str],
    resolved_dimensions: list[ResolvedDimension],
    main_alias: str,
    dim_aliases: dict[tuple[str, Optional[str]], str],
    parent_node: Node,
    nodes: Optional[dict[str, Node]] = None,
) -> Optional[ast.Expression]:
    """Parse user filters and resolve column references for the outer WHERE clause.

    Returns the combined WHERE expression with table-qualified column names,
    or None if no filters parse successfully.
    """
    where_clause = parse_and_resolve_filters(
        filters,
        filter_column_aliases,
        cte_alias=None,
        nodes=nodes,
    )
    if where_clause:  # pragma: no branch
        _add_table_prefixes_to_filter(
            where_clause,
            resolved_dimensions,
            main_alias,
            dim_aliases,
            parent_node,
        )
    return where_clause


def _apply_outer_where_atoms(
    select: ast.Select,
    where_clause: Optional[ast.Expression],
    main_alias: str,
    parent_pushdown_active: bool = False,
) -> None:
    """Apply each AND atom of ``where_clause`` to ``select.where``.

    Three categories of atoms:

    - **Parent-alias atoms** (``main_alias.col`` only) when
      ``parent_pushdown_active`` is True — DROPPED from outer WHERE.
      The same predicate has already been pushed into the parent CTE's
      WHERE, so the outer-level copy is redundant *and* unsafe (it
      would defeat downstream RIGHT/FULL OUTER joins to dims).

    - **Parent-alias atoms** when parent pushdown wasn't applied (e.g.
      parent is materialized or has a set-op body that blocks
      pushdown) — routed through :func:`inject_filter_into_select` as
      the safety backstop.

    - **Dim-alias atoms** (and unqualified atoms) — ANDed into the outer
      WHERE.  These rely on the standard dim-filter narrowing
      semantics.  Note: under a downstream RIGHT/FULL OUTER JOIN this
      can silently defeat that join — the wrapper-CTE absorption step
      runs earlier to handle the unsafe cases.
    """
    if where_clause is None:
        return
    from datajunction_server.construction.build_v3.cte import (
        _filter_namespaces,
        _split_and_atoms,
    )

    for atom in _split_and_atoms(where_clause):
        ns = _filter_namespaces(atom)
        is_parent_only = bool(ns) and ns.issubset({main_alias})
        if is_parent_only and parent_pushdown_active:
            continue
        if is_parent_only:
            inject_filter_into_select(select, atom)
        elif select.where:
            select.where = ast.BinaryOp.And(select.where, atom)
        else:
            select.where = atom


def _col_table_name(col: ast.Column) -> Optional[str]:
    """Return the table-qualifier short name for a column, or None.

    Handles both qualification styles:
    - ``_table`` (set by :func:`make_column_ref`) — projection / GROUP BY.
    - ``name.namespace`` (set by ``_add_table_prefixes_to_filter``) —
      filter atoms.
    """
    tbl = col._table
    if tbl is not None:
        tname = getattr(tbl, "name", None)
        if tname is None:  # pragma: no cover
            return None
        return tname.name if hasattr(tname, "name") else str(tname)
    if col.name and col.name.namespace:
        ns = col.name.namespace
        return ns.name if hasattr(ns, "name") else str(ns)
    return None  # pragma: no cover


def _set_col_table_alias(col: ast.Column, new_alias: str) -> None:
    """Rewrite the table-qualifier on a column to ``new_alias``,
    matching the qualification style already on the column.
    """
    if col._table is not None and getattr(col._table, "name", None) is not None:
        col._table.name = ast.Name(new_alias)  # type: ignore[union-attr]
    elif col.name and col.name.namespace:  # pragma: no branch
        col.name = ast.Name(col.name.name, namespace=ast.Name(new_alias))


def _and_atoms(atoms: list[ast.Expression]) -> Optional[ast.Expression]:
    """Fold a list of atoms into a left-associative AND, or ``None``
    for an empty list."""
    result: Optional[ast.Expression] = None
    for atom in atoms:
        result = atom if result is None else ast.BinaryOp.And(result, atom)
    return result


def _maybe_coalesce_with_sibling(
    col_ref: ast.Column,
    resolved_dim: ResolvedDimension,
    resolved_dimensions: Optional[list[ResolvedDimension]],
    dim_aliases: dict[tuple[str, Optional[str]], str],
    parent_node_name: Optional[str],
) -> ast.Expression:
    """Return ``COALESCE(col_ref, sibling_col)`` when the dim was
    full-skipped to a parent FK and another joined dim shares the same
    FK alignment; otherwise return ``col_ref`` unchanged.

    Used by both the projection and GROUP BY builders so they stay in
    lockstep (the GROUP BY must mirror the projection's COALESCE).
    """
    if resolved_dimensions is None or parent_node_name is None:
        return col_ref
    partner = _coalesce_partner_for_full_skipped_fk(
        resolved_dim,
        resolved_dimensions,
        dim_aliases,
        parent_node_name,
    )
    if partner is None:
        return col_ref
    return ast.Function(ast.Name("COALESCE"), args=[col_ref, partner])


def _absorb_filtered_joins_for_outer_safety(
    select: ast.Select,
    where_clause: Optional[ast.Expression],
    main_alias: str,
    dim_aliases: dict[tuple[str, Optional[str]], str],
    parent_cte_name: str,
    group_by: list[ast.Expression],
) -> tuple[Optional[ast.Expression], Optional[tuple[str, ast.Query]]]:
    """Absorb LEFT/INNER joins whose dim-alias filter would defeat a
    downstream RIGHT/FULL OUTER JOIN into a subquery on the parent
    side.

    Why this is needed: applying a WHERE on a non-preserved-side dim
    alias *after* a downstream OUTER JOIN preserving the dim side
    drops the preserved null-fill rows.  The fix is to apply the dim
    filter *before* the OUTER JOIN — by absorbing the LEFT/INNER join
    plus its WHERE into a subquery that the OUTER JOIN's preserved
    side reaches via a clean ``RIGHT/FULL OUTER JOIN <wrapper>``
    pattern.

    Builds a filtered CTE named ``<parent_cte_name>_filtered`` that
    contains the parent + absorbed LEFT/INNER joins with the absorbed
    WHERE applied inside.  The outer FROM is rewritten to read from
    that CTE (still aliased as ``main_alias``).  Rewrites references
    to absorbed aliases (in projection, GROUP BY, remaining join ONs,
    remaining WHERE atoms) so they point at ``main_alias``.

    Returns ``(updated_where_clause, new_cte_or_None)``.  The caller is
    responsible for appending ``new_cte`` (if non-None) to the CTE
    list attached to the final query.
    """
    if where_clause is None or not select.from_ or not select.from_.relations:
        return where_clause, None

    relation = select.from_.relations[0]
    extensions = relation.extensions
    if not extensions:
        return where_clause, None

    from datajunction_server.construction.build_v3.cte import (
        _filter_namespaces,
        _get_relation_side_id,
        _split_and_atoms,
    )

    atoms = _split_and_atoms(where_clause)
    atoms_by_alias: dict[str, list[ast.Expression]] = {}
    other_atoms: list[ast.Expression] = []
    for atom in atoms:
        ns = _filter_namespaces(atom)
        if len(ns) == 1 and main_alias not in ns:
            atoms_by_alias.setdefault(next(iter(ns)), []).append(atom)
        else:
            other_atoms.append(atom)

    # Single pass over join chain: track whether any RIGHT/FULL OUTER is
    # present (gate) and which LEFT/INNER joins have defeating filters
    # (candidates).  Absorption only commits when both conditions hold —
    # otherwise pure-LEFT-chain narrowing semantics are preserved.
    has_right_full = False
    to_absorb_idx: list[int] = []
    absorbed_aliases: set[str] = set()
    for i, jn in enumerate(extensions):
        jt = (jn.join_type or "").upper().strip()
        if "RIGHT" in jt or "FULL" in jt:
            has_right_full = True
            continue
        side_alias = _get_relation_side_id(jn.right)
        if side_alias and side_alias in atoms_by_alias:
            to_absorb_idx.append(i)
            absorbed_aliases.add(side_alias)

    if not has_right_full or not absorbed_aliases:
        return where_clause, None

    absorb_set = set(to_absorb_idx)
    absorbed_joins = [extensions[i] for i in to_absorb_idx]
    kept_joins = [j for i, j in enumerate(extensions) if i not in absorb_set]

    # Single pass over outer-scope nodes: record column names referenced
    # from absorbed aliases (for the wrapper projection) AND rewrite the
    # alias qualifier in place to point at ``main_alias``.  The wrapper
    # captures ``relation.primary`` and ``absorbed_joins`` by reference
    # before we mutate them below, so doing both in one pass is safe.
    absorbed_cols: dict[str, set[str]] = {a: set() for a in absorbed_aliases}

    def _process(node: ast.Node) -> None:
        for col in node.find_all(ast.Column):
            tname_str = _col_table_name(col)
            if tname_str is not None and tname_str in absorbed_aliases:
                absorbed_cols[tname_str].add(col.name.name)
                _set_col_table_alias(col, main_alias)

    for proj_item in select.projection:
        _process(proj_item)
    for kj in kept_joins:
        if kj.criteria and kj.criteria.on:  # pragma: no branch
            _process(kj.criteria.on)
    for atom in other_atoms:
        _process(atom)
    for gb_item in group_by:
        _process(gb_item)

    # Build wrapper projection: parent.* + each absorbed col.
    wrapper_proj: list[Any] = [
        ast.Column(name=ast.Name("*"), _table=ast.Table(ast.Name(main_alias))),
    ]
    for alias in absorbed_aliases:
        for col_name in sorted(absorbed_cols[alias]):
            wrapper_proj.append(make_column_ref(col_name, alias))

    absorbed_atoms: list[ast.Expression] = []
    for alias in absorbed_aliases:
        absorbed_atoms.extend(atoms_by_alias[alias])

    wrapper_select = ast.Select(
        projection=wrapper_proj,
        from_=ast.From(
            relations=[
                ast.Relation(primary=relation.primary, extensions=absorbed_joins),
            ],
        ),
        where=_and_atoms(absorbed_atoms),
    )
    wrapper_query = ast.Query(select=wrapper_select)
    filtered_cte_name = f"{parent_cte_name}_filtered"

    # Outer FROM now reads from the filtered CTE, still aliased as main_alias.
    relation.primary = cast(
        ast.Expression,
        ast.Alias(
            child=ast.Table(ast.Name(filtered_cte_name)),
            alias=ast.Name(main_alias),
        ),
    )
    relation.extensions = kept_joins

    # Remap dim_aliases so downstream lookups (e.g. _build_group_by)
    # find main_alias instead of an absorbed dim alias.
    for key in list(dim_aliases.keys()):
        if dim_aliases[key] in absorbed_aliases:
            dim_aliases[key] = main_alias

    # Remaining outer-WHERE atoms: compound/cross-namespace atoms +
    # single-dim atoms whose alias wasn't absorbed (typically the
    # preserved RIGHT/FULL OUTER side).
    remaining_atoms: list[ast.Expression] = list(other_atoms)
    for alias, atoms_for_alias in atoms_by_alias.items():
        if alias not in absorbed_aliases:
            remaining_atoms.extend(atoms_for_alias)
    return _and_atoms(remaining_atoms), (filtered_cte_name, wrapper_query)


def _coalesce_partner_for_full_skipped_fk(
    resolved_dim: ResolvedDimension,
    resolved_dimensions: list[ResolvedDimension],
    dim_aliases: dict[tuple[str, Optional[str]], str],
    parent_node_name: str,
) -> Optional[ast.Column]:
    """Find a joined dim whose first link is FK-aligned with the fact FK
    column we just full-skip-resolved, and return a column reference to
    that dim's equivalent column.

    Two dims sharing the same fact FK both carry an equivalent value via
    each link's ``foreign_keys_reversed``.  When the requested dim was
    fully skipped (its own CTE isn't joined) but another joined dim
    shares the FK, the joined dim's PK is the correct COALESCE partner
    for the fact FK under any OUTER JOIN that preserves the dim side.

    Returns ``None`` when no co-joined sibling carries the FK.
    """
    if resolved_dim.pre_skip_join_path is None:
        return None

    parent_fk_fqn = f"{parent_node_name}{SEPARATOR}{resolved_dim.column_name}"

    for other in resolved_dimensions:
        if other.original_ref == resolved_dim.original_ref:
            continue
        if not other.join_path or not other.join_path.links:  # pragma: no cover
            continue
        first_link = other.join_path.links[0]
        # foreign_keys_reversed: {dim_pk_fqn -> fact_fk_fqn}
        for dim_pk_fqn, fact_fk_fqn in first_link.foreign_keys_reversed.items():
            if fact_fk_fqn != parent_fk_fqn:
                continue
            dim_short = dim_pk_fqn.rsplit(SEPARATOR, 1)[-1]
            dim_node_name = first_link.dimension.name
            role = first_link.role or ""
            alias = dim_aliases.get(
                (dim_node_name, role),
            ) or dim_aliases.get((dim_node_name, None))
            if alias is not None:  # pragma: no branch
                return make_column_ref(dim_short, alias)
    return None


def build_dimension_col_expr(
    resolved_dim: ResolvedDimension,
    main_alias: str,
    dim_aliases: dict[tuple[str, Optional[str]], str],
    clean_alias: str,
    ctx: Optional[BuildContext] = None,
    resolved_dimensions: Optional[list[ResolvedDimension]] = None,
    parent_node_name: Optional[str] = None,
) -> Any:
    """Build a SELECT expression for a single resolved dimension.

    Returns a column reference, optionally wrapped in COALESCE (when the
    dimension link has a ``default_value``, or when the FK column was
    substituted for a dim PK via the full-skip optimization but the dim
    CTE is still joined for other columns — in which case ``t1.fk`` may
    be NULL under a downstream OUTER JOIN preserving the dim side, and
    we COALESCE in the dim PK so the projected value matches the dim).
    """
    table_alias = get_dimension_table_alias(resolved_dim, main_alias, dim_aliases)
    col_ref = make_column_ref(resolved_dim.column_name, table_alias)

    default_value = None
    if resolved_dim.join_path and resolved_dim.join_path.links:
        last_link = resolved_dim.join_path.links[-1]
        default_value = last_link.default_value

    if default_value is not None:
        coalesce_func = ast.Function(
            ast.Name("COALESCE"),
            args=[col_ref, ast.String(f"'{default_value}'")],
        )
        col_expr = coalesce_func.set_alias(ast.Name(clean_alias))
        col_expr.set_as(True)
        return col_expr

    # Full-skip + co-joined-dim COALESCE.  When the FK was substituted
    # in via full-skip but a sibling dim shares the same alignment,
    # wrap the projection so OUTER JOINs that preserve the sibling
    # don't surface NULL where the sibling has the value.
    expr = _maybe_coalesce_with_sibling(
        col_ref,
        resolved_dim,
        resolved_dimensions,
        dim_aliases,
        parent_node_name,
    )
    if expr is not col_ref:
        aliased = expr.set_alias(ast.Name(clean_alias))
        aliased.set_as(True)
        return aliased

    if clean_alias != resolved_dim.column_name:
        col_ref.alias = ast.Name(clean_alias)
    return col_ref


def _build_metric_col_expr(
    expr: ast.Expression,
    clean_alias: str,
    main_alias: str,
) -> ast.Alias:
    """Rewrite column references in a metric expression and wrap with an alias.

    Adds the ``main_alias`` table qualifier to every unqualified column in
    ``expr`` (mutates in place), then returns an ``ast.Alias`` node.
    """
    _rewrite_col_refs(expr, main_alias)
    return ast.Alias(
        alias=ast.Name(clean_alias),
        child=expr,  # type: ignore[arg-type]
    )


def _build_group_by(
    resolved_dimensions: list[ResolvedDimension],
    dim_aliases: dict[tuple[str, Optional[str]], str],
    main_alias: str,
    grain_col_specs: list[tuple[ast.Expression, str]],
    projected_dim_col_names: set[str],
    filter_dimensions: set[str],
    parent_node_name: Optional[str] = None,
) -> list[ast.Expression]:
    """Build the GROUP BY clause from dimensions and grain columns.

    Dimensions marked as filter-only are excluded.  Grain columns that are
    simple identifiers already present as a dimension are also skipped to
    avoid duplicates.

    When a dimension's projection is wrapped in COALESCE (full-skip with
    a co-joined sibling dim — see :func:`build_dimension_col_expr`), the
    GROUP BY expression mirrors the COALESCE so rows aren't bucketed by
    the NULL-side alone.
    """
    group_by: list[ast.Expression] = []

    for resolved_dim in resolved_dimensions:
        if resolved_dim.original_ref in filter_dimensions:
            continue
        table_alias = get_dimension_table_alias(resolved_dim, main_alias, dim_aliases)
        col_ref = make_column_ref(resolved_dim.column_name, table_alias)
        group_by.append(
            _maybe_coalesce_with_sibling(
                col_ref,
                resolved_dim,
                resolved_dimensions,
                dim_aliases,
                parent_node_name,
            ),
        )

    for gc_expr, gc_alias in grain_col_specs:
        if isinstance(gc_expr, ast.Column):
            if gc_expr.name.name not in projected_dim_col_names:
                group_by.append(make_column_ref(gc_expr.name.name, main_alias))
        else:
            group_by.append(ast.Column(name=ast.Name(gc_alias)))

    return group_by


def _parse_grain_col_specs(
    grain_columns: list[str],
    grain_col_aliases: dict[str, str] | None,
) -> list[tuple[ast.Expression, str]]:
    """Parse grain column strings into (expression, alias) pairs.

    When ``grain_col_aliases`` provides an override (keyed by the raw expression
    string), that alias is used instead of the raw string.  This lets callers
    pass ``component.name`` so the SQL alias matches the decompose component
    identifier exactly.
    """
    alias_map = grain_col_aliases or {}
    specs: list[tuple[ast.Expression, str]] = []
    for gc in grain_columns:
        expr = cast(ast.Expression, parse(f"SELECT {gc}").select.projection[0])
        alias = alias_map.get(gc) or gc
        specs.append((expr, alias))
    return specs


def _collect_spark_hints(
    resolved_dimensions: list[ResolvedDimension],
    dim_aliases: dict[tuple[str, Optional[str]], str],
) -> list[ast.Hint]:
    """Collect Spark join hints (e.g. BROADCAST) from dimension links."""
    hints: list[ast.Hint] = []
    for resolved_dim in resolved_dimensions:
        if not resolved_dim.is_local and resolved_dim.join_path:
            accumulated_role_parts: list[str] = []
            for link in resolved_dim.join_path.links:
                link_role = link.role or ""
                if link_role:
                    accumulated_role_parts.append(link_role)
                accumulated_role = (
                    "->".join(accumulated_role_parts) if accumulated_role_parts else ""
                )
                dim_key = (link.dimension.name, accumulated_role)
                if link.spark_hints and dim_key in dim_aliases:
                    hint_name = link.spark_hints.value.upper()
                    dim_alias = dim_aliases[dim_key]
                    hints.append(
                        ast.Hint(
                            name=ast.Name(hint_name),
                            parameters=[ast.Column(name=ast.Name(dim_alias))],
                        ),
                    )
    return hints


def build_dimension_joins(
    ctx: BuildContext,
    resolved_dimensions: list[ResolvedDimension],
    main_alias: str,
) -> tuple[dict[tuple[str, Optional[str]], str], list[ast.Join]]:
    """Build JOIN clauses for non-local dimensions.

    Walks each resolved dimension's join path, deduplicating joins when two
    dimensions share a common prefix (same dimension node + accumulated role).

    Returns:
        (dim_aliases, joins) where dim_aliases maps (node_name, accumulated_role)
        to the table alias used in the JOIN.
    """
    dim_aliases: dict[tuple[str, Optional[str]], str] = {}
    joins: list[ast.Join] = []

    for resolved_dim in resolved_dimensions:
        if not resolved_dim.is_local and resolved_dim.join_path:
            current_left_alias = main_alias
            accumulated_role_parts: list[str] = []

            for link in resolved_dim.join_path.links:
                dim_node_name = link.dimension.name

                link_role = link.role or ""
                if link_role:
                    accumulated_role_parts.append(link_role)
                accumulated_role = (
                    "->".join(accumulated_role_parts) if accumulated_role_parts else ""
                )

                dim_key = (dim_node_name, accumulated_role)

                if dim_key not in dim_aliases:  # pragma: no branch
                    if accumulated_role:
                        alias_base = accumulated_role.replace("->", "_")
                    else:
                        alias_base = get_short_name(dim_node_name)
                    dim_alias = ctx.next_table_alias(alias_base)
                    dim_aliases[dim_key] = dim_alias

                    join = build_join_clause(ctx, link, current_left_alias, dim_alias)
                    joins.append(join)

                current_left_alias = dim_aliases[dim_key]

    return dim_aliases, joins


def build_select_ast(
    ctx: BuildContext,
    metric_expressions: list[tuple[str, ast.Expression]],
    resolved_dimensions: list[ResolvedDimension],
    parent_node: Node,
    grain_columns: list[str] | None = None,
    grain_col_aliases: dict[str, str] | None = None,
    filters: list[str] | None = None,
    skip_aggregation: bool = False,
) -> tuple[ast.Query, list[str]]:
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
        grain_col_aliases: Optional mapping from grain column expression string to
                           the SQL alias to use in the generated SELECT.  When provided,
                           the alias is taken from this dict (keyed by the raw expression
                           string from ``rule.level``).
        filters: Optional list of filter strings to apply as WHERE clause.
                 Filter strings can reference dimensions (e.g., "v3.product.category = 'Electronics'")
                 or local columns (e.g., "status = 'active'").
        skip_aggregation: If True, skip adding GROUP BY clause. Used for non-decomposable
                          metrics where raw rows need to be passed through.

    Returns:
        Tuple of (query, scanned_sources):
        - query: AST Query node
        - scanned_sources: List of source node names accessed during SQL generation
    """
    # Build projection (SELECT clause)
    # Use Any type to satisfy ast.Select.projection which accepts Union[Aliasable, Expression, Column]
    projection: list[Any] = []
    grain_columns = grain_columns or []

    # Generate alias for the main table
    main_alias = ctx.next_table_alias(parent_node.name)

    dim_aliases, joins = build_dimension_joins(ctx, resolved_dimensions, main_alias)
    spark_hints = _collect_spark_hints(resolved_dimensions, dim_aliases)

    # Add dimension columns to projection
    # Filter-only dimensions are excluded from projection but included in GROUP BY
    for resolved_dim in resolved_dimensions:
        clean_alias = ctx.alias_registry.register(resolved_dim.original_ref)
        if resolved_dim.original_ref in ctx.filter_dimensions:
            continue
        col_expr = build_dimension_col_expr(
            resolved_dim,
            main_alias,
            dim_aliases,
            clean_alias,
            ctx=ctx,
            resolved_dimensions=resolved_dimensions,
            parent_node_name=parent_node.name,
        )
        projection.append(col_expr)

    grain_col_specs = _parse_grain_col_specs(grain_columns, grain_col_aliases)

    # Add grain columns for LIMITED aggregability (e.g., customer_id for COUNT DISTINCT)
    # These are added to the output so the result can be re-aggregated.
    # When the grain column is a complex expression (e.g., IF(...)), rewrite
    # column references to use the table alias, and select it with a clean alias.
    #
    # Skip adding to projection if the column was already projected as a dimension
    # (e.g., order_id requested as both a dimension and a COUNT DISTINCT level).
    projected_dim_col_names = {
        rd.column_name
        for rd in resolved_dimensions
        if rd.original_ref not in ctx.filter_dimensions
    }
    grain_col_refs: list[ast.Column] = []
    for gc_expr, gc_alias in grain_col_specs:
        if isinstance(gc_expr, ast.Column):
            col_name = gc_expr.name.name
            col_ref = make_column_ref(col_name, main_alias)
            grain_col_refs.append(col_ref)
            if col_name not in projected_dim_col_names:
                projection.append(col_ref)
        else:
            _rewrite_col_refs(gc_expr, main_alias)
            projection.append(ast.Alias(child=gc_expr, alias=ast.Name(gc_alias)))
            grain_col_refs.append(ast.Column(name=ast.Name(gc_alias)))

    # Resolve dim-namespaced refs in metric expressions (e.g. ``v3.customer.tier``)
    # to the dim's joined table alias, and remember which columns each dim
    # CTE must keep. Done up-front so the rewrite is visible to both the
    # projection loop below and CTE pruning.
    dim_node_to_alias: dict[str, str] = {}
    for (dim_node_name, role), alias in dim_aliases.items():
        if dim_node_name not in dim_node_to_alias or not role:
            dim_node_to_alias[dim_node_name] = alias
    metric_dim_cols = _resolve_dim_namespace_refs(metric_expressions, dim_node_to_alias)

    # Add metric expressions
    for alias_name, expr in metric_expressions:
        clean_alias = ctx.alias_registry.register(alias_name)
        projection.append(_build_metric_col_expr(expr, clean_alias, main_alias))

    group_by = _build_group_by(
        resolved_dimensions,
        dim_aliases,
        main_alias,
        grain_col_specs,
        projected_dim_col_names,
        ctx.filter_dimensions,
        parent_node_name=parent_node.name,
    )

    # Collect all nodes that need CTEs and the minimal columns each must project.
    nodes_for_ctes, needed_columns_by_node = collect_cte_nodes_and_needed_columns(
        ctx,
        parent_node,
        resolved_dimensions,
        grain_col_specs,
        metric_expressions,
    )
    for dim_name, cols in metric_dim_cols.items():
        needed_columns_by_node.setdefault(dim_name, set()).update(cols)

    temporal_filter_ast, injected_cte_filters = _build_temporal_pushdown(
        ctx,
        parent_node,
        main_alias,
    )

    # Merge in upstream-link filter pushdowns (registered when a filter-only
    # dim is reachable only via an upstream's dim link). Multiple filters per
    # target node are ANDed together; if temporal pushdown already targeted
    # the same node, AND them together too.
    for tgt_name, exprs in ctx.upstream_pushdown_filters.items():
        if not exprs:
            continue  # pragma: no cover
        combined = exprs[0]
        for extra in exprs[1:]:
            combined = ast.BinaryOp.And(combined, extra)
        if tgt_name in injected_cte_filters:
            injected_cte_filters[tgt_name] = ast.BinaryOp.And(
                injected_cte_filters[tgt_name],
                combined,
            )
        else:
            injected_cte_filters[tgt_name] = combined

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

    # Filters fully handled via upstream-link pushdown are excluded from
    # outer WHERE / per-CTE pushdown — they're already injected into the
    # appropriate upstream CTE.
    all_filters = [f for f in (filters or []) if f not in ctx.pushdown_consumed_filters]

    # Build outer WHERE clause from filters
    where_clause: Optional[ast.Expression] = None
    filter_column_aliases: dict[str, str] = {}
    if all_filters:
        filter_column_aliases = build_filter_column_aliases(
            ctx,
            resolved_dimensions,
            parent_node,
        )
        where_clause = build_outer_where(
            all_filters,
            filter_column_aliases,
            resolved_dimensions,
            main_alias,
            dim_aliases,
            parent_node,
            nodes=ctx.nodes,
        )

    # Build CTEs for all non-source nodes. Requested dimension filters and their
    # resolution map are passed through so each CTE can independently decide
    # whether to push a filter into its own WHERE clause.
    ctes, scanned_sources = collect_node_ctes(
        ctx,
        nodes_for_ctes,
        needed_columns_by_node,
        injected_filters=injected_cte_filters or None,
        pushdown=PushdownFilters(
            filters=all_filters,
            column_aliases=filter_column_aliases,
        )
        if all_filters
        else None,
    )

    # Build SELECT.
    # For non-decomposable metrics, skip GROUP BY to pass through raw rows.
    effective_group_by = [] if skip_aggregation else (group_by if group_by else [])
    select = ast.Select(
        projection=projection,
        from_=from_clause,
        group_by=effective_group_by,
        hints=spark_hints if spark_hints else None,
    )

    # Apply outer WHERE atoms.  Parent-alias atoms are dropped from outer
    # WHERE when the parent CTE pushdown is active (filter is already in
    # the parent CTE; the outer copy is redundant and would defeat
    # downstream OUTER joins).  Dim-alias atoms continue to land in
    # outer WHERE for standard dim-filter semantics.  See
    # ``_apply_outer_where_atoms`` for the full rule.
    parent_pushdown_active = bool(all_filters) and not should_use_materialized_table(
        ctx,
        parent_node,
    )
    # Absorb LEFT/INNER-joined dims whose filter would defeat a
    # downstream RIGHT/FULL OUTER JOIN into a filtered CTE on the
    # parent side.  Mutates ``select.from_`` (re-pointing the parent
    # FROM at the new CTE), rewrites references to absorbed aliases,
    # remaps ``dim_aliases``, and removes the absorbed atoms from
    # ``where_clause``.  Returns a new CTE to append to the query.
    where_clause, filtered_cte = _absorb_filtered_joins_for_outer_safety(
        select=select,
        where_clause=where_clause,
        main_alias=main_alias,
        dim_aliases=dim_aliases,
        parent_cte_name=get_cte_name(parent_node.name),
        group_by=effective_group_by,
    )
    if filtered_cte is not None:
        ctes.append(filtered_cte)
    _apply_outer_where_atoms(
        select,
        where_clause,
        main_alias,
        parent_pushdown_active=parent_pushdown_active,
    )
    if temporal_filter_ast is not None:  # pragma: no cover
        # ``_build_temporal_pushdown`` always pushes the filter into a
        # CTE and returns ``None``, so this branch is currently dead.
        # Kept as a safety backstop in case the pushdown logic changes
        # in the future.
        inject_filter_into_select(select, temporal_filter_ast)

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

    return query, scanned_sources


def build_temporal_filter(
    ctx: BuildContext,
    parent_node: Node,
    table_alias: Optional[str],
) -> tuple[Optional[ast.Expression], Optional[str]]:
    """
    Build temporal filter expression based on cube's temporal partition columns.

    Checks if the parent node has dimension links to any of the cube's temporal
    partition columns, and generates filters for those columns.

    Returns:
        Tuple of (filter_expression, fk_col_name):
        - filter_expression: BinaryOp (col = expr) for exact match, Between for lookback, or None
        - fk_col_name: the parent node's FK column name used in the filter, or None
    """
    if not ctx.temporal_partition_columns or not parent_node.current:
        return None, None

    # For each temporal partition column specified by the cube
    for partition_col_ref, partition_metadata in ctx.temporal_partition_columns.items():
        # Parse "v3.date.date_id" -> dimension node and column
        parsed = parse_dimension_ref(partition_col_ref)

        # Check if this parent has a dimension link to the partition column's node
        if not parent_node.current.dimension_links:
            continue  # pragma: no cover

        for link in parent_node.current.dimension_links:  # pragma: no branch
            if link.dimension.name == parsed.node_name:
                # Found a dimension link to the temporal partition dimension
                # Find the column on the dimension (cube already declared this as temporal)
                temporal_col = None
                for col in link.dimension.current.columns:  # pragma: no branch
                    if col.name == parsed.column_name:  # pragma: no branch
                        temporal_col = col
                        break

                if not temporal_col:
                    continue  # pragma: no cover

                # Get the parent's foreign key column that joins to this dimension
                # e.g., if dimension is v3.date.date_id, we need the parent's column like "order_date"
                fk_columns = link.foreign_key_column_names
                if not fk_columns:
                    continue  # pragma: no cover

                # Use the first foreign key column (typically there's only one for temporal dimensions)
                parent_col_name = next(iter(fk_columns))

                # Generate the temporal filter expression using the parent's column
                col_ref = make_column_ref(parent_col_name, table_alias)

                # Get the end expression (current logical timestamp) from cube's partition metadata
                end_expr = partition_metadata.temporal_expression(interval=None)

                if ctx.lookback_window and end_expr:
                    # For lookback, generate BETWEEN filter using cube's partition metadata
                    if start_expr := partition_metadata.temporal_expression(
                        interval=ctx.lookback_window,
                    ):  # pragma: no branch
                        return ast.Between(
                            expr=col_ref,
                            low=start_expr,
                            high=end_expr,
                        ), parent_col_name
                elif end_expr:  # pragma: no branch
                    # No lookback - exact partition match
                    return ast.BinaryOp(
                        left=col_ref,
                        right=end_expr,
                        op=ast.BinaryOpKind.Eq,
                    ), parent_col_name

    return None, None  # pragma: no cover


def find_upstream_temporal_source_node(
    ctx: BuildContext,
    parent_node: Node,
    fk_col_name: str,
) -> Optional[Node]:
    """
    Find the upstream node that directly provides the temporal FK column to parent_node.

    Walks parent_node's FROM clause to find the primary (driving) table of any join,
    then checks if that table's corresponding DJ node exposes the FK column. This is
    used to push temporal filters down into the upstream CTE rather than applying them
    on the outer grain-group query.

    For example, if parent_node is a rolling-window transform that joins a date-spine
    transform against a windowed fact, this returns the date-spine node so the filter
    can be injected there — avoiding a full cross-date join before filtering.

    Returns the upstream Node if found, otherwise None (caller falls back to outer WHERE).
    """
    if not parent_node.current or not parent_node.current.query:  # pragma: no cover
        return None

    try:
        query_ast = ctx.get_parsed_query(parent_node)
    except Exception:  # pragma: no cover
        return None

    if not query_ast.select or not query_ast.select.from_:
        return None

    for relation in query_ast.select.from_.relations:
        primary = relation.primary

        # Unwrap alias: Alias(child=Table(...), alias=Name("dd")) -> Table(...)
        table_expr = (
            getattr(primary, "child", primary)
            if isinstance(primary, ast.Alias)
            else primary
        )

        if not isinstance(table_expr, ast.Table):
            continue  # pragma: no cover

        table_name = str(table_expr.name)
        upstream_node = ctx.nodes.get(table_name)
        if not upstream_node or not upstream_node.current:  # pragma: no cover
            continue

        # Only push into non-source nodes — source nodes don't become CTEs,
        # so injecting there would silently drop the filter
        if upstream_node.type == NodeType.SOURCE:
            continue

        # Check if this upstream node exposes the FK column
        upstream_col_names = {col.name for col in (upstream_node.current.columns or [])}
        if fk_col_name in upstream_col_names:
            return upstream_node

    return None


def build_grain_group_from_preagg(
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

    if not avail:
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
        if not measure_col:
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
    preagg_table = SEPARATOR.join(table_parts)
    from_clause = ast.From.Table(preagg_table)

    # Build SELECT statement
    select = ast.Select(
        projection=select_items,
        from_=from_clause,
        group_by=group_by,
    )

    # Build the query
    query = ast.Query(select=select)

    # Pre-aggregation path: no raw sources scanned (uses materialized table)
    # TODO: Consider tracking the pre-agg table itself as a "materialized source"
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
        scanned_sources=[preagg_table],
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
    if ctx.use_materialized and ctx.available_preaggs:
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
                # LIMITED: grain column is already in GROUP BY, no output needed.
                # grain_alias was set by _make_component: plain column → column name,
                # complex expression → component.name.
                component_aliases[component.name] = (
                    component.grain_alias or component.name
                )
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
        # These are represented by grain columns instead.
        # grain_alias was set by _make_component: plain column → column name,
        # complex expression → component.name.
        if component.rule.type == Aggregability.LIMITED and not component.aggregation:
            component_aliases[component.name] = component.grain_alias or component.name
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
        query_ast, scanned_sources = build_select_ast(
            ctx,
            metric_expressions=[],  # No aggregated expressions
            resolved_dimensions=resolved_dimensions,
            parent_node=parent_node,
            grain_columns=pass_through_columns,
            filters=ctx.dimension_filters,  # Use dimension_filters only (not metric_filters)
            skip_aggregation=True,  # Don't add GROUP BY
        )
    else:
        # Normal case: combine component expressions with non-decomposable columns
        all_metric_expressions = component_expressions + non_decomposable_columns
        query_ast, scanned_sources = build_select_ast(
            ctx,
            metric_expressions=all_metric_expressions,
            resolved_dimensions=resolved_dimensions,
            parent_node=parent_node,
            grain_columns=effective_grain_columns,
            grain_col_aliases=grain_group.grain_col_aliases or None,
            filters=ctx.dimension_filters,  # Use dimension_filters only (not metric_filters)
        )

    # Build column metadata
    columns_metadata = []

    # Add dimension columns (skip filter-only dimensions as they're not in projection)
    for resolved_dim in resolved_dimensions:
        # Skip filter-only dimensions from column metadata
        if resolved_dim.original_ref in ctx.filter_dimensions:
            continue

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

    # Add grain columns (for LIMITED and NONE).
    # The alias comes directly from grain_col_aliases; no AST parsing needed here
    # since column metadata only uses the alias string, not the expression.
    # Skip plain grain columns that are already represented as a requested dimension
    # to avoid duplicate entries (e.g., order_id requested as both a dimension and
    # the COUNT DISTINCT level column).
    projected_dim_col_names_meta = {
        rd.column_name
        for rd in resolved_dimensions
        if rd.original_ref not in ctx.filter_dimensions
    }
    _eff_alias_map = grain_group.grain_col_aliases
    effective_grain_aliases = [
        _eff_alias_map.get(gc) or gc for gc in effective_grain_columns
    ]

    for gc, gc_alias in zip(effective_grain_columns, effective_grain_aliases):
        if gc in projected_dim_col_names_meta:
            continue
        col_type = get_column_type(parent_node, gc_alias)
        columns_metadata.append(
            ColumnMetadata(
                name=gc_alias,
                semantic_name=f"{parent_node.name}{SEPARATOR}{gc_alias}",
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
    # Skip filter-only dimensions as they're not part of the output grain
    full_grain = []
    if grain_group.aggregability != Aggregability.NONE:
        # FULL/LIMITED: dimensions are part of the grain
        for resolved_dim in resolved_dimensions:
            # Skip filter-only dimensions from grain
            if resolved_dim.original_ref in ctx.filter_dimensions:
                continue
            alias = (
                ctx.alias_registry.get_alias(resolved_dim.original_ref)
                or resolved_dim.column_name
            )
            full_grain.append(alias)

    # Add any additional grain columns (from LIMITED/NONE aggregability).
    # Use the alias for complex expressions so the grain list stays clean.
    for gc_alias in effective_grain_aliases:
        if gc_alias not in full_grain:  # pragma: no branch
            full_grain.append(gc_alias)

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
        scan_estimate=calculate_scan_estimate(scanned_sources, ctx),
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


def build_window_metric_grain_groups(
    ctx: BuildContext,
    window_metric_grains: dict[str, set[str]],
    existing_grain_groups: list[GrainGroupSQL],
    decomposed_metrics: dict,
) -> list[GrainGroupSQL]:
    """
    Build additional grain groups for window metrics that require grain-level aggregation.

    LAG/LEAD window functions need pre-aggregation at their ORDER BY grain before the
    window function is applied. This function creates grain groups at those grains.

    For example, for a WoW metric `LAG(revenue, 1) OVER (ORDER BY v3.date.week)`:
    - The base metric `revenue` is already computed at the user's requested grain (e.g., daily)
    - We need an additional grain group at the WEEKLY grain for the LAG to compare properly
    - The weekly grain group excludes finer-grained time dimensions (like daily)

    These grain groups go through normal pre-agg matching, so if a pre-agg exists at the
    window metric's grain (e.g., weekly), it will be used instead of re-scanning source tables.

    Args:
        ctx: Build context
        window_metric_grains: Mapping of metric_name -> set of ORDER BY column refs
            (e.g., {"v3.wow_revenue": {"v3.date.week"}})
        existing_grain_groups: Grain groups already built (for finding components)
        decomposed_metrics: Decomposed metric info

    Returns:
        List of additional GrainGroupSQL for window metrics at their ORDER BY grains
    """
    if not window_metric_grains:
        return []  # pragma: no cover

    # First, determine the parent fact for each window metric
    # This is based on which grain group the base metric(s) belong to
    def find_grain_group_parent(metric_name: str, visited: set[str]) -> set[str]:
        """
        Recursively find the grain group parent(s) for a metric.

        For base metrics: returns the grain group parent directly
        For derived metrics: traces through to find the underlying base metrics

        Returns set of grain group parent names (e.g., {"thumb_rating"})
        """
        if metric_name in visited:
            return set()  # pragma: no cover
        visited.add(metric_name)

        # Check if this metric is directly in a grain group
        for gg in existing_grain_groups:
            if metric_name in gg.metrics:
                return {gg.parent_name}

        # Not in a grain group - might be a derived metric
        # Check parent_map for dependencies
        parent_names = ctx.parent_map.get(metric_name, [])
        grain_group_parents: set[str] = set()
        for parent_name in parent_names:
            metric_parent = ctx.nodes.get(parent_name)
            if (  # pragma: no branch
                metric_parent and metric_parent.type.value == "metric"
            ):
                # Recursively find grain group parents
                grain_group_parents.update(
                    find_grain_group_parent(parent_name, visited),
                )

        return grain_group_parents

    def find_parent_for_window_metric(
        metric_name: str,
    ) -> tuple[Optional[str], set[str]]:
        """
        Find the parent fact name and base metrics for a window metric.

        Returns:
            Tuple of (parent_name, base_metrics_set)
            parent_name is None if base metrics span multiple facts (cross-fact)
        """
        decomposed = decomposed_metrics.get(metric_name)
        if not decomposed or not isinstance(decomposed, DecomposedMetricInfo):
            return None, set()  # pragma: no cover

        base_metrics: set[str] = set()

        # Find parent metrics from parent_map
        metric_parent_names = ctx.parent_map.get(metric_name, [])
        for parent_name in metric_parent_names:
            metric_parent = ctx.nodes.get(parent_name)
            if (
                metric_parent and metric_parent.type.value == "metric"
            ):  # pragma: no branch
                base_metrics.add(parent_name)

        # Trace through to find the grain group parents (handles derived metrics)
        all_grain_group_parents: set[str] = set()
        for base_metric in base_metrics:
            all_grain_group_parents.update(
                find_grain_group_parent(base_metric, set()),
            )

        if len(all_grain_group_parents) == 1:
            return next(iter(all_grain_group_parents)), base_metrics
        elif len(all_grain_group_parents) > 1:
            # Cross-fact window metric
            return None, base_metrics
        else:
            return None, base_metrics  # pragma: no cover

    # Group window metrics by (ORDER BY grain, parent fact)
    # This ensures window metrics from different facts are processed separately
    # Key: (frozenset of grain cols, parent_name or "cross_fact")
    grain_parent_to_metrics: dict[tuple[frozenset[str], str], list[str]] = {}
    grain_parent_to_base_metrics: dict[tuple[frozenset[str], str], set[str]] = {}

    for metric_name, grains in window_metric_grains.items():
        grain_key = frozenset(grains)
        parent_name, base_metrics = find_parent_for_window_metric(metric_name)
        # Use "cross_fact" as a marker for cross-fact window metrics
        parent_key = parent_name if parent_name else "cross_fact"

        group_key = (grain_key, parent_key)
        if group_key not in grain_parent_to_metrics:
            grain_parent_to_metrics[group_key] = []
            grain_parent_to_base_metrics[group_key] = set()
        grain_parent_to_metrics[group_key].append(metric_name)
        grain_parent_to_base_metrics[group_key].update(base_metrics)

    additional_grain_groups: list[GrainGroupSQL] = []

    for (grain_cols, parent_key), metric_names in grain_parent_to_metrics.items():
        # Build dimension node mapping for this grain
        # grain_cols contains full refs like "v3.date.week" or "v3.date.week[order]"
        grain_dim_nodes: set[str] = set()
        grain_col_names: set[str] = set()
        for grain_col in grain_cols:
            clean_ref = strip_role_suffix(grain_col)
            grain_dim_nodes.add(extract_dimension_node(clean_ref))
            # Extract just the column name
            col_name = clean_ref.rsplit(SEPARATOR, 1)[-1]
            grain_col_names.add(col_name)

        # Get the base metrics needed for this group
        base_metrics_needed = grain_parent_to_base_metrics[(grain_cols, parent_key)]

        if not base_metrics_needed:
            continue  # pragma: no cover

        # Find the components for these base metrics from existing grain groups
        # Also identify the parent node for the grain group
        components_for_grain: list[tuple[Node, MetricComponent]] = []
        parent_node: Optional[Node] = None
        component_aggregabilities: dict[str, Aggregability] = {}
        is_cross_fact = parent_key == "cross_fact"

        for gg in existing_grain_groups:
            for base_metric_name in base_metrics_needed:
                if base_metric_name in gg.metrics:
                    # Found a grain group containing this base metric
                    if parent_node is None:
                        parent_node = ctx.nodes.get(gg.parent_name)

                    # Get the components for this metric from decomposed_metrics
                    base_decomposed = decomposed_metrics.get(base_metric_name)
                    if base_decomposed and isinstance(  # pragma: no branch
                        base_decomposed,
                        DecomposedMetricInfo,
                    ):
                        metric_node = base_decomposed.metric_node
                        for component in base_decomposed.components:
                            components_for_grain.append((metric_node, component))
                            component_aggregabilities[component.name] = (
                                base_decomposed.aggregability
                            )

        if not parent_node or not components_for_grain:
            continue

        # Determine which dimensions to include at this grain
        # Exclude all dimensions from the same dimension node as the ORDER BY columns
        # (except the ORDER BY columns themselves)
        filtered_dimensions: list[str] = []
        for dim_ref in ctx.dimensions:
            clean_ref = strip_role_suffix(dim_ref)
            dim_node = extract_dimension_node(clean_ref)
            col_name = clean_ref.rsplit(SEPARATOR, 1)[-1]

            if dim_node in grain_dim_nodes:
                # Same dimension node as ORDER BY - only include if it's the ORDER BY column
                if col_name in grain_col_names:
                    filtered_dimensions.append(dim_ref)
            else:
                # Different dimension node - include it
                filtered_dimensions.append(dim_ref)

        # Skip creating a window grain group if the grain matches the user-requested grain
        # In this case, the window function can be applied directly to base_metrics
        if set(filtered_dimensions) == set(ctx.dimensions):
            continue

        # Save original dimensions and temporarily set filtered dimensions
        # We can't deepcopy ctx because it contains AsyncSession
        original_dimensions = ctx.dimensions
        ctx.dimensions = filtered_dimensions
        ctx.alias_registry = AliasRegistry()
        ctx._table_alias_counter = 0

        # Resolve dimensions for this grain
        resolved_dimensions = resolve_dimensions(ctx, parent_node)

        # Create GrainGroup for the window metrics
        # Use FULL aggregability since we're aggregating to a specific grain
        grain_group = GrainGroup(
            parent_node=parent_node,
            aggregability=Aggregability.FULL,
            grain_columns=[],
            components=components_for_grain,
            is_merged=False,
            component_aggregabilities=component_aggregabilities,
        )

        # Build GrainGroupSQL
        components_per_metric: dict[str, int] = {}
        for metric_name in base_metrics_needed:
            base_decomposed = decomposed_metrics.get(metric_name)
            if base_decomposed and isinstance(  # pragma: no branch
                base_decomposed,
                DecomposedMetricInfo,
            ):
                components_per_metric[metric_name] = len(base_decomposed.components)

        grain_group_sql = build_grain_group_sql(
            ctx,
            grain_group,
            resolved_dimensions,
            components_per_metric,
        )

        # Restore original dimensions
        ctx.dimensions = original_dimensions

        # Tag this grain group with the window metrics it serves
        # This helps metrics SQL identify which grain group to use
        grain_group_sql.metrics = list(metric_names)

        # Mark as a window grain group with metadata for metrics phase
        grain_group_sql.is_window_grain_group = True
        grain_group_sql.window_metrics_served = list(metric_names)
        # Use the first grain column as the ORDER BY dimension
        grain_group_sql.window_order_by_dim = (
            next(iter(grain_cols)) if grain_cols else None
        )
        # Mark if this is a cross-fact window group (needs base_metrics as source)
        grain_group_sql.is_cross_fact_window = is_cross_fact

        additional_grain_groups.append(grain_group_sql)

    return additional_grain_groups
