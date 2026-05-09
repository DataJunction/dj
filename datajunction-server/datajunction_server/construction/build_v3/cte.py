"""
CTE building and AST transformation utilities
"""

from __future__ import annotations

from copy import deepcopy
from typing import Optional, cast

from datajunction_server.construction.build_v3.filters import (
    extract_subscript_role,
    parse_filter,
)
from datajunction_server.construction.build_v3.materialization import (
    get_table_reference_parts_with_materialization,
    should_use_materialized_table,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    GrainGroupSQL,
    PushdownFilters,
)
from datajunction_server.construction.build_v3.utils import get_cte_name
from datajunction_server.database.node import Node
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.utils import SEPARATOR


def get_table_references_from_ast(query_ast: ast.Query) -> set[str]:
    """
    Extract all table references from a query AST.

    Returns set of table names (as dotted strings like 'v3.src_orders').
    """
    table_names: set[str] = set()
    for table in query_ast.find_all(ast.Table):
        # Get the full table name including namespace
        table_name = str(table.name)
        if table_name:  # pragma: no branch
            table_names.add(table_name)
    return table_names


def get_column_full_name(col: ast.Column) -> str:
    """
    Get the full dotted name of a column, including its table/namespace.

    For example, a column like v3.date.month returns "v3.date.month".
    """
    parts: list[str] = []

    # Get table prefix if present
    if col.table and hasattr(col.table, "alias_or_name"):
        table_name = col.table.alias_or_name
        if table_name:  # pragma: no branch
            if isinstance(table_name, str):  # pragma: no cover
                parts.append(table_name)
            else:
                # It's an ast.Name with possible namespace
                parts.append(table_name.identifier(quotes=False))

    # Get column name with its namespace chain
    if col.name:  # pragma: no branch
        parts.append(col.name.identifier(quotes=False))

    return SEPARATOR.join(parts) if parts else ""  # pragma: no cover


def extract_dimension_node(dim_ref: str) -> str:
    """
    Extract the dimension node name from a full dimension reference.

    For example:
        "v3.date.date_id" -> "v3.date"
        "v3.date.month" -> "v3.date"
        "v3.product.category" -> "v3.product"

    Args:
        dim_ref: Full dimension reference (node.column format)

    Returns:
        The dimension node name (everything before the last separator)
    """
    parts = dim_ref.rsplit(SEPARATOR, 1)
    return parts[0] if len(parts) > 1 else dim_ref


def build_alias_to_dimension_node(
    dim_info: list[tuple[str, str]],
) -> dict[str, str]:
    """
    Build a mapping from column alias to dimension node.

    This is used to determine which aliases belong to the same dimension node,
    so that when a window function orders by one attribute of a dimension (e.g., week_code),
    we can exclude all attributes of that dimension (e.g., dateint) from PARTITION BY.

    Args:
        dim_info: List of (original_dim_ref, col_alias) tuples
            e.g., [("v3.date.date_id", "date_id"),
                   ("v3.date.month", "month")]

    Returns:
        Mapping from alias to dimension node
            e.g., {"date_id": "v3.date",
                   "month": "v3.date"}
    """
    return {
        col_alias: extract_dimension_node(dim_ref) for dim_ref, col_alias in dim_info
    }


def build_dimension_node_to_aliases(
    alias_to_node: dict[str, str],
) -> dict[str, set[str]]:
    """
    Build a mapping from dimension node to all its aliases.

    Args:
        alias_to_node: Mapping from alias to dimension node
            e.g., {"week": "v3.date", "month": "v3.date", "category": "v3.product"}

    Returns:
        Mapping from dimension node to set of aliases
            e.g., {"v3.date": {"week", "month"}, "v3.product": {"category"}}
    """
    node_to_aliases: dict[str, set[str]] = {}
    for alias, node in alias_to_node.items():
        if node not in node_to_aliases:
            node_to_aliases[node] = set()
        node_to_aliases[node].add(alias)
    return node_to_aliases


def strip_role_suffix(ref: str) -> str:
    """
    Strip role suffix like [order], [filter] from a dimension reference.

    For example:
        "v3.date.week[order]" -> "v3.date.week"
        "v3.date.month" -> "v3.date.month"
    """
    if "[" in ref:
        return ref.split("[")[0]
    return ref


def extract_dim_info_from_grain_groups(
    grain_groups: list[GrainGroupSQL],
) -> list[tuple[str, str]]:
    """
    Extract dimension info (dim_ref, alias) tuples from all grain group columns.

    This includes ALL dimensions in the grain groups, not just user-requested ones.
    This is important for window function PARTITION BY logic, which needs to know
    about all dimensions from the same dimension node (e.g., date_id, week, month
    all come from v3.date).

    Args:
        grain_groups: List of grain group SQLs

    Returns:
        List of (dim_ref, alias) tuples for all dimension columns
    """
    dim_info: list[tuple[str, str]] = []
    seen_aliases: set[str] = set()

    for gg in grain_groups:
        for col in gg.columns:
            if col.semantic_type == "dimension" and col.name not in seen_aliases:
                # Strip role suffix from semantic_name for consistent dimension node extraction
                dim_ref = strip_role_suffix(col.semantic_name)
                dim_info.append((dim_ref, col.name))
                seen_aliases.add(col.name)

    return dim_info


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
    for col in list(expr_ast.find_all(ast.Column)):
        # Get the column name (might be in name.name or just name)
        col_name = col.name.name if col.name else None
        if not col_name:  # pragma: no cover
            continue

        # Check if this column name matches a component
        if col_name in component_aliases:  # pragma: no branch
            table_alias, actual_col = component_aliases[col_name]
            col.name = ast.Name(actual_col)
            # Only set table if alias is non-empty (empty = no CTE prefix)
            col._table = ast.Table(ast.Name(table_alias)) if table_alias else None


def replace_metric_refs_in_ast(
    expr_ast: ast.Node,
    metric_aliases: dict[str, tuple[str, str]],
) -> None:
    """
    Replace metric name references in an AST with qualified column references.

    For derived metrics like `avg_order_value = total_revenue / order_count`,
    the combiner AST contains references to metric names like `v3.total_revenue`.
    This function replaces them with proper CTE column references like `cte.total_revenue`.

    Args:
        expr_ast: The AST expression to modify (mutated in place)
        metric_aliases: Mapping from metric name to (cte_alias, column_name)
            e.g., {"v3.total_revenue": ("order_details_0", "total_revenue")}
    """
    for col in expr_ast.find_all(ast.Column):
        # Get the full metric name (e.g., "v3.total_revenue")
        full_name = get_column_full_name(col)
        if not full_name:  # pragma: no cover
            continue

        # Check if this matches a metric name
        if full_name in metric_aliases:
            cte_alias, col_name = metric_aliases[full_name]
            col.name = ast.Name(col_name)
            # Only set table if alias is non-empty (empty = no CTE prefix)
            col._table = ast.Table(ast.Name(cte_alias)) if cte_alias else None


def replace_dimension_refs_in_ast(
    expr_ast: ast.Node,
    dimension_refs: dict[str, tuple[str, str]],
) -> None:
    """
    Replace dimension references in an AST with CTE-qualified column references.

    Modifies the AST in place. Handles two patterns:

    1. Simple column references: v3.date.month -> cte.month_order
    2. Subscript (role) references: v3.date.month[order] -> cte.month_order
       (SQL parser interprets [role] as array subscript)

    Args:
        expr_ast: The AST expression to modify (mutated in place)
        dimension_refs: Mapping from dimension refs to (cte_alias, column_name)
            e.g., {"v3.date.month": ("base_metrics", "month"),
                   "v3.date.month[order]": ("base_metrics", "month_order")}
    """
    # First pass: handle Subscript nodes (role syntax like v3.date.week[order])
    # SQL parser interprets [order] as array subscript, not DJ role syntax
    # We need to reconstruct the dimension ref and replace the whole subscript
    for subscript in list(expr_ast.find_all(ast.Subscript)):
        if not isinstance(subscript.expr, ast.Column):
            continue  # pragma: no cover

        # Get the base column name (e.g., "v3.date.week")
        base_col_name = get_column_full_name(subscript.expr)
        if not base_col_name:  # pragma: no cover
            continue

        role = extract_subscript_role(subscript)
        if not role:  # pragma: no cover
            continue

        # Build the full dimension ref with role: "v3.date.week[order]"
        dim_ref_with_role = f"{base_col_name}[{role}]"

        # Look up in dimension_refs
        ref_tuple = None
        if dim_ref_with_role in dimension_refs:
            ref_tuple = dimension_refs[dim_ref_with_role]  # pragma: no cover
        elif base_col_name in dimension_refs:  # pragma: no branch
            # Also try just the base name (if user requested v3.date.week without role)
            ref_tuple = dimension_refs[base_col_name]

        if ref_tuple:  # pragma: no branch
            cte_alias, col_name = ref_tuple
            # Replace the Subscript with a column reference using swap
            # Only set table if alias is non-empty (empty = no CTE prefix)
            replacement = ast.Column(
                name=ast.Name(col_name),
                _table=ast.Table(ast.Name(cte_alias)) if cte_alias else None,
            )
            subscript.swap(replacement)

    # Second pass: handle regular Column references (no subscript)
    for col in expr_ast.find_all(ast.Column):
        full_name = get_column_full_name(col)
        if not full_name:  # pragma: no cover
            continue

        # Check for exact match first (handles roles like "v3.date.month[order]")
        if full_name in dimension_refs:
            cte_alias, col_name = dimension_refs[full_name]
            # Replace with column reference
            col.name = ast.Name(col_name)
            # Only set table if alias is non-empty (empty = no CTE prefix)
            if cte_alias:
                col._table = ast.Table(ast.Name(cte_alias))
            else:
                col._table = None
            continue

        # Check without role suffix (for base dimension refs)
        # The column AST might be just "v3.date.month" but we have "v3.date.month[order]"
        for dim_ref, ref_tuple in dimension_refs.items():
            # Match if the dim_ref starts with our full_name and has a role suffix
            if dim_ref.startswith(full_name) and (
                dim_ref == full_name or dim_ref[len(full_name)] == "["
            ):  # pragma: no cover
                cte_alias, col_name = ref_tuple
                col.name = ast.Name(col_name)
                # Only set table if alias is non-empty (empty = no CTE prefix)
                if cte_alias:
                    col._table = ast.Table(ast.Name(cte_alias))
                else:
                    col._table = None
                break


def has_window_function(expr_ast: ast.Node) -> bool:
    """
    Check if an AST contains any window function (function with OVER clause).

    Window functions (both aggregate like AVG OVER and navigation like LAG)
    require base metrics to be pre-computed before the window function is applied.

    Args:
        expr_ast: The AST expression to check

    Returns:
        True if the expression contains any window function
    """
    for func in expr_ast.find_all(ast.Function):
        if func.over:  # Has OVER clause = window function
            return True
    return False


# Window functions that need PARTITION BY injection for period-over-period calculations
# These are navigation/ranking functions where comparing across partitions is meaningful
# Aggregate functions (SUM, AVG, etc.) with OVER () are intentionally left alone
# as they compute grand totals which is often the desired behavior (e.g., weighted CPM)
PARTITION_BY_INJECTION_FUNCTIONS = frozenset(
    {
        # Navigation functions (need partitioning for period comparisons)
        "LAG",
        "LEAD",
        "FIRST_VALUE",
        "LAST_VALUE",
        "NTH_VALUE",
        # Ranking functions (need partitioning for per-group ranking)
        "ROW_NUMBER",
        "RANK",
        "DENSE_RANK",
        "NTILE",
        "PERCENT_RANK",
        "CUME_DIST",
    },
)

# Navigation functions that require grain-level aggregation for period-over-period
# LAG/LEAD compare values across rows at a specific grain, so we need to pre-aggregate
# to that grain before applying the window function
GRAIN_LEVEL_AGGREGATION_FUNCTIONS = frozenset(
    {
        "LAG",
        "LEAD",
    },
)


def needs_grain_level_aggregation(expr_ast: ast.Node) -> bool:
    """
    Check if an expression uses LAG/LEAD window functions that need grain-level aggregation.

    LAG/LEAD functions compare values across rows at a specific grain (e.g., week-over-week).
    Unlike frame-based functions (SUM OVER ROWS BETWEEN), these need pre-aggregation to the
    ORDER BY grain before the window function is applied.

    Args:
        expr_ast: The AST expression to check

    Returns:
        True if the expression contains LAG/LEAD window functions
    """
    for func in expr_ast.find_all(ast.Function):
        if func.over and func.name:
            func_name = (
                str(func.name.name).upper()
                if hasattr(func.name, "name")
                else str(func.name).upper()
            )
            if func_name in GRAIN_LEVEL_AGGREGATION_FUNCTIONS:
                return True
    return False


def get_grain_level_window_info(expr_ast: ast.Node) -> list[tuple[str, set[str]]]:
    """
    Get information about LAG/LEAD window functions that need grain-level aggregation.

    Returns a list of (function_name, order_by_columns) tuples for each LAG/LEAD
    window function in the expression.

    Args:
        expr_ast: The AST expression to analyze

    Returns:
        List of (function_name, order_by_columns) tuples
    """
    results: list[tuple[str, set[str]]] = []
    for func in expr_ast.find_all(ast.Function):
        if func.over and func.name:
            func_name = (
                str(func.name.name).upper()
                if hasattr(func.name, "name")
                else str(func.name).upper()
            )
            if func_name in GRAIN_LEVEL_AGGREGATION_FUNCTIONS:  # pragma: no branch
                order_by_cols: set[str] = set()
                if func.over.order_by:  # pragma: no branch
                    for sort_item in func.over.order_by:
                        col_expr = sort_item.expr
                        # Handle Subscript expressions (role suffix like [order])
                        if isinstance(col_expr, ast.Subscript):
                            col_expr = col_expr.expr
                        if (
                            isinstance(col_expr, ast.Column) and col_expr.name
                        ):  # pragma: no branch
                            col_name = get_column_full_name(col_expr)
                            if col_name:  # pragma: no branch
                                order_by_cols.add(col_name)
                results.append((func_name, order_by_cols))
    return results


def detect_window_metrics_requiring_grain_groups(
    ctx: "BuildContext",
    decomposed_metrics: dict,
    base_grain_group_metrics: set[str],
) -> dict[str, set[str]]:
    """
    Detect window metrics that require grain-level grain groups.

    Analyzes all requested metrics and identifies those with LAG/LEAD window functions
    that operate at a different grain than the user-requested grain. Returns a mapping
    of metric names to their required ORDER BY columns (grains).

    Args:
        ctx: Build context with metrics and nodes
        decomposed_metrics: Decomposed metric info (metric_name -> DecomposedMetricInfo)
        base_grain_group_metrics: Set of base metrics already in grain groups

    Returns:
        Dict mapping metric_name -> set of ORDER BY column refs (e.g., {"v3.date.week"})
    """
    from datajunction_server.construction.build_v3.types import DecomposedMetricInfo

    window_metric_grains: dict[str, set[str]] = {}

    for metric_name in ctx.metrics:
        # Skip base metrics - they're already in grain groups
        if metric_name in base_grain_group_metrics:
            continue

        decomposed = decomposed_metrics.get(metric_name)
        if not decomposed:
            continue  # pragma: no cover

        # Check if this metric uses LAG/LEAD that needs grain-level aggregation
        if (  # pragma: no branch
            isinstance(decomposed, DecomposedMetricInfo) and decomposed.combiner_ast
        ):
            if needs_grain_level_aggregation(decomposed.combiner_ast):
                # Get the ORDER BY columns for this metric
                grain_info = get_grain_level_window_info(decomposed.combiner_ast)
                order_by_cols: set[str] = set()
                for _, cols in grain_info:
                    order_by_cols.update(cols)
                if order_by_cols:  # pragma: no branch
                    window_metric_grains[metric_name] = order_by_cols

    return window_metric_grains


def inject_partition_by_into_windows(
    expr_ast: ast.Node,
    all_dimension_aliases: list[str],
    alias_to_dimension_node: dict[str, str] | None = None,
    partition_cte_alias: str | None = None,
) -> None:
    """
    Inject PARTITION BY clauses into navigation/ranking window functions.

    For period-over-period metrics with window functions like LAG/LEAD, the PARTITION BY
    should include all requested dimensions EXCEPT:
    1. Those in the ORDER BY clause
    2. Other columns from the same dimension node as the ORDER BY column

    The second rule is critical for period-over-period metrics. For example, if ordering
    by month (from v3.date), we should NOT partition by date_id
    (also from v3.date), because date_id is a finer grain that would
    break the month-over-month comparison.

    This ensures that comparisons (e.g., month-over-month) are done within each partition
    (e.g., per category, per product) rather than across the entire result set.

    IMPORTANT: This only applies to navigation/ranking functions (LAG, LEAD, RANK, etc.).
    Aggregate window functions (SUM, AVG, COUNT, MIN, MAX with OVER ()) are NOT modified,
    as they often intentionally compute grand totals (e.g., for weighted average calculations).

    For example, given:
        LAG(revenue, 1) OVER (ORDER BY month)
    And requested dimensions: [category, date_id, month, quarter]
    Where date_id, month, quarter are all from "v3.date"

    This function transforms it to:
        LAG(revenue, 1) OVER (PARTITION BY category ORDER BY month)

    Note: date_id and quarter are excluded because they're from the same dimension
    node as month.

    But this is left unchanged:
        SUM(impressions) OVER ()  -- grand total, no partition injection

    Args:
        expr_ast: The AST expression to modify (mutated in place)
        all_dimension_aliases: List of all requested dimension column aliases
            (already resolved, e.g., ["category", "country_iso_code", "week_code"])
        alias_to_dimension_node: Optional mapping from alias to dimension node name.
            If provided, all aliases from the same dimension node as ORDER BY columns
            will be excluded from PARTITION BY.
        partition_cte_alias: Optional CTE alias to qualify PARTITION BY columns.
            If provided, columns will be qualified as cte_alias.column.
            Important for JOINs where column names may be ambiguous.
    """
    # Build reverse mapping: dimension_node -> set of aliases
    node_to_aliases: dict[str, set[str]] = {}
    if alias_to_dimension_node:
        node_to_aliases = build_dimension_node_to_aliases(alias_to_dimension_node)

    # Find all Function nodes with an OVER clause (window functions)
    for func in expr_ast.find_all(ast.Function):
        if not func.over:
            continue

        func_name = func.name.name.upper() if func.name else ""

        # Determine if we should inject PARTITION BY:
        # 1. Navigation/ranking functions (LAG, LEAD, etc.) - always inject
        # 2. Aggregate functions with ORDER BY (trailing/rolling) - inject
        # 3. Aggregate functions with empty OVER () (grand totals) - skip
        should_inject = False
        if func_name in PARTITION_BY_INJECTION_FUNCTIONS:
            # Navigation/ranking functions always need partitioning
            should_inject = True
        elif func.over.order_by:
            # Aggregate with ORDER BY = trailing/rolling metric, needs partitioning
            should_inject = True
        # else: OVER () with no ORDER BY = grand total, skip partitioning

        if not should_inject:
            continue

        # Get dimensions used in ORDER BY (these should NOT be in PARTITION BY)
        order_by_dims: set[str] = set()
        for sort_item in func.over.order_by:
            # Extract the column name from the sort expression
            if (  # pragma: no branch
                isinstance(sort_item.expr, ast.Column) and sort_item.expr.name
            ):
                order_by_dims.add(sort_item.expr.name.name)

        # Build set of aliases to exclude from PARTITION BY
        # Start with ORDER BY dimensions, then add all aliases from the same dimension nodes
        excluded_aliases: set[str] = set(order_by_dims)
        if alias_to_dimension_node:
            for order_dim in order_by_dims:
                # Find the dimension node for this ORDER BY column
                dim_node = alias_to_dimension_node.get(order_dim)
                if dim_node:  # pragma: no branch
                    # Exclude all aliases from the same dimension node
                    excluded_aliases.update(node_to_aliases.get(dim_node, set()))

        # Add all other dimensions to PARTITION BY
        # Only add if PARTITION BY is currently empty (don't override explicit partitions)
        if not func.over.partition_by:
            for dim_alias in all_dimension_aliases:
                if dim_alias not in excluded_aliases:
                    # Optionally qualify with CTE alias to avoid ambiguity in JOINs
                    if partition_cte_alias:
                        func.over.partition_by.append(
                            ast.Column(
                                name=ast.Name(dim_alias),
                                _table=ast.Table(ast.Name(partition_cte_alias)),
                            ),
                        )
                    else:
                        func.over.partition_by.append(
                            ast.Column(name=ast.Name(dim_alias)),
                        )


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
            elif table_name in cte_names:  # pragma: no branch
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

    Columns referenced by name in GROUP BY are always protected from pruning,
    and positional references (eg GROUP BY 1, 2, 3) are handled safely:
    - Columns at referenced positions are protected from pruning.
    - After pruning, positional integers are renumbered to reflect the new
      SELECT order, so they continue to point at the correct columns.

    This avoids rewriting positional references to alias names, which is
    invalid in dialects like Trino where GROUP BY cannot reference SELECT
    aliases.

    Args:
        query_ast: The query AST to modify
        columns_to_select: Set of column names to keep

    Returns:
        Modified query AST with filtered projection
    """
    if not query_ast.select.projection:  # pragma: no cover
        return query_ast

    projection = query_ast.select.projection

    def _col_name(expr: object) -> str | None:
        if isinstance(expr, ast.Alias):
            return str(expr.alias.name) if expr.alias else None
        if isinstance(expr, ast.Column):
            return str(expr.alias.name) if expr.alias else str(expr.name.name)
        return None

    # Protect columns that GROUP BY depends on so we never prune them.
    # Positional references (integers) protect the column at that SELECT position;
    # named references protect by column name.
    effective_cols = set(columns_to_select)
    positional_refs: set[int] = set()  # 1-indexed positions from GROUP BY
    for item in query_ast.select.group_by:
        if isinstance(item, ast.Number) and isinstance(item.value, int):
            pos = int(item.value)
            if 1 <= pos <= len(projection):
                positional_refs.add(pos)
                if name := _col_name(projection[pos - 1]):
                    effective_cols.add(name)
        elif isinstance(item, ast.Column):
            name = str(item.alias.name) if item.alias else str(item.name.name)
            effective_cols.add(name)

    # Build the filtered projection, tracking old to new position for renumbering.
    new_projection = []
    old_to_new: dict[int, int] = {}  # 1-indexed old pos to 1-indexed new pos
    for i, expr in enumerate(projection):
        old_pos = i + 1
        col_name = _col_name(expr)
        if col_name is None:  # pragma: no cover
            # Keep expressions we can't analyze (defensive)
            new_projection.append(expr)
            old_to_new[old_pos] = len(new_projection)
        elif col_name in effective_cols:
            new_projection.append(expr)
            old_to_new[old_pos] = len(new_projection)
        # else: pruned — no mapping entry

    # If we filtered everything, keep original (shouldn't happen)
    if new_projection:
        query_ast.select.projection = new_projection

    # Renumber positional GROUP BY references if any columns were removed before them.
    if positional_refs:
        new_group_by: list[ast.Expression] = []
        for item in query_ast.select.group_by:
            if isinstance(item, ast.Number) and isinstance(item.value, int):
                old_pos = int(item.value)
                new_pos = old_to_new.get(old_pos, old_pos)
                new_group_by.append(
                    ast.Number(value=new_pos) if new_pos != old_pos else item,
                )
            else:
                new_group_by.append(item)
        query_ast.select.group_by = new_group_by

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


# ---------------------------------------------------------------------------
# Filter pushdown into CTEs
# ---------------------------------------------------------------------------


_OUTER_JOIN_KINDS_LEFT = {"LEFT", "LEFT OUTER"}
_OUTER_JOIN_KINDS_RIGHT = {"RIGHT", "RIGHT OUTER"}
_OUTER_JOIN_KINDS_FULL = {"FULL", "FULL OUTER", "OUTER"}


def _get_relation_side_id(expr: ast.Expression) -> Optional[str]:
    """Extract the alias or unqualified name used to qualify column refs to a side.

    Returns ``None`` when the expression has no extractable identifier (e.g.
    an unaliased subquery — defensive only; the v3 builder always emits
    aliased relations).
    """
    alias = getattr(expr, "alias", None)
    if alias is not None:
        return alias.name
    if isinstance(expr, ast.Table):
        return expr.name.name
    return None  # pragma: no cover


def _filter_namespaces(filter_ast: ast.Expression) -> set[str]:
    """Collect the namespace identifiers used to qualify columns in a filter."""
    return {
        col.name.namespace.name
        for col in filter_ast.find_all(ast.Column)
        if col.name.namespace is not None
    }


def _classify_outer_join_target(
    relation: ast.Relation,
    filter_namespaces: set[str],
) -> Optional[ast.Expression]:
    """Return the relation-side expression that should host a filter as a wrapped
    subquery, or ``None`` when WHERE injection is safe.

    A filter applied in WHERE after an OUTER JOIN silently turns it into an
    INNER JOIN whenever the filter touches a non-preserved side: the NULL
    fill-in rows from the JOIN evaluate the predicate to NULL → false and get
    dropped. This walks the relation's joins, tracks which side identifiers
    are non-preserved, and returns the target expression to wrap when all of
    the filter's namespace references resolve to a single non-preserved side.
    """
    if not relation.extensions or not filter_namespaces:
        return None

    # ``_get_relation_side_id`` returns None only for unaliased subqueries the
    # v3 builder never emits — branches guarding ``is not None`` are
    # defensive only, hence the no-branch pragmas on this loop.
    preserved: dict[str, ast.Expression] = {}
    non_preserved: dict[str, ast.Expression] = {}
    primary_id = _get_relation_side_id(relation.primary)
    if primary_id is not None:  # pragma: no branch
        preserved[primary_id] = relation.primary

    for join in relation.extensions:
        right_id = _get_relation_side_id(join.right)
        kind = (join.join_type or "").upper().strip()

        if kind in _OUTER_JOIN_KINDS_LEFT:
            if right_id is not None:  # pragma: no branch
                non_preserved[right_id] = join.right
        elif kind in _OUTER_JOIN_KINDS_RIGHT:
            non_preserved.update(preserved)
            preserved = {}
            if right_id is not None:  # pragma: no branch
                preserved[right_id] = join.right
        elif kind in _OUTER_JOIN_KINDS_FULL:
            non_preserved.update(preserved)
            if right_id is not None:  # pragma: no branch
                non_preserved[right_id] = join.right
        elif right_id is not None:  # pragma: no branch
            preserved[right_id] = join.right

    if not non_preserved or not filter_namespaces.issubset(non_preserved):
        return None
    targets = {non_preserved[ns] for ns in filter_namespaces}
    return next(iter(targets)) if len(targets) == 1 else None


def _wrap_relation_side_with_filter(
    target_expr: ast.Expression,
    filter_ast: ast.Expression,
) -> ast.Query:
    """Wrap a relation-side expression as ``(SELECT * FROM <expr> WHERE filter) <alias>``.

    The original alias is preserved on the outer wrapping query so column refs
    qualified by the original side identifier continue to resolve.
    """
    side_id = _get_relation_side_id(target_expr)
    inner_select = ast.Select(
        projection=[ast.Wildcard()],
        from_=ast.From(relations=[ast.Relation(primary=deepcopy(target_expr))]),
        where=deepcopy(filter_ast),
    )
    wrapped = ast.Query(select=inner_select)
    wrapped.parenthesized = True
    # Mark this Query as an outer-join-safety wrap so subsequent filters
    # targeting the same side AND into its WHERE instead of nesting another
    # layer.  Inside the wrap, the original alias is preserved on the inner
    # FROM, so re-qualifying isn't necessary.
    wrapped._outer_join_filter_wrap = True  # type: ignore[attr-defined]
    if side_id is not None:  # pragma: no branch
        wrapped.alias = ast.Name(side_id)
        wrapped.as_ = False
    return wrapped


def _try_push_filter_into_outer_join_side(
    select: ast.Select,
    filter_expr: ast.Expression,
) -> bool:
    """Route a filter into the inner-side of an outer join when WHERE would be unsafe.

    Returns True when the filter has been applied via inner-side wrapping
    (caller must NOT also AND it into WHERE). Returns False when no outer-join
    hazard applies, leaving the caller to fall back to standard WHERE injection.
    """
    if select.from_ is None:
        return False
    namespaces = _filter_namespaces(filter_expr)
    if not namespaces:
        return False

    for relation in select.from_.relations:
        target = _classify_outer_join_target(relation, namespaces)
        if target is None:
            continue
        # If the target is already an outer-join-safety wrap from a previous
        # call, AND the new atom into its inner WHERE rather than nesting
        # another wrap layer.  The inner FROM preserves the original alias,
        # so column refs qualified by that alias resolve correctly inside.
        if isinstance(target, ast.Query) and getattr(
            target,
            "_outer_join_filter_wrap",
            False,
        ):
            inner_select = cast(ast.Select, target.select)
            # The wrap was created with a non-None WHERE; the cast keeps mypy
            # happy without a runtime branch.
            existing_where = cast(ast.Expression, inner_select.where)
            inner_select.where = ast.BinaryOp.And(
                existing_where,
                deepcopy(filter_expr),
            )
            return True
        wrapped = _wrap_relation_side_with_filter(target, filter_expr)
        if relation.primary is target:
            relation.primary = wrapped
        else:
            for join in relation.extensions:  # pragma: no branch
                if join.right is target:  # pragma: no branch
                    join.right = wrapped
                    break
        return True
    return False


def _split_and_atoms(expr: ast.Expression) -> list[ast.Expression]:
    """Flatten an AND-tree into its leaf predicates."""
    if (
        isinstance(expr, ast.BinaryOp) and expr.op == ast.BinaryOpKind.And  # type: ignore[attr-defined]
    ):
        return _split_and_atoms(expr.left) + _split_and_atoms(expr.right)
    return [expr]


def inject_filter_into_select(
    select: ast.Select,
    filter_expr: ast.Expression,
) -> None:
    """AND a filter expression into a SELECT's WHERE clause, safely.

    When the SELECT's FROM contains an OUTER JOIN whose non-preserved side
    owns every column referenced by ``filter_expr``, the filter is instead
    wrapped around that inner-side relation as a subquery — preventing the
    WHERE from silently turning the OUTER JOIN into an INNER JOIN.

    AND-trees are split so each atom is classified independently: a single
    bundled WHERE may have some atoms safe for WHERE and others that need
    inner-side wrapping.
    """
    for atom in _split_and_atoms(filter_expr):
        if _try_push_filter_into_outer_join_side(select, atom):
            continue
        if select.where:
            select.where = ast.BinaryOp.And(select.where, atom)
        else:
            select.where = atom


def _inject_filter_into_where(
    query_ast: ast.Query,
    filter_expr: ast.Expression,
) -> None:
    """AND a filter into a Query's WHERE — wrapper around :func:`inject_filter_into_select`."""
    inject_filter_into_select(cast(ast.Select, query_ast.select), filter_expr)


def _resolve_pushdown_filters_for_cte(
    node: "Node",
    cte_query: ast.Query,
    pushdown_filters: list[str],
    filter_column_aliases: dict[str, str],
) -> list[ast.Expression]:
    """Determine which user filters can be pushed into this CTE and return them.

    For each filter, extracts the dimension references, resolves them to bare
    column names via filter_column_aliases, and checks whether this CTE outputs
    those columns.  If all referenced columns are present, the filter is rewritten
    using the CTE's internal table-qualified column names and returned.

    Returns a list of parsed filter AST expressions ready for injection.
    """
    node_output_cols = (
        {col.name for col in (node.current.columns or [])} if node.current else set()
    )

    if not node_output_cols:  # pragma: no cover
        return []

    results: list[ast.Expression] = []
    for filter_str in pushdown_filters:
        rewritten = _rewrite_filter_for_cte(
            filter_str,
            filter_column_aliases,
            node_output_cols,
            cte_query,
        )
        if rewritten is None:
            continue
        results.append(rewritten)
    return results


def _cte_has_set_operation(cte_query: ast.Query) -> bool:
    """Detect whether the CTE body is a UNION / INTERSECT / EXCEPT.

    Projection inspection only sees the first arm, so a set operation with
    asymmetric arms can't be safely pushed into.  Callers should skip
    pushdown when this returns True.
    """
    return bool(cte_query.select and cte_query.select.set_op)


def _build_name(parts: list[str]) -> ast.Name:
    """Build a nested ``ast.Name`` matching a dotted reference.

    ``["o", "order_date"]`` becomes ``Name("order_date", namespace=Name("o"))``
    which serializes as ``o.order_date``.
    """
    if len(parts) == 1:
        return ast.Name(parts[0])
    return ast.Name(parts[-1], namespace=_build_name(parts[:-1]))


def _column_from_qualified(qualified: str) -> ast.Column:
    """Build an ``ast.Column`` node from a dotted reference like ``o.order_date``."""
    return ast.Column(name=_build_name(qualified.split(SEPARATOR)))


def _resolve_pushdown_form(
    output_col: str,
    cte_output_cols: set[str],
    projection_map: dict[str, str | None],
) -> str | None:
    """Determine the WHERE-safe form for a filter column in a specific CTE.

    Returns the WHERE-safe reference as a string (qualified or bare), or
    ``None`` when the filter cannot be safely pushed into this CTE — either
    because the column isn't exposed at all, or because the projection is a
    non-column expression that can't be inlined into WHERE.
    """
    if output_col not in cte_output_cols:
        return None
    if output_col in projection_map:
        return projection_map[output_col]  # may be None: unsafe projection
    return output_col  # pruned from CTE SELECT; falls through to bare name


def _rewrite_filter_for_cte(
    filter_str: str,
    filter_column_aliases: dict[str, str],
    cte_output_cols: set[str],
    cte_query: ast.Query,
) -> ast.Expression | None:
    """Rewrite a dimension filter for injection into a specific CTE.

    Resolves each dimension reference (e.g., ``v3.product.category``) to the
    form that's safe in the CTE's WHERE clause.  Three projection cases:

    1. CTE projects the column as a simple (possibly aliased) column: replace
       with the underlying qualified form (e.g., ``p.category``).  This is the
       correctness-critical case — emitting a SELECT-list alias in WHERE is
       rejected by Spark SQL and standard SQL.
    2. CTE doesn't project the column at all (pruned): fall through to the
       bare column name.  Safe because the CTE's underlying source exposes
       the column, even if it's not selected into the outer query.
    3. CTE projects the column via a non-column expression (e.g.
       ``SUM(x) AS y``): skip — inlining is unsafe.

    Multi-predicate handling: a single filter may reference several dim refs
    (``a.x = 1 OR b.y = 2``).  All matching refs are rewritten, but if ANY
    ref's column isn't exposed by this CTE, the whole filter is skipped —
    pushing a partial OR-predicate into the wrong CTE produces invalid SQL.

    Returns the rewritten filter AST, or None when the filter can't be
    safely pushed into this CTE.
    """
    # Set-operation CTEs can't be safely pushed into via the first arm alone.
    if _cte_has_set_operation(cte_query):
        return None

    projection_map = _build_cte_projection_map(cte_query)
    filter_ast = parse_filter(filter_str)

    # First pass: plan the rewrites by walking the AST.  Role-qualified refs
    # appear as Subscript(Column(base), Column/Lambda(role)) and are handled
    # whole; plain Column refs are handled individually.  Collect rewrites
    # into buffers so we can bail out atomically if any ref can't be pushed.
    subscript_rewrites: list[tuple[ast.Subscript, ast.Column]] = []
    column_rewrites: list[tuple[ast.Column, ast.Column]] = []
    # Columns that are children of a rewritten Subscript — exclude from the
    # plain-Column pass so we don't double-process them.
    handled_col_ids: set[int] = set()

    # Users can write filters like v3.date.date_id[order] >= 20240101 where
    # [order] is a role, a disambiguator when the same dimension is linked to
    # the fact multiple times. A filter like that would be stored in the AST as:
    #   BinaryOp(
    #     >=,
    #     Subscript(
    #       expr=Column("v3.date.date_id"),
    #       index=Column("order"),
    #     ),
    #     Literal(20240101),
    #  )
    for subscript in filter_ast.find_all(ast.Subscript):
        # Skip ones whose target isn't a Column as these are real SQL array subscripts, not role refs
        if not isinstance(subscript.expr, ast.Column):
            continue  # pragma: no cover
        # Reconstruct the original role-qualified form: base = "v3.date.date_id", role = "order"
        base = get_column_full_name(subscript.expr)
        role = extract_subscript_role(subscript)
        if not role:
            continue  # pragma: no cover
        full_name = f"{base}[{role}]"

        # Look up in the filter alias map. Prefers the role-specific key over the fallback.
        output_col = filter_column_aliases.get(
            full_name,
        ) or filter_column_aliases.get(base)
        if output_col is None:
            continue  # pragma: no cover
        form = _resolve_pushdown_form(output_col, cte_output_cols, projection_map)
        if form is None:
            return None
        replacement = _column_from_qualified(form)
        subscript_rewrites.append((subscript, replacement))

        # Safety checks and queue for subscript to column swap
        handled_col_ids.add(id(subscript.expr))
        if isinstance(subscript.index, ast.Column):
            handled_col_ids.add(id(subscript.index))

    # Handle dim refs that don't have a role qualifier
    for col in filter_ast.find_all(ast.Column):
        # Skip columns accounted for in the subscript pass
        if id(col) in handled_col_ids:
            continue
        full_name = get_column_full_name(col)
        if not full_name or full_name not in filter_column_aliases:
            continue
        output_col = filter_column_aliases[full_name]
        form = _resolve_pushdown_form(output_col, cte_output_cols, projection_map)
        if form is None:
            return None
        column_rewrites.append((col, _column_from_qualified(form)))

    if not subscript_rewrites and not column_rewrites:
        return None

    # Second pass: apply.  Safe to mutate now that every ref has been validated.
    for subscript, replacement in subscript_rewrites:
        subscript.swap(replacement)
    for col, replacement in column_rewrites:
        col.swap(replacement)

    return filter_ast


def _build_cte_projection_map(cte_query: ast.Query) -> dict[str, str | None]:
    """Map a CTE's output column name to its underlying qualified reference.

    Output name is the SELECT-list alias when present, else the bare column
    name.  Value is either:

    - A string — the form that's safe to reference in a WHERE clause pushed
      into this CTE (a table-qualified column when qualified in the
      projection, else the bare column name).
    - ``None`` — the projection is a non-column expression under an alias
      (e.g., ``SUM(x) AS y``); pushdown should skip this CTE to avoid
      inlining an expression that would be semantically wrong in WHERE.

    Columns that the CTE doesn't project at all are absent from the map;
    callers should treat that as "fall through to the bare name" since the
    CTE's underlying source still exposes them.

    Examples::

        SELECT o.placed_on AS order_date
          becomes {"order_date": "o.placed_on"}
        SELECT T.test_id
          becomes {"test_id": "T.test_id"}
        SELECT CAST(x AS INT) AS x
          becomes {"x": "x"}                   -- CAST is a transparent passthrough
        SELECT SUM(x) AS total
          becomes {"total": None}
    """
    result: dict[str, str | None] = {}
    if not cte_query.select:  # pragma: no cover
        return result
    for expr in cte_query.select.projection:
        inner = getattr(expr, "child", expr)
        alias = getattr(expr, "alias", None)
        # Unwrap CAST around a column — CAST(col AS T) is a transparent
        # passthrough for filter pushdown.  Equality, IN, and range
        # comparisons against a literal of the cast target type are
        # value-equivalent to the same comparison against the unwrapped
        # column (modulo lossy casts, which are rare and generally
        # intentional in the projection).  Recognising this lets filters
        # land in CTEs whose projection wraps a column in CAST for type
        # normalization (e.g. ``CAST(x AS INT) AS x`` from a LATERAL
        # VIEW EXPLODE'd column).
        if isinstance(inner, ast.Cast) and isinstance(inner.expression, ast.Column):
            inner = inner.expression
        if isinstance(inner, ast.Column) and inner.name:
            bare = inner.name.name
            underlying = (
                f"{inner.namespace[0].name}.{bare}" if inner.namespace else bare
            )
            output_name = alias.name if alias else bare
            result[output_name] = underlying
        elif alias is not None:
            # Non-column projection with an output name we can address;
            # mark as unsafe to inline.
            result[alias.name] = None
    return result


def collect_node_ctes(
    ctx: BuildContext,
    nodes_to_include: list[Node],
    needed_columns_by_node: Optional[dict[str, set[str]]] = None,
    injected_filters: Optional[dict[str, ast.Expression]] = None,
    pushdown: Optional[PushdownFilters] = None,
) -> tuple[list[tuple[str, ast.Query]], list[str]]:
    """
    Collect CTEs for all non-source nodes, recursively expanding table references.

    When ``pushdown`` is provided, each CTE independently checks whether any
    user-supplied dimension filter can be pushed into its WHERE clause.

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
        injected_filters: Optional dict of node_name -> filter expression to inject
            as a WHERE clause into that node's CTE. Used to push temporal partition
            filters down into upstream CTEs (e.g. a date-spine) rather than applying
            them on the outer query after an expensive join.

    Returns:
        Tuple of (cte_list, scanned_sources):
        - cte_list: List of (cte_name, query_ast) tuples in dependency order
        - scanned_sources: List of source node names encountered during traversal
    """
    # Collect all node names that need CTEs (including transitive dependencies)
    all_node_names: set[str] = set()
    # Track source nodes encountered during traversal
    scanned_source_names: set[str] = set()
    mat_check_time = 0.0
    parse_check_time = 0.0
    ref_extract_time = 0.0
    call_count = 0

    def collect_refs(node: Node, visited: set[str]) -> None:
        nonlocal mat_check_time, parse_check_time, ref_extract_time, call_count
        call_count += 1

        if node.name in visited:  # pragma: no branch
            return  # pragma: no cover
        visited.add(node.name)

        if node.type == NodeType.SOURCE:
            # Track this source node
            scanned_source_names.add(node.name)
            return  # Sources don't become CTEs

        # Skip materialized nodes - they use physical tables, not CTEs
        is_mat = should_use_materialized_table(ctx, node)
        if is_mat:  # pragma: no cover
            return

        all_node_names.add(node.name)

        if node.current and node.current.query:  # pragma: no branch
            try:
                # Use cached parsed query for reference extraction
                query_ast = ctx.get_parsed_query(node)

                refs = get_table_references_from_ast(query_ast)

                for ref in refs:
                    ref_node = ctx.nodes.get(ref)
                    if ref_node:  # pragma: no branch
                        collect_refs(ref_node, visited)
            except Exception:  # pragma: no cover
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
        if node.type == NodeType.SOURCE:  # pragma: no cover
            continue

        # Skip materialized nodes (they use physical tables directly)
        if should_use_materialized_table(ctx, node):  # pragma: no cover
            continue

        if not node.current or not node.current.query:  # pragma: no cover
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
        if needed_columns_by_node:  # pragma: no branch
            needed_cols = needed_columns_by_node.get(node.name)

        if needed_cols:  # pragma: no branch
            query_ast = filter_cte_projection(query_ast, needed_cols)

        # Inject filters into this CTE's WHERE clause from two sources:
        # (1) Temporal/explicit filters targeted at this node by name
        # (2) User dimension filters that reference columns this CTE outputs
        if injected_filters and node.name in injected_filters:
            _inject_filter_into_where(query_ast, injected_filters[node.name])

        if pushdown:
            for filter_ast in _resolve_pushdown_filters_for_cte(
                node,
                query_ast,
                pushdown.filters,
                pushdown.column_aliases,
            ):
                _inject_filter_into_where(query_ast, filter_ast)

        ctes.append((cte_name, query_ast))

    return ctes, list(scanned_source_names)


def process_metric_combiner_expression(
    combiner_ast: ast.Expression,
    dimension_refs: dict[str, tuple[str, str]],
    component_refs: dict[str, tuple[str, str]] | None = None,
    metric_refs: dict[str, tuple[str, str]] | None = None,
    partition_dimensions: list[str] | None = None,
    alias_to_dimension_node: dict[str, str] | None = None,
) -> ast.Expression:
    """
    Process a metric combiner expression for final output.

    Transforms a raw combiner AST into the final SQL expression by replacing
    component, metric, and dimension references with qualified column refs.

    Used by:
    - build_derived_metric_expr in generate_metrics_sql
    - cube materialization for storing metric_expression in config

    Transformations applied (in order):
    1. Replace metric references (e.g., "v3.total_revenue" -> column ref)
    2. Replace component references (e.g., "revenue_sum_abc123" -> column ref)
    3. Replace dimension references (e.g., "v3.date.dateint" -> column ref)
    4. Inject PARTITION BY clauses for window functions

    Args:
        combiner_ast: The metric combiner expression AST
        dimension_refs: Mapping from dimension refs to (cte_alias, column_name)
            e.g., {"v3.date.dateint": ("base_metrics", "dateint")}
            For cube queries, use empty string for cte_alias: ("", "dateint")
        component_refs: Optional mapping from component names to (cte_alias, column_name)
            e.g., {"revenue_sum_abc123": ("gg0", "revenue_sum_abc123")}
        metric_refs: Optional mapping from metric names to (cte_alias, column_name)
            e.g., {"v3.total_revenue": ("base_metrics", "total_revenue")}
            For derived metrics that reference other metrics
        partition_dimensions: Optional list of dimension aliases for PARTITION BY.
            If provided, window functions will have PARTITION BY injected.
        alias_to_dimension_node: Optional mapping from alias to dimension node name.
            Used to exclude related dimensions from PARTITION BY (e.g., if ordering
            by week_code, also exclude dateint from the same time dimension node).

    Returns:
        A deep copy of the expression with all transformations applied.
    """
    # Deep copy to avoid mutating the original
    expr_ast = deepcopy(combiner_ast)

    # Replace metric references (for derived metrics referencing other metrics)
    if metric_refs:
        replace_metric_refs_in_ast(expr_ast, metric_refs)

    # Replace component references
    if component_refs:
        replace_component_refs_in_ast(expr_ast, component_refs)

    # Replace dimension references
    replace_dimension_refs_in_ast(expr_ast, dimension_refs)

    # Inject PARTITION BY for window functions if dimensions provided
    if partition_dimensions:
        # Get CTE alias from dimension refs (all should have same alias)
        # Use None if empty string (for cube queries)
        cte_alias = None
        if dimension_refs:  # pragma: no branch
            first_alias = next(iter(dimension_refs.values()))[0]
            cte_alias = first_alias if first_alias else None

        inject_partition_by_into_windows(
            expr_ast,
            partition_dimensions,
            alias_to_dimension_node,
            partition_cte_alias=cte_alias,
        )

    return expr_ast
