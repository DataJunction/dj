from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from datajunction_server.database.node import Node
from datajunction_server.sql.parsing import ast
from datajunction_server.utils import SEPARATOR

if TYPE_CHECKING:
    from datajunction_server.construction.build_v3.types import (
        BuildContext,
        DecomposedMetricInfo,
    )

logger = logging.getLogger(__name__)


def get_short_name(full_name: str) -> str:
    """
    Get the last segment of a dot-separated name.

    Examples:
        get_short_name("v3.order_details") -> "order_details"
        get_short_name("v3.product.category") -> "category"
        get_short_name("simple_name") -> "simple_name"
    """
    return full_name.split(SEPARATOR)[-1]


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
    if not parts:  # pragma: no cover
        return ast.Name("")

    # Build from left to right, each becoming the namespace of the next
    result = ast.Name(parts[0])
    for part in parts[1:]:
        result = ast.Name(part, namespace=result)

    return result


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
    return ast.Column(name=ast.Name(col_name))  # pragma: no cover


def get_cte_name(node_name: str) -> str:
    """
    Generate a CTE-safe name from a node name.

    Replaces dots with underscores to create valid SQL identifiers.
    Uses the same logic as amenable_name for consistency.
    """
    return node_name.replace(SEPARATOR, "_").replace("-", "_")


def get_column_type(node: Node, column_name: str) -> str:
    """
    Look up the column type from a node's columns.

    Returns the string representation of the column type, or "string" as fallback.
    """
    if node.current and node.current.columns:  # pragma: no branch
        for col in node.current.columns:
            if col.name == column_name:
                return str(col.type) if col.type else "string"  # pragma: no cover
    return "string"  # pragma: no cover


def extract_columns_from_expression(expr: ast.Expression) -> set[str]:
    """
    Extract all column names referenced in an expression.
    """
    columns: set[str] = set()
    for col in expr.find_all(ast.Column):
        # Get the column name (last part of the identifier)
        if col.name:  # pragma: no branch
            columns.add(col.name.name)
    return columns


def collect_required_dimensions(
    nodes: dict[str, Node],
    metrics: list[str],
) -> list[str]:
    """
    Collect required dimensions from all requested metrics.

    Required dimensions are dimensions that MUST be included in the grain
    for metrics with window functions (LAG, LEAD, etc.) to work correctly.

    For example, a metric like:
        (revenue - LAG(revenue, 1) OVER (ORDER BY dateint)) / ...

    Requires `dateint` in the grain, otherwise LAG() would see only one row
    and always return NULL.

    Required dimensions are stored as Column objects on a dimension node.
    This function reconstructs the full path: "node_name.column_name"

    Args:
        nodes: Dict of loaded nodes (node_name -> Node)
        metrics: List of requested metric names

    Returns:
        List of required dimension references (full paths like "node.column")
    """
    required_dims: set[str] = set()

    for metric_name in metrics:
        metric_node = nodes.get(metric_name)
        if not metric_node or not metric_node.current:
            continue

        # Check required_dimensions on the metric node
        # These are Column objects stored on dimension nodes
        # We need to reconstruct the full path: "dimension_node.column_name"
        if metric_node.current.required_dimensions:
            for col in metric_node.current.required_dimensions:
                # Get the dimension node name from the column's node_revision
                if col.node_revision and col.node_revision.node:
                    dim_node_name = col.node_revision.node.name
                    full_path = f"{dim_node_name}{SEPARATOR}{col.name}"
                    required_dims.add(full_path)
                else:
                    # Fallback: just use the column name (shouldn't happen)
                    required_dims.add(col.name)  # pragma: no cover

    # Sort for deterministic ordering
    return sorted(required_dims)


def add_dimensions_from_metric_expressions(
    ctx: "BuildContext",
    decomposed_metrics: dict[str, "DecomposedMetricInfo"],
) -> None:
    """
    Scan combiner ASTs for dimension references and add them to ctx.dimensions.

    This handles dimensions used in metric expressions (e.g., LAG ORDER BY) that
    weren't explicitly requested by the user or marked as required_dimensions.
    We add them so they're included in the grain group SQL.

    Args:
        ctx: BuildContext with dimensions list to update
        decomposed_metrics: Dict of metric_name -> DecomposedMetricInfo with combiner ASTs
    """
    # Import here to avoid circular imports
    from datajunction_server.construction.build_v3.cte import get_column_full_name
    from datajunction_server.construction.build_v3.dimensions import parse_dimension_ref

    existing_dims = set(ctx.dimensions)
    for decomposed in decomposed_metrics.values():
        combiner_ast = decomposed.combiner_ast
        for col in combiner_ast.find_all(ast.Column):
            full_name = get_column_full_name(col)
            if full_name and SEPARATOR in full_name and full_name not in existing_dims:
                # Skip if this is a metric reference (e.g., in derived metric combiners)
                # Metrics should not be added as dimensions
                if full_name in ctx.metrics:
                    continue

                # Check if any existing dimension already covers this (node, column)
                dim_ref = parse_dimension_ref(full_name)
                is_covered = False
                for existing_dim in ctx.dimensions:
                    existing_ref = parse_dimension_ref(existing_dim)
                    if (
                        existing_ref.node_name == dim_ref.node_name
                        and existing_ref.column_name == dim_ref.column_name
                    ):
                        is_covered = True
                        break
                if not is_covered:
                    logger.info(
                        f"[BuildV3] Auto-adding dimension {full_name} from metric expression",
                    )
                    ctx.dimensions.append(full_name)
                    existing_dims.add(full_name)


def add_dimensions_from_filters(ctx: "BuildContext") -> None:
    """
    Scan filter expressions for dimension references and add them to ctx.dimensions.

    Dimensions referenced in filters but not in the GROUP BY dimensions are needed
    for filter resolution but should not appear in the output projection. These are
    tracked in ctx.filter_dimensions.

    Args:
        ctx: BuildContext with filters and dimensions lists to update
    """
    # Import here to avoid circular imports
    from datajunction_server.construction.build_v3.cte import get_column_full_name
    from datajunction_server.construction.build_v3.dimensions import parse_dimension_ref
    from datajunction_server.construction.build_v3.filters import parse_filter

    if not ctx.filters:
        return

    existing_dims = set(ctx.dimensions)

    for filter_str in ctx.filters:
        try:
            filter_ast = parse_filter(filter_str)
        except Exception:  # pragma: no cover
            logger.warning(f"[BuildV3] Failed to parse filter: {filter_str}")
            continue

        # Find all column references in the filter
        for col in filter_ast.find_all(ast.Column):
            full_name = get_column_full_name(col)
            if not full_name or SEPARATOR not in full_name:
                # Simple column name (e.g., "status") - will be resolved from parent node
                continue

            if full_name in existing_dims:
                # Already in dimensions, no need to add
                continue

            # Skip if this is a metric reference, not a dimension
            # Metrics in WHERE clauses should be treated as HAVING conditions,
            # not dimension joins
            if full_name in ctx.metrics:
                continue

            # Check if any existing dimension already covers this (node, column)
            dim_ref = parse_dimension_ref(full_name)
            is_covered = False
            for existing_dim in ctx.dimensions:
                existing_ref = parse_dimension_ref(existing_dim)
                if (
                    existing_ref.node_name == dim_ref.node_name
                    and existing_ref.column_name == dim_ref.column_name
                ):
                    is_covered = True
                    break

            if not is_covered:
                logger.info(
                    f"[BuildV3] Auto-adding filter-only dimension {full_name}",
                )
                ctx.dimensions.append(full_name)
                ctx.filter_dimensions.add(full_name)
                existing_dims.add(full_name)


def build_join_from_clause(
    cte_names: list[str],
    table_refs: dict[str, ast.Table],
    shared_grain: list[str],
) -> ast.From:
    """
    Build FROM clause with FULL OUTER JOINs on CTEs.

    Example output (CTEs are defined in the WITH clause):
        FROM gg1
        FULL OUTER JOIN gg2 ON gg1.dim1 = gg2.dim1 AND gg1.dim2 = gg2.dim2
        FULL OUTER JOIN gg3 ON gg1.dim1 = gg3.dim1 AND gg1.dim2 = gg3.dim2
    """
    first_name = cte_names[0]

    # Build JOIN extensions for remaining CTEs
    join_extensions = []
    for name in cte_names[1:]:
        # Build JOIN criteria on shared grain columns
        join_criteria = _build_join_criteria(
            table_refs[first_name],
            table_refs[name],
            shared_grain,
        )

        join_extension = ast.Join(
            join_type="FULL OUTER",
            right=ast.Table(name=ast.Name(name)),
            criteria=ast.JoinCriteria(on=join_criteria),
        )

        join_extensions.append(join_extension)

    # Build the FROM clause - primary is first CTE, extensions are JOINs
    from_relation = ast.Relation(
        primary=ast.Table(name=ast.Name(first_name)),
        extensions=join_extensions,
    )

    return ast.From(relations=[from_relation])


def _build_join_criteria(
    left_table: ast.Table,
    right_table: ast.Table,
    grain_columns: list[str],
) -> ast.Expression:
    """
    Build JOIN ON condition for grain columns.

    Example output:
        left.dim1 = right.dim1 AND left.dim2 = right.dim2
    """
    if not grain_columns:
        # No grain columns - use TRUE (cartesian join)
        return ast.Boolean(True)  # type: ignore

    conditions = [
        ast.BinaryOp.Eq(
            ast.Column(name=ast.Name(col), _table=left_table),
            ast.Column(name=ast.Name(col), _table=right_table),
        )
        for col in grain_columns
    ]

    if len(conditions) == 1:
        return conditions[0]

    return ast.BinaryOp.And(*conditions)
