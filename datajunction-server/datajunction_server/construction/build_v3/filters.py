"""
Utilities to parse and resolve filter expressions
"""

from __future__ import annotations

from copy import deepcopy
from functools import reduce

from datajunction_server.errors import DJInvalidInputException
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import SEPARATOR


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

    Example::

        expr = parse_filter("v3.product.category = 'Electronics'")
        # Returns ast.BinaryOp with comparison
    """
    # Parse as "SELECT 1 WHERE <filter>" and extract the WHERE clause
    query = parse(f"SELECT 1 WHERE {filter_str}")
    if query.select.where is None:  # pragma: no cover
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

    Example::

        filter_ast = parse_filter("v3.product.category = 'Electronics'")
        aliases = {"v3.product.category": "category"}
        resolve_filter_references(filter_ast, aliases, "t2")
        # Now filter_ast contains "t2.category = 'Electronics'"
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
        if hasattr(node, "children"):  # pragma: no branch
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
        if name_node is None:  # pragma: no branch
            return  # pragma: no cover
        if name_node.namespace:
            collect_names(name_node.namespace)
        parts.append(name_node.name)

    collect_names(col.name)
    return SEPARATOR.join(parts)


def combine_filters(filters: list[ast.Expression]) -> ast.Expression | None:
    """
    Combine multiple filter expressions with AND.

    Args:
        filters: List of filter AST expressions

    Returns:
        Combined expression or None if empty list

    Example::

        f1 = parse_filter("status = 'active'")
        f2 = parse_filter("year >= 2024")
        combined = combine_filters([f1, f2])
        # Returns (status = 'active') AND (year >= 2024)
    """
    if not filters:  # pragma: no cover
        return None

    if len(filters) == 1:
        return filters[0]

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

    Example::

        filters = ["v3.product.category = 'Electronics'", "status = 'active'"]
        aliases = {"v3.product.category": "category", "status": "status"}
        where_clause = parse_and_resolve_filters(filters, aliases, "t1")
    """
    if not filter_strs:  # pragma: no cover
        return None

    parsed_filters = []
    for f in filter_strs:
        filter_ast = parse_filter(f)
        resolved = resolve_filter_references(
            deepcopy(filter_ast),  # Make a copy to avoid mutating cache
            column_aliases,
            cte_alias,
        )
        parsed_filters.append(resolved)

    return combine_filters(parsed_filters)
