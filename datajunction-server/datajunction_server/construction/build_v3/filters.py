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


def extract_subscript_role(subscript: ast.Subscript) -> str | None:
    """
    Extract the role string from a subscript index node.

    Handles the three forms that can appear as a subscript index:
    - ``ast.Column``  → simple role like ``order``
    - ``ast.Name``    → simple role like ``order``
    - ``ast.Lambda``  → multi-hop role like ``customer->home``

    Returns the role string, or None if the index is not a recognised form.
    """
    if isinstance(subscript.index, ast.Column):
        return subscript.index.name.name if subscript.index.name else None
    if isinstance(subscript.index, ast.Name):  # pragma: no cover
        return subscript.index.name
    if isinstance(subscript.index, ast.Lambda):
        return str(subscript.index)
    return None  # pragma: no cover


def resolve_filter_references(
    filter_ast: ast.Expression,
    column_aliases: dict[str, str],
    cte_alias: str | None = None,
) -> ast.Expression:
    """
    Resolve dimension/column references in a filter AST to their actual column aliases.

    This replaces references like "v3.product.category" with the appropriate
    table-qualified column reference like "t2.category".

    Also handles role-suffixed dimensions expressed as subscript syntax, e.g.:
    "v3.date.month[order] >= 2024" where [order] is the role indicator.
    The SQL parser interprets [order] as an array subscript (ast.Subscript).

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
    # First pass: handle Subscript nodes that represent role-suffixed dimension refs.
    # The SQL parser interprets "dim.col[role]" as Subscript(Column("dim.col"), Column("role")).
    # We need to reconstruct the full dim ref with role and replace the entire Subscript.
    for subscript in list(filter_ast.find_all(ast.Subscript)):
        if not isinstance(subscript.expr, ast.Column):
            continue  # pragma: no cover

        base_col_ref = _extract_full_column_ref(subscript.expr)
        if not base_col_ref:
            continue  # pragma: no cover

        role = extract_subscript_role(subscript)
        if not role:
            continue  # pragma: no cover

        # Look up with role first, then fall back to base ref without role
        dim_ref_with_role = f"{base_col_ref}[{role}]"
        alias_to_use = column_aliases.get(dim_ref_with_role) or column_aliases.get(
            base_col_ref,
        )

        if alias_to_use:
            if cte_alias:
                # Outer query: reference the grain group CTE column by registered alias
                # (e.g., "order_details_0.year_order")
                col_name_for_replacement = alias_to_use
            else:
                # Inner query: use the resolved alias directly. For regular dimensions
                # this is the raw column name (e.g., "year"), which _add_table_prefixes_to_filter
                # maps to the dimension's table alias. For skip-join (local) dimensions this is
                # the FK column on the fact table (e.g., "utc_date_id").
                col_name_for_replacement = alias_to_use
            replacement = ast.Column(
                name=ast.Name(col_name_for_replacement),
                _table=ast.Table(ast.Name(cte_alias)) if cte_alias else None,
            )
            subscript.swap(replacement)

    # Second pass: handle regular Column references (no subscript role syntax)
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


def get_filter_column_references(filter_str: str) -> list[str]:
    """
    Extract all column references from a filter string.

    Args:
        filter_str: A SQL predicate expression

    Returns:
        List of full column reference strings (e.g., ["v3.product.category", "v3.status"])

    Example::

        refs = get_filter_column_references("v3.product.category = 'Electronics'")
        # Returns: ["v3.product.category"]

        refs = get_filter_column_references("v3.total_revenue > 10000 AND v3.order_count > 100")
        # Returns: ["v3.total_revenue", "v3.order_count"]
    """
    filter_ast = parse_filter(filter_str)
    references = []

    for col in filter_ast.find_all(ast.Column):
        full_ref = _extract_full_column_ref(col)
        if full_ref and full_ref not in references:  # pragma: no branch
            references.append(full_ref)

    return references
