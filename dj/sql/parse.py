"""
SQL parsing functions.
"""

from typing import Any, Dict, Iterator, Optional, Set, Tuple

from sqloxide import parse_sql

from dj.sql.functions import function_registry
from dj.typing import Expression, Projection


def find_nodes_by_key(element: Any, target: str) -> Iterator[Any]:
    """
    Find all nodes in a SQL tree matching a given key.
    """
    for node, _ in find_nodes_by_key_with_parent(element, target):
        yield node


def find_nodes_by_key_with_parent(
    element: Any,
    target: str,
) -> Iterator[Tuple[Any, Any]]:
    """
    Find all nodes in a SQL tree matching a given key, and their parent.
    """
    if isinstance(element, list):
        for child in element:
            yield from find_nodes_by_key_with_parent(child, target)
    elif isinstance(element, dict):
        for key, value in element.items():
            if key == target:
                yield value, element
            else:
                yield from find_nodes_by_key_with_parent(value, target)


def get_dependencies(query: str) -> Set[str]:
    """
    Return all the dependencies from a SQL query.
    """
    tree = parse_sql(query, dialect="ansi")

    return {
        ".".join(part["value"] for part in table["name"])
        for table in find_nodes_by_key(tree, "Table")
    }


def get_expression_from_projection(projection: Projection) -> Expression:
    """
    Return an expression from a projection, handling aliases.
    """
    if "UnnamedExpr" in projection:
        return projection["UnnamedExpr"]
    if "ExprWithAlias" in projection:
        return projection["ExprWithAlias"]["expr"]

    raise NotImplementedError(f"Unable to handle expression: {projection}")


def contains_agg_function_if_any(expr: Expression) -> bool:
    """
    Checks if the expression contains an aggregation function.
    """
    while expr:
        if not isinstance(expr, Dict):
            break
        head = list(expr.keys())[0]
        if head == "Function":
            name = expr[head]["name"][0]["value"]  # type: ignore
            return function_registry[name].is_aggregation
        expr = expr[head]  # type: ignore
    return False


def is_metric(query: Optional[str]) -> bool:
    """
    Return if a SQL query defines a metric.

    The SQL query should have a single expression in its projections, and it should
    be an aggregation function in order for it to be considered a metric.
    """
    if query is None:
        return False

    tree = parse_sql(query, dialect="ansi")
    projection = next(find_nodes_by_key(tree, "projection"))

    # must have a single expression
    expressions = list(projection)
    if len(expressions) != 1:
        return False

    # must be a function
    expression = get_expression_from_projection(expressions[0])

    # must be an aggregation
    if "Case" in expression:
        case_when = expression["Case"]
        return all(
            contains_agg_function_if_any(result) for result in case_when["results"]
        )
    return contains_agg_function_if_any(expression)
