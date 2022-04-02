"""
SQL parsing functions.
"""

from typing import Any, Iterator, Set

from sqloxide import parse_sql

from datajunction.sql.functions import function_registry


def find_nodes_by_key(element: Any, target: str) -> Iterator[Any]:
    """
    Find all nodes in a SQL tree matching a given key.
    """
    if isinstance(element, list):
        for child in element:
            yield from find_nodes_by_key(child, target)
    elif isinstance(element, dict):
        for key, value in element.items():
            if key == target:
                yield value
            else:
                yield from find_nodes_by_key(value, target)


def get_dependencies(sql: str) -> Set[str]:
    """
    Return all the dependencies from a SQL expression.
    """
    tree = parse_sql(sql, dialect="ansi")

    return {
        ".".join(part["value"] for part in table["name"])
        for table in find_nodes_by_key(tree, "Table")
    }


def is_metric(sql: str) -> bool:
    """
    Return if a SQL expression defines a metric.

    The SQL expression should have a single expression in its projections, and it should
    be an aggregation function in order for it to be considered a metric.
    """
    tree = parse_sql(sql, dialect="ansi")
    projection = next(find_nodes_by_key(tree, "projection"))

    # must have a single expression
    expressions = list(projection)
    if len(expressions) != 1:
        return False

    # must be a function
    expression = expressions[0]
    if "UnnamedExpr" in expression:
        expression = expression["UnnamedExpr"]
    elif "ExprWithAlias" in expression:
        expression = expression["ExprWithAlias"]["expr"]
    else:
        raise NotImplementedError(f"Unable to handle expression: {expression}")
    if "Function" not in expression:
        return False

    # must be an aggregation
    function = expression["Function"]
    name = function["name"][0]["value"]
    dj_function = function_registry[name.upper()]
    return dj_function.is_aggregation
