"""
SQL parsing functions.
"""

from typing import Any, Iterator, Set

from sqloxide import parse_sql


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


def get_dependencies(expression: str) -> Set[str]:
    """
    Return all the dependencies from a SQL expression.
    """
    tree = parse_sql(expression, dialect="ansi")

    return {
        ".".join(part["value"] for part in table["name"])
        for table in find_nodes_by_key(tree, "Table")
    }
