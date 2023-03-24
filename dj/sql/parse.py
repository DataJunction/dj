"""
SQL parsing functions.
"""

from typing import Optional


def is_metric(query: Optional[str]) -> bool:
    """
    Return if a SQL query defines a metric.

    The SQL query should have a single expression in its projections, and it should
    be an aggregation function in order for it to be considered a metric.
    """

    from dj.sql.parsing.backends.antlr4 import parse  # pylint: disable=C0415

    if query is None:
        return False

    tree = parse(query)

    # must have a single expression
    if len(tree.select.projection) != 1:
        return False

    return tree.select.projection[0].is_aggregation()  # type: ignore
