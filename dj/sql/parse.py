"""
SQL parsing functions.
"""

from typing import Optional


def is_metric(query: Optional[str], dialect: Optional[str] = None) -> bool:
    """
    Return if a SQL query defines a metric.

    The SQL query should have a single expression in its projections, and it should
    be an aggregation function in order for it to be considered a metric.
    """

    from dj.sql.parsing.backends.sqloxide import parse  # pylint: disable=C0415

    if query is None:
        return False

    tree = parse(query, dialect=dialect)

    # must have a single expression
    if len(tree.select.projection) != 1:
        return False

    return tree.select.projection[0].is_aggregation()
