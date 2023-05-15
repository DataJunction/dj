"""
SQL parsing functions.
"""


from typing import TYPE_CHECKING

from dj.errors import DJInvalidInputException

if TYPE_CHECKING:
    from dj.models.node import NodeRevision


def check_is_metric(node: "NodeRevision"):
    """
    Check if a SQL query defines a metric.

    The SQL query should have a single expression in its projections, it should
    be an aggregation function and should have the same name
    as the node in order for it to be considered a metric.
    """

    from dj.sql.parsing.backends.antlr4 import parse  # pylint: disable=C0415

    if node.query is None:
        raise DJInvalidInputException("Invalid Metric. Node has no query.")

    tree = parse(node.query)

    # must have a single expression
    if len(tree.select.projection) != 1:
        raise DJInvalidInputException(
            "Invalid Metric. Node query has does not have "
            "a single expression in the projection.",
        )

    if (
        not hasattr(tree.select.projection[0], "alias_or_name")
        or tree.select.projection[0].alias_or_name.name != node.name  # type: ignore
    ):
        raise DJInvalidInputException(
            "Invalid Metric. The expression in the projection "
            "must have a name or alias the same as the node name.",
        )

    if (
        not hasattr(tree.select.projection[0], "is_aggregation")
        or not tree.select.projection[0].is_aggregation()  # type: ignore
    ):
        raise DJInvalidInputException(
            "Invalid Metric. The expression in the "
            "projection must be an aggregation.",
        )
