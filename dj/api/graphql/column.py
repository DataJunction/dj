"""
Models for columns.
"""

# pylint: disable=too-few-public-methods

from typing import TYPE_CHECKING, NewType

import strawberry

from dj.models.column import Column as Column_
from dj.typing import ColumnType as ColumnType_

if TYPE_CHECKING:
    from dj.models.node import Node


ColumnType = strawberry.scalar(
    NewType("ColumnType", ColumnType_),
    serialize=str,
    parse_value=ColumnType_,
    description="The type of a column represented as `String`.",
)


@strawberry.experimental.pydantic.type(
    model=Column_,
    fields=["id", "name", "dimension_id", "dimension_column"],
)
class Column:  # type: ignore
    """
    A column.

    Columns can be physical (associated with ``Table`` objects) or abstract (associated
    with ``Node`` objects).
    """

    type: ColumnType  # type: ignore
