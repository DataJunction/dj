"""
Models for columns.
"""

# pylint: disable=too-few-public-methods

from typing import TYPE_CHECKING

import strawberry

from dj.models.column import Column as Column_
from dj.typing import ColumnType as ColumnType_

if TYPE_CHECKING:
    from dj.models.node import Node

ColumnType = strawberry.enum(ColumnType_)


@strawberry.experimental.pydantic.type(model=Column_, all_fields=True)
class Column:  # type: ignore
    """
    A column.

    Columns can be physical (associated with ``Table`` objects) or abstract (associated
    with ``Node`` objects).
    """
