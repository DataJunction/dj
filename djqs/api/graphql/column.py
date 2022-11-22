"""
Models for columns.
"""

# pylint: disable=too-few-public-methods

from typing import TYPE_CHECKING

import strawberry

from djqs.models.column import Column as Column_
from djqs.typing import ColumnType as ColumnType_

if TYPE_CHECKING:
    from djqs.models.node import Node

ColumnType = strawberry.enum(ColumnType_)


@strawberry.experimental.pydantic.type(model=Column_, all_fields=True)
class Column:  # type: ignore
    """
    A column.

    Columns can be physical (associated with ``Table`` objects) or abstract (associated
    with ``Node`` objects).
    """
