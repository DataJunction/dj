"""
Models for columns.
"""

from typing import TYPE_CHECKING

import strawberry

from dj.models.column import Column as _Column
from dj.typing import ColumnType as _ColumnType

if TYPE_CHECKING:
    from dj.models.node import Node

ColumnType = strawberry.enum(_ColumnType)


@strawberry.experimental.pydantic.type(model=_Column, all_fields=True)
class Column:  # type: ignore
    """
    A column.

    Columns can be physical (associated with ``Table`` objects) or abstract (associated
    with ``Node`` objects).
    """
