"""
Models for columns.
"""

# pylint: disable=too-few-public-methods

from typing import TYPE_CHECKING

import strawberry

from dj.models.column import Column as Column_
from dj.typing import ColumnType

if TYPE_CHECKING:
    from dj.models.node import Node




@strawberry.experimental.pydantic.type(model=Column_, fields=['id', 'name', 'dimension_id', 'dimension_column'])
class Column:  # type: ignore
    """
    A column.

    Columns can be physical (associated with ``Table`` objects) or abstract (associated
    with ``Node`` objects).
    """
    type: str
