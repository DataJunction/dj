"""
Models for source node CRUD operations.
"""
from typing import Dict, List, Optional

from typing_extensions import TypedDict

from dj.models import Column, Table
from dj.models.node import NodeBase
from dj.typing import ColumnType


class SourceNodeColumnType(TypedDict, total=False):
    """
    Schema of a column for a table defined in a source node
    """

    type: ColumnType
    dimension: Optional[str]


class SourceNodeCreator(NodeBase):
    """A create object for adding a new source node"""

    columns: Dict[str, SourceNodeColumnType]


class ExistingSourceNode(NodeBase):
    """A source read from the DJ system"""

    id: int
    columns: List[Column]
    tables: List[Table]
