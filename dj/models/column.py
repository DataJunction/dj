"""
Models for columns.
"""

from typing import TYPE_CHECKING, Optional, TypedDict

from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlmodel import Field, Relationship, SQLModel

from dj.typing import ColumnType, ColumnTypeDecorator

if TYPE_CHECKING:
    from dj.models.node import Node, NodeRevision


class ColumnYAML(TypedDict, total=False):
    """
    Schema of a column in the YAML file.
    """

    type: str
    dimension: str


class Column(SQLModel, table=True):  # type: ignore
    """
    A column.

    Columns can be physical (associated with ``Table`` objects) or abstract (associated
    with ``Node`` objects).
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    type: ColumnType = Field(sa_column=SqlaColumn(ColumnTypeDecorator, nullable=False))

    dimension_id: Optional[int] = Field(default=None, foreign_key="node.id")
    dimension: "Node" = Relationship()
    dimension_column: Optional[str] = None

    def to_yaml(self) -> ColumnYAML:
        """
        Serialize the column for YAML.
        """
        return {
            "type": str(self.type),  # pylint: disable=no-member
        }

    def __hash__(self) -> int:
        return hash(self.id)
