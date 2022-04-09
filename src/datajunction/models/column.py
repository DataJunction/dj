"""
Models for columns.
"""

from typing import TYPE_CHECKING, Optional, TypedDict

from sqlmodel import Field, Relationship, SQLModel

from datajunction.typing import ColumnType

if TYPE_CHECKING:
    from datajunction.models.node import Node


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
    type: ColumnType

    dimension_id: Optional[int] = Field(default=None, foreign_key="node.id")
    dimension: "Node" = Relationship()
    dimension_column: Optional[str] = None

    def to_yaml(self) -> ColumnYAML:
        """
        Serialize the column for YAML.
        """
        return {
            "type": self.type.value,
        }

    def __hash__(self) -> int:
        return hash(self.id)
