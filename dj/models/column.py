"""
Models for columns.
"""
from typing import TYPE_CHECKING, List, Optional, Tuple, TypedDict

from pydantic import root_validator
from sqlalchemy import TypeDecorator
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.types import Text
from sqlmodel import Field, Relationship

from dj.models.base import BaseSQLModel, NodeColumns
from dj.sql.parsing.types import ColumnType

if TYPE_CHECKING:
    from dj.models.attribute import ColumnAttribute
    from dj.models.node import Node, NodeRevision


class ColumnYAML(TypedDict, total=False):
    """
    Schema of a column in the YAML file.
    """

    type: str
    dimension: str


class ColumnTypeDecorator(TypeDecorator):  # pylint: disable=abstract-method
    """
    Converts a column type from the database to a `ColumnType` class
    """

    impl = Text

    def process_bind_param(self, value: ColumnType, dialect):
        return str(value)

    def process_result_value(self, value, dialect):
        from dj.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
            parse_rule,
        )

        if not value:
            return value
        return parse_rule(value, "dataType")


class Column(BaseSQLModel, table=True):  # type: ignore
    """
    A column.

    Columns can be physical (associated with ``Table`` objects) or abstract (associated
    with ``Node`` objects).
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    type: ColumnType = Field(sa_column=SqlaColumn(ColumnTypeDecorator, nullable=False))

    dimension_id: Optional[int] = Field(default=None, foreign_key="node.id")
    dimension: "Node" = Relationship(
        sa_relationship_kwargs={
            "lazy": "joined",
        },
    )
    dimension_column: Optional[str] = None
    node_revisions: List["NodeRevision"] = Relationship(
        back_populates="columns",
        link_model=NodeColumns,
        sa_relationship_kwargs={
            "lazy": "select",
        },
    )
    attributes: List["ColumnAttribute"] = Relationship(
        back_populates="column",
        sa_relationship_kwargs={
            "lazy": "joined",
        },
    )

    def identifier(self) -> Tuple[str, ColumnType]:
        """
        Unique identifier for this column.
        """
        return self.name, self.type

    def has_dimension_attribute(self) -> bool:
        """
        Whether the dimension attribute is set on this column.
        """
        return any(
            attr.attribute_type.name == "dimension"
            for attr in self.attributes  # pylint: disable=not-an-iterable
        )

    def __hash__(self) -> int:
        return hash(self.id)

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        arbitrary_types_allowed = True

    @root_validator
    def type_string(cls, values):  # pylint: disable=no-self-argument
        """
        Processes the column type
        """
        values["type"] = str(values.get("type"))
        return values


class ColumnAttributeInput(BaseSQLModel):
    """
    A column attribute input
    """

    attribute_type_namespace: Optional[str] = "system"
    attribute_type_name: str
    column_name: str
