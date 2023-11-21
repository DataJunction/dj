"""
Models for columns.
"""
import enum
from typing import TYPE_CHECKING, List, Optional, Tuple, TypedDict

from pydantic import root_validator
from sqlalchemy import TypeDecorator
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.types import String, Text
from sqlmodel import Field, Relationship

from datajunction_server.models.base import (
    BaseSQLModel,
    NodeColumns,
    generate_display_name,
    labelize,
)
from datajunction_server.sql.parsing.types import ColumnType

if TYPE_CHECKING:
    from datajunction_server.models.attribute import ColumnAttribute
    from datajunction_server.models.measure import Measure
    from datajunction_server.models.node import Node, NodeRevision
    from datajunction_server.models.partition import Partition, PartitionType


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
        from datajunction_server.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
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
    display_name: Optional[str] = Field(
        sa_column=SqlaColumn(
            "display_name",
            String,
            default=generate_display_name("name"),
        ),
    )
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
    measure_id: Optional[int] = Field(default=None, foreign_key="measures.id")
    measure: "Measure" = Relationship(back_populates="columns")

    partition_id: Optional[int] = Field(default=None, foreign_key="partition.id")
    partition: "Partition" = Relationship(
        sa_relationship_kwargs={
            "lazy": "joined",
            "primaryjoin": "Column.id==Partition.column_id",
            "uselist": False,
            "cascade": "all,delete",
        },
    )

    @root_validator(pre=True)
    def default_display_name(cls, values):  # pylint: disable=no-self-argument
        """
        Populate unset display name based on the column's name
        """
        values = dict(values)
        if "display_name" not in values or not values["display_name"]:
            values["display_name"] = labelize(values["name"])
        return values

    def identifier(self) -> Tuple[str, ColumnType]:
        """
        Unique identifier for this column.
        """
        return self.name, self.type

    def is_dimensional(self) -> bool:
        """
        Whether this column is considered dimensional
        """
        return (
            self.has_dimension_attribute()
            or self.has_primary_key_attribute()
            or self.dimension
        )

    def has_dimension_attribute(self) -> bool:
        """
        Whether the dimension attribute is set on this column.
        """
        return self.has_attribute("dimension")

    def has_primary_key_attribute(self) -> bool:
        """
        Whether the primary key attribute is set on this column.
        """
        return self.has_attribute("primary_key")

    def has_attribute(self, attribute_name: str) -> bool:
        """
        Whether the given attribute is set on this column.
        """
        return any(
            attr.attribute_type.name == attribute_name
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

    def node_revision(self) -> Optional["NodeRevision"]:
        """
        Returns the most recent node revision associated with this column
        """
        available_revisions = sorted(self.node_revisions, key=lambda n: n.updated_at)
        return available_revisions[-1] if available_revisions else None

    def full_name(self) -> str:
        """
        Full column name that includes the node it belongs to, i.e., default.hard_hat.first_name
        """
        return f"{self.node_revision().name}.{self.name}"  # type: ignore


class ColumnAttributeInput(BaseSQLModel):
    """
    A column attribute input
    """

    attribute_type_namespace: Optional[str] = "system"
    attribute_type_name: str
    column_name: str


class SemanticType(str, enum.Enum):
    """
    Semantic type of a column
    """

    MEASURE = "measure"
    METRIC = "metric"
    DIMENSION = "dimension"
