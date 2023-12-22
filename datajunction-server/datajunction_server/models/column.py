"""
Models for columns.
"""
from typing import TYPE_CHECKING, List, Optional, Tuple, TypedDict

from pydantic.main import BaseModel
from sqlalchemy import BigInteger, Integer, TypeDecorator
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.types import String, Text

from datajunction_server.database.connection import Base
from datajunction_server.enum import StrEnum
from datajunction_server.models.base import labelize
from datajunction_server.sql.parsing.types import ColumnType

if TYPE_CHECKING:
    from datajunction_server.models.attribute import ColumnAttribute
    from datajunction_server.models.measure import Measure
    from datajunction_server.models.node import Node, NodeRevision
    from datajunction_server.models.partition import Partition


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


class Column(Base):  # type: ignore
    """
    A column.

    Columns can be physical (associated with ``Table`` objects) or abstract (associated
    with ``Node`` objects).
    """

    __tablename__ = "column"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column()
    display_name: Mapped[str] = mapped_column(
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    type: Mapped[ColumnType] = mapped_column(ColumnTypeDecorator, nullable=True)

    dimension_id: Mapped[Optional[int]] = mapped_column(ForeignKey("node.id"))
    dimension: Mapped[Optional["Node"]] = relationship(
        "Node",
        lazy="joined",
    )
    dimension_column: Mapped[Optional[str]] = mapped_column()
    node_revisions: Mapped[List["NodeRevision"]] = relationship(
        back_populates="columns",
        secondary="nodecolumns",
        lazy="select",
    )
    attributes: Mapped[List["ColumnAttribute"]] = relationship(
        back_populates="column",
        lazy="joined",
        cascade="all,delete",
    )
    measure_id: Mapped[Optional[int]] = mapped_column(ForeignKey("measures.id"))
    measure: Mapped["Measure"] = relationship(back_populates="columns")

    partition_id: Mapped[Optional[int]] = mapped_column(ForeignKey("partition.id"))
    partition: Mapped["Partition"] = relationship(
        lazy="joined",
        primaryjoin="Column.id==Partition.column_id",
        uselist=False,
        cascade="all,delete",
    )

    def identifier(self) -> Tuple[str, ColumnType]:
        """
        Unique identifier for this column.
        """
        return self.name, self.type

    def is_dimensional(self) -> bool:
        """
        Whether this column is considered dimensional
        """
        return (  # pragma: no cover
            self.has_dimension_attribute()
            or self.has_primary_key_attribute()
            or self.dimension
        )

    def has_dimension_attribute(self) -> bool:
        """
        Whether the dimension attribute is set on this column.
        """
        return self.has_attribute("dimension")  # pragma: no cover

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


class ColumnAttributeInput(BaseModel):
    """
    A column attribute input
    """

    attribute_type_namespace: Optional[str] = "system"
    attribute_type_name: str
    column_name: str


class SemanticType(StrEnum):
    """
    Semantic type of a column
    """

    MEASURE = "measure"
    METRIC = "metric"
    DIMENSION = "dimension"
