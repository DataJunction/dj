"""Column database schema."""
from typing import TYPE_CHECKING, List, Optional, Tuple

from sqlalchemy import BigInteger, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.attributetype import ColumnAttribute
from datajunction_server.database.base import Base
from datajunction_server.models.base import labelize
from datajunction_server.models.column import ColumnTypeDecorator
from datajunction_server.sql.parsing.types import ColumnType

if TYPE_CHECKING:
    from datajunction_server.database.measure import Measure
    from datajunction_server.database.node import Node, NodeRevision
    from datajunction_server.database.partition import Partition


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
    order: Mapped[Optional[int]]
    name: Mapped[str] = mapped_column()
    display_name: Mapped[Optional[str]] = mapped_column(
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    type: Mapped[Optional[ColumnType]] = mapped_column(ColumnTypeDecorator)

    dimension_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("node.id", ondelete="SET NULL", name="fk_column_dimension_id_node"),
    )
    dimension: Mapped[Optional["Node"]] = relationship(
        "Node",
        lazy="joined",
    )
    dimension_column: Mapped[Optional[str]] = mapped_column()
    node_revisions: Mapped[List["NodeRevision"]] = relationship(
        back_populates="columns",
        secondary="nodecolumns",
        lazy="selectin",
    )
    attributes: Mapped[List["ColumnAttribute"]] = relationship(
        back_populates="column",
        lazy="joined",
        cascade="all,delete",
    )
    measure_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "measures.id",
            name="fk_column_measure_id_measures",
            ondelete="SET NULL",
        ),
    )
    measure: Mapped["Measure"] = relationship(back_populates="columns")

    partition_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "partition.id",
            name="fk_column_partition_id_partition",
            ondelete="SET NULL",
        ),
    )
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

    def has_attributes_besides(self, attribute_name: str) -> bool:
        """
        Whether the column has any attribute besides the one specified.
        """
        return any(
            attr.attribute_type.name != attribute_name
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
        return f"{self.node_revision().name}.{self.name}"  # type: ignore  # pragma: no cover

    def copy(self) -> "Column":
        """
        Returns a full copy of the column
        """
        return Column(
            order=self.order,
            name=self.name,
            display_name=self.display_name,
            type=self.type,
            dimension_id=self.dimension_id,
            dimension_column=self.dimension_column,
            attributes=[
                ColumnAttribute(attribute_type_id=attr.attribute_type_id)
                for attr in self.attributes
            ],
            measure_id=self.measure_id,
            partition_id=self.partition_id,
        )
