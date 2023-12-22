"""
Models for attributes.
"""
from typing import TYPE_CHECKING, List, Optional

import sqlalchemy as sa
from pydantic.main import BaseModel
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.connection import Base
from datajunction_server.enum import StrEnum
from datajunction_server.models.node_type import NodeType

if TYPE_CHECKING:
    from datajunction_server.models import Column


RESERVED_ATTRIBUTE_NAMESPACE = "system"


class AttributeTypeIdentifier(BaseModel):
    """
    Fields that can be used to identify an attribute type.
    """

    namespace: str = "system"
    name: str


class MutableAttributeTypeFields(AttributeTypeIdentifier):
    """
    Fields on attribute types that users can set.
    """

    description: str
    allowed_node_types: List[NodeType]


class UniquenessScope(StrEnum):
    """
    The scope at which this attribute needs to be unique.
    """

    NODE = "node"
    COLUMN_TYPE = "column_type"


class RestrictedAttributeTypeFields(BaseModel):
    """
    Fields on attribute types that aren't configurable by users.
    """

    uniqueness_scope: List[UniquenessScope] = []


class AttributeTypeBase(MutableAttributeTypeFields, RestrictedAttributeTypeFields):
    """Base attribute type."""

    id: int

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class AttributeType(Base):  # pylint: disable=too-few-public-methods
    """
    Available attribute types for column metadata.
    """

    __tablename__ = "attributetype"
    __table_args__ = (UniqueConstraint("namespace", "name"),)

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
        nullable=True,
        default=None,
    )
    namespace: Mapped[str] = mapped_column(nullable=False, default="system")
    name: Mapped[str] = mapped_column(nullable=False)
    description: Mapped[str] = mapped_column(nullable=False)
    allowed_node_types: Mapped[List[str]] = mapped_column(sa.JSON, nullable=True)
    uniqueness_scope: Mapped[List[str]] = mapped_column(sa.JSON, nullable=True)

    def __hash__(self):
        return hash(self.id)


class ColumnAttribute(Base):  # pylint: disable=too-few-public-methods
    """
    Column attributes.
    """

    __tablename__ = "columnattribute"
    __table_args__ = (UniqueConstraint("attribute_type_id", "column_id"),)

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )

    attribute_type_id: Mapped[int] = mapped_column(sa.ForeignKey("attributetype.id"))
    attribute_type: Mapped[AttributeType] = relationship(
        foreign_keys=[attribute_type_id],
    )

    column_id: Mapped[Optional[int]] = mapped_column(sa.ForeignKey("column.id"))
    column: Mapped[Optional["Column"]] = relationship(
        back_populates="attributes",
        foreign_keys=[column_id],
    )
