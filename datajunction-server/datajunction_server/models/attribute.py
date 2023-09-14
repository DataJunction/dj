"""
Models for attributes.
"""
import enum
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import JSON, String, UniqueConstraint
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlmodel import Field, Relationship

from datajunction_server.models.base import BaseSQLModel
from datajunction_server.models.node import NodeType

if TYPE_CHECKING:
    from datajunction_server.models import Column


RESERVED_ATTRIBUTE_NAMESPACE = "system"


class AttributeTypeIdentifier(BaseSQLModel):
    """
    Fields that can be used to identify an attribute type.
    """

    namespace: str = "system"
    name: str = Field(sa_column=SqlaColumn("name", String))


class MutableAttributeTypeFields(AttributeTypeIdentifier):
    """
    Fields on attribute types that users can set.
    """

    description: str
    allowed_node_types: List[NodeType] = Field(sa_column=SqlaColumn(JSON))


class UniquenessScope(str, enum.Enum):
    """
    The scope at which this attribute needs to be unique.
    """

    NODE = "node"
    COLUMN_TYPE = "column_type"


class RestrictedAttributeTypeFields(BaseSQLModel):
    """
    Fields on attribute types that aren't configurable by users.
    """

    uniqueness_scope: List[UniquenessScope] = Field(
        default=[],
        sa_column=SqlaColumn(JSON),
    )


class AttributeTypeBase(MutableAttributeTypeFields, RestrictedAttributeTypeFields):
    """Base attribute type."""


class AttributeType(AttributeTypeBase, table=True):  # type: ignore  # pylint: disable=too-many-ancestors
    """
    Available attribute types for column metadata.
    """

    __table_args__ = (UniqueConstraint("namespace", "name"),)

    id: Optional[int] = Field(default=None, primary_key=True)

    def __hash__(self):
        return hash(self.id)


class ColumnAttribute(BaseSQLModel, table=True):  # type: ignore
    """
    Column attributes.
    """

    __table_args__ = (UniqueConstraint("attribute_type_id", "column_id"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    attribute_type_id: Optional[int] = Field(
        default=None,
        foreign_key="attributetype.id",
    )
    attribute_type: AttributeType = Relationship()

    column_id: Optional[int] = Field(default=None, foreign_key="column.id")
    column: "Column" = Relationship(back_populates="attributes")
