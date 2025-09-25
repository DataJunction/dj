"""
Models for attributes.
"""

from enum import Enum
from typing import List, Optional

from pydantic.main import BaseModel
from pydantic import ConfigDict

from datajunction_server.enum import StrEnum
from datajunction_server.models.node_type import NodeType

RESERVED_ATTRIBUTE_NAMESPACE = "system"


class AttributeTypeIdentifier(BaseModel):
    """
    Fields that can be used to identify an attribute type.
    """

    namespace: str = RESERVED_ATTRIBUTE_NAMESPACE
    name: str


class UniquenessScope(StrEnum):
    """
    The scope at which this attribute needs to be unique.
    """

    NODE = "node"
    COLUMN_TYPE = "column_type"


class MutableAttributeTypeFields(AttributeTypeIdentifier):
    """
    Fields on attribute types that users can set.
    """

    description: str
    allowed_node_types: List[NodeType]
    uniqueness_scope: Optional[List[UniquenessScope]] = None


class AttributeTypeBase(MutableAttributeTypeFields):
    """Base attribute type."""

    id: int

    model_config = ConfigDict(from_attributes=True)


class ColumnAttributes(str, Enum):
    """
    Managed by default column attributes
    """

    PRIMARY_KEY = "primary_key"
    DIMENSION = "dimension"
    HIDDEN = "hidden"
