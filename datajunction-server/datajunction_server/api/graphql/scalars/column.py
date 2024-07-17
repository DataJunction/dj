"""Column scalars"""
from typing import List, Optional

import strawberry

from datajunction_server.models.partition import PartitionType as PartitionType_

PartitionType = strawberry.enum(PartitionType_)


@strawberry.type
class AttributeTypeName:  # pylint: disable=too-few-public-methods
    """
    Attribute type name.
    """

    namespace: str
    name: str


@strawberry.type
class Attribute:  # pylint: disable=too-few-public-methods
    """
    Column attribute
    """

    attribute_type: AttributeTypeName


@strawberry.type
class NodeName:  # pylint: disable=too-few-public-methods
    """
    Node name
    """

    name: str


@strawberry.type
class Partition:  # pylint: disable=too-few-public-methods
    """
    A partition configuration for a column
    """

    type_: PartitionType  # type: ignore
    format: Optional[str]
    granularity: Optional[str]
    expression: Optional[str]


@strawberry.type
class Column:  # pylint: disable=too-few-public-methods
    """
    A column on a node
    """

    name: str
    display_name: Optional[str]
    type: str
    attributes: Optional[List[Attribute]]
    dimension: Optional[NodeName]
    partition: Optional[Partition]
    # order: Optional[int]
