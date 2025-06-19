"""Column scalars"""

from typing import List, Optional

import strawberry

from datajunction_server.models.partition import PartitionType as PartitionType_

PartitionType = strawberry.enum(PartitionType_)


@strawberry.type
class AttributeTypeName:
    """
    Attribute type name.
    """

    namespace: str
    name: str


@strawberry.type
class Attribute:
    """
    Column attribute
    """

    attribute_type: AttributeTypeName


@strawberry.type
class NodeName:
    """
    Node name
    """

    name: str


@strawberry.type
class Partition:
    """
    A partition configuration for a column
    """

    type_: PartitionType  # type: ignore
    format: Optional[str]
    granularity: Optional[str]
    expression: Optional[str]


@strawberry.type
class Column:
    """
    A column on a node
    """

    name: str
    display_name: Optional[str]
    type: str
    attributes: List[Attribute] = strawberry.field(default_factory=list)
    dimension: Optional[NodeName]
    partition: Optional[Partition]
