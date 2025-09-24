"""Models for dimension links"""

from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict

from datajunction_server.enum import StrEnum
from datajunction_server.models.node_type import NodeNameOutput


class JoinCardinality(StrEnum):
    """
    The version upgrade type
    """

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class JoinType(StrEnum):
    """
    Join type
    """

    LEFT = "left"
    RIGHT = "right"
    INNER = "inner"
    FULL = "full"
    CROSS = "cross"


class LinkType(StrEnum):
    """
    There are two types of dimensions links supported: join links or reference links
    """

    JOIN = "join"
    REFERENCE = "reference"


class LinkDimensionIdentifier(BaseModel):
    """
    Input for linking a dimension to a node
    """

    dimension_node: str
    role: Optional[str] = None


class JoinLinkInput(BaseModel):
    """
    Input for creating a join link between a dimension node and node
    """

    dimension_node: str
    join_type: Optional[JoinType] = JoinType.LEFT
    join_on: Optional[str] = None
    join_cardinality: Optional[JoinCardinality] = JoinCardinality.MANY_TO_ONE
    role: Optional[str] = None


class LinkDimensionOutput(BaseModel):
    """
    Input for linking a dimension to a node
    """

    dimension: NodeNameOutput
    join_type: JoinType
    join_sql: str
    join_cardinality: Optional[JoinCardinality] = None
    role: Optional[str] = None
    foreign_keys: Dict[str, str | None]

    model_config = ConfigDict(from_attributes=True)
