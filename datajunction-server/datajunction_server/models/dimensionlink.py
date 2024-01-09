"""Models for dimension links"""
from typing import Optional

from pydantic import BaseModel

from datajunction_server.enum import StrEnum
from datajunction_server.models.node_type import NodeNameOutput


class JoinKind(StrEnum):
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


class LinkDimensionInput(BaseModel):
    """
    Input for linking a dimension to a node
    """

    dimension_node: str
    join_sql: str
    join_kind: Optional[JoinKind]


class LinkDimensionOutput(BaseModel):
    """
    Input for linking a dimension to a node
    """

    dimension: NodeNameOutput
    join_sql: str
    join_kind: Optional[JoinKind]

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        orm_mode = True
