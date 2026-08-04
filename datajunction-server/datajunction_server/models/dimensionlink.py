"""Models for dimension links"""

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


class SparkJoinStrategy(StrEnum):
    """
    Spark SQL join strategy hint
    """

    BROADCAST = "broadcast"
    MERGE = "merge"
    SHUFFLE_HASH = "shuffle_hash"
    SHUFFLE_REPLICATE_NL = "shuffle_replicate_nl"


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
    role: str | None = None


class JoinLinkInput(BaseModel):
    """
    Input for creating a join link between a dimension node and node
    """

    dimension_node: str
    join_type: JoinType | None = JoinType.LEFT
    join_on: str | None = None
    join_cardinality: JoinCardinality | None = JoinCardinality.MANY_TO_ONE
    role: str | None = None
    default_value: str | None = None
    spark_hints: SparkJoinStrategy | None = None


class LinkDimensionOutput(BaseModel):
    """
    Input for linking a dimension to a node
    """

    dimension: NodeNameOutput
    join_type: JoinType
    join_sql: str
    join_cardinality: JoinCardinality | None = None
    role: str | None = None
    foreign_keys: dict[str, str | None]
    default_value: str | None = None
    spark_hints: SparkJoinStrategy | None = None

    model_config = ConfigDict(from_attributes=True)
