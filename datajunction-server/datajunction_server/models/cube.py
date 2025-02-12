"""
Models for cubes.
"""

from typing import List, Optional

from pydantic import Field, root_validator
from pydantic.main import BaseModel

from datajunction_server.models.materialization import MaterializationConfigOutput
from datajunction_server.models.node import (
    AvailabilityStateBase,
    ColumnOutput,
    NodeMode,
    NodeStatus,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionOutput
from datajunction_server.models.tag import TagOutput
from datajunction_server.typing import UTCDatetime


class CubeElementMetadata(BaseModel):
    """
    Metadata for an element in a cube
    """

    name: str
    display_name: str
    node_name: str
    type: str
    partition: Optional[PartitionOutput]

    @root_validator(pre=True)
    def type_string(cls, values):
        """
        Extracts the type as a string
        """
        values = dict(values)
        if "node_revisions" in values:
            values["node_name"] = values["node_revisions"][0].name
            values["type"] = (
                values["node_revisions"][0].type
                if values["node_revisions"][0].type == NodeType.METRIC
                else NodeType.DIMENSION
            )
        return values

    class Config:
        orm_mode = True


class CubeRevisionMetadata(BaseModel):
    """
    Metadata for a cube node
    """

    id: int = Field(alias="node_revision_id")
    node_id: int
    type: NodeType
    name: str
    display_name: str
    version: str
    status: NodeStatus
    mode: NodeMode
    description: str = ""
    availability: Optional[AvailabilityStateBase] = None
    cube_elements: List[CubeElementMetadata]
    cube_node_metrics: List[str]
    cube_node_dimensions: List[str]
    query: Optional[str]
    columns: List[ColumnOutput]
    sql_columns: Optional[List[ColumnOutput]]
    updated_at: UTCDatetime
    materializations: List[MaterializationConfigOutput]
    tags: Optional[List[TagOutput]]

    class Config:
        allow_population_by_field_name = True
        orm_mode = True


class DimensionValue(BaseModel):
    """
    Dimension value and count
    """

    value: List[str]
    count: Optional[int]


class DimensionValues(BaseModel):
    """
    Dimension values
    """

    dimensions: List[str]
    values: List[DimensionValue]
    cardinality: int
