"""
Models for cubes.
"""

from typing import List, Optional

from pydantic import Field, root_validator
from sqlmodel import SQLModel

from datajunction_server.models.materialization import MaterializationConfigOutput
from datajunction_server.models.node import AvailabilityState, ColumnOutput, NodeType
from datajunction_server.models.partition import PartitionOutput
from datajunction_server.typing import UTCDatetime


class CubeElementMetadata(SQLModel):
    """
    Metadata for an element in a cube
    """

    name: str
    display_name: str
    node_name: str
    type: str
    partition: Optional[PartitionOutput]

    @root_validator(pre=True)
    def type_string(cls, values):  # pylint: disable=no-self-argument
        """
        Extracts the type as a string
        """
        values = dict(values)
        values["node_name"] = values["node_revisions"][0].name
        values["type"] = (
            values["node_revisions"][0].type
            if values["node_revisions"][0].type == NodeType.METRIC
            else NodeType.DIMENSION
        )
        return values


class CubeRevisionMetadata(SQLModel):
    """
    Metadata for a cube node
    """

    id: int = Field(alias="node_revision_id")
    node_id: int
    type: NodeType
    name: str
    display_name: str
    version: str
    description: str = ""
    availability: Optional[AvailabilityState] = None
    cube_elements: List[CubeElementMetadata]
    query: str
    columns: List[ColumnOutput]
    updated_at: UTCDatetime
    materializations: List[MaterializationConfigOutput]

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        allow_population_by_field_name = True
