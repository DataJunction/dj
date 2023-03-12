"""
Models for cubes.
"""

from typing import List, Optional

from pydantic import Field
from sqlmodel import SQLModel

from dj.models.node import AvailabilityState, NodeType
from dj.typing import UTCDatetime


class CubeElementMetadata(SQLModel):
    """
    Metadata for an element in a cube
    """

    id: int
    current_version: str
    name: str


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
    updated_at: UTCDatetime

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        allow_population_by_field_name = True
