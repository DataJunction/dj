"""
Cube related APIs.
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, Depends
from pydantic import Field
from sqlmodel import Session, SQLModel

from dj.api.helpers import get_node_by_name
from dj.models.node import AvailabilityState, NodeType
from dj.utils import UTCDatetime, get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


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


@router.get("/cubes/{name}/", response_model=CubeRevisionMetadata)
def read_cube(
    name: str, *, session: Session = Depends(get_session)
) -> CubeRevisionMetadata:
    """
    Get information on a cube
    """
    node = get_node_by_name(session=session, name=name, node_type=NodeType.CUBE)
    return node.current
