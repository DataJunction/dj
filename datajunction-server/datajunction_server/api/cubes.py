"""
Cube related APIs.
"""
import logging

from fastapi import APIRouter, Depends
from fastapi.security import HTTPBearer
from sqlmodel import Session

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.models.cube import CubeRevisionMetadata
from datajunction_server.models.node import NodeType
from datajunction_server.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter(tags=["cubes"], dependencies=[Depends(HTTPBearer())])


@router.get("/cubes/{name}/", response_model=CubeRevisionMetadata, name="Get a Cube")
def get_cube(
    name: str, *, session: Session = Depends(get_session)
) -> CubeRevisionMetadata:
    """
    Get information on a cube
    """
    node = get_node_by_name(session=session, name=name, node_type=NodeType.CUBE)
    return node.current
