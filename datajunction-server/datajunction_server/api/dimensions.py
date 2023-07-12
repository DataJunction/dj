"""
Dimensions related APIs.
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, Depends
from sqlmodel import Session

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.models.node import NodeRevisionOutput, NodeType
from datajunction_server.sql.dag import get_nodes_with_dimension
from datajunction_server.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/dimensions/{name}/nodes/", response_model=List[NodeRevisionOutput])
def find_nodes_with_dimension(
    name: str,
    *,
    node_types: Optional[List[NodeType]] = None,
    session: Session = Depends(get_session),
) -> List[NodeRevisionOutput]:
    """
    List all nodes that have the specified dimension
    """
    dimension_node = get_node_by_name(session, name)
    nodes = get_nodes_with_dimension(session, dimension_node, node_types)
    return nodes
