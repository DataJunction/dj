"""
Dimensions related APIs.
"""
import logging
from typing import List, Union

from fastapi import Depends, Query
from sqlalchemy.sql.operators import is_
from sqlmodel import Session, select
from typing_extensions import Annotated

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.internal.authentication.http import SecureAPIRouter
from datajunction_server.models.node import Node, NodeRevisionOutput, NodeType
from datajunction_server.sql.dag import (
    get_nodes_with_common_dimensions,
    get_nodes_with_dimension,
)
from datajunction_server.utils import get_session, get_settings

settings = get_settings()
_logger = logging.getLogger(__name__)
router = SecureAPIRouter(tags=["dimensions"])


@router.get("/dimensions/", response_model=List[str])
def list_dimensions(*, session: Session = Depends(get_session)) -> List[str]:
    """
    List all available dimensions.
    """
    return session.exec(
        select(Node.name)
        .where(Node.type == NodeType.DIMENSION)
        .where(is_(Node.deactivated_at, None)),
    ).all()


@router.get("/dimensions/{name}/nodes/", response_model=List[NodeRevisionOutput])
def find_nodes_with_dimension(
    name: str,
    *,
    node_type: Annotated[Union[List[NodeType], None], Query()] = Query(None),
    session: Session = Depends(get_session),
) -> List[NodeRevisionOutput]:
    """
    List all nodes that have the specified dimension
    """
    dimension_node = get_node_by_name(session, name)
    nodes = get_nodes_with_dimension(session, dimension_node, node_type)
    return nodes


@router.get("/dimensions/common/", response_model=List[NodeRevisionOutput])
def find_nodes_with_common_dimensions(
    dimension: Annotated[Union[List[str], None], Query()] = Query(None),
    node_type: Annotated[Union[List[NodeType], None], Query()] = Query(None),
    *,
    session: Session = Depends(get_session),
) -> List[NodeRevisionOutput]:
    """
    Find all nodes that have the list of common dimensions
    """
    nodes = get_nodes_with_common_dimensions(
        session,
        [get_node_by_name(session, dim) for dim in dimension],  # type: ignore
        node_type,
    )
    return nodes
