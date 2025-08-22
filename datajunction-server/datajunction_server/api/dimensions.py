"""
Dimensions related APIs.
"""

import logging
from typing import List, Optional

from fastapi import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.api.nodes import list_nodes
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    validate_access,
    validate_access_requests,
)
from datajunction_server.models import access
from datajunction_server.models.node import NodeIndegreeOutput, NodeRevisionOutput
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import (
    get_dimension_dag_indegree,
    get_nodes_with_common_dimensions,
    get_nodes_with_dimension,
)
from datajunction_server.utils import (
    get_current_user,
    get_session,
    get_settings,
)

settings = get_settings()
_logger = logging.getLogger(__name__)
router = SecureAPIRouter(tags=["dimensions"])


@router.get("/dimensions/", response_model=List[NodeIndegreeOutput])
async def list_dimensions(
    prefix: Optional[str] = None,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
) -> List[NodeIndegreeOutput]:
    """
    List all available dimensions.
    """
    node_names = await list_nodes(
        node_type=NodeType.DIMENSION,
        prefix=prefix,
        session=session,
        current_user=current_user,
        validate_access=validate_access,
    )
    node_indegrees = await get_dimension_dag_indegree(session, node_names)
    return sorted(
        [
            NodeIndegreeOutput(name=node, indegree=node_indegrees[node])
            for node in node_names
        ],
        key=lambda n: -n.indegree,
    )


@router.get("/dimensions/{name}/nodes/", response_model=List[NodeRevisionOutput])
async def find_nodes_with_dimension(
    name: str,
    *,
    node_type: List[NodeType] = Query([]),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
) -> List[NodeRevisionOutput]:
    """
    List all nodes that have the specified dimension
    """
    dimension_node = await Node.get_by_name(session, name)
    nodes = await get_nodes_with_dimension(session, dimension_node, node_type)  # type: ignore
    resource_requests = [
        access.ResourceRequest(
            verb=access.ResourceRequestVerb.READ,
            access_object=access.Resource.from_node(node),
        )
        for node in nodes
    ]
    approvals = validate_access_requests(
        validate_access=validate_access,
        user=current_user,
        resource_requests=resource_requests,
    )

    approved_nodes: List[str] = [request.access_object.name for request in approvals]
    return [node for node in nodes if node.name in approved_nodes]


@router.get("/dimensions/common/", response_model=List[NodeRevisionOutput])
async def find_nodes_with_common_dimensions(
    dimension: List[str] = Query([]),
    node_type: List[NodeType] = Query([]),
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
) -> List[NodeRevisionOutput]:
    """
    Find all nodes that have the list of common dimensions
    """
    nodes = await get_nodes_with_common_dimensions(
        session,
        [await get_node_by_name(session, dim) for dim in dimension],  # type: ignore
        node_type,
    )
    approvals = [
        approval.access_object.name
        for approval in validate_access_requests(
            validate_access,
            current_user,
            [
                access.ResourceRequest(
                    verb=access.ResourceRequestVerb.READ,
                    access_object=access.Resource.from_node(node),
                )
                for node in nodes
            ],
        )
    ]
    return [node for node in nodes if node.name in approvals]
