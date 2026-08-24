"""
Dimensions related APIs.
"""

import logging
from typing import cast

from fastapi import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only

from datajunction_server.api.nodes import list_nodes
from datajunction_server.database.node import Node
from datajunction_server.errors import DJNodeNotFound
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    AccessChecker,
    get_access_checker,
)
from datajunction_server.models import access
from datajunction_server.models.node import NodeIndegreeOutput, NodeNameOutput
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import (
    get_dimension_dag_indegree,
    get_nodes_with_common_dimensions,
)
from datajunction_server.utils import (
    get_session,
    get_settings,
)

settings = get_settings()
_logger = logging.getLogger(__name__)
router = SecureAPIRouter(tags=["dimensions"])


@router.get("/dimensions/", response_model=list[NodeIndegreeOutput])
async def list_dimensions(
    prefix: str | None = None,
    limit: int | None = None,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> list[NodeIndegreeOutput]:
    """
    List available dimensions, most useful first.

    Dimensions on a repo's default branch come before those on feature branches,
    and within each group the most linked-to dimensions come first. `limit` caps
    how many are returned; omit it for the full list.
    """
    node_names = await list_nodes(
        node_type=NodeType.DIMENSION,
        prefix=prefix,
        session=session,
        access_checker=access_checker,
    )
    node_indegrees = await get_dimension_dag_indegree(session, node_names)
    main_branch = await Node.main_branch_names(session, node_names)
    ranked = sorted(
        [
            NodeIndegreeOutput(name=node, indegree=node_indegrees[node])
            for node in node_names
        ],
        key=lambda n: (n.name not in main_branch, -n.indegree),
    )
    return ranked[:limit] if limit is not None else ranked


@router.get("/dimensions/{name}/nodes/", response_model=list[NodeNameOutput])
async def find_nodes_with_dimension(
    name: str,
    *,
    node_type: list[NodeType] = Query([]),
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> list[NodeNameOutput]:
    """
    List all nodes that have the specified dimension
    """
    dimension_node = cast(
        Node,
        await Node.get_by_name(
            session,
            name,
            options=[load_only(Node.id, Node.name)],
            raise_if_not_exists=True,
        ),
    )
    access_checker.add_node(dimension_node, access.ResourceAction.READ)

    nodes = await get_nodes_with_common_dimensions(
        session,
        [dimension_node],
        node_type if node_type else None,
    )
    access_checker.add_nodes(nodes, access.ResourceAction.READ)
    approved_nodes = await access_checker.approved_resource_names()
    return [node for node in nodes if node.name in approved_nodes]


@router.get("/dimensions/common/", response_model=list[NodeNameOutput])
async def find_nodes_with_common_dimensions(
    dimension: list[str] = Query([]),
    node_type: list[NodeType] = Query([]),
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> list[NodeNameOutput]:
    """
    Find all nodes that have the list of common dimensions
    """
    dimension_nodes = await Node.get_by_names(
        session,
        dimension,
        options=[load_only(Node.id, Node.name)],
    )
    found_names = {node.name for node in dimension_nodes}
    missing = [dim for dim in dimension if dim not in found_names]
    if missing:
        raise DJNodeNotFound(
            message=f"A node with name `{missing[0]}` does not exist.",
            http_status_code=404,
        )
    nodes = await get_nodes_with_common_dimensions(
        session,
        dimension_nodes,
        node_type,
    )
    access_checker.add_nodes(nodes, access.ResourceAction.READ)
    approved_resource_names = await access_checker.approved_resource_names()
    return [node for node in nodes if node.name in approved_resource_names]
