"""
Impact preview API endpoints.

Read-only endpoints for previewing the downstream blast radius of
proposed node changes before committing them.
"""

import logging

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.impact import compute_impact
from datajunction_server.models.impact_preview import (
    NodeChange,
    NodeDiff,
    SingleNodePreviewResponse,
)
from datajunction_server.sql.dag import _node_output_options
from datajunction_server.utils import get_session

_logger = logging.getLogger(__name__)
router = SecureAPIRouter(tags=["impact-preview"])


@router.post(
    "/nodes/{node_name}/impact-preview",
    response_model=SingleNodePreviewResponse,
    name="Preview downstream impact of a proposed node change",
)
async def single_node_impact_preview(
    node_name: str,
    body: NodeChange,
    *,
    session: AsyncSession = Depends(get_session),
) -> SingleNodePreviewResponse:
    """
    Preview the downstream blast radius of a proposed change to a single node.

    The request body is a ``NodeChange`` describing what would change: columns
    added/removed/type-changed, dimension links added/removed, or whether the
    node would be deleted entirely.

    This endpoint is **read-only** — no changes are written to the database.
    """
    node = await Node.get_by_name(session, node_name, options=_node_output_options())
    if not node:
        raise DJDoesNotExistException(
            message=f"Node '{node_name}' does not exist.",
            http_status_code=404,
        )

    node_diff = NodeDiff(
        name=node.name,
        node_type=node.type,
        change_type="deleted" if body.is_deleted else "modified",
        diff=body,
    )

    downstream = await compute_impact(session, {node_name: body})

    return SingleNodePreviewResponse(
        node_diff=node_diff,
        downstream_impact=downstream,
    )
