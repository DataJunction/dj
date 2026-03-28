"""
Impact preview API endpoints.

Read-only endpoints for previewing the downstream blast radius of
proposed node changes before committing them.
"""

import json
import logging
from collections.abc import AsyncGenerator

from fastapi import Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.impact import _ExplicitDiffSpec, compute_impact
from datajunction_server.internal.validation import validate_node_data
from datajunction_server.models.impact_preview import (
    NodeChange,
    NodeDiff,
)
from datajunction_server.models.node import NodeRevisionBase, NodeStatus
from datajunction_server.sql.dag import _node_output_options
from datajunction_server.utils import get_session

_logger = logging.getLogger(__name__)
router = SecureAPIRouter(tags=["impact-preview"])


async def _stream_impact_preview(
    node: Node,
    body: NodeChange,
    session: AsyncSession,
) -> AsyncGenerator[str, None]:
    """Async generator that yields NDJSON lines for the impact preview stream.

    First line: ``{"type": "node_diff", "data": {...}}``
    Subsequent lines: ``{"type": "impact", "data": {...}}`` — one per impacted node,
    emitted in BFS level order (direct casualties first).
    """
    # Compute projected validity of the changed node itself
    projected_status: NodeStatus | None
    if body.is_deleted:
        projected_status = None
    elif body.new_query:
        stub = NodeRevisionBase(name=node.name, type=node.type, query=body.new_query)
        try:
            v = await validate_node_data(stub, session)
            projected_status = v.status
        except Exception:
            projected_status = NodeStatus.INVALID
    else:
        projected_status = node.current.status

    node_diff = NodeDiff(
        name=node.name,
        node_type=node.type,
        change_type="deleted" if body.is_deleted else "modified",
        diff=body,
        projected_status=projected_status,
    )
    yield (
        json.dumps({"type": "node_diff", "data": node_diff.model_dump(mode="json")})
        + "\n"
    )

    diff_spec = _ExplicitDiffSpec(
        node_type=node.type,
        new_query=body.new_query,
        explicit_columns_removed=set(body.columns_removed),
        explicit_columns_changed=list(body.columns_changed),
        dim_links_removed=set(body.dim_links_removed),
    )
    async for impacted_node in compute_impact(
        session,
        {node.name: (diff_spec if not body.is_deleted else None, node, set())},
    ):
        yield (
            json.dumps(
                {"type": "impact", "data": impacted_node.model_dump(mode="json")},
            )
            + "\n"
        )


@router.post(
    "/nodes/{node_name}/impact-preview",
    name="Preview downstream impact of a proposed node change",
)
async def single_node_impact_preview(
    node_name: str,
    body: NodeChange,
    *,
    session: AsyncSession = Depends(get_session),
) -> StreamingResponse:
    """
    Preview the downstream blast radius of a proposed change to a single node.

    The request body is a ``NodeChange`` describing what would change: columns
    added/removed/type-changed, dimension links added/removed, or whether the
    node would be deleted entirely.

    Returns an NDJSON stream (``application/x-ndjson``).  The first line is the
    node diff (``"type": "node_diff"``); subsequent lines are impacted downstream
    nodes (``"type": "impact"``), emitted in BFS level order as they are discovered.

    This endpoint is **read-only** — no changes are written to the database.
    """
    node = await Node.get_by_name(session, node_name, options=_node_output_options())
    if not node:
        raise DJDoesNotExistException(
            message=f"Node '{node_name}' does not exist.",
            http_status_code=404,
        )

    return StreamingResponse(
        _stream_impact_preview(node, body, session),
        media_type="application/x-ndjson",
    )
