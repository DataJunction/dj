"""
History related APIs.
"""

import logging
from typing import List

from fastapi import Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_history
from datajunction_server.database.history import EntityType, History
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.history import HistoryOutput
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["history"])


@router.get("/history/{entity_type}/{entity_name}/", response_model=List[HistoryOutput])
async def list_history(
    entity_type: EntityType,
    entity_name: str,
    offset: int = 0,
    limit: int = Query(default=100, lte=100),
    *,
    session: AsyncSession = Depends(get_session),
) -> List[HistoryOutput]:
    """
    List history for an entity type (i.e. Node) and entity name
    """
    hist = await get_history(
        session=session,
        entity_name=entity_name,
        entity_type=entity_type,
        offset=offset,
        limit=limit,
    )
    return [HistoryOutput.from_orm(entry) for entry in hist]


@router.get("/history/", response_model=List[HistoryOutput])
async def list_history_by_node_context(
    node: str,
    offset: int = 0,
    limit: int = Query(default=100, lte=100),
    *,
    session: AsyncSession = Depends(get_session),
) -> List[HistoryOutput]:
    """
    List all activity history for a node context
    """
    hist = (
        (
            await session.execute(
                select(History)
                .where(History.node == node)
                .order_by(History.created_at.desc())
                .offset(offset)
                .limit(limit),
            )
        )
        .scalars()
        .all()
    )
    return [HistoryOutput.from_orm(entry) for entry in hist]
