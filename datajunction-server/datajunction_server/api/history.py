"""
History related APIs.
"""

import logging
from typing import List, Optional

from fastapi import Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy import and_, cast, func, String

from datajunction_server.api.helpers import get_history
from datajunction_server.database.history import History
from datajunction_server.database.notification_preference import NotificationPreference
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.history import EntityType
from datajunction_server.models.history import HistoryOutput
from datajunction_server.utils import (
    get_current_user,
    get_session,
    get_settings,
)

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
    node: Optional[str] = None,
    only_subscribed: bool = False,
    offset: int = 0,
    limit: int = Query(default=100, lte=100),
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> List[HistoryOutput]:
    """
    List all activity history for a node context
    """
    statement = select(History)
    if node:
        statement = statement.where(History.node == node)
    if only_subscribed:
        np_alias = aliased(NotificationPreference)
        statement = statement.join(
            np_alias,
            and_(
                cast(History.entity_type, String) == cast(np_alias.entity_type, String),
                History.entity_name == np_alias.entity_name,
                cast(History.activity_type, String).in_(
                    func.array_to_string(np_alias.activity_types, ","),
                ),
                np_alias.user_id == current_user.id,
            ),
        )

    statement = (
        statement.order_by(History.created_at.desc()).offset(offset).limit(limit)
    )
    result = await session.execute(statement)
    hist = result.scalars().all()
    return [HistoryOutput.from_orm(entry) for entry in hist]
