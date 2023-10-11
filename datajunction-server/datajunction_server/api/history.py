"""
History related APIs.
"""

import logging
from typing import List

from fastapi import Depends, Query
from sqlmodel import Session, select

from datajunction_server.api.helpers import get_history
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.history import EntityType, History
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["history"])


@router.get("/history/{entity_type}/{entity_name}/", response_model=List[History])
def list_history(
    entity_type: EntityType,
    entity_name: str,
    offset: int = 0,
    limit: int = Query(default=100, lte=100),
    *,
    session: Session = Depends(get_session),
) -> List[History]:
    """
    List history for an entity type (i.e. Node) and entity name
    """
    hist = get_history(
        session=session,
        entity_name=entity_name,
        entity_type=entity_type,
        offset=offset,
        limit=limit,
    )
    return hist


@router.get("/history/", response_model=List[History])
def list_history_by_node_context(
    node: str,
    offset: int = 0,
    limit: int = Query(default=100, lte=100),
    *,
    session: Session = Depends(get_session),
) -> List[History]:
    """
    List all activity history for a node context
    """
    hist = session.exec(
        select(History)
        .where(History.node == node)
        .order_by(History.created_at)
        .offset(offset)
        .limit(limit),
    ).all()
    return hist
