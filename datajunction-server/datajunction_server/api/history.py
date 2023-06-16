"""
History related APIs.
"""

import logging
from typing import List

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session

from datajunction_server.api.helpers import get_history
from datajunction_server.models.history import EntityType, History
from datajunction_server.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/history/{entity_type}/{entity_name}/", response_model=List[History])
def list_history(
    entity_type: EntityType,
    entity_name: str,
    offset: int = 0,
    limit: int = Query(default=100, lte=100),
    *,
    session: Session = Depends(get_session)
) -> List[History]:
    """
    List history for an entity type (i.e. Node) and entity name
    """
    return get_history(
        session=session,
        entity_name=entity_name,
        entity_type=entity_type,
        offset=offset,
        limit=limit,
    )
