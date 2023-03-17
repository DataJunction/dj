"""
SQL related APIs.
"""

import logging
from typing import List

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session

from dj.api.helpers import get_query
from dj.models.metric import TranslatedSQL
from dj.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/sql/{node_name}/", response_model=TranslatedSQL)
def get_sql_for_node(
    node_name: str,
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    *,
    session: Session = Depends(get_session),
) -> TranslatedSQL:
    """
    Return SQL for a node.
    """
    query_ast = get_query(
        session=session,
        node_name=node_name,
        dimensions=dimensions,
        filters=filters,
    )
    return TranslatedSQL(
        sql=str(query_ast),
    )
