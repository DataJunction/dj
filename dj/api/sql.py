"""
SQL related APIs.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session

from dj.api.helpers import get_engine, get_query
from dj.models.metric import TranslatedSQL
from dj.models.query import ColumnMetadata
from dj.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/sql/{node_name}/", response_model=TranslatedSQL)
def get_sql(
    node_name: str,
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    *,
    session: Session = Depends(get_session),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
) -> TranslatedSQL:
    """
    Return SQL for a node.
    """
    engine = (
        get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else None
    )
    query_ast = get_query(
        session=session,
        node_name=node_name,
        dimensions=dimensions,
        filters=filters,
        engine=engine,
    )
    columns = [
        ColumnMetadata(name=col.alias_or_name.name, type=str(col.type))  # type: ignore
        for col in query_ast.select.projection
    ]
    return TranslatedSQL(
        sql=str(query_ast),
        columns=columns,
        dialect=engine.dialect if engine else None,
    )
