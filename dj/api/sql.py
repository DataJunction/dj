"""
Metric related APIs.
"""

from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Response
from sqlmodel import Session

from dj.api.helpers import get_dj_query
from dj.api.metrics import TranslatedSQL
from dj.api.queries import save_query_and_run
from dj.config import Settings
from dj.models.query import QueryCreate, QueryWithResults
from dj.utils import get_session, get_settings

router = APIRouter()


@router.get("/sql/data/", response_model=QueryWithResults)
async def read_sql_data(
    query: str,
    database_name: Optional[str] = None,
    *,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    response: Response,
    background_tasks: BackgroundTasks,
) -> QueryWithResults:
    """
    Return data for a DJ Query.
    """
    query_ast, optimal_database = await get_dj_query(
        session=session,
        query=query,
        database_name=database_name,
    )
    create_query = QueryCreate(
        submitted_query=str(query_ast),
        database_id=optimal_database.id,
    )

    return save_query_and_run(
        create_query,
        session,
        settings,
        response,
        background_tasks,
    )


@router.get("/sql/", response_model=TranslatedSQL)
async def read_metrics_sql(
    query: str,
    database_name: Optional[str] = None,
    *,
    session: Session = Depends(get_session),
) -> TranslatedSQL:
    """
    Return SQL for a DJ Query.

    A database can be optionally specified. If no database is specified the optimal one
    will be used.
    """
    query_ast, optimal_database = await get_dj_query(
        session=session,
        query=query,
        database_name=database_name,
    )
    return TranslatedSQL(
        database_id=optimal_database.id,
        sql=str(query_ast),
    )
