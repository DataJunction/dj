"""
Metric related APIs.
"""

from typing import Optional

from fastapi import APIRouter, Depends
from sqlmodel import Session, SQLModel

from dj.api.helpers import get_dj_query
from dj.api.metrics import TranslatedSQL
from dj.utils import get_session

router = APIRouter()


class DJSQL(SQLModel):
    """
    Class for DJ SQL request.
    """

    database_name: Optional[str]
    sql: str


@router.get("/query/validate", response_model=TranslatedSQL)
async def read_metrics_sql(
    dj_sql: DJSQL,
    *,
    session: Session = Depends(get_session),
) -> TranslatedSQL:
    """
    Return SQL for a DJ Query.

    A database can be optionally specified. If no database is specified the optimal one
    will be used.
    """

    query, database_name = dj_sql.sql, dj_sql.database_name
    query_ast, optimal_database = await get_dj_query(
        session=session,
        query=query,
        database_name=database_name,
    )
    return TranslatedSQL(
        database_id=optimal_database.id,
        sql=str(query_ast),
    )
