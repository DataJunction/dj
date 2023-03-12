"""
DJ Query related APIs.
"""


from fastapi import APIRouter, Depends
from sqlmodel import Session

from dj.api.helpers import get_dj_query
from dj.api.metrics import TranslatedSQL
from dj.utils import get_session

router = APIRouter()


@router.get("/query/{sql}", response_model=TranslatedSQL)
def read_metrics_sql(
    sql: str,
    *,
    session: Session = Depends(get_session),
) -> TranslatedSQL:
    """
    Return SQL for a DJ Query.

    A database can be optionally specified. If no database is specified the optimal one
    will be used.
    """
    query_ast = get_dj_query(
        session=session,
        query=sql,
    )
    return TranslatedSQL(
        sql=str(query_ast),
    )
