"""
DJ Query related APIs.
"""


from fastapi import APIRouter, Depends
from sqlmodel import Session

from datajunction_server.api.helpers import get_dj_query
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.utils import get_session

router = APIRouter(tags=["query"])


@router.get("/query/{sql}", response_model=TranslatedSQL)
def build_a_dj_query(
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
