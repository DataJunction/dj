"""
Data related APIs.
"""

from typing import Optional

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

from datajunction_server.api.helpers import build_sql_for_dj_query, query_event_stream
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import (
    get_current_user,
    get_query_service_client,
    get_session,
    get_settings,
)

settings = get_settings()
router = SecureAPIRouter(tags=["DJSQL"])


@router.get("/djsql/data", response_model=QueryWithResults)
async def get_data_for_djsql(
    query: str,
    async_: bool = False,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
) -> QueryWithResults:
    """
    Return data for a DJ SQL query
    """
    request_headers = dict(request.headers)
    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=current_user,
        base_verb=access.ResourceRequestVerb.EXECUTE,
    )
    translated_sql, engine, catalog = await build_sql_for_dj_query(
        session,
        query,
        access_control,
        engine_name,
        engine_version,
    )

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=catalog.name,
        engine_version=engine.version,
        submitted_query=translated_sql.sql,
        async_=async_,
    )

    result = query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )

    # Inject column info if there are results
    if result.results.__root__:  # pragma: no cover
        result.results.__root__[0].columns = translated_sql.columns or []
    return result


@router.get("/djsql/stream/", response_model=QueryWithResults)
async def get_data_stream_for_djsql(
    query: str,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
) -> QueryWithResults:  # pragma: no cover
    """
    Return data for a DJ SQL query using server side events
    """
    request_headers = dict(request.headers)
    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=current_user,
        base_verb=access.ResourceRequestVerb.EXECUTE,
    )
    translated_sql, engine, catalog = await build_sql_for_dj_query(
        session,
        query,
        access_control,
        engine_name,
        engine_version,
    )

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=catalog.name,
        engine_version=engine.version,
        submitted_query=translated_sql.sql,
        async_=True,
    )

    # Submits the query, equivalent to calling POST /data/ directly
    initial_query_info = query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )
    return EventSourceResponse(
        query_event_stream(
            query=initial_query_info,
            request_headers=request_headers,
            query_service_client=query_service_client,
            columns=translated_sql.columns,  # type: ignore
            request=request,
        ),
    )
