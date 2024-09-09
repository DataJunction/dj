"""
Query related APIs.
"""
import asyncio
import json
import logging
import uuid
from http import HTTPStatus
from typing import Any, Dict, List, Optional

import msgpack
from accept_types import get_best_match
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    Header,
    HTTPException,
    Request,
    Response,
)
from psycopg_pool import AsyncConnectionPool

from djqs.db.postgres import DBQuery, get_postgres_pool
from djqs.config import Settings
from djqs.engine import process_query
from djqs.models.query import (
    Query,
    QueryCreate,
    QueryResults,
    QueryState,
    StatementResults,
    decode_results,
    encode_results,
)
from djqs.utils import get_settings

_logger = logging.getLogger(__name__)
router = APIRouter(tags=["SQL Queries"])


@router.post(
    "/queries/",
    response_model=QueryResults,
    status_code=HTTPStatus.OK,
    responses={
        200: {
            "content": {"application/msgpack": {}},
            "description": "Return results as JSON or msgpack",
        },
    },
)
async def submit_query(  # pylint: disable=too-many-arguments
    accept: Optional[str] = Header(None),
    *,
    settings: Settings = Depends(get_settings),
    request: Request,
    response: Response,
    postgres_pool: AsyncConnectionPool = Depends(get_postgres_pool),
    background_tasks: BackgroundTasks,
    body: Any = Body(...),
) -> QueryResults:
    """
    Run or schedule a query.

    This endpoint is different from others in that it accepts both JSON and msgpack, and
    can also return JSON or msgpack, depending on HTTP headers.
    """
    content_type = request.headers.get("content-type")
    if content_type == "application/json":
        data = body
    elif content_type == "application/msgpack":
        data = msgpack.unpackb(body, ext_hook=decode_results)
    elif content_type is None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail="Content type must be specified",
        )
    else:
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=f"Content type not accepted: {content_type}",
        )
    create_query = QueryCreate(**data)


    query_with_results = await save_query_and_run(
        create_query=create_query,
        settings=settings,
        response=response,
        background_tasks=background_tasks,
        postgres_pool=postgres_pool,
        headers=request.headers,
    )

    return query_with_results


async def save_query_and_run(  # pylint: disable=R0913
    create_query: QueryCreate,
    settings: Settings,
    response: Response,
    background_tasks: BackgroundTasks,
    postgres_pool: AsyncConnectionPool,
    headers: Optional[Dict[str, str]] = None,
) -> QueryResults:
    """
    Store a new query to the DB and run it.
    """
    query = Query(
        submitted_query=create_query.submitted_query,
        async_=create_query.async_,
    )
    query.state = QueryState.ACCEPTED

    async with postgres_pool.connection() as conn:
        results = await DBQuery().save_query(
            query_id=query.id,
            submitted_query=query.submitted_query,
            async_=query.async_
        ).execute(conn=conn)
        query_save_result = results[0]
        if not query_save_result:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Query failed to save")

        if query.async_:
            background_tasks.add_task(asyncio.create_task, process_query(settings=settings, postgres_pool=postgres_pool, query=query, headers=headers))

            response.status_code = HTTPStatus.CREATED
            return QueryResults(results=[], errors=[], **query.dict())

    query_results = await process_query(settings=settings, postgres_pool=postgres_pool, query=query, headers=headers)
    return query_results

@router.get("/queries/{query_id}/", response_model=QueryResults)
async def read_query(
    query_id: uuid.UUID,
    *,
    settings: Settings = Depends(get_settings),
    postgres_pool: AsyncConnectionPool = Depends(get_postgres_pool),
) -> QueryResults:
    """
    Fetch information about a query.

    For paginated queries we move the data from the results backend to the cache for a
    short period, anticipating additional requests.
    """
    async with postgres_pool.connection() as conn:
        results = await DBQuery().get_query(query_id=query_id).execute(conn=conn)
        queries = results[0]
        if not queries:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Query not found")
        else:
            query = queries[0]

    prev = next_ = None

    return QueryResults(
        results=results, next=next_, previous=prev, errors=[], **query.dict()
    )
