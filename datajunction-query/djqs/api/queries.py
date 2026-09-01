"""
Query related APIs.
"""

import json
import logging
import uuid
from dataclasses import asdict, replace
from http import HTTPStatus
from typing import Any

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

from djqs.config import Settings
from djqs.db.postgres import DBQuery, get_postgres_pool
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
from djqs.result_cache import (
    CachedQueryResult,
    build_result_cache_key,
    get_cached_result,
    has_stale_while_revalidate,
    parse_cache_control,
)
from djqs.utils import get_settings

_logger = logging.getLogger(__name__)
router = APIRouter(tags=["SQL Queries"])
_pending_refresh_keys: set[str] = set()


async def _process_refresh(
    settings: Settings,
    postgres_pool: AsyncConnectionPool,
    query: Query,
    headers: dict[str, str] | None,
    result_cache_key: str | None,
    result_cache_timeout: int | None,
    refresh_key: str,
) -> QueryResults:
    """Run a result refresh and always release its in-process lease."""
    try:
        return await process_query(
            settings=settings,
            postgres_pool=postgres_pool,
            query=query,
            headers=headers,
            result_cache_key=result_cache_key,
            result_cache_timeout=result_cache_timeout,
        )
    finally:
        _pending_refresh_keys.discard(refresh_key)


async def get_cached_or_schedule_refresh(
    create_query: QueryCreate,
    settings: Settings,
    response: Response,
    background_tasks: BackgroundTasks,
    postgres_pool: AsyncConnectionPool,
    request: Request,
) -> QueryResults | None:
    """Return an SWR result or schedule its refresh when the caller opted in."""
    cache_control = request.headers.get("cache-control")
    if not has_stale_while_revalidate(cache_control):
        return None

    no_cache, no_store, retention = parse_cache_control(cache_control)
    if no_cache:
        return None

    cache_key = build_result_cache_key(create_query, dict(request.headers))
    cached_result, is_fresh = get_cached_result(settings.results_backend, cache_key)
    if (
        cached_result is not None
        and not is_fresh
        and not no_store
        and cache_key not in _pending_refresh_keys
    ):
        _pending_refresh_keys.add(cache_key)
        try:
            await save_query_and_run(
                create_query=replace(create_query, async_=True),
                settings=settings,
                response=response,
                background_tasks=background_tasks,
                postgres_pool=postgres_pool,
                headers=request.headers,
                result_cache_key=cache_key,
                result_cache_timeout=retention,
                refresh_key=cache_key,
            )
        except Exception:
            _pending_refresh_keys.discard(cache_key)
            raise
        response.status_code = HTTPStatus.OK
    return cached_result


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
    accept: str | None = Header(None),
    *,
    settings: Settings = Depends(get_settings),
    request: Request,
    response: Response,
    postgres_pool: AsyncConnectionPool = Depends(get_postgres_pool),
    background_tasks: BackgroundTasks,
    body: Any = Body(
        ...,
        example={
            "catalog_name": "warehouse",
            "engine_name": "trino",
            "engine_version": "451",
            "submitted_query": "select * from tpch.sf1.customer limit 10",
        },
    ),
) -> QueryResults:
    """
    Run or schedule a query.

    This endpoint is different from others in that it accepts both JSON and msgpack, and
    can also return JSON or msgpack, depending on HTTP headers.

    Request cache policy:
      - ``Cache-Control: stale-while-revalidate`` opts into reusable completed-result
        caching. Fresh results are returned immediately; retained stale results are
        returned while an asynchronous refresh runs.
      - ``max-age=N`` overrides result retention in seconds (up to one week) without
        changing the 12-hour freshness window.
      - ``no-cache`` bypasses reusable results and ``no-store`` prevents writing or
        refreshing them.
    """
    content_type = request.headers.get("content-type")
    if content_type == "application/json":
        data = body
    elif content_type == "application/msgpack":
        data = json.loads(msgpack.unpackb(body, ext_hook=decode_results))
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

    # Set default catalog and engine if not explicitly specified in submitted query
    if not data.get("engine_name") and not data.get("engine_version"):
        data["engine_name"] = settings.default_engine
        data["engine_version"] = settings.default_engine_version
    else:
        data["engine_name"] = data.get("engine_name") or settings.default_engine
        data["engine_version"] = data.get("engine_version") or ""
    data["catalog_name"] = data.get("catalog_name") or settings.default_catalog

    create_query = QueryCreate(**data)
    query_with_results = await get_cached_or_schedule_refresh(
        create_query,
        settings,
        response,
        background_tasks,
        postgres_pool,
        request,
    )
    if query_with_results is None:
        use_result_cache = has_stale_while_revalidate(
            request.headers.get("cache-control"),
        )
        no_store = False
        retention = None
        cache_key = None
        if use_result_cache:
            _, no_store, retention = parse_cache_control(
                request.headers.get("cache-control"),
            )
            cache_key = build_result_cache_key(create_query, dict(request.headers))
        query_with_results = await save_query_and_run(
            create_query=create_query,
            settings=settings,
            response=response,
            background_tasks=background_tasks,
            postgres_pool=postgres_pool,
            headers=request.headers,
            result_cache_key=None if no_store else cache_key,
            result_cache_timeout=retention,
        )

    return_type = get_best_match(accept, ["application/json", "application/msgpack"])
    if not return_type:
        raise HTTPException(
            status_code=HTTPStatus.NOT_ACCEPTABLE,
            detail="Client MUST accept: application/json, application/msgpack",
        )

    if return_type == "application/msgpack":
        content = msgpack.packb(
            asdict(query_with_results),
            default=encode_results,
        )
    else:
        content = json.dumps(asdict(query_with_results), default=str)

    return Response(
        content=content,
        media_type=return_type,
        status_code=response.status_code or HTTPStatus.OK,
    )


async def save_query_and_run(  # pylint: disable=R0913
    create_query: QueryCreate,
    settings: Settings,
    response: Response,
    background_tasks: BackgroundTasks,
    postgres_pool: AsyncConnectionPool,
    headers: dict[str, str] | None = None,
    result_cache_key: str | None = None,
    result_cache_timeout: int | None = None,
    refresh_key: str | None = None,
) -> QueryResults:
    """
    Store a new query to the DB and run it.
    """
    query = Query(
        catalog_name=create_query.catalog_name,  # type: ignore
        engine_name=create_query.engine_name,  # type: ignore
        engine_version=create_query.engine_version,  # type: ignore
        submitted_query=create_query.submitted_query,
        async_=create_query.async_,
    )
    query.state = QueryState.ACCEPTED

    async with postgres_pool.connection() as conn:
        results = (
            await DBQuery()
            .save_query(
                query_id=query.id,
                catalog_name=query.catalog_name,
                engine_name=query.engine_name,
                engine_version=query.engine_version,
                submitted_query=query.submitted_query,
                async_=query.async_,
                state=query.state.value,
            )
            .execute(conn=conn)
        )
        query_save_result = results[0]
        if not query_save_result:  # pragma: no cover
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail="Query failed to save",
            )

        if query.async_:
            if refresh_key:
                background_tasks.add_task(
                    _process_refresh,
                    settings,
                    postgres_pool,
                    query,
                    headers,
                    result_cache_key,
                    result_cache_timeout,
                    refresh_key,
                )
            else:
                background_tasks.add_task(
                    process_query,
                    settings,
                    postgres_pool,
                    query,
                    headers,
                    result_cache_key,
                    result_cache_timeout,
                )

            response.status_code = HTTPStatus.CREATED
            return QueryResults(
                id=query.id,
                catalog_name=query.catalog_name,
                engine_name=query.engine_name,
                engine_version=query.engine_version,
                submitted_query=query.submitted_query,
                executed_query=query.executed_query,
                state=QueryState.SCHEDULED,
                results=[],
                errors=[],
            )

    query_results = await process_query(
        settings=settings,
        postgres_pool=postgres_pool,
        query=query,
        headers=headers,
        result_cache_key=result_cache_key,
        result_cache_timeout=result_cache_timeout,
        refresh_key=refresh_key,
    )
    return query_results


def load_query_results(
    settings: Settings,
    key: str,
) -> list[StatementResults]:
    """
    Load results from backend, if available.

    If ``paginate`` is true we also load the results into the cache, anticipating more
    paginated queries.
    """
    if settings.results_backend.has(key):
        _logger.info("Reading results from results backend")
        cached = settings.results_backend.get(key)
        query_results = json.loads(cached)
    else:  # pragma: no cover
        _logger.warning("No results found")
        query_results = []

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
        dbquery_results = (
            await DBQuery().get_query(query_id=query_id).execute(conn=conn)
        )
        queries = dbquery_results[0]
        if not queries:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail="Query not found",
            )
        query = queries[0]

    query_results = load_query_results(settings, str(query_id))

    prev = next_ = None

    return QueryResults(
        results=query_results,
        next=next_,
        previous=prev,
        errors=[],
        **query,
    )
