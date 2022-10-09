"""
Query related APIs.
"""

import json
import logging
import urllib.parse
import uuid
from http import HTTPStatus
from typing import Any, List, Optional

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
from sqlmodel import Session

from dj.config import Settings
from dj.constants import DJ_DATABASE_ID
from dj.engine import process_query
from dj.models.query import (
    Query,
    QueryCreate,
    QueryResults,
    QueryState,
    QueryWithResults,
    StatementResults,
    decode_results,
    encode_results,
)
from dj.sql.build import get_query_for_sql
from dj.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
router = APIRouter()
celery = get_settings().celery  # pylint: disable=invalid-name


@router.post(
    "/queries/",
    response_model=QueryWithResults,
    status_code=HTTPStatus.OK,
    responses={
        200: {
            "content": {"application/msgpack": {}},
            "description": "Return results as JSON or msgpack",
        },
    },
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "schema": QueryCreate.schema(
                        ref_template="#/components/schemas/{model}",
                    ),
                },
                "application/msgpack": {
                    "schema": QueryCreate.schema(
                        ref_template="#/components/schemas/{model}",
                    ),
                },
            },
        },
    },
)
async def submit_query(
    accept: Optional[str] = Header(None),
    *,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    request: Request,
    response: Response,
    background_tasks: BackgroundTasks,
    body: Any = Body(...),
) -> QueryWithResults:
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

    if create_query.database_id == DJ_DATABASE_ID:
        create_query = await get_query_for_sql(create_query.submitted_query)

    query_with_results = save_query_and_run(
        create_query,
        session,
        settings,
        response,
        background_tasks,
    )

    return_type = get_best_match(accept, ["application/json", "application/msgpack"])
    if not return_type:
        raise HTTPException(
            status_code=HTTPStatus.NOT_ACCEPTABLE,
            detail="Client MUST accept: application/json, application/msgpack",
        )

    if return_type == "application/msgpack":
        content = msgpack.packb(
            query_with_results.dict(by_alias=True),
            default=encode_results,
        )
    else:
        content = query_with_results.json(by_alias=True)

    return Response(
        content=content,
        media_type=return_type,
        status_code=response.status_code or HTTPStatus.OK,
    )


def save_query_and_run(
    create_query: QueryCreate,
    session: Session,
    settings: Settings,
    response: Response,
    background_tasks: BackgroundTasks,
) -> QueryWithResults:
    """
    Store a new query to the DB and run it.
    """
    query = Query(**create_query.dict(by_alias=True))
    query.state = QueryState.ACCEPTED

    session.add(query)
    session.commit()
    session.refresh(query)

    if query.database.async_:  # pylint: disable=no-member
        if settings.celery_broker:
            dispatch_query.delay(query.id)
        else:
            background_tasks.add_task(process_query, session, settings, query)

        response.status_code = HTTPStatus.CREATED
        return QueryWithResults(results=[], errors=[], **query.dict())

    return process_query(session, settings, query)


@celery.task
def dispatch_query(query_id: uuid.UUID) -> None:
    """
    Celery task for processing a query.
    """
    session = next(get_session())
    settings = get_settings()

    query = session.get(Query, query_id)
    if not query:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Query not found")

    process_query(session, settings, query).dict()


def load_query_results(
    settings: Settings,
    key: str,
    paginated: bool,
) -> List[StatementResults]:
    """
    Load results from backend, if available.

    If ``paginate`` is true we also load the results into the cache, anticipating more
    paginated queries.
    """
    if settings.cache and settings.cache.has(key):
        _logger.info("Reading results from cache")
        cached = settings.cache.get(key)
        query_results = json.loads(cached)
    elif settings.results_backend.has(key):
        _logger.info("Reading results from results backend")
        cached = settings.results_backend.get(key)
        query_results = json.loads(cached)
        if paginated and settings.cache:
            settings.cache.add(
                key,
                cached,
                timeout=int(settings.paginating_timeout.total_seconds()),
            )
    else:
        _logger.warning("No results found")
        query_results = []

    return query_results


@router.get("/queries/{query_id}", response_model=QueryWithResults)
def read_query(
    query_id: uuid.UUID,
    limit: int = 0,
    offset: int = 0,
    *,
    session: Session = Depends(get_session),
    request: Request,
    settings: Settings = Depends(get_settings),
) -> QueryWithResults:
    """
    Fetch information about a query.

    For paginated queries we move the data from the results backend to the cache for a
    short period, anticipating additional requests.
    """
    query = session.get(Query, query_id)
    if not query:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Query not found")

    paginated = limit > 0 or offset > 0
    query_results = load_query_results(settings, str(query_id), paginated)

    prev = next_ = None
    if paginated:
        for statement_results in query_results:
            statement_results["rows"] = statement_results["rows"][
                offset : offset + limit
            ]

        baseurl = request.url_for("read_query", query_id=query_id)
        parts = list(urllib.parse.urlparse(baseurl))
        if any(
            statement_results["row_count"] > offset + limit
            for statement_results in query_results
        ):
            parts[4] = urllib.parse.urlencode(dict(limit=limit, offset=offset + limit))
            next_ = urllib.parse.urlunparse(parts)
        if offset > 0:
            parts[4] = urllib.parse.urlencode(dict(limit=limit, offset=offset - limit))
            prev = urllib.parse.urlunparse(parts)

    results = QueryResults(__root__=query_results)

    return QueryWithResults(
        results=results, next=next_, previous=prev, errors=[], **query.dict()
    )
