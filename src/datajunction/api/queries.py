"""
Run a DJ server.
"""

import json
import logging
import urllib.parse
import uuid

from fastapi import BackgroundTasks, Depends, HTTPException, Request, Response, status
from sqlmodel import Session

from datajunction.api.main import app, celery
from datajunction.config import Settings
from datajunction.models import (
    Query,
    QueryCreate,
    QueryResults,
    QueryState,
    QueryWithResults,
)
from datajunction.queries import process_query
from datajunction.utils import get_session, get_settings

_logger = logging.getLogger(__name__)


@app.post("/queries/", response_model=QueryWithResults, status_code=status.HTTP_200_OK)
def submit_query(
    *,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    create_query: QueryCreate,
    response: Response,
    background_tasks: BackgroundTasks,
) -> QueryWithResults:
    """
    Run or schedule a query.
    """
    query = Query.from_orm(create_query)
    query.state = QueryState.ACCEPTED

    session.add(query)
    session.commit()
    session.refresh(query)

    if query.database.async_:
        if settings.celery_broker:
            dispatch_query.delay(query.id)
        else:
            background_tasks.add_task(process_query, session, settings, query)

        response.status_code = status.HTTP_201_CREATED
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
        raise HTTPException(status_code=404, detail="Query not found")

    process_query(session, settings, query).dict()


@app.get("/queries/{query_id}", response_model=QueryWithResults)
def read_query(  # pylint: disable=too-many-locals
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
        raise HTTPException(status_code=404, detail="Query not found")

    paginated = limit > 0 or offset > 0

    key = str(query_id)
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
