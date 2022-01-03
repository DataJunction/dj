"""
Run a DJ server.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Response, status
from sqlmodel import Session, SQLModel, select

from datajunction.config import Settings
from datajunction.models import BaseQuery, Database, Query, QueryState
from datajunction.queries import ColumnMetadata, run_query
from datajunction.utils import create_db_and_tables, get_session, get_settings

app = FastAPI()
celery = get_settings().celery  # pylint: disable=invalid-name


@app.on_event("startup")
def on_startup() -> None:
    """
    Ensure the database and tables exist on startup.
    """
    create_db_and_tables()


@app.get("/databases/", response_model=List[Database])
def read_databases(*, session: Session = Depends(get_session)) -> List[Database]:
    """
    List the available databases.
    """
    databases = session.exec(select(Database)).all()
    return databases


class QueryCreate(BaseQuery):
    """
    Model for submitted queries.
    """

    submitted_query: str


class StatementResults(SQLModel):
    """
    Results for a given statement.

    This contains the SQL, column names and types, and rows
    """

    sql: str
    columns: List[ColumnMetadata]
    rows: List[Tuple[Any, ...]]


class QueryResults(SQLModel):
    """
    Results for a given query.
    """

    __root__: List[StatementResults]


class QueryWithResults(BaseQuery):
    """
    Model for query with results.
    """

    id: uuid.UUID

    submitted_query: str
    executed_query: Optional[str] = None

    scheduled: Optional[datetime] = None
    started: Optional[datetime] = None
    finished: Optional[datetime] = None

    state: QueryState = QueryState.UNKNOWN
    progress: float = 0.0

    results: QueryResults
    errors: List[str]


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


def process_query(
    session: Session,
    settings: Settings,
    query: Query,
) -> QueryWithResults:
    """
    Process a query.
    """
    query.scheduled = datetime.now(timezone.utc)
    query.state = QueryState.SCHEDULED
    query.executed_query = query.submitted_query

    errors = []
    query.started = datetime.now(timezone.utc)
    try:
        results = QueryResults(
            __root__=[
                StatementResults(sql=sql, columns=columns, rows=list(stream))
                for sql, columns, stream in run_query(query)
            ],
        )
        query.state = QueryState.FINISHED
        query.progress = 1.0
    except Exception as ex:  # pylint: disable=broad-except
        results = QueryResults(__root__=[])
        query.state = QueryState.FAILED
        errors = [str(ex)]

    query.finished = datetime.now(timezone.utc)

    session.add(query)
    session.commit()
    session.refresh(query)

    settings.results_backend.add(str(query.id), results.json())

    return QueryWithResults(results=results, errors=errors, **query.dict())


@app.get("/queries/{query_id}", response_model=QueryWithResults)
def read_query(
    query_id: uuid.UUID,
    *,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> QueryWithResults:
    """
    Fetch information about a query.
    """
    query = session.get(Query, query_id)
    if not query:
        raise HTTPException(status_code=404, detail="Query not found")

    if cached := settings.results_backend.get(str(query_id)):
        results = QueryResults(__root__=json.loads(cached))
    else:
        results = QueryResults(__root__=[])

    return QueryWithResults(results=results, errors=[], **query.dict())
