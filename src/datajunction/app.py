"""
Run a DJ server.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

from fastapi import Depends, FastAPI, HTTPException, Response, status
from sqlmodel import Session, select

from datajunction.models import BaseQuery, Database, Query, QueryState
from datajunction.queries import ColumnMetadata, run_query
from datajunction.utils import create_db_and_tables, get_session

app = FastAPI()


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

    columns: List[ColumnMetadata]
    results: List[Tuple[Any, ...]]
    errors: List[str]


@app.post("/queries/", response_model=QueryWithResults, status_code=status.HTTP_200_OK)
def submit_query(
    *,
    session: Session = Depends(get_session),
    create_query: QueryCreate,
    response: Response,
) -> QueryWithResults:
    """
    Run or schedule a query.
    """
    query = Query.from_orm(create_query)

    session.add(query)
    session.commit()
    session.refresh(query)

    if query.database.async_:
        query.state = QueryState.ACCEPTED
        response.status_code = status.HTTP_201_CREATED
        return QueryWithResults(columns=[], results=[], errors=[], **query.dict())

    query.scheduled = datetime.now(timezone.utc)
    query.state = QueryState.SCHEDULED
    query.executed_query = query.submitted_query

    errors = []
    query.started = datetime.now(timezone.utc)
    try:
        columns, results = run_query(query)
        query.state = QueryState.FINISHED
        query.progress = 1.0
    except Exception as ex:  # pylint: disable=broad-except
        columns, results = [], iter([])
        query.state = QueryState.FAILED
        errors = [str(ex)]

    query.finished = datetime.now(timezone.utc)

    session.add(query)
    session.commit()
    session.refresh(query)

    return QueryWithResults(
        columns=columns, results=list(results), errors=errors, **query.dict()
    )


@app.get("/queries/{query_id}", response_model=QueryWithResults)
def read_query(
    query_id: uuid.UUID,
    *,
    session: Session = Depends(get_session),
) -> QueryWithResults:
    """
    Fetch information about a query.
    """
    query = session.get(Query, query_id)
    if not query:
        raise HTTPException(status_code=404, detail="Query not found")

    # XXX fetch results/columns/errors from somewhere?  # pylint: disable=fixme
    return QueryWithResults(columns=[], results=[], errors=[], **query.dict())
