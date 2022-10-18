"""
Query related functions.
"""

import logging
from datetime import datetime, timezone
from typing import List, Tuple

import sqlparse
from sqlalchemy import text
from sqlmodel import Session

from dj.config import Settings
from dj.models.query import (
    ColumnMetadata,
    Query,
    QueryResults,
    QueryState,
    QueryWithResults,
    StatementResults,
)
from dj.typing import ColumnType, Description, SQLADialect, Stream, TypeEnum

_logger = logging.getLogger(__name__)


def get_columns_from_description(
    description: Description,
    dialect: SQLADialect,
) -> List[ColumnMetadata]:
    """
    Extract column metadata from the cursor description.

    For now this uses the information from the cursor description, which only allow us to
    distinguish between 4 types (see ``TypeEnum``). In the future we should use a type
    inferrer to determine the types based on the query.
    """
    type_map = {
        TypeEnum.STRING: ColumnType.STR,
        TypeEnum.BINARY: ColumnType.BYTES,
        TypeEnum.NUMBER: ColumnType.FLOAT,
        TypeEnum.DATETIME: ColumnType.DATETIME,
    }

    columns = []
    for column in description or []:
        name, native_type = column[:2]
        for dbapi_type in TypeEnum:
            if native_type == getattr(dialect.dbapi, dbapi_type.value, None):
                type_ = type_map[dbapi_type]
                break
        else:
            # fallback to string
            type_ = ColumnType.STR

        columns.append(ColumnMetadata(name=name, type=type_))

    return columns


def run_query(query: Query) -> List[Tuple[str, List[ColumnMetadata], Stream]]:
    """
    Run a query and return its results.

    For each statement we return a tuple with the statement SQL, a description of the
    columns (name and type) and a stream of rows (tuples).
    """
    _logger.info("Running query on database %s", query.database.name)
    engine = query.database.engine
    connection = engine.connect()

    output: List[Tuple[str, List[ColumnMetadata], Stream]] = []
    statements = sqlparse.parse(query.executed_query)
    for statement in statements:
        # Druid doesn't like statements that end in a semicolon...
        sql = str(statement).strip().rstrip(";")
        # import pdb; pdb.set_trace()
        results = connection.execute(text(sql))
        stream = (tuple(row) for row in results)
        columns = get_columns_from_description(
            results.cursor.description,
            engine.dialect,
        )
        output.append((sql, columns, stream))

    return output


def process_query(
    session: Session,
    settings: Settings,
    query: Query,
    save: bool = True,
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
        root = []
        for sql, columns, stream in run_query(query):
            rows = list(stream)
            root.append(
                StatementResults(
                    sql=sql,
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                ),
            )
        results = QueryResults(__root__=root)

        query.state = QueryState.FINISHED
        query.progress = 1.0
    except Exception as ex:  # pylint: disable=broad-except
        results = QueryResults(__root__=[])
        query.state = QueryState.FAILED
        errors = [str(ex)]

    query.finished = datetime.now(timezone.utc)

    if save:
        session.add(query)
        session.commit()
        session.refresh(query)

    settings.results_backend.add(str(query.id), results.json())

    return QueryWithResults(results=results, errors=errors, **query.dict())
