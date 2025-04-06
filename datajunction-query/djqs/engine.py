"""
Query related functions.
"""

import json
import logging
import os
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import duckdb
import snowflake.connector
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import create_engine, text

from djqs.config import EngineType, Settings
from djqs.constants import SQLALCHEMY_URI
from djqs.db.postgres import DBQuery
from djqs.exceptions import DJDatabaseError
from djqs.models.query import (
    ColumnMetadata,
    Query,
    QueryResults,
    QueryState,
    StatementResults,
)
from djqs.typing import ColumnType, Description, SQLADialect, Stream, TypeEnum
from djqs.utils import get_settings

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
            if native_type == getattr(
                dialect.dbapi,
                dbapi_type.value,
                None,
            ):  # pragma: no cover
                type_ = type_map[dbapi_type]
                break
        else:
            # fallback to string
            type_ = ColumnType.STR  # pragma: no cover

        columns.append(ColumnMetadata(name=name, type=type_))

    return columns


def run_query(  # pylint: disable=R0914
    query: Query,
    headers: Optional[Dict[str, str]] = None,
) -> List[Tuple[str, List[ColumnMetadata], Stream]]:
    """
    Run a query and return its results.

    For each statement we return a tuple with the statement SQL, a description of the
    columns (name and type) and a stream of rows (tuples).
    """

    _logger.info("Running query on catalog %s", query.catalog_name)

    settings = get_settings()
    engine_name = query.engine_name or settings.default_engine
    engine_version = query.engine_version or settings.default_engine_version
    engine = settings.find_engine(
        engine_name=engine_name,
        engine_version=engine_version,
    )
    query_server = headers.get(SQLALCHEMY_URI) if headers else None

    if query_server:
        _logger.info(
            "Creating sqlalchemy engine using request header param %s",
            SQLALCHEMY_URI,
        )
        sqla_engine = create_engine(query_server)
    elif engine.type == EngineType.DUCKDB:
        _logger.info("Creating duckdb connection")
        conn = (
            duckdb.connect()
            if engine.uri == "duckdb:///:memory:"
            else duckdb.connect(
                database=engine.extra_params["location"],
                read_only=True,
            )
        )
        return run_duckdb_query(query, conn)
    elif engine.type == EngineType.SNOWFLAKE:
        _logger.info("Creating snowflake connection")
        conn = snowflake.connector.connect(
            **engine.extra_params,
            password=os.getenv("SNOWSQL_PWD"),
        )
        cur = conn.cursor()

        return run_snowflake_query(query, cur)

    _logger.info(
        "Creating sqlalchemy engine using engine name and version defined on query",
    )
    sqla_engine = create_engine(engine.uri, connect_args=engine.extra_params)
    connection = sqla_engine.connect()

    output: List[Tuple[str, List[ColumnMetadata], Stream]] = []
    results = connection.execute(text(query.executed_query))
    stream = (tuple(row) for row in results)
    columns = get_columns_from_description(
        results.cursor.description,
        sqla_engine.dialect,
    )
    output.append((query.executed_query, columns, stream))  # type: ignore

    return output


def run_duckdb_query(
    query: Query,
    conn: duckdb.DuckDBPyConnection,
) -> List[Tuple[str, List[ColumnMetadata], Stream]]:
    """
    Run a duckdb query against the local duckdb database
    """
    output: List[Tuple[str, List[ColumnMetadata], Stream]] = []
    rows = conn.execute(query.submitted_query).fetchall()
    columns: List[ColumnMetadata] = []
    output.append((query.submitted_query, columns, rows))
    return output


def run_snowflake_query(
    query: Query,
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> List[Tuple[str, List[ColumnMetadata], Stream]]:
    """
    Run a query against a snowflake warehouse
    """
    output: List[Tuple[str, List[ColumnMetadata], Stream]] = []
    rows = cur.execute(query.submitted_query).fetchall()
    columns: List[ColumnMetadata] = []
    output.append((query.submitted_query, columns, rows))
    return output


async def process_query(
    settings: Settings,
    postgres_pool: AsyncConnectionPool,
    query: Query,
    headers: Optional[Dict[str, str]] = None,
) -> QueryResults:
    """
    Process a query.
    """
    query.scheduled = datetime.now(timezone.utc)
    query.state = QueryState.SCHEDULED
    query.executed_query = query.submitted_query

    errors = []
    query.started = datetime.now(timezone.utc)
    try:
        results = []
        for sql, columns, stream in run_query(
            query=query,
            headers=headers,
        ):
            rows = list(stream)
            results.append(
                StatementResults(
                    sql=sql,
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                ),
            )

        query.state = QueryState.FINISHED
        query.progress = 1.0
    except Exception as ex:  # pylint: disable=broad-except
        results = []
        query.state = QueryState.FAILED
        errors = [str(ex)]

    query.finished = datetime.now(timezone.utc)

    async with postgres_pool.connection() as conn:
        dbquery_results = (
            await DBQuery()
            .save_query(
                query_id=query.id,
                submitted_query=query.submitted_query,
                state=QueryState.FINISHED.value,
                async_=query.async_,
            )
            .execute(conn=conn)
        )
        query_save_result = dbquery_results[0]
        if not query_save_result:  # pragma: no cover
            raise DJDatabaseError("Query failed to save")

    settings.results_backend.add(
        str(query.id),
        json.dumps([asdict(statement_result) for statement_result in results]),
    )

    return QueryResults(
        id=query.id,
        catalog_name=query.catalog_name,
        engine_name=query.engine_name,
        engine_version=query.engine_version,
        submitted_query=query.submitted_query,
        executed_query=query.executed_query,
        scheduled=query.scheduled,
        started=query.started,
        finished=query.finished,
        state=query.state,
        progress=query.progress,
        results=results,
        errors=errors,
    )
