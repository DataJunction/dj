"""
Data related APIs.
"""

import logging
from typing import List, Optional

from fastapi import Depends, Query, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

from datajunction_server.api.helpers import query_event_stream
from datajunction_server.construction.build_v3.builder import build_metrics_sql
from datajunction_server.construction.build_v3.cube_matcher import (
    resolve_dialect_and_engine_for_metrics,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import (
    get_query_service_client,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["DJSQL"])


class DJSQLColumn(BaseModel):
    """Column metadata for DJ SQL response."""

    name: str
    type: str
    semantic_name: Optional[str] = None
    semantic_type: Optional[str] = None  # "dimension", "metric", etc.


class TranslatedDJSQL(BaseModel):
    """Response model for translated DJ SQL."""

    sql: str
    columns: List[DJSQLColumn]
    dialect: str


def selects_from_metrics(select: ast.SelectExpression) -> bool:
    """Check if a SELECT sources from the 'metrics' table."""
    return (
        select.from_ is not None
        and len(select.from_.relations) == 1
        and len(select.from_.relations[0].extensions) == 0
        and str(select.from_.relations[0].primary).lower() == "metrics"
    )


def parse_dj_sql(
    query: str,
) -> tuple[List[str], List[str], List[str], List[str], Optional[int]]:
    """
    Parse a DJ SQL query and extract metrics, dimensions, filters, orderby, limit.

    Args:
        query: DJ SQL query string like:
            SELECT metric1, metric2, dim1, dim2
            FROM metrics
            GROUP BY dim1, dim2
            WHERE filter1 AND filter2
            ORDER BY dim1 ASC
            LIMIT 10

    Returns:
        Tuple of (metrics, dimensions, filters, orderby, limit)

    Note: Validation of metric/dimension nodes is delegated to build_metrics_sql.
    """
    tree = parse(query)
    select = tree.select

    if not selects_from_metrics(select):
        raise DJInvalidInputException(
            "DJ SQL queries must SELECT FROM metrics. "
            "Example: SELECT metric1, dim1 FROM metrics GROUP BY dim1",
        )

    # Validate no unsupported clauses
    if any((select.having, select.lateral_views, select.set_op)):
        raise DJInvalidInputException(
            "HAVING, LATERAL VIEWS, and SET OPERATIONS are not allowed in DJ SQL queries.",
        )

    # Extract dimensions from GROUP BY
    dimensions = [str(exp) for exp in select.group_by]

    # Extract metrics: projection columns that are not in GROUP BY dimensions
    # Validation that these are actual metric nodes is delegated to build_metrics_sql
    metrics = []
    for col in select.projection:
        if not isinstance(col, ast.Column):
            raise DJInvalidInputException(
                f"Only direct columns are allowed in DJ SQL queries, found: {col}",
            )

        col_ident = col.identifier(False)
        if col_ident not in dimensions:
            metrics.append(col_ident)

    # Extract filters from WHERE
    filters = [str(select.where)] if select.where else []

    # Extract ORDER BY
    orderby = []
    if select.organization:
        orderby = [
            str(sort) for sort in (select.organization.order + select.organization.sort)
        ]

    # Extract LIMIT
    limit = None
    if select.limit:
        try:
            limit = int(str(select.limit))
        except ValueError as exc:
            raise DJInvalidInputException(
                f"LIMIT must be an integer, got: {select.limit}",
            ) from exc

    return metrics, dimensions, filters, orderby, limit


@router.get("/djsql/", response_model=TranslatedDJSQL)
async def get_sql_for_djsql(
    query: str = Query(..., description="DJ SQL query"),
    dialect: Optional[str] = Query(
        None,
        description="SQL dialect (spark, trino, druid)",
    ),
    *,
    session: AsyncSession = Depends(get_session),
) -> TranslatedDJSQL:
    """
    Translate a DJ SQL query to executable SQL using the v3 builder.

    DJ SQL syntax:
    ```sql
    SELECT <metric1>, <metric2>, <dim1>, <dim2>
    FROM metrics
    GROUP BY <dim1>, <dim2>
    WHERE <filter1> AND <filter2>
    ORDER BY <dim1> ASC
    LIMIT 10
    ```

    Returns the generated SQL that can be executed against your data warehouse.
    """
    # Parse the DJ SQL query (validation delegated to build_metrics_sql)
    metrics, dimensions, filters, orderby, limit = parse_dj_sql(query)

    # Map dialect string to enum (None means use builder default)
    dialect_enum: Optional[Dialect] = None
    if dialect:
        dialect_map = {
            "spark": Dialect.SPARK,
            "trino": Dialect.TRINO,
            "druid": Dialect.DRUID,
        }
        dialect_enum = dialect_map.get(dialect.lower())

    # Build SQL using v3 builder
    result = await build_metrics_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        orderby=orderby if orderby else None,
        limit=limit,
        dialect=dialect_enum,
    )
    _logger.info(
        "[/djsql/] generated SQL via build_metrics_sql (v3 path) — sql=%s",
        result.sql,
    )

    return TranslatedDJSQL(
        sql=result.sql,
        columns=[
            DJSQLColumn(
                name=col.name,
                type=col.type,
                semantic_name=col.semantic_name,
                semantic_type=col.semantic_type,
            )
            for col in result.columns
        ],
        dialect=result.dialect.value,
    )


async def _build_djsql_query(
    session: AsyncSession,
    query: str,
    use_materialized: bool,
    engine_name: Optional[str],
    engine_version: Optional[str],
):
    """
    Shared SQL-build path for ``/djsql/data`` and ``/djsql/stream/``. Routes
    through the v3 builder so that all four endpoints (``/djsql/``,
    ``/djsql/data``, ``/djsql/stream/``, ``/data/``) emit identical SQL for
    the same metrics + dimensions.
    """
    metrics, dimensions, filters, orderby, limit = parse_dj_sql(query)
    execution_ctx = await resolve_dialect_and_engine_for_metrics(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        use_materialized=use_materialized,
        engine_name=engine_name,
        engine_version=engine_version,
    )
    generated_sql = await build_metrics_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters if filters else None,
        orderby=orderby if orderby else None,
        limit=limit,
        dialect=execution_ctx.dialect,
        use_materialized=use_materialized,
    )
    return generated_sql, execution_ctx


@router.get("/djsql/data", response_model=QueryWithResults)
async def get_data_for_djsql(
    query: str,
    async_: bool = False,
    use_materialized: bool = Query(
        default=True,
        description="Whether to use materialized tables when available",
    ),
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
) -> QueryWithResults:
    """
    Return data for a DJ SQL query.

    Uses the v3 SQL builder via ``parse_dj_sql`` + ``build_metrics_sql``,
    matching ``/djsql/`` and ``/data/``.
    """
    request_headers = dict(request.headers)
    generated_sql, execution_ctx = await _build_djsql_query(
        session=session,
        query=query,
        use_materialized=use_materialized,
        engine_name=engine_name,
        engine_version=engine_version,
    )
    _logger.info(
        "[/djsql/data] engine=%s catalog=%s dialect=%s sql=%s",
        execution_ctx.engine.name,
        execution_ctx.catalog_name,
        execution_ctx.dialect,
        generated_sql.sql,
    )

    query_create = QueryCreate(
        engine_name=execution_ctx.engine.name,
        catalog_name=execution_ctx.catalog_name,
        engine_version=execution_ctx.engine.version,
        submitted_query=generated_sql.sql,
        async_=async_,
    )

    result = await query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )

    # Inject column info if there are results
    if result.results.root:  # pragma: no cover
        result.results.root[0].columns = generated_sql.columns or []
    return result


@router.get("/djsql/stream/", response_model=QueryWithResults)
async def get_data_stream_for_djsql(
    query: str,
    use_materialized: bool = Query(
        default=True,
        description="Whether to use materialized tables when available",
    ),
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
) -> QueryWithResults:  # pragma: no cover
    """
    Return data for a DJ SQL query using server side events.

    Uses the v3 SQL builder via ``parse_dj_sql`` + ``build_metrics_sql``,
    matching ``/djsql/`` and ``/data/``.
    """
    request_headers = dict(request.headers)
    generated_sql, execution_ctx = await _build_djsql_query(
        session=session,
        query=query,
        use_materialized=use_materialized,
        engine_name=engine_name,
        engine_version=engine_version,
    )
    _logger.info(
        "[/djsql/stream/] engine=%s catalog=%s dialect=%s sql=%s",
        execution_ctx.engine.name,
        execution_ctx.catalog_name,
        execution_ctx.dialect,
        generated_sql.sql,
    )

    query_create = QueryCreate(
        engine_name=execution_ctx.engine.name,
        catalog_name=execution_ctx.catalog_name,
        engine_version=execution_ctx.engine.version,
        submitted_query=generated_sql.sql,
        async_=True,
    )

    initial_query_info = await query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )
    return EventSourceResponse(
        query_event_stream(
            query=initial_query_info,
            request_headers=request_headers,
            query_service_client=query_service_client,
            columns=generated_sql.columns,  # type: ignore
            request=request,
        ),
    )
