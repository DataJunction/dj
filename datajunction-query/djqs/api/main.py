"""
Main DJ query server app.
"""

# All the models need to be imported here so that SQLModel can define their
# relationships at runtime without causing circular imports.
# See https://sqlmodel.tiangolo.com/tutorial/code-structure/#make-circular-imports-work.
# pylint: disable=unused-import,expression-not-assigned

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from djqs import __version__
from djqs.api import queries, tables
from djqs.exceptions import DJException
from djqs.utils import get_settings

_logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    """
    Create a postgres connection pool and store it in the app state
    """
    _logger.info("Starting PostgreSQL connection pool...")
    pool = AsyncConnectionPool(
        settings.index,
        kwargs={"row_factory": dict_row},
        check=AsyncConnectionPool.check_connection,
        min_size=5,
        max_size=20,
        timeout=15,
    )
    fastapi_app.state.pool = pool
    try:
        _logger.info("PostgreSQL connection pool started with DSN: %s", settings.index)
        yield
    finally:
        _logger.info("Closing PostgreSQL connection pool")
        await pool.close()
        _logger.info("PostgreSQL connection pool closed")


app = FastAPI(
    title=settings.name,
    description=settings.description,
    version=__version__,
    license_info={
        "name": "MIT License",
        "url": "https://mit-license.org/",
    },
    lifespan=lifespan,
)
app.include_router(queries.router)
app.include_router(tables.router)

app.router.lifespan_context = lifespan


@app.exception_handler(DJException)
async def dj_exception_handler(  # pylint: disable=unused-argument
    request: Request,
    exc: DJException,
) -> JSONResponse:
    """
    Capture errors and return JSON.
    """
    return JSONResponse(  # pragma: no cover
        status_code=exc.http_status_code,
        content=exc.to_dict(),
        headers={"X-DJ-Error": "true", "X-DBAPI-Exception": exc.dbapi_exception},
    )
