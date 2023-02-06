"""
Main DJ server app.
"""

# All the models need to be imported here so that SQLModel can define their
# relationships at runtime without causing circular imports.
# See https://sqlmodel.tiangolo.com/tutorial/code-structure/#make-circular-imports-work.
# pylint: disable=unused-import

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from dj import __version__
from dj.api import catalogs, data, databases, engines, metrics, nodes, queries
from dj.api.graphql.main import graphql_app
from dj.errors import DJException
from dj.models.catalog import Catalog
from dj.models.column import Column
from dj.models.database import Database
from dj.models.engine import Engine
from dj.models.node import NodeRevision
from dj.models.query import Query
from dj.models.table import Table
from dj.utils import get_settings

_logger = logging.getLogger(__name__)


settings = get_settings()
app = FastAPI(
    title=settings.name,
    description=settings.description,
    version=__version__,
    license_info={
        "name": "MIT License",
        "url": "https://mit-license.org/",
    },
)
app.include_router(catalogs.router)
app.include_router(databases.router)
app.include_router(engines.router)
app.include_router(queries.router)
app.include_router(metrics.router)
app.include_router(nodes.router)
app.include_router(data.router)
app.include_router(graphql_app, prefix="/graphql")


@app.exception_handler(DJException)
async def dj_exception_handler(  # pylint: disable=unused-argument
    request: Request,
    exc: DJException,
) -> JSONResponse:
    """
    Capture errors and return JSON.
    """
    return JSONResponse(
        status_code=exc.http_status_code,
        content=exc.to_dict(),
        headers={"X-DJ-Error": "true", "X-DBAPI-Exception": exc.dbapi_exception},
    )
