"""
Main DJ server app.
"""

# All the models need to be imported here so that SQLModel can define their
# relationships at runtime without causing circular imports.
# See https://sqlmodel.tiangolo.com/tutorial/code-structure/#make-circular-imports-work.
# pylint: disable=unused-import

import logging

import marko
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from dj import __version__
from dj.api import (
    catalogs,
    cubes,
    data,
    databases,
    engines,
    health,
    metrics,
    nodes,
    query,
    tags,
)
from dj.api.graphql.main import graphql_app
from dj.errors import DJException, registry
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
app.include_router(metrics.router)
app.include_router(query.router)
app.include_router(nodes.router)
app.include_router(data.router)
app.include_router(health.router)
app.include_router(cubes.router)
app.include_router(tags.router)
app.include_router(graphql_app, prefix="/graphql")

templates = Jinja2Templates(directory="templates")
templates.env.filters["markdown"] = marko.convert


@app.exception_handler(DJException)
async def dj_exception_handler(  # pylint: disable=unused-argument
    request: Request,
    exc: DJException,
) -> JSONResponse:
    """
    Capture errors and return problem detail JSON.

    See https://www.rfc-editor.org/rfc/rfc7807.
    """
    content = exc.to_dict()
    content["type"] = app.url_path_for("error_detail", type_=exc.__class__.__name__)

    return JSONResponse(
        status_code=exc.http_status_code,
        content=content,
        headers={
            "Content-type": "application/problem+json",
            "X-DJ-Error": "true",
            "X-DBAPI-Exception": exc.dbapi_exception,
        },
    )


@app.get("/error/{type_}")
def error_detail(
    type_: str,
    *,
    request: Request,
) -> JSONResponse:
    """
    Return information about an error.
    """
    exc = registry.get(type_)
    if exc is None:
        raise HTTPException(status_code=404, detail="Exception not found")

    return templates.TemplateResponse(
        "error_detail.html",
        {
            "request": request,
            "exception": exc,
        },
    )
