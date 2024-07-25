"""
Main DJ server app.
"""

# All the models need to be imported here so that SQLModel can define their
# relationships at runtime without causing circular imports.
# See https://sqlmodel.tiangolo.com/tutorial/code-structure/#make-circular-imports-work.
# pylint: disable=unused-import,ungrouped-imports

import logging
from http import HTTPStatus
from logging import config
from os import path
from typing import TYPE_CHECKING

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from jose import JWTError
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from sqlalchemy import select
from starlette.middleware.cors import CORSMiddleware

from datajunction_server import __version__
from datajunction_server.api import (
    attributes,
    catalogs,
    client,
    collection,
    cubes,
    data,
    dimensions,
    djsql,
    engines,
    health,
    history,
    materializations,
    measures,
    metrics,
    namespaces,
    nodes,
    sql,
    tags,
    users,
)
from datajunction_server.api.access.authentication import basic, whoami
from datajunction_server.api.attributes import default_attribute_types
from datajunction_server.api.catalogs import default_catalog
from datajunction_server.api.graphql.main import graphql_app
from datajunction_server.constants import AUTH_COOKIE, LOGGED_IN_FLAG_COOKIE
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.column import Column
from datajunction_server.database.database import Table
from datajunction_server.database.engine import Engine
from datajunction_server.database.node import NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.utils import get_settings

if TYPE_CHECKING:  # pragma: no cover
    from opentelemetry import trace

_logger = logging.getLogger(__name__)
settings = get_settings()

config.fileConfig(
    path.join(path.dirname(path.abspath(__file__)), "logging.conf"),
    disable_existing_loggers=False,
)

dependencies = [Depends(default_attribute_types), Depends(default_catalog)]

app = FastAPI(
    title=settings.name,
    description=settings.description,
    version=__version__,
    license_info={
        "name": "MIT License",
        "url": "https://mit-license.org/",
    },
    dependencies=dependencies,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_whitelist,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(catalogs.router)
app.include_router(collection.router)
app.include_router(engines.router)
app.include_router(metrics.router)
app.include_router(djsql.router)
app.include_router(nodes.router)
app.include_router(namespaces.router)
app.include_router(materializations.router)
app.include_router(measures.router)
app.include_router(data.router)
app.include_router(health.router)
app.include_router(history.router)
app.include_router(cubes.router)
app.include_router(tags.router)
app.include_router(attributes.router)
app.include_router(sql.router)
app.include_router(client.router)
app.include_router(dimensions.router)
app.include_router(graphql_app, prefix="/graphql")
app.include_router(whoami.router)
app.include_router(users.router)
app.include_router(basic.router)


@app.on_event("startup")
async def startup():
    """
    Initialize FastAPI cache when the server starts up
    """
    FastAPICache.init(InMemoryBackend(), prefix="inmemory-cache")  # pragma: no cover


@app.exception_handler(DJException)
async def dj_exception_handler(  # pylint: disable=unused-argument
    request: Request,
    exc: DJException,
) -> JSONResponse:
    """
    Capture errors and return JSON.
    """
    _logger.exception(exc)
    response = JSONResponse(
        status_code=exc.http_status_code,
        content=exc.to_dict(),
        headers={"X-DJ-Error": "true", "X-DBAPI-Exception": exc.dbapi_exception},
    )
    # If unauthorized, clear out any DJ cookies
    if exc.http_status_code == HTTPStatus.UNAUTHORIZED:
        response.delete_cookie(AUTH_COOKIE, httponly=True)
        response.delete_cookie(LOGGED_IN_FLAG_COOKIE)
    return response


# Only mount github auth router if a github client id and secret are configured
if all(
    [
        settings.secret,
        settings.github_oauth_client_id,
        settings.github_oauth_client_secret,
    ],
):  # pragma: no cover
    from datajunction_server.api.access.authentication import github

    app.include_router(github.router)

# Only mount google auth router if a google oauth is configured
if all(
    [
        settings.secret,
        settings.google_oauth_client_id,
        settings.google_oauth_client_secret,
        settings.google_oauth_client_secret_file,
    ],
):  # pragma: no cover
    from datajunction_server.api.access.authentication import google

    app.include_router(google.router)
