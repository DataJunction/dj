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
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette.middleware.cors import CORSMiddleware

from datajunction_server import __version__
from datajunction_server.api import (
    attributes,
    catalogs,
    client,
    cubes,
    data,
    dimensions,
    djsql,
    engines,
    health,
    history,
    materializations,
    metrics,
    namespaces,
    nodes,
    sql,
    tags,
)
from datajunction_server.api.attributes import default_attribute_types
from datajunction_server.errors import DJError, DJException
from datajunction_server.models.catalog import Catalog
from datajunction_server.models.column import Column
from datajunction_server.models.engine import Engine
from datajunction_server.models.node import NodeRevision
from datajunction_server.models.table import Table
from datajunction_server.utils import get_settings

if TYPE_CHECKING:  # pragma: no cover
    from opentelemetry import trace

_logger = logging.getLogger(__name__)
settings = get_settings()

UNAUTHENTICATED_ENDPOINTS = [
    "/docs",
    "/openapi.json",
    "/basic/user/",
    "/basic/login/",
    "/github/login/",
    "/github/token/",
]
BASIC_OAUTH_CONFIGURED = settings.basic_oauth_client_secret or False
GITHUB_OAUTH_CONFIGURED = (
    all([settings.github_oauth_client_id, settings.github_oauth_client_secret]) or False
)

config.fileConfig(
    path.join(path.dirname(path.abspath(__file__)), "logging.conf"),
    disable_existing_loggers=False,
)
app = FastAPI(
    title=settings.name,
    description=settings.description,
    version=__version__,
    license_info={
        "name": "MIT License",
        "url": "https://mit-license.org/",
    },
    dependencies=[Depends(default_attribute_types)],
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_whitelist,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(catalogs.router)
app.include_router(engines.router)
app.include_router(metrics.router)
app.include_router(djsql.router)
app.include_router(nodes.router)
app.include_router(namespaces.router)
app.include_router(materializations.router)
app.include_router(data.router)
app.include_router(health.router)
app.include_router(history.router)
app.include_router(cubes.router)
app.include_router(tags.router)
app.include_router(attributes.router)
app.include_router(sql.router)
app.include_router(client.router)
app.include_router(dimensions.router)


@app.exception_handler(DJException)
async def dj_exception_handler(  # pylint: disable=unused-argument
    request: Request,
    exc: DJException,
) -> JSONResponse:
    """
    Capture errors and return JSON.
    """
    _logger.exception(exc)
    return JSONResponse(
        status_code=exc.http_status_code,
        content=exc.to_dict(),
        headers={"X-DJ-Error": "true", "X-DBAPI-Exception": exc.dbapi_exception},
    )


if GITHUB_OAUTH_CONFIGURED:
    _logger.info(
        "GITHUB_OAUTH_CLIENT_ID and GITHUB_OAUTH_CLIENT_SECRET configured, "
        "enabling GitHub OAuth router and middleware",
    )
    from datajunction_server.api.authentication import github
    from datajunction_server.internal.authentication.github import (
        get_github_user_from_cookie,
    )

    app.include_router(github.router)

    @app.middleware("http")
    async def parse_github_auth_cookie(request: Request, call_next):
        """
        Parse an "access_token" cookie for GitHub auth
        """
        if not hasattr(request.state, "user") or not request.state.user:
            _logger.info(
                "Attempting to get GitHub authenticated user from request cookie",
            )
            user = get_github_user_from_cookie(request=request)
            if not user:
                if request.url.path not in UNAUTHENTICATED_ENDPOINTS:
                    # We return an unauthorized response here because
                    # this is the final layer where a user can be authenticated
                    return Response(
                        status_code=HTTPStatus.UNAUTHORIZED,
                    )
            request.state.user = user
        else:
            _logger.info(
                "GitHub authentication not checked, user already "
                "set through a higher ranked auth scheme",
            )
        response = await call_next(request)
        return response


if BASIC_OAUTH_CONFIGURED:
    _logger.info(
        "BASIC_OAUTH_CLIENT_SECRET configured, enabling basic OAuth router and middleware",
    )
    from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

    from datajunction_server.api.authentication import basic
    from datajunction_server.internal.authentication.basic import (
        get_basic_user_from_cookie,
    )

    oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

    app.include_router(basic.router)

    @app.middleware("http")
    async def parse_basic_auth_cookie(request: Request, call_next):
        """
        Parse an "access_token" cookie for basic auth
        """
        _logger.info("Attempting to get basic authenticated user from request cookie")
        user = get_basic_user_from_cookie(request=request)
        request.state.user = user
        if (
            not user
            and not any([GITHUB_OAUTH_CONFIGURED])
            and request.url.path not in UNAUTHENTICATED_ENDPOINTS
        ):
            # We must respond as unauthorized here if user is None
            # because there are no more layers of auth middleware
            return Response(
                status_code=HTTPStatus.UNAUTHORIZED,
            )
        response = await call_next(request)
        return response
