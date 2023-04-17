"""
Main DJ server app.
"""

# All the models need to be imported here so that SQLModel can define their
# relationships at runtime without causing circular imports.
# See https://sqlmodel.tiangolo.com/tutorial/code-structure/#make-circular-imports-work.
# pylint: disable=unused-import

import logging
from typing import TYPE_CHECKING, Optional

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette.middleware.cors import CORSMiddleware

from dj import __version__
from dj.api import (
    attributes,
    catalogs,
    cubes,
    data,
    engines,
    health,
    metrics,
    namespaces,
    nodes,
    query,
    sql,
    tags,
)
from dj.api.attributes import default_attribute_types
from dj.errors import DJException
from dj.models.catalog import Catalog
from dj.models.column import Column
from dj.models.engine import Engine
from dj.models.node import NodeRevision
from dj.models.table import Table
from dj.utils import get_settings

if TYPE_CHECKING:  # pragma: no cover
    from opentelemetry import trace

_logger = logging.getLogger(__name__)


def get_dj_app(
    tracer_provider: Optional["trace.TracerProvider"] = None,
) -> FastAPI:
    """
    Get the DJ FastAPI app and optionally inject an OpenTelemetry tracer provider
    """
    settings = get_settings()
    application = FastAPI(
        title=settings.name,
        description=settings.description,
        version=__version__,
        license_info={
            "name": "MIT License",
            "url": "https://mit-license.org/",
        },
        dependencies=[Depends(default_attribute_types)],
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_whitelist,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    application.include_router(catalogs.router)
    application.include_router(engines.router)
    application.include_router(metrics.router)
    application.include_router(query.router)
    application.include_router(nodes.router)
    application.include_router(namespaces.router)
    application.include_router(data.router)
    application.include_router(health.router)
    application.include_router(cubes.router)
    application.include_router(tags.router)
    application.include_router(attributes.router)
    application.include_router(sql.router)

    @application.exception_handler(DJException)
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

    FastAPIInstrumentor.instrument_app(application, tracer_provider=tracer_provider)

    return application


app = get_dj_app()
