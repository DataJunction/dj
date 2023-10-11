"""
Main DJ query server app.
"""

# All the models need to be imported here so that SQLModel can define their
# relationships at runtime without causing circular imports.
# See https://sqlmodel.tiangolo.com/tutorial/code-structure/#make-circular-imports-work.
# pylint: disable=unused-import,expression-not-assigned

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from djqs import __version__
from djqs.api import catalogs, engines, queries, tables
from djqs.config import load_djqs_config
from djqs.exceptions import DJException
from djqs.utils import get_session, get_settings

_logger = logging.getLogger(__name__)

settings = get_settings()
session = next(get_session())
load_djqs_config(settings=settings, session=session)

app = FastAPI(
    title=settings.name,
    description=settings.description,
    version=__version__,
    license_info={
        "name": "MIT License",
        "url": "https://mit-license.org/",
    },
)
app.include_router(catalogs.get_router)
app.include_router(engines.get_router)
app.include_router(queries.router)
app.include_router(tables.router)
app.include_router(catalogs.post_router) if settings.enable_dynamic_config else None
app.include_router(engines.post_router) if settings.enable_dynamic_config else None


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
