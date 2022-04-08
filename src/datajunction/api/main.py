"""
Main DJ server app.
"""

# All the models need to be imported here so that SQLModel can define their
# relationships at runtime without causing circular imports.
# See https://sqlmodel.tiangolo.com/tutorial/code-structure/#make-circular-imports-work.
# pylint: disable=unused-import

import logging

from fastapi import FastAPI

from datajunction import __version__
from datajunction.api import databases, metrics, queries
from datajunction.models.database import Column, Database, Table
from datajunction.models.node import Node
from datajunction.models.query import Query
from datajunction.utils import create_db_and_tables, get_settings

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
app.include_router(databases.router)
app.include_router(queries.router)
app.include_router(metrics.router)


@app.on_event("startup")
def on_startup() -> None:
    """
    Ensure the database and tables exist on startup.
    """
    create_db_and_tables()
