"""
Main DJ server app.
"""

# All the models need to be imported here so that SQLModel can define their
# relationships at runtime without causing circular imports.
# See https://sqlmodel.tiangolo.com/tutorial/code-structure/#make-circular-imports-work.
# pylint: disable=unused-import

import logging

from fastapi import FastAPI

from datajunction.api import databases, queries
from datajunction.models.database import Column, Database, Table
from datajunction.models.node import Node
from datajunction.models.query import Query
from datajunction.utils import create_db_and_tables

_logger = logging.getLogger(__name__)

app = FastAPI()
app.include_router(databases.router)
app.include_router(queries.router)


@app.on_event("startup")
def on_startup() -> None:
    """
    Ensure the database and tables exist on startup.
    """
    create_db_and_tables()
