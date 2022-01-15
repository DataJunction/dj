"""
Main DJ server app.
"""

import logging

from fastapi import FastAPI

from datajunction.utils import create_db_and_tables, get_settings

_logger = logging.getLogger(__name__)

app = FastAPI()
celery = get_settings().celery  # pylint: disable=invalid-name


@app.on_event("startup")
def on_startup() -> None:
    """
    Ensure the database and tables exist on startup.
    """
    create_db_and_tables()
