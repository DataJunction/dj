"""
Run a DJ server.
"""

from typing import List

from fastapi import Depends, FastAPI
from sqlmodel import Session, select

from datajunction.models import Database
from datajunction.utils import create_db_and_tables, get_session

app = FastAPI()


@app.on_event("startup")
def on_startup() -> None:
    """
    Ensure the database and tables exist on startup.
    """
    create_db_and_tables()


@app.get("/databases/")
def read_databases(*, session: Session = Depends(get_session)) -> List[Database]:
    """
    List the available databases.
    """
    databases = session.exec(select(Database)).all()
    return databases
