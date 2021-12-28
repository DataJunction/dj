"""
Run a DJ server.
"""

from functools import lru_cache
from typing import Iterator

from fastapi import FastAPI, Depends
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine, select

from datajunction.config import Settings
from datajunction.models import Database

app = FastAPI()


@lru_cache()
def get_settings() -> Settings:
    """
    Returned a cached settings object.
    """
    return Settings()


def get_engine() -> Engine:
    """
    Create the engine.
    """
    settings = get_settings()
    connect_args = {"check_same_thread": False}
    engine = create_engine(settings.index, echo=True, connect_args=connect_args)

    return engine


def get_session() -> Iterator[Session]:
    """
    Per-request session.
    """
    engine = get_engine()

    with Session(engine) as session:
        yield session


def create_db_and_tables():
    """
    Create the database and tables.
    """
    engine = get_engine()
    SQLModel.metadata.create_all(engine)


@app.on_event("startup")
def on_startup():
    """
    Ensure the database and tables exist on startup.
    """
    create_db_and_tables()


@app.get("/databases/")
def read_databases(*, session: Session = Depends(get_session)):
    """
    List the available databases.
    """
    databases = session.exec(select(Database)).all()
    return databases
