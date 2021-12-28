"""
Utility functions.
"""

import logging
from functools import lru_cache
from pathlib import Path
from typing import Iterator

from dotenv import load_dotenv
from rich.logging import RichHandler
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine

from datajunction.config import Settings


def setup_logging(loglevel: str) -> None:
    """
    Setup basic logging.
    """
    level = getattr(logging, loglevel.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid log level: {loglevel}")

    logformat = "[%(asctime)s] %(levelname)s: %(name)s: %(message)s"
    logging.basicConfig(
        level=level,
        format=logformat,
        datefmt="[%X]",
        handlers=[RichHandler()],
        force=True,
    )


def get_project_repository() -> Path:
    """
    Return the project repository.

    This is used for unit tests.
    """
    return Path(__file__).parent.parent.parent


@lru_cache
def get_settings() -> Settings:
    """
    Return a cached settings object.
    """
    load_dotenv()
    return Settings()


def get_engine() -> Engine:
    """
    Create the metadata engine.
    """
    settings = get_settings()
    connect_args = {"check_same_thread": False}
    engine = create_engine(settings.index, echo=False, connect_args=connect_args)

    return engine


def create_db_and_tables() -> None:
    """
    Create the database and tables.
    """
    engine = get_engine()
    SQLModel.metadata.create_all(engine)


def get_session() -> Iterator[Session]:
    """
    Per-request session.
    """
    engine = get_engine()

    with Session(engine) as session:
        yield session
