"""
Utility functions.
"""
# pylint: disable=line-too-long

import datetime
import logging
import os
from functools import lru_cache
from typing import Iterator

from dotenv import load_dotenv
from pydantic.datetime_parse import parse_datetime
from rich.logging import RichHandler
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine

from djqs.config import Settings


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
        handlers=[RichHandler(rich_tracebacks=True)],
        force=True,
    )


@lru_cache
def get_settings() -> Settings:
    """
    Return a cached settings object.
    """
    dotenv_file = os.environ.get("DOTENV_FILE", ".env")
    load_dotenv(dotenv_file)
    return Settings()


def get_metadata_engine() -> Engine:
    """
    Create the metadata engine.
    """
    settings = get_settings()
    engine = create_engine(settings.index)

    return engine


def get_session() -> Iterator[Session]:
    """
    Per-request session.
    """
    engine = get_metadata_engine()

    with Session(engine, autoflush=False) as session:  # pragma: no cover
        yield session


class UTCDatetime(datetime.datetime):  # pragma: no cover
    """
    A UTC extension of pydantic's normal datetime handling
    """

    @classmethod
    def __get_validators__(cls):
        """
        Extend the builtin pydantic datetime parser with a custom validate method
        """
        yield parse_datetime
        yield cls.validate

    @classmethod
    def validate(cls, value) -> str:
        """
        Convert to UTC
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)

        return value.astimezone(datetime.timezone.utc)
