"""
Database connection functions.
"""
from functools import lru_cache

# pylint: disable=line-too-long
from typing import Iterator, List, Optional

from dotenv import load_dotenv
from rich.logging import RichHandler
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine
from starlette.requests import Request
from yarl import URL

from datajunction_server.config import Settings
from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJException
from datajunction_server.models.user import User
from datajunction_server.service_clients import QueryServiceClient
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session as SaSession
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from datajunction_server.utils import get_settings, get_engine

Base = declarative_base()

def get_direct_session() -> Iterator[SaSession]:
    """
    Direct SQLAlchemy session.
    """
    engine = get_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_async_session() -> Iterator[AsyncSession]:
    settings = get_settings()
    async_engine = create_async_engine(settings.index)
    SessionLocal = sessionmaker(
        bind=async_engine, expire_on_commit=False, class_=AsyncSession
    )
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
