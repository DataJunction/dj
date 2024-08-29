"""
Utility functions.
"""
import asyncio
import logging
import os
import re
from functools import lru_cache
from http import HTTPStatus

# pylint: disable=line-too-long
from typing import AsyncIterator, List, Optional

from dotenv import load_dotenv
from fastapi import Depends
from rich.logging import RichHandler
from sqlalchemy import AsyncAdaptedQueuePool
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
    create_async_engine,
)
from starlette.requests import Request
from yarl import URL

from datajunction_server.config import Settings
from datajunction_server.database.user import User
from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJException
from datajunction_server.service_clients import QueryServiceClient


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


class DatabaseSessionManager:
    """
    DB session context manager
    """

    def __init__(self):
        self.engine: AsyncEngine | None = None
        self.session_maker = None
        self.session = None

    def init_db(self):
        """
        Initialize the database engine
        """
        settings = get_settings()
        self.engine = create_async_engine(
            settings.index,
            future=True,
            echo=settings.db_echo,
            pool_pre_ping=settings.db_pool_pre_ping,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            poolclass=AsyncAdaptedQueuePool,
            connect_args={
                "connect_timeout": settings.db_connect_timeout,
                "keepalives": settings.db_keepalives,
                "keepalives_idle": settings.db_keepalives_idle,
                "keepalives_interval": settings.db_keepalives_interval,
                "keepalives_count": settings.db_keepalives_count,
            },
        )

        async_session_factory = async_sessionmaker(
            bind=self.engine,
            autocommit=False,
            expire_on_commit=False,  # prevents attributes from being expired on commit
        )
        # Create a scoped session
        self.session = async_scoped_session(  # pragma: no cover
            async_session_factory,
            scopefunc=asyncio.current_task,
        )

    async def close(self):
        """
        Close database session
        """
        if self.engine is None:  # pragma: no cover
            raise DJException("DatabaseSessionManager is not initialized")
        await self.engine.dispose()  # pragma: no cover


@lru_cache(maxsize=None)
def get_session_manager() -> DatabaseSessionManager:
    """
    Get session manager
    """
    session_manager = DatabaseSessionManager()
    session_manager.init_db()
    return session_manager


@lru_cache(maxsize=None)
def get_engine() -> AsyncEngine:
    """
    Create the metadata engine.
    """
    settings = get_settings()
    engine = create_async_engine(
        settings.index,
        future=True,
        echo=settings.db_echo,
        pool_pre_ping=settings.db_pool_pre_ping,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        poolclass=AsyncAdaptedQueuePool,
        connect_args={
            "connect_timeout": settings.db_connect_timeout,
        },
    )
    return engine


async def get_session() -> AsyncIterator[AsyncSession]:
    """
    Async database session.
    """
    session_manager = get_session_manager()
    session = session_manager.session()
    try:
        yield session
    except Exception as exc:  # pylint: disable=broad-exception-raised
        await session.rollback()  # pragma: no cover
        raise exc  # pragma: no cover
    finally:
        await session.close()


def get_query_service_client() -> Optional[QueryServiceClient]:
    """
    Return query service client
    """
    settings = get_settings()
    if not settings.query_service:  # pragma: no cover
        return None
    return QueryServiceClient(settings.query_service)


def get_issue_url(
    baseurl: URL = URL("https://github.com/DataJunction/dj/issues/new"),
    title: Optional[str] = None,
    body: Optional[str] = None,
    labels: Optional[List[str]] = None,
) -> URL:
    """
    Return the URL to file an issue on GitHub.

    https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-an-issue#creating-an-issue-from-a-url-query
    """
    query_arguments = {
        "title": title,
        "body": body,
        "labels": ",".join(label.strip() for label in labels) if labels else None,
    }
    query_arguments = {k: v for k, v in query_arguments.items() if v is not None}

    return baseurl % query_arguments


class VersionUpgrade(StrEnum):
    """
    The version upgrade type
    """

    MAJOR = "major"
    MINOR = "minor"


class Version:
    """
    Represents a basic semantic version with only major & minor parts.
    Used for tracking node versioning.
    """

    def __init__(self, major, minor):
        self.major = major
        self.minor = minor

    def __str__(self) -> str:
        return f"v{self.major}.{self.minor}"

    @classmethod
    def parse(cls, version_string) -> "Version":
        """
        Parse a version string.
        """
        version_regex = re.compile(r"^v(?P<major>[0-9]+)\.(?P<minor>[0-9]+)")
        matcher = version_regex.search(version_string)
        if not matcher:
            raise DJException(
                http_status_code=500,
                message=f"Unparseable version {version_string}!",
            )
        results = matcher.groupdict()
        return Version(int(results["major"]), int(results["minor"]))

    def next_minor_version(self) -> "Version":
        """
        Returns the next minor version
        """
        return Version(self.major, self.minor + 1)

    def next_major_version(self) -> "Version":
        """
        Returns the next major version
        """
        return Version(self.major + 1, 0)


def get_namespace_from_name(name: str) -> str:
    """
    Splits a qualified node name into it's namespace and name parts
    """
    if "." in name:
        node_namespace, _ = name.rsplit(".", 1)
    else:  # pragma: no cover
        raise DJException(f"No namespace provided: {name}")
    return node_namespace


async def get_current_user(request: Request) -> "User":
    """
    Returns the current authenticated user
    """
    if not hasattr(request.state, "user"):  # pragma: no cover
        raise DJException(
            message="Unauthorized, request state has no user",
            http_status_code=HTTPStatus.UNAUTHORIZED,
        )
    return request.state.user


async def get_and_update_current_user(
    session: AsyncSession = Depends(get_session),
    current_user: "User" = Depends(get_current_user),
) -> "User":
    """
    Wrapper for the get_current_user dependency that creates a DJ user object if required
    """
    statement = insert(User).values(
        username=current_user.username,
        email=current_user.email,
        name=current_user.name,
        oauth_provider=current_user.oauth_provider,
    )
    update_dict = {
        "email": current_user.email,
        "name": current_user.name,
        "oauth_provider": current_user.oauth_provider,
    }
    statement = statement.on_conflict_do_update(
        index_elements=["username"],
        set_=update_dict,
    )
    await session.execute(statement)
    await session.commit()
    refreshed_user = await User.get_by_username(session, current_user.username)
    return refreshed_user  # type: ignore


SEPARATOR = "."
