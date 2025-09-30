"""
Utility functions.
"""

import asyncio
from contextlib import asynccontextmanager
import json
import logging
import os
import re
from functools import lru_cache
from http import HTTPStatus

from typing import AsyncIterator, List, Optional

from dotenv import load_dotenv
from fastapi import Depends
from rich.logging import RichHandler
from sqlalchemy import AsyncAdaptedQueuePool
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import MissingGreenlet, OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.sql import Select

from starlette.requests import Request
from yarl import URL

from datajunction_server.config import DatabaseConfig, Settings
from datajunction_server.database.user import User
from datajunction_server.enum import StrEnum
from datajunction_server.errors import (
    DJAuthenticationException,
    DJDatabaseException,
    DJInternalErrorException,
    DJInvalidInputException,
    DJUninitializedResourceException,
)
from datajunction_server.service_clients import QueryServiceClient

logger = logging.getLogger(__name__)


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
        self._reader_engine: AsyncEngine | None = None
        self._writer_engine: AsyncEngine | None = None
        self._writer_sessionmaker: async_sessionmaker[AsyncSession] | None = None
        self._reader_sessionmaker: async_sessionmaker[AsyncSession] | None = None

    @property
    def reader_engine(self) -> AsyncEngine:
        if self._reader_engine is None:
            raise DJUninitializedResourceException(
                "DatabaseSessionManager is not initialized",
            )
        return self._reader_engine

    @property
    def writer_engine(self) -> AsyncEngine:
        if self._writer_engine is None:
            raise DJUninitializedResourceException(
                "DatabaseSessionManager is not initialized",
            )
        return self._writer_engine

    @property
    def reader_sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        if self._reader_sessionmaker is None:
            raise DJUninitializedResourceException(
                "DatabaseSessionManager is not initialized",
            )
        return self._reader_sessionmaker

    @property
    def writer_sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        if self._writer_sessionmaker is None:
            raise DJUninitializedResourceException(
                "DatabaseSessionManager is not initialized",
            )
        return self._writer_sessionmaker

    def init_db(self):
        """
        Initialize the database engine
        """
        settings = get_settings()
        self._writer_engine, self._writer_sessionmaker = self.create_engine_and_session(
            settings.writer_db,
        )
        if settings.reader_db:
            self._reader_engine, self._reader_sessionmaker = (
                self.create_engine_and_session(
                    settings.reader_db,
                )
            )
        else:
            self._reader_engine, self._reader_sessionmaker = (  # pragma: no cover
                self._writer_engine,
                self._writer_sessionmaker,
            )

    @classmethod
    def create_engine_and_session(
        cls,
        database_config: DatabaseConfig,
    ) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
        engine = create_async_engine(
            database_config.uri,
            future=True,
            echo=database_config.echo,
            pool_pre_ping=database_config.pool_pre_ping,
            pool_size=database_config.pool_size,
            max_overflow=database_config.max_overflow,
            pool_timeout=database_config.pool_timeout,
            pool_recycle=database_config.pool_recycle,
            poolclass=AsyncAdaptedQueuePool,
            connect_args={
                "connect_timeout": database_config.connect_timeout,
                "keepalives": database_config.keepalives,
                "keepalives_idle": database_config.keepalives_idle,
                "keepalives_interval": database_config.keepalives_interval,
                "keepalives_count": database_config.keepalives_count,
            },
        )
        async_session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
        return engine, async_session_factory

    @property
    def sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        """
        Default to writer sessionmaker
        """
        return self._writer_sessionmaker

    @property
    def session(self):
        return self._writer_sessionmaker  # pragma: no cover

    def get_writer_session_factory(self) -> async_sessionmaker[AsyncSession]:
        return async_sessionmaker(  # pragma: no cover
            bind=self._writer_engine,
            autocommit=False,
            expire_on_commit=False,
        )

    async def close(self):
        """
        Close database session
        """
        if (  # pragma: no cover
            self._reader_engine is None and self._writer_engine is None
        ):
            raise DJUninitializedResourceException(
                "DatabaseSessionManager is not initialized",
            )
        if self._reader_engine:  # pragma: no cover
            await self._reader_engine.dispose()  # pragma: no cover
        if self._writer_engine:  # pragma: no cover
            await self._writer_engine.dispose()  # pragma: no cover


@lru_cache(maxsize=None)
def get_session_manager() -> DatabaseSessionManager:
    """
    Get session manager
    """
    session_manager = DatabaseSessionManager()
    session_manager.init_db()
    return session_manager


async def is_graphql_query(request: Request) -> bool:
    """
    Check if the request is a GraphQL query and not a mutation.
    """
    if request.url.path != "/graphql":
        return False
    try:
        body = await request.body()
        body_json = json.loads(body)
        query_text = body_json.get("query", "")
        return query_text.strip().lower().startswith("query")
    except Exception:
        return False


async def get_session(request: Request = None) -> AsyncIterator[AsyncSession]:
    """
    Async database session.
    """
    session_manager = get_session_manager()
    session_maker = (
        session_manager.reader_sessionmaker
        if request
        and (request.method.upper() == "GET" or await is_graphql_query(request))
        else session_manager.writer_sessionmaker
    )
    async with session_maker() as session:
        try:
            yield session
        except Exception as exc:
            await session.rollback()
            raise exc


@asynccontextmanager
async def session_context(request: Request = None) -> AsyncIterator[AsyncSession]:
    gen = get_session(request)
    session = await gen.__anext__()
    try:
        yield session
    finally:
        await gen.aclose()  # type: ignore


async def refresh_if_needed(session: AsyncSession, obj, attributes: list[str]):
    """
    Conditionally refresh a list of attributes for a SQLAlchemy ORM object.
    """
    attributes_to_refresh = []

    for attr_name in attributes:
        try:
            getattr(obj, attr_name)
        except MissingGreenlet:
            attributes_to_refresh.append(attr_name)

    if attributes_to_refresh:
        await session.refresh(obj, attributes_to_refresh)


async def execute_with_retry(
    session: AsyncSession,
    statement: Select,
    retries: int = 3,
    base_delay: float = 1.0,
):
    """
    Execute a SQLAlchemy statement with retry logic for transient errors.
    """
    attempt = 0
    while True:
        try:
            return await session.execute(statement)
        except OperationalError as exc:
            attempt += 1
            if attempt > retries:
                raise DJDatabaseException(
                    "Database operation failed after retries",
                ) from exc
            delay = base_delay * (2 ** (attempt - 1))
            await asyncio.sleep(delay)


def get_query_service_client(
    request: Request = None,
) -> Optional[QueryServiceClient]:
    """
    Return query service client
    """
    from datajunction_server.service_clients import QueryServiceClient

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
            raise DJInternalErrorException(f"Unparseable version {version_string}!")
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
        raise DJInvalidInputException(f"No namespace provided: {name}")
    return node_namespace


async def get_current_user(request: Request) -> User:
    """
    Returns the current authenticated user
    """
    # from datajunction_server.database.user import User
    if not hasattr(request.state, "user"):  # pragma: no cover
        raise DJAuthenticationException(
            message="Unauthorized, request state has no user",
            http_status_code=HTTPStatus.UNAUTHORIZED,
        )
    return request.state.user


async def get_and_update_current_user(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> User:
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
