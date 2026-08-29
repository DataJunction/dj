"""
Shared bootstrap for the scripts that populate template databases.

Both ``populate_template.py`` and ``populate_preaggs_template.py`` need the same
thing: an authenticated HTTP client talking to the DJ app, with the app pointed
at one specific database. Getting there involves setting environment variables
before ``datajunction_server`` is imported at all, building a ``Settings``,
registering dialect plugins, stubbing the query service and installing four
dependency overrides -- none of which is interesting to either caller.

Nothing here imports ``datajunction_server`` at module scope: the caller must be
able to ``import`` this module, call :func:`configure_database_env`, and only
then have any DJ module read its settings.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import httpx

TEST_SECRET = "a-fake-secretkey"
TEST_ISSUER = "http://localhost:8000/"


def configure_database_env(db_url: str) -> str:
    """
    Point the DJ settings at ``db_url``, and return the reader URL.

    Must be called before any ``datajunction_server`` module is imported, since
    ``Settings`` reads these at import time.
    """
    reader_db_url = db_url.replace("dj:dj@", "readonly_user:readonly@")
    os.environ["DJ_DATABASE__URI"] = db_url
    os.environ["WRITER_DB__URI"] = db_url
    os.environ["READER_DB__URI"] = reader_db_url
    return reader_db_url


def build_settings(db_url: str, reader_db_url: str) -> Any:
    """Build the Settings the app should use, and patch the seed module's copy."""
    from cachelib.simple import SimpleCache

    from datajunction_server.config import DatabaseConfig, Settings
    from datajunction_server.internal import seed as seed_module
    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import SQLTranspilationPlugin
    from datajunction_server.utils import get_settings

    get_settings.cache_clear()

    settings = Settings(
        writer_db=DatabaseConfig(uri=db_url),
        reader_db=DatabaseConfig(uri=reader_db_url),
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret=TEST_SECRET,
        transpilation_plugins=["default"],
    )
    # The seed module caches settings at import time.
    seed_module.settings = settings

    for dialect in ("spark", "trino", "druid"):
        register_dialect_plugin(dialect, SQLTranspilationPlugin)

    return settings


def build_mock_query_service_client(column_mappings: dict | None = None) -> Any:
    """
    A query service client that answers from ``column_mappings`` and reports every
    submitted query as finished, so template population never needs a live one.
    """
    from datajunction_server.database.column import Column
    from datajunction_server.database.engine import Engine
    from datajunction_server.models.query import QueryCreate, QueryWithResults
    from datajunction_server.service_clients import QueryServiceClient
    from datajunction_server.typing import QueryState

    mappings = column_mappings or {}
    client = QueryServiceClient(uri="query_service:8001")

    def get_columns_for_table(
        catalog: str,
        schema: str,
        table: str,
        engine: Engine | None = None,
        request_headers: dict[str, str] | None = None,
    ) -> list[Column]:
        return mappings.get(f"{catalog}.{schema}.{table}", [])

    def submit_query(
        query_create: QueryCreate,
        request_headers: dict[str, str] | None = None,
    ) -> QueryWithResults:
        return QueryWithResults(
            id="bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            submitted_query=query_create.submitted_query,
            state=QueryState.FINISHED,
            results=[
                {"columns": [], "rows": [], "sql": query_create.submitted_query},
            ],
            errors=[],
        )

    async def get_columns_for_table_async(
        catalog: str,
        schema: str,
        table: str,
        request_headers: dict[str, str] | None = None,
        engine: Engine | None = None,
    ) -> list[Column]:
        return get_columns_for_table(catalog, schema, table, engine, request_headers)

    async def submit_query_async(
        query_create: QueryCreate,
        request_headers: dict[str, str] | None = None,
    ) -> QueryWithResults:
        return submit_query(query_create, request_headers)

    client.get_columns_for_table = get_columns_for_table_async  # type: ignore[method-assign]
    client.submit_query = submit_query_async  # type: ignore[method-assign]
    return client


async def create_schema(db_url: str) -> None:
    """
    Create every table on a fresh database.

    Importing the app is what registers the models on ``Base.metadata``. Without
    it ``create_all`` silently creates only whichever subset happens to have
    been imported, so tables belonging to newer features go missing and the
    failure surfaces much later as ``relation "..." does not exist``.
    """
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import StaticPool

    import datajunction_server.api.main  # noqa: F401  registers every model
    from datajunction_server.database.base import Base

    engine = create_async_engine(url=db_url, poolclass=StaticPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
            await conn.run_sync(Base.metadata.create_all)
    finally:
        await engine.dispose()


@asynccontextmanager
async def template_app_client(
    db_url: str,
    reader_db_url: str,
    column_mappings: dict | None = None,
) -> AsyncIterator[tuple[Any, httpx.AsyncClient]]:
    """
    Yield ``(session, client)`` for a DJ app bound to ``db_url``.

    The session is the one the app resolves through ``get_session``, so a caller
    can read back whatever its HTTP calls wrote. Dependency overrides and the
    engine are torn down on exit; committing is left to the caller.
    """
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import StaticPool

    from datajunction_server.api.main import app
    from datajunction_server.internal.access.authentication.tokens import create_token
    from datajunction_server.internal.access.authorization import (
        PassthroughAuthorizationService,
        get_authorization_service,
    )
    from datajunction_server.utils import (
        get_query_service_client,
        get_session,
        get_settings,
    )

    settings = build_settings(db_url, reader_db_url)
    query_service_client = build_mock_query_service_client(column_mappings)

    engine = create_async_engine(url=db_url, poolclass=StaticPool)
    session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )

    try:
        async with session_factory() as session:
            app.dependency_overrides[get_session] = lambda: session
            app.dependency_overrides[get_settings] = lambda: settings
            app.dependency_overrides[get_authorization_service] = lambda: (
                PassthroughAuthorizationService()
            )
            app.dependency_overrides[get_query_service_client] = lambda request=None: (
                query_service_client
            )

            jwt_token = create_token(
                {"username": "dj"},
                secret=TEST_SECRET,
                iss=TEST_ISSUER,
                expires_delta=timedelta(hours=24),
            )

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                client.headers.update({"Authorization": f"Bearer {jwt_token}"})
                yield session, client

            app.dependency_overrides.clear()
    finally:
        await engine.dispose()
