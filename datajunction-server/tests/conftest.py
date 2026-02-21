"""
Fixtures for testing.
"""

import asyncio
import subprocess
import sys
from collections import namedtuple
from sqlalchemy.pool import NullPool
from contextlib import ExitStack, asynccontextmanager, contextmanager
from datetime import timedelta
import os
import pathlib
import re
from http.client import HTTPException
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Collection,
    Coroutine,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
)
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import urlparse

from psycopg import connect

import duckdb
import httpx
import sqlglot
import pytest
import pytest_asyncio
from cachelib.simple import SimpleCache
from fastapi import Request
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from httpx import AsyncClient
from pytest_mock import MockerFixture
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from fastapi import BackgroundTasks

from datajunction_server.api.main import app
from datajunction_server.api.attributes import default_attribute_types
from datajunction_server.internal.seed import seed_default_catalogs
from datajunction_server.config import DatabaseConfig, Settings
from datajunction_server.database.base import Base
from datajunction_server.database.column import Column
from datajunction_server.database.engine import Engine
from datajunction_server.database.user import User
from datajunction_server.errors import DJQueryServiceClientEntityNotFound
from datajunction_server.internal.access.authorization import (
    get_authorization_service,
    PassthroughAuthorizationService,
)
from datajunction_server.models.materialization import MaterializationInfo
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.models.user import OAuthProvider
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.typing import QueryState
from datajunction_server.utils import (
    DatabaseSessionManager,
    get_query_service_client,
    get_session,
    get_session_manager,
    get_settings,
)

from .examples import COLUMN_MAPPINGS, EXAMPLES, QUERY_DATA_MAPPINGS, SERVICE_SETUP


def transpile_to_duckdb(sql: str) -> str:
    """
    Transpile SQL from Spark dialect to DuckDB dialect for test execution.
    """
    try:
        # Transpile from Spark to DuckDB
        transpiled = sqlglot.transpile(sql, read="spark", write="duckdb", pretty=True)[
            0
        ]
        print(f"\n=== TRANSPILED SQL ===\n{transpiled}\n=== END ===\n")
        return transpiled
    except Exception as e:
        # If transpilation fails, return original SQL and let DuckDB try
        print(f"\n=== TRANSPILATION FAILED: {e} ===\n{sql}\n=== END ===\n")
        return sql


PostgresCluster = namedtuple("PostgresCluster", ["writer", "reader"])


@pytest.fixture(scope="module")
def module__background_tasks() -> Generator[
    list[tuple[Callable, tuple, dict]],
    None,
    None,
]:
    original_add_task = BackgroundTasks.add_task
    tasks = []

    def fake_add_task(self, func, *args, **kwargs):
        tasks.append((func, args, kwargs))
        return None

    BackgroundTasks.add_task = fake_add_task
    yield tasks
    BackgroundTasks.add_task = original_add_task


@pytest.fixture
def background_tasks() -> Generator[list[tuple[Callable, tuple, dict]], None, None]:
    original_add_task = BackgroundTasks.add_task
    tasks = []

    def fake_add_task(self, func, *args, **kwargs):
        tasks.append((func, args, kwargs))
        return None

    BackgroundTasks.add_task = fake_add_task
    yield tasks
    BackgroundTasks.add_task = original_add_task


@pytest.fixture(scope="module")
def jwt_token() -> str:
    """
    JWT token fixture for testing.
    """
    return create_token(
        {"username": "dj"},
        secret="a-fake-secretkey",
        iss="http://localhost:8000/",
        expires_delta=timedelta(hours=24),
    )


@pytest.fixture(autouse=True)
def _init_cache() -> Generator[Any, Any, None]:
    """
    Initialize FastAPI caching
    """
    FastAPICache.init(InMemoryBackend())
    yield
    FastAPICache.reset()


@pytest_asyncio.fixture
def settings(
    mocker: MockerFixture,
    postgres_container: PostgresContainer,
) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    """
    writer_db = DatabaseConfig(uri=postgres_container.get_connection_url())
    reader_db = DatabaseConfig(
        uri=postgres_container.get_connection_url().replace(
            "dj:dj@",
            "readonly_user:readonly@",
        ),
    )
    settings = Settings(
        writer_db=writer_db,
        reader_db=reader_db,
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service="query_service:8001",
        secret="a-fake-secretkey",
        transpilation_plugins=["default"],
    )

    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import SQLTranspilationPlugin

    register_dialect_plugin("spark", SQLTranspilationPlugin)

    mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

    yield settings


class FuncPostgresContainer:
    """Wrapper that provides function-specific database URL from shared container."""

    def __init__(self, container: PostgresContainer, db_url: str, dbname: str):
        self._container = container
        self._db_url = db_url
        self._dbname = dbname

    def get_connection_url(self) -> str:
        return self._db_url

    def __getattr__(self, name):
        return getattr(self._container, name)


@pytest.fixture
def func__postgres_container(
    request,
    postgres_container: PostgresContainer,
    template_database: str,
) -> Generator[PostgresContainer, None, None]:
    """
    Function-scoped database container - clones from template for each test.
    This provides test isolation while being fast (~100ms per clone vs 60s+ for HTTP loading).
    """
    # Create a unique database name for this test
    test_name = request.node.name
    dbname = f"test_func_{abs(hash(test_name)) % 10000000}_{id(request)}"

    # Clone from template
    db_url = clone_database_from_template(
        postgres_container,
        template_name=template_database,
        target_name=dbname,
    )

    wrapper = FuncPostgresContainer(postgres_container, db_url, dbname)
    yield wrapper  # type: ignore

    # Clean up the test database
    cleanup_database_for_module(postgres_container, dbname)


@pytest.fixture
def func__clean_postgres_container(
    request,
    postgres_container: PostgresContainer,
) -> Generator[PostgresContainer, None, None]:
    """
    Function-scoped CLEAN database container - creates an empty database (no template).
    Use this for tests that need full control over their data and don't want pre-loaded examples.
    """
    # Create a unique database name for this test
    test_name = request.node.name
    dbname = f"test_clean_{abs(hash(test_name)) % 10000000}_{id(request)}"

    # Create a fresh empty database (no template)
    db_url = create_database_for_module(postgres_container, dbname)

    wrapper = FuncPostgresContainer(postgres_container, db_url, dbname)
    yield wrapper  # type: ignore

    # Clean up the test database
    cleanup_database_for_module(postgres_container, dbname)


@pytest_asyncio.fixture
def settings_no_qs(
    mocker: MockerFixture,
    func__postgres_container: PostgresContainer,
) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    Uses the function-scoped database for test isolation.
    """
    writer_db = DatabaseConfig(uri=func__postgres_container.get_connection_url())
    reader_db = DatabaseConfig(
        uri=func__postgres_container.get_connection_url().replace(
            "dj:dj@",
            "readonly_user:readonly@",
        ),
    )
    settings = Settings(
        writer_db=writer_db,
        reader_db=reader_db,
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret="a-fake-secretkey",
        transpilation_plugins=["default"],
    )

    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import SQLTranspilationPlugin

    register_dialect_plugin("spark", SQLTranspilationPlugin)
    register_dialect_plugin("trino", SQLTranspilationPlugin)
    register_dialect_plugin("druid", SQLTranspilationPlugin)

    mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

    yield settings

    # Cleanup is handled by func__postgres_container fixture


@pytest.fixture(scope="session")
def duckdb_conn() -> duckdb.DuckDBPyConnection:
    """
    DuckDB connection fixture with mock roads data loaded.

    Creates a 'default' catalog so that queries like "default".roads.table work.
    """
    with open(
        os.path.join(os.path.dirname(__file__), "duckdb.sql"),
    ) as mock_data:
        with duckdb.connect(
            ":memory:",
        ) as conn:
            # Attach memory database as 'default' catalog so "default".schema.table works
            conn.execute("""ATTACH ':memory:' AS "default" """)
            conn.execute("""USE "default" """)
            conn.execute(mock_data.read())
            yield conn


@pytest.fixture(scope="session")
def postgres_container() -> PostgresContainer:
    """
    Setup a single Postgres container for the entire test session.

    This container hosts:
    1. The 'dj' database (default)
    2. The template database with all examples pre-loaded
    3. Per-module databases cloned from the template
    """
    postgres = PostgresContainer(
        image="postgres:latest",
        username="dj",
        password="dj",
        dbname="dj",
        port=5432,
        driver="psycopg",
    )
    with postgres:
        wait_for_logs(
            postgres,
            r"UTC \[1\] LOG:  database system is ready to accept connections",
            10,
        )
        create_readonly_user(postgres)
        yield postgres


def create_session_factory(postgres_container) -> Awaitable[AsyncSession]:
    """
    Returns a factory function that creates a new AsyncSession each time it is called.
    """
    engine = create_async_engine(
        url=postgres_container.get_connection_url(),
        poolclass=NullPool,
    )

    async def init_db():
        async with engine.begin() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
            await conn.run_sync(Base.metadata.create_all)

    # Make sure DB is initialized once
    # NOTE: Use asyncio.run() instead of get_event_loop().run_until_complete()
    # for Python 3.10+ compatibility. The old method can hang in pytest-xdist
    # due to event loop lifecycle changes in Python 3.10+.
    asyncio.run(init_db())

    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )

    # Return a callable that produces a new session
    async def get_session_factory() -> AsyncSession:
        return async_session_factory()

    # Return the session factory and cleanup
    return get_session_factory  # type: ignore


@pytest_asyncio.fixture
async def session(
    func__postgres_container: PostgresContainer,
) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a Postgres session to test models.

    Uses the function-scoped database container for test isolation.
    Database is cloned from template with all examples pre-loaded.
    """
    engine = create_async_engine(
        url=func__postgres_container.get_connection_url(),
        poolclass=NullPool,  # NullPool avoids lock binding issues across event loops
    )

    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )

    async with async_session_factory() as session:
        session.remove = AsyncMock(return_value=None)
        yield session

    await engine.dispose()
    # Cleanup is handled by func__postgres_container fixture


@pytest_asyncio.fixture
async def clean_session(
    func__clean_postgres_container: PostgresContainer,
) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a Postgres session with an empty database (no pre-loaded examples).

    Use this for tests that need full control over their data state,
    like construction tests that create their own nodes directly.
    """
    # Register dialect plugins
    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import (
        SQLTranspilationPlugin,
        SQLGlotTranspilationPlugin,
    )

    register_dialect_plugin("spark", SQLTranspilationPlugin)
    register_dialect_plugin("trino", SQLTranspilationPlugin)
    register_dialect_plugin("druid", SQLTranspilationPlugin)
    register_dialect_plugin("postgres", SQLGlotTranspilationPlugin)

    engine = create_async_engine(
        url=func__clean_postgres_container.get_connection_url(),
        poolclass=NullPool,  # NullPool avoids lock binding issues across event loops
    )

    # Create tables in the clean database
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        await conn.run_sync(Base.metadata.create_all)

    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )

    async with async_session_factory() as session:
        session.remove = AsyncMock(return_value=None)
        yield session

    await engine.dispose()
    # Cleanup is handled by func__clean_postgres_container fixture


@pytest_asyncio.fixture
def clean_settings_no_qs(
    mocker: MockerFixture,
    func__clean_postgres_container: PostgresContainer,
) -> Iterator[Settings]:
    """
    Custom settings for clean (empty) database tests.
    """
    writer_db = DatabaseConfig(uri=func__clean_postgres_container.get_connection_url())
    reader_db = DatabaseConfig(
        uri=func__clean_postgres_container.get_connection_url().replace(
            "dj:dj@",
            "readonly_user:readonly@",
        ),
    )
    settings = Settings(
        writer_db=writer_db,
        reader_db=reader_db,
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret="a-fake-secretkey",
        transpilation_plugins=["default"],
    )

    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import SQLTranspilationPlugin

    register_dialect_plugin("spark", SQLTranspilationPlugin)
    register_dialect_plugin("trino", SQLTranspilationPlugin)
    register_dialect_plugin("druid", SQLTranspilationPlugin)

    mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

    yield settings


@pytest_asyncio.fixture
async def clean_client(
    request,
    postgres_container: PostgresContainer,
    jwt_token: str,
    background_tasks,
    mocker: MockerFixture,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Create a client with an EMPTY database (no pre-loaded examples).

    Use this for tests that need full control over their data state,
    such as dimension_links tests that use COMPLEX_DIMENSION_LINK data
    which conflicts with the template database.

    NOTE: This fixture manages everything internally to avoid fixture dependency issues.
    """
    use_patch = getattr(request, "param", True)

    # Create a unique database for this test
    test_name = request.node.name
    dbname = f"test_clean_{abs(hash(test_name)) % 10000000}_{id(request)}"
    db_url = create_database_for_module(postgres_container, dbname)

    # Create settings for this clean database
    writer_db = DatabaseConfig(uri=db_url)
    reader_db = DatabaseConfig(
        uri=db_url.replace("dj:dj@", "readonly_user:readonly@"),
    )
    settings = Settings(
        writer_db=writer_db,
        reader_db=reader_db,
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret="a-fake-secretkey",
        transpilation_plugins=["default"],
    )

    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import SQLTranspilationPlugin

    register_dialect_plugin("spark", SQLTranspilationPlugin)
    register_dialect_plugin("trino", SQLTranspilationPlugin)
    register_dialect_plugin("druid", SQLTranspilationPlugin)

    mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

    # Create engine and session
    engine = create_async_engine(
        url=db_url,
        poolclass=NullPool,  # Avoids lock binding issues across event loops
    )

    # Create tables in the clean database
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        await conn.run_sync(Base.metadata.create_all)

    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )

    async with async_session_factory() as session:
        session.remove = AsyncMock(return_value=None)

        # Initialize the empty database with required seed data
        from datajunction_server.api.attributes import default_attribute_types
        from datajunction_server.internal.seed import seed_default_catalogs

        await default_attribute_types(session)
        await seed_default_catalogs(session)
        await create_default_user(session)
        await session.commit()

        def get_session_override() -> AsyncSession:
            return session

        def get_settings_override() -> Settings:
            return settings

        def get_passthrough_auth_service():
            """Override to approve all requests in tests."""
            return PassthroughAuthorizationService()

        if use_patch:
            app.dependency_overrides[get_session] = get_session_override
        app.dependency_overrides[get_settings] = get_settings_override
        app.dependency_overrides[get_authorization_service] = (
            get_passthrough_auth_service
        )

        async with AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as test_client:
            test_client.headers.update({"Authorization": f"Bearer {jwt_token}"})
            test_client.app = app

            # Wrap the request method to run background tasks after each request
            original_request = test_client.request

            async def wrapped_request(method, url, *args, **kwargs):
                response = await original_request(method, url, *args, **kwargs)
                for func, f_args, f_kwargs in background_tasks:
                    result = func(*f_args, **f_kwargs)
                    if asyncio.iscoroutine(result):
                        await result
                background_tasks.clear()
                return response

            test_client.request = wrapped_request
            yield test_client

        app.dependency_overrides.clear()

    await engine.dispose()
    cleanup_database_for_module(postgres_container, dbname)


@pytest.fixture
def query_service_client(
    mocker: MockerFixture,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> Iterator[QueryServiceClient]:
    """
    Custom settings for unit tests.
    """
    qs_client = QueryServiceClient(uri="query_service:8001")

    def mock_get_columns_for_table(
        catalog: str,
        schema: str,
        table: str,
        engine: Optional[Engine] = None,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> List[Column]:
        return COLUMN_MAPPINGS[f"{catalog}.{schema}.{table}"]

    mocker.patch.object(
        qs_client,
        "get_columns_for_table",
        mock_get_columns_for_table,
    )

    def mock_submit_query(
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        # Transpile from Spark to DuckDB before executing
        transpiled_sql = transpile_to_duckdb(query_create.submitted_query)
        result = duckdb_conn.sql(transpiled_sql)
        columns = [
            {
                "name": col,
                "type": str(type_).lower(),
                "semantic_name": col,  # Use column name as semantic name
                "semantic_type": "dimension",  # Default to dimension
            }
            for col, type_ in zip(result.columns, result.types)
        ]
        return QueryWithResults(
            id="bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            submitted_query=query_create.submitted_query,
            state=QueryState.FINISHED,
            results=[
                {
                    "columns": columns,
                    "rows": result.fetchall(),
                    "sql": query_create.submitted_query,
                },
            ],
            errors=[],
        )

    mocker.patch.object(
        qs_client,
        "submit_query",
        mock_submit_query,
    )

    def mock_create_view(
        view_name: str,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> str:
        # Transpile from Spark to DuckDB before executing
        transpiled_sql = transpile_to_duckdb(query_create.submitted_query)
        duckdb_conn.sql(transpiled_sql)
        return f"View {view_name} created successfully."

    mocker.patch.object(
        qs_client,
        "create_view",
        mock_create_view,
    )

    def mock_get_query(
        query_id: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Collection[Collection[str]]:
        if query_id == "foo-bar-baz":
            raise DJQueryServiceClientEntityNotFound("Query foo-bar-baz not found.")
        for _, response in QUERY_DATA_MAPPINGS.items():
            if response.id == query_id:
                return response
        raise RuntimeError(f"No mocked query exists for id {query_id}")

    mocker.patch.object(
        qs_client,
        "get_query",
        mock_get_query,
    )

    mock_materialize = MagicMock()
    mock_materialize.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=["common.a", "common.b"],
    )
    mocker.patch.object(
        qs_client,
        "materialize",
        mock_materialize,
    )

    mock_deactivate_materialization = MagicMock()
    mock_deactivate_materialization.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=[],
    )
    mocker.patch.object(
        qs_client,
        "deactivate_materialization",
        mock_deactivate_materialization,
    )

    mock_get_materialization_info = MagicMock()
    mock_get_materialization_info.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=["common.a", "common.b"],
    )
    mocker.patch.object(
        qs_client,
        "get_materialization_info",
        mock_get_materialization_info,
    )

    mock_run_backfill = MagicMock()
    mock_run_backfill.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=[],
    )
    mocker.patch.object(
        qs_client,
        "run_backfill",
        mock_run_backfill,
    )
    yield qs_client


@pytest.fixture
def session_factory(func__postgres_container) -> Awaitable[AsyncSession]:
    """Function-scoped session factory using the shared function-scoped database."""
    return create_session_factory(func__postgres_container)


@pytest.fixture(scope="module")
def module__session_factory(module__postgres_container) -> Awaitable[AsyncSession]:
    return create_session_factory(module__postgres_container)


@contextmanager
def patch_session_contexts(
    session_factory,
    use_patch: bool = True,
) -> Iterator[None]:
    patch_targets = (
        [
            "datajunction_server.internal.caching.query_cache_manager.session_context",
            "datajunction_server.internal.nodes.session_context",
            "datajunction_server.internal.materializations.session_context",
            "datajunction_server.api.deployments.session_context",
        ]
        if use_patch
        else []
    )

    @asynccontextmanager
    async def fake_session_context(
        request: Request = None,
    ) -> AsyncGenerator[AsyncSession, None]:
        session = await session_factory()
        try:
            yield session
        finally:
            await session.close()

    with ExitStack() as stack:
        for target in patch_targets:
            stack.enter_context(patch(target, fake_session_context))
        yield


@pytest_asyncio.fixture
async def client(
    request,
    session: AsyncSession,
    settings_no_qs: Settings,
    jwt_token: str,
    background_tasks,
    session_factory,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Create a client for testing APIs.

    This is function-scoped for test isolation - each test gets a fresh
    transactional session that rolls back at the end.

    NOTE: The template database already has default attributes, catalogs,
    and user seeded, so we skip those initialization steps.
    """
    use_patch = getattr(request, "param", True)

    # Skip seeding - template database already has everything:
    # - default_attribute_types
    # - seed_default_catalogs
    # - create_default_user

    def get_session_override() -> AsyncSession:
        return session

    def get_settings_override() -> Settings:
        return settings_no_qs

    def get_passthrough_auth_service():
        """Override to approve all requests in tests."""
        return PassthroughAuthorizationService()

    if use_patch:
        app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override
    app.dependency_overrides[get_authorization_service] = get_passthrough_auth_service

    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as test_client:
        with patch_session_contexts(session_factory):
            test_client.headers.update({"Authorization": f"Bearer {jwt_token}"})
            test_client.app = app

            # Wrap the request method to run background tasks after each request
            original_request = test_client.request

            async def wrapped_request(method, url, *args, **kwargs):
                response = await original_request(method, url, *args, **kwargs)
                for func, f_args, f_kwargs in background_tasks:
                    result = func(*f_args, **f_kwargs)
                    if asyncio.iscoroutine(result):
                        await result
                background_tasks.clear()
                return response

            test_client.request = wrapped_request
            yield test_client

    app.dependency_overrides.clear()


async def post_and_raise_if_error(client: AsyncClient, endpoint: str, json: dict):
    """
    Post the payload to the client and raise if there's an error
    """
    response = await client.post(endpoint, json=json)
    if response.status_code not in (200, 201):
        raise HTTPException(response.text)


async def post_and_dont_raise_if_error(client: AsyncClient, endpoint: str, json: dict):
    """
    Post the payload to the client and don't raise if there's an error
    """
    await client.post(endpoint, json=json)


async def load_examples_in_client(
    client: AsyncClient,
    examples_to_load: Optional[List[str]] = None,
):
    """
    Load the DJ client with examples.
    NOTE: Uses post_and_dont_raise_if_error to handle cases where examples
    already exist in the template database.
    """
    # Basic service setup always has to be done (i.e., create catalogs, engines, namespaces etc)
    for endpoint, json in SERVICE_SETUP:
        await post_and_dont_raise_if_error(
            client=client,
            endpoint="http://test" + endpoint,
            json=json,  # type: ignore
        )

    # Load only the selected examples if any are specified
    if examples_to_load is not None:
        for example_name in examples_to_load:
            for endpoint, json in EXAMPLES[example_name]:  # type: ignore
                await post_and_dont_raise_if_error(
                    client=client,
                    endpoint=endpoint,
                    json=json,  # type: ignore
                )
        return client

    # Load all examples if none are specified
    for example_name, examples in EXAMPLES.items():
        for endpoint, json in examples:  # type: ignore
            await post_and_dont_raise_if_error(
                client=client,
                endpoint=endpoint,
                json=json,  # type: ignore
            )
    return client


@pytest_asyncio.fixture
async def client_example_loader(
    client: AsyncClient,
) -> Callable[[list[str] | None], Coroutine[Any, Any, AsyncClient]]:
    """
    Provides a callable fixture for loading examples into a DJ client.

    NOTE: Since function-scoped fixtures now use the module's database which
    has all examples pre-loaded from the template, we just return the client
    without loading any examples.
    """

    async def _load_examples(examples_to_load: Optional[List[str]] = None):
        # Examples are already loaded in the template database
        return client

    return _load_examples


@pytest_asyncio.fixture
async def client_with_examples(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with all examples
    """
    return await client_example_loader(None)


@pytest_asyncio.fixture
async def client_with_service_setup(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with just the service setup
    """
    return await client_example_loader([])


@pytest_asyncio.fixture
async def client_with_roads(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with roads examples
    """
    return await client_example_loader(["ROADS"])


@pytest_asyncio.fixture
async def client_with_namespaced_roads(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with namespaced roads examples
    """
    return await client_example_loader(["NAMESPACED_ROADS"])


@pytest_asyncio.fixture
async def client_with_basic(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with basic examples
    """
    return await client_example_loader(["BASIC"])


@pytest_asyncio.fixture
async def client_with_account_revenue(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with account revenue examples
    """
    return await client_example_loader(["ACCOUNT_REVENUE"])


@pytest_asyncio.fixture
async def client_with_event(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with event examples
    """
    return await client_example_loader(["EVENT"])


@pytest_asyncio.fixture
async def client_with_dbt(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with dbt examples
    """
    return await client_example_loader(["DBT"])


@pytest_asyncio.fixture
async def client_with_build_v3(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with BUILD_V3 examples.
    This is the comprehensive test model for V3 SQL generation, including:
    - Multi-hop dimension traversal with roles
    - Dimension hierarchies (date, location)
    - Cross-fact derived metrics (orders + page_views)
    - Period-over-period metrics (window functions)
    - Multiple aggregability levels
    """
    return await client_example_loader(["BUILD_V3"])


def compare_parse_trees(tree1, tree2):
    """
    Recursively compare two ANTLR parse trees for equality.
    """
    # Check if the node types are the same
    if type(tree1) is not type(tree2):
        return False

    # Check if the node texts are the same
    if tree1.getText() is not tree2.getText():
        return False

    # Check if the number of child nodes is the same
    if tree1.getChildCount() != tree2.getChildCount():
        return False

    # Recursively compare child nodes
    for i in range(tree1.getChildCount()):
        child1 = tree1.getChild(i)
        child2 = tree2.getChild(i)
        if not compare_parse_trees(child1, child2):
            return False

    # If all checks passed, the trees are equal
    return True


COMMENT = re.compile(r"(--.*)|(/\*[\s\S]*?\*/)")
TRAILING_ZEROES = re.compile(r"(\d+\.\d*?[1-9])0+|\b(\d+)\.0+\b")
DIFF_IGNORE = re.compile(r"[\';\s]+")


def compare_query_strings(str1, str2):
    """
    Recursively compare two ANTLR parse trees for equality, ignoring certain elements.
    """

    str1 = DIFF_IGNORE.sub("", TRAILING_ZEROES.sub("", COMMENT.sub("", str1))).upper()
    str2 = DIFF_IGNORE.sub("", TRAILING_ZEROES.sub("", COMMENT.sub("", str2))).upper()

    return str1 == str2


@pytest.fixture
def compare_query_strings_fixture():
    """
    Fixture for comparing two query strings.
    """
    return compare_query_strings


@pytest_asyncio.fixture
async def client_qs(
    session: AsyncSession,
    settings: Settings,
    query_service_client: QueryServiceClient,
    mocker: MockerFixture,
    session_factory: AsyncSession,
    jwt_token: str,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Create a client for testing APIs.
    """
    # Use on_conflict_do_nothing to handle case where user already exists in template
    statement = (
        insert(User)
        .values(
            username="dj",
            email=None,
            name=None,
            oauth_provider="basic",
            is_admin=False,
        )
        .on_conflict_do_nothing(index_elements=["username"])
    )
    await session.execute(statement)
    await default_attribute_types(session)
    await seed_default_catalogs(session)

    def get_query_service_client_override(
        request: Request = None,
    ) -> QueryServiceClient:
        return query_service_client

    def get_settings_override() -> Settings:
        return settings

    def get_passthrough_auth_service():
        """Override to approve all requests in tests."""
        return PassthroughAuthorizationService()

    def get_session_override() -> AsyncSession:
        return session

    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override
    app.dependency_overrides[get_authorization_service] = get_passthrough_auth_service
    app.dependency_overrides[get_query_service_client] = (
        get_query_service_client_override
    )

    with mocker.patch(
        "datajunction_server.api.materializations.get_query_service_client",
        return_value=query_service_client,
    ):
        async with AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as test_client:
            with patch_session_contexts(session_factory):
                test_client.headers.update(
                    {
                        "Authorization": f"Bearer {jwt_token}",
                    },
                )
                test_client.app = app
                yield test_client

        app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def client_with_query_service_example_loader(
    client_qs: AsyncClient,
) -> Callable[[Optional[List[str]]], AsyncClient]:
    """
    Provides a callable fixture for loading examples into a test client
    fixture that additionally has a mocked query service.
    """

    def _load_examples(examples_to_load: Optional[List[str]] = None):
        return load_examples_in_client(client_qs, examples_to_load)

    return _load_examples


@pytest_asyncio.fixture
async def client_with_query_service(
    client_with_query_service_example_loader: Callable[
        [Optional[List[str]]],
        AsyncClient,
    ],
) -> AsyncClient:
    """
    Client with query service and all examples loaded.
    """
    return await client_with_query_service_example_loader(None)


def pytest_addoption(parser):
    """
    Add flags that enable groups of tests
    """
    parser.addoption(
        "--tpcds",
        action="store_true",
        dest="tpcds",
        default=False,
        help="include tests for parsing TPC-DS queries",
    )

    parser.addoption(
        "--auth",
        action="store_true",
        dest="auth",
        default=False,
        help="Run authentication tests",
    )


@pytest_asyncio.fixture(scope="module")
async def module__client_example_loader(
    module__client: AsyncClient,
) -> Callable[[list[str] | None], Coroutine[Any, Any, AsyncClient]]:
    """
    Provides a callable fixture for loading examples into a DJ client.

    NOTE: Examples are already loaded in the template database that was cloned,
    so this just returns the client directly.
    """

    async def _load_examples(examples_to_load: Optional[List[str]] = None):
        # Examples already loaded in template - just return the client
        return module__client

    return _load_examples


@pytest.fixture(scope="session")
def session_manager_per_worker():
    """
    Create a unique session manager per pytest-xdist worker.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    db_suffix = f"test_{worker_id}"

    settings = get_settings()
    settings.writer_db.uri = settings.writer_db.uri.replace("test", db_suffix)

    manager = DatabaseSessionManager()
    manager.init_db()
    yield manager
    asyncio.run(manager.close())


async def create_default_user(session: AsyncSession) -> User:
    """
    A user fixture.
    """
    new_user = User(
        username="dj",
        password="dj",
        email="dj@datajunction.io",
        name="DJ",
        oauth_provider=OAuthProvider.BASIC,
        is_admin=False,
    )
    existing_user = await User.get_by_username(session, new_user.username)
    if not existing_user:
        session.add(new_user)
        await session.commit()
        user = new_user
    else:
        user = existing_user
    await session.refresh(user)
    return user


@pytest_asyncio.fixture
async def default_user(session: AsyncSession):
    """
    Create a default user for testing.
    """
    user = await create_default_user(session)
    yield user


@pytest_asyncio.fixture(scope="module")
async def module__client(
    request,
    module__session: AsyncSession,
    module__settings: Settings,
    module__query_service_client: QueryServiceClient,
    module_mocker: MockerFixture,
    jwt_token: str,
    module__session_factory: AsyncSession,
    module__background_tasks: list[tuple[Callable, tuple, dict]],
) -> AsyncGenerator[AsyncClient, None]:
    """
    Create a client for testing APIs.

    NOTE: The database is cloned from a template that already has:
    - Default attribute types
    - Default catalogs
    - Default user
    - All examples pre-loaded
    So we skip those initialization steps.
    """
    # Clear caches to prevent stale database connections (important for CI)
    app.dependency_overrides.clear()
    get_settings.cache_clear()
    get_session_manager.cache_clear()

    use_patch = getattr(request, "param", True)

    # NOTE: Skip these - already in template:
    # await default_attribute_types(module__session)
    # await seed_default_catalogs(module__session)
    # await create_default_user(module__session)

    def get_query_service_client_override(
        request: Request = None,
    ) -> QueryServiceClient:
        return module__query_service_client

    def get_session_override() -> AsyncSession:
        return module__session

    def get_settings_override() -> Settings:
        return module__settings

    def get_passthrough_auth_service():
        """Override to approve all requests in tests."""
        return PassthroughAuthorizationService()

    module_mocker.patch(
        "datajunction_server.api.materializations.get_query_service_client",
        get_query_service_client_override,
    )

    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override
    app.dependency_overrides[get_authorization_service] = get_passthrough_auth_service
    app.dependency_overrides[get_query_service_client] = (
        get_query_service_client_override
    )

    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as test_client:
        with patch_session_contexts(module__session_factory, use_patch=use_patch):
            test_client.headers.update({"Authorization": f"Bearer {jwt_token}"})
            test_client.app = app

            # Wrap the request method to run background tasks after each request
            original_request = test_client.request

            async def wrapped_request(method, url, *args, **kwargs):
                response = await original_request(method, url, *args, **kwargs)
                for func, f_args, f_kwargs in module__background_tasks:
                    result = func(*f_args, **f_kwargs)
                    if asyncio.iscoroutine(result):
                        await result
                module__background_tasks.clear()
                return response

            test_client.request = wrapped_request
            yield test_client

    app.dependency_overrides.clear()


@pytest_asyncio.fixture(scope="module")
async def module__session(
    module__postgres_container: PostgresContainer,
) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a Postgres session to test models.

    NOTE: The database is cloned from a template that already has all tables
    and examples loaded, so we skip table creation.
    """
    engine = create_async_engine(
        url=module__postgres_container.get_connection_url(),
        poolclass=NullPool,  # Avoids lock binding issues across event loops
    )
    # NOTE: Skip table creation - tables already exist from template clone

    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )
    async with async_session_factory() as session:
        session.remove = AsyncMock(return_value=None)
        yield session

    # NOTE: Skip dropping tables - entire database is dropped by cleanup

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()


@pytest_asyncio.fixture(scope="module")
def module__settings(
    module_mocker: MockerFixture,
    module__postgres_container: PostgresContainer,
) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    """
    writer_db = DatabaseConfig(uri=module__postgres_container.get_connection_url())
    reader_db = DatabaseConfig(
        uri=module__postgres_container.get_connection_url().replace(
            "dj:dj@",
            "readonly_user:readonly@",
        ),
    )
    settings = Settings(
        writer_db=writer_db,
        reader_db=reader_db,
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret="a-fake-secretkey",
        transpilation_plugins=["default"],
    )

    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import (
        SQLTranspilationPlugin,
        SQLGlotTranspilationPlugin,
    )
    from datajunction_server.internal import seed as seed_module

    register_dialect_plugin("spark", SQLTranspilationPlugin)
    register_dialect_plugin("trino", SQLTranspilationPlugin)
    register_dialect_plugin("druid", SQLTranspilationPlugin)
    register_dialect_plugin("postgres", SQLGlotTranspilationPlugin)

    module_mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )
    # Also patch the cached settings in seed module
    seed_module.settings = settings

    yield settings


@pytest_asyncio.fixture(scope="module")
def regular_settings(
    module_mocker: MockerFixture,
    module__postgres_container: PostgresContainer,
) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    """
    writer_db = DatabaseConfig(uri=module__postgres_container.get_connection_url())
    settings = Settings(
        writer_db=writer_db,
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret="a-fake-secretkey",
    )

    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import SQLGlotTranspilationPlugin

    register_dialect_plugin("spark", SQLGlotTranspilationPlugin)
    register_dialect_plugin("trino", SQLGlotTranspilationPlugin)
    register_dialect_plugin("druid", SQLGlotTranspilationPlugin)

    module_mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

    yield settings


@pytest_asyncio.fixture(scope="module")
async def module__client_with_dimension_link(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with dbt examples
    """
    return await module__client_example_loader(["DIMENSION_LINK"])


@pytest_asyncio.fixture(scope="module")
async def module__client_with_roads(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with roads examples
    """
    return await module__client_example_loader(["ROADS"])


@pytest_asyncio.fixture(scope="module")
async def module__client_with_namespaced_roads(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with roads examples
    """
    return await module__client_example_loader(["NAMESPACED_ROADS"])


@pytest_asyncio.fixture(scope="module")
async def module__client_with_account_revenue(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with account revenue examples
    """
    return await module__client_example_loader(["ACCOUNT_REVENUE"])


@pytest_asyncio.fixture(scope="module")
async def module__client_with_roads_and_acc_revenue(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with roads examples
    """
    return await module__client_example_loader(["ROADS", "ACCOUNT_REVENUE"])


@pytest_asyncio.fixture(scope="module")
async def module__client_with_basic(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with account revenue examples
    """
    return await module__client_example_loader(["BASIC"])


@pytest_asyncio.fixture(scope="module")
async def module__client_with_simple_hll(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a minimal DJ client fixture for HLL/APPROX_COUNT_DISTINCT testing.
    """
    return await module__client_example_loader(["SIMPLE_HLL"])


@pytest_asyncio.fixture(scope="module")
async def module__client_with_both_basics(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with account revenue examples
    """
    return await module__client_example_loader(["BASIC", "BASIC_IN_DIFFERENT_CATALOG"])


@pytest_asyncio.fixture(scope="module")
async def module__client_with_examples(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with all examples
    """
    return await module__client_example_loader(None)


@pytest_asyncio.fixture(scope="module")
async def module__client_with_build_v3(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a module-scoped DJ client fixture with BUILD_V3 examples.
    This is the comprehensive test model for V3 SQL generation, including:
    - Multi-hop dimension traversal with roles
    - Dimension hierarchies (date, location)
    - Cross-fact derived metrics (orders + page_views)
    - Period-over-period metrics (window functions)
    - Multiple aggregability levels
    """
    return await module__client_example_loader(["BUILD_V3"])


@pytest_asyncio.fixture(scope="module")
async def module__clean_client(
    request,
    postgres_container: PostgresContainer,
    module_mocker: MockerFixture,
    module__background_tasks,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Module-scoped client with a CLEAN database (no pre-loaded examples).

    Use this for test modules that need full control over their data state,
    such as dimension_links tests that use COMPLEX_DIMENSION_LINK data
    which conflicts with the template database.
    """
    # Create a unique database for this module
    module_name = request.module.__name__
    dbname = f"test_mod_clean_{abs(hash(module_name)) % 10000000}"
    db_url = create_database_for_module(postgres_container, dbname)

    # Create settings for this clean database
    writer_db = DatabaseConfig(uri=db_url)
    reader_db = DatabaseConfig(
        uri=db_url.replace("dj:dj@", "readonly_user:readonly@"),
    )
    settings = Settings(
        writer_db=writer_db,
        reader_db=reader_db,
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret="a-fake-secretkey",
        transpilation_plugins=["default"],
    )

    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import SQLTranspilationPlugin

    register_dialect_plugin("spark", SQLTranspilationPlugin)
    register_dialect_plugin("trino", SQLTranspilationPlugin)
    register_dialect_plugin("druid", SQLTranspilationPlugin)

    module_mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

    # Create engine and session
    engine = create_async_engine(
        url=db_url,
        poolclass=NullPool,  # Avoids lock binding issues across event loops
    )

    # Create tables in the clean database
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        await conn.run_sync(Base.metadata.create_all)

    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )

    async with async_session_factory() as session:
        session.remove = AsyncMock(return_value=None)

        # Initialize the empty database with required seed data
        from datajunction_server.api.attributes import default_attribute_types
        from datajunction_server.internal.seed import seed_default_catalogs

        await default_attribute_types(session)
        await seed_default_catalogs(session)
        await create_default_user(session)
        await session.commit()

        def get_session_override() -> AsyncSession:
            return session

        def get_settings_override() -> Settings:
            return settings

        def get_passthrough_auth_service():
            """Override to approve all requests in tests."""
            return PassthroughAuthorizationService()

        app.dependency_overrides[get_session] = get_session_override
        app.dependency_overrides[get_settings] = get_settings_override
        app.dependency_overrides[get_authorization_service] = (
            get_passthrough_auth_service
        )

        # Create JWT token
        jwt_token = create_token(
            {"username": "dj"},
            secret="a-fake-secretkey",
            iss="http://localhost:8000/",
            expires_delta=timedelta(hours=24),
        )

        async with AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as test_client:
            test_client.headers.update({"Authorization": f"Bearer {jwt_token}"})
            test_client.app = app

            # Wrap the request method to run background tasks after each request
            original_request = test_client.request

            async def wrapped_request(method, url, *args, **kwargs):
                response = await original_request(method, url, *args, **kwargs)
                for func, f_args, f_kwargs in module__background_tasks:
                    result = func(*f_args, **f_kwargs)
                    if asyncio.iscoroutine(result):
                        await result
                module__background_tasks.clear()
                return response

            test_client.request = wrapped_request
            yield test_client

        app.dependency_overrides.clear()

    await engine.dispose()
    cleanup_database_for_module(postgres_container, dbname)


@pytest_asyncio.fixture
async def isolated_client(
    request,
    postgres_container: PostgresContainer,
    mocker: MockerFixture,
    background_tasks,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Function-scoped client with a CLEAN database (no template, no pre-loaded examples).

    Use this for tests that need complete isolation and will load their own data.
    Each test function gets its own fresh database that is cleaned up after.
    """
    # Clear any stale overrides and caches from previous tests
    app.dependency_overrides.clear()
    get_settings.cache_clear()
    get_session_manager.cache_clear()  # Clear the cached DatabaseSessionManager

    # Create a unique database for this test function
    test_name = request.node.name
    dbname = f"test_isolated_{abs(hash(test_name)) % 10000000}_{id(request)}"
    db_url = create_database_for_module(postgres_container, dbname)

    # Create settings for this clean database
    writer_db = DatabaseConfig(uri=db_url)
    reader_db = DatabaseConfig(
        uri=db_url.replace("dj:dj@", "readonly_user:readonly@"),
    )
    settings = Settings(
        writer_db=writer_db,
        reader_db=reader_db,
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret="a-fake-secretkey",
        transpilation_plugins=["default"],
    )

    from datajunction_server.models.dialect import register_dialect_plugin
    from datajunction_server.transpilation import SQLTranspilationPlugin

    register_dialect_plugin("spark", SQLTranspilationPlugin)
    register_dialect_plugin("trino", SQLTranspilationPlugin)
    register_dialect_plugin("druid", SQLTranspilationPlugin)

    mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

    # Create engine and session
    engine = create_async_engine(
        url=db_url,
        poolclass=NullPool,  # Avoids lock binding issues across event loops
    )

    # Create tables in the clean database
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        await conn.run_sync(Base.metadata.create_all)

    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )

    async with async_session_factory() as session:
        session.remove = AsyncMock(return_value=None)

        # Initialize the empty database with required seed data
        from datajunction_server.api.attributes import default_attribute_types
        from datajunction_server.internal.seed import seed_default_catalogs

        await default_attribute_types(session)
        await seed_default_catalogs(session)
        await create_default_user(session)
        await session.commit()

        def get_session_override() -> AsyncSession:
            return session

        def get_settings_override() -> Settings:
            return settings

        def get_passthrough_auth_service():
            """Override to approve all requests in tests."""
            return PassthroughAuthorizationService()

        app.dependency_overrides[get_session] = get_session_override
        app.dependency_overrides[get_settings] = get_settings_override
        app.dependency_overrides[get_authorization_service] = (
            get_passthrough_auth_service
        )

        # Create JWT token
        jwt_token = create_token(
            {"username": "dj"},
            secret="a-fake-secretkey",
            iss="http://localhost:8000/",
            expires_delta=timedelta(hours=24),
        )

        async with AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as test_client:
            test_client.headers.update({"Authorization": f"Bearer {jwt_token}"})
            test_client.app = app

            # Wrap the request method to run background tasks after each request
            original_request = test_client.request

            async def wrapped_request(method, url, *args, **kwargs):
                response = await original_request(method, url, *args, **kwargs)
                for func, f_args, f_kwargs in background_tasks:
                    result = func(*f_args, **f_kwargs)
                    if asyncio.iscoroutine(result):
                        await result
                background_tasks.clear()
                return response

            test_client.request = wrapped_request
            yield test_client

        app.dependency_overrides.clear()

    await engine.dispose()
    cleanup_database_for_module(postgres_container, dbname)


def create_readonly_user(postgres: PostgresContainer):
    """
    Create a read-only user in the Postgres container.
    """
    url = urlparse(postgres.get_connection_url())
    with connect(
        host=url.hostname,
        port=url.port,
        dbname=url.path.lstrip("/"),
        user=url.username,
        password=url.password,
        autocommit=True,
    ) as conn:
        # Create read-only user
        conn.execute("DROP ROLE IF EXISTS readonly_user")
        conn.execute("CREATE ROLE readonly_user WITH LOGIN PASSWORD 'readonly'")

        # Create dj if it doesn't exist
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'dj'")
            if not cur.fetchone():
                cur.execute("CREATE DATABASE dj")

        # Grant permissions to readonly_user
        conn.execute("GRANT CONNECT ON DATABASE dj TO readonly_user")
        conn.execute("GRANT USAGE ON SCHEMA public TO readonly_user")
        conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user")
        conn.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user",
        )


def create_database_for_module(postgres: PostgresContainer, dbname: str) -> str:
    """
    Create a new database within the shared postgres container for module isolation.
    Returns the connection URL for the new database.
    """
    url = urlparse(postgres.get_connection_url())

    with connect(
        host=url.hostname,
        port=url.port,
        dbname=url.path.lstrip("/"),
        user=url.username,
        password=url.password,
        autocommit=True,
    ) as conn:
        conn.execute(
            f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{dbname}'
            AND pid <> pg_backend_pid()
            """,
        )
        conn.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        conn.execute(f'CREATE DATABASE "{dbname}"')
        conn.execute(f'GRANT CONNECT ON DATABASE "{dbname}" TO readonly_user')

    with connect(
        host=url.hostname,
        port=url.port,
        dbname=dbname,
        user=url.username,
        password=url.password,
        autocommit=True,
    ) as conn:
        conn.execute("GRANT USAGE ON SCHEMA public TO readonly_user")
        conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user")
        conn.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user",
        )

    base_url = postgres.get_connection_url()
    return base_url.rsplit("/", 1)[0] + f"/{dbname}"


def cleanup_database_for_module(postgres: PostgresContainer, dbname: str) -> None:
    """Drop the database after module tests are complete."""
    url = urlparse(postgres.get_connection_url())
    with connect(
        host=url.hostname,
        port=url.port,
        dbname=url.path.lstrip("/"),
        user=url.username,
        password=url.password,
        autocommit=True,
    ) as conn:
        conn.execute(
            f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{dbname}'
            AND pid <> pg_backend_pid()
            """,
        )
        conn.execute(f'DROP DATABASE IF EXISTS "{dbname}"')


def clone_database_from_template(
    postgres: PostgresContainer,
    template_name: str,
    target_name: str,
) -> str:
    """
    Clone a database from a template. This is MUCH faster than creating
    an empty database and loading data via HTTP (~100ms vs ~30-60s).
    """
    url = urlparse(postgres.get_connection_url())

    with connect(
        host=url.hostname,
        port=url.port,
        dbname=url.path.lstrip("/"),
        user=url.username,
        password=url.password,
        autocommit=True,
    ) as conn:
        conn.execute(
            f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{target_name}'
            AND pid <> pg_backend_pid()
            """,
        )
        conn.execute(f'DROP DATABASE IF EXISTS "{target_name}"')
        conn.execute(
            f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{template_name}'
            AND pid <> pg_backend_pid()
            """,
        )
        conn.execute(f'CREATE DATABASE "{target_name}" TEMPLATE "{template_name}"')
        conn.execute(f'GRANT CONNECT ON DATABASE "{target_name}" TO readonly_user')

    with connect(
        host=url.hostname,
        port=url.port,
        dbname=target_name,
        user=url.username,
        password=url.password,
        autocommit=True,
    ) as conn:
        conn.execute("GRANT USAGE ON SCHEMA public TO readonly_user")
        conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user")
        conn.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user",
        )

    base_url = postgres.get_connection_url()
    return base_url.rsplit("/", 1)[0] + f"/{target_name}"


TEMPLATE_DB_NAME = "template_all_examples"


def _populate_template_via_subprocess(template_url: str) -> None:
    """Run template population in a subprocess."""
    script_path = pathlib.Path(__file__).parent / "helpers" / "populate_template.py"
    # Ensure the subprocess uses the local development version of datajunction_server
    project_root = pathlib.Path(__file__).parent.parent
    env = os.environ.copy()
    # Prepend the local source to PYTHONPATH so it takes precedence over site-packages
    env["PYTHONPATH"] = str(project_root) + os.pathsep + env.get("PYTHONPATH", "")

    result = subprocess.run(
        [sys.executable, str(script_path), template_url],
        capture_output=True,
        text=True,
        cwd=str(project_root),
        env=env,
    )
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        raise RuntimeError(f"Failed to populate template: {result.stderr}")
    print(result.stdout)


@pytest.fixture(scope="session")
def template_database(postgres_container: PostgresContainer) -> str:
    """
    Session-scoped fixture that creates a template database with ALL examples.
    This runs ONCE per test session and then each module clones from it.
    """
    template_url = create_database_for_module(postgres_container, TEMPLATE_DB_NAME)
    _populate_template_via_subprocess(template_url)
    return TEMPLATE_DB_NAME


@pytest.fixture(scope="module")
def module__postgres_container(
    request,
    postgres_container: PostgresContainer,
    template_database: str,
) -> PostgresContainer:
    """
    Provides module-level database isolation by CLONING from the template.
    Each module gets its own database cloned from the template with all examples.
    """
    path = pathlib.Path(request.module.__file__).resolve()
    dbname = f"test_mod_{abs(hash(path)) % 10000000}"

    module_db_url = clone_database_from_template(
        postgres_container,
        template_name=template_database,
        target_name=dbname,
    )

    class ModulePostgresContainer:
        def __init__(self, container: PostgresContainer, db_url: str):
            self._container = container
            self._db_url = db_url

        def get_connection_url(self) -> str:
            return self._db_url

        def __getattr__(self, name):
            return getattr(self._container, name)

    wrapper = ModulePostgresContainer(postgres_container, module_db_url)
    yield wrapper  # type: ignore

    cleanup_database_for_module(postgres_container, dbname)


@pytest.fixture(scope="module")
def module__query_service_client(
    module_mocker: MockerFixture,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> Iterator[QueryServiceClient]:
    """
    Custom settings for unit tests.
    """
    qs_client = QueryServiceClient(uri="query_service:8001")

    def mock_get_columns_for_table(
        catalog: str,
        schema: str,
        table: str,
        engine: Optional[Engine] = None,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> List[Column]:
        return COLUMN_MAPPINGS[f"{catalog}.{schema}.{table}"]

    module_mocker.patch.object(
        qs_client,
        "get_columns_for_table",
        mock_get_columns_for_table,
    )

    def mock_submit_query(
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        # Transpile from Spark to DuckDB before executing
        transpiled_sql = transpile_to_duckdb(query_create.submitted_query)
        result = duckdb_conn.sql(transpiled_sql)
        columns = [
            {
                "name": col,
                "type": str(type_).lower(),
                "semantic_name": col,  # Use column name as semantic name
                "semantic_type": "dimension",  # Default to dimension
            }
            for col, type_ in zip(result.columns, result.types)
        ]
        rows = result.fetchall()
        print(
            f"\n=== QUERY RESULT ===\nColumns: {columns}\nRows: {rows}\n=== END ===\n",
        )
        return QueryWithResults(
            id="bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            submitted_query=query_create.submitted_query,
            state=QueryState.FINISHED,
            results=[
                {
                    "columns": columns,
                    "rows": rows,
                    "sql": query_create.submitted_query,
                },
            ],
            errors=[],
        )

    module_mocker.patch.object(
        qs_client,
        "submit_query",
        mock_submit_query,
    )

    def mock_create_view(
        view_name: str,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> str:
        # Transpile from Spark to DuckDB before executing
        transpiled_sql = transpile_to_duckdb(query_create.submitted_query)
        duckdb_conn.sql(transpiled_sql)
        return f"View {view_name} created successfully."

    module_mocker.patch.object(
        qs_client,
        "create_view",
        mock_create_view,
    )

    def mock_get_query(
        query_id: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Collection[Collection[str]]:
        if query_id == "foo-bar-baz":
            raise DJQueryServiceClientEntityNotFound("Query foo-bar-baz not found.")
        for _, response in QUERY_DATA_MAPPINGS.items():
            if response.id == query_id:
                return response
        raise RuntimeError(f"No mocked query exists for id {query_id}")

    module_mocker.patch.object(
        qs_client,
        "get_query",
        mock_get_query,
    )

    mock_materialize = MagicMock()
    mock_materialize.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=["common.a", "common.b"],
    )
    module_mocker.patch.object(
        qs_client,
        "materialize",
        mock_materialize,
    )

    mock_materialize_cube = MagicMock()
    mock_materialize_cube.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=["common.a", "common.b"],
    )
    module_mocker.patch.object(
        qs_client,
        "materialize_cube",
        mock_materialize_cube,
    )

    mock_deactivate_materialization = MagicMock()
    mock_deactivate_materialization.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=[],
    )
    module_mocker.patch.object(
        qs_client,
        "deactivate_materialization",
        mock_deactivate_materialization,
    )

    mock_get_materialization_info = MagicMock()
    mock_get_materialization_info.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=["common.a", "common.b"],
    )
    module_mocker.patch.object(
        qs_client,
        "get_materialization_info",
        mock_get_materialization_info,
    )

    mock_run_backfill = MagicMock()
    mock_run_backfill.return_value = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=[],
    )
    module_mocker.patch.object(
        qs_client,
        "run_backfill",
        mock_run_backfill,
    )
    yield qs_client


@pytest_asyncio.fixture(scope="module")
async def module__client_with_all_examples(
    module__client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
) -> AsyncClient:
    """
    Provides a DJ client fixture with all examples
    """
    return await module__client_example_loader(None)


@pytest_asyncio.fixture
async def current_user(session: AsyncSession) -> User:
    """
    A user fixture.
    """

    new_user = User(
        username="dj",
        password="dj",
        email="dj@datajunction.io",
        name="DJ",
        oauth_provider=OAuthProvider.BASIC,
        is_admin=False,
    )
    existing_user = await User.get_by_username(session, new_user.username)
    if not existing_user:
        session.add(new_user)
        await session.commit()
        user = new_user
    else:
        user = existing_user
    return user


@pytest_asyncio.fixture
async def clean_current_user(clean_session: AsyncSession) -> User:
    """
    A user fixture for clean database tests.
    Creates a user in the clean (empty) database.
    """
    new_user = User(
        username="dj",
        password="dj",
        email="dj@datajunction.io",
        name="DJ",
        oauth_provider=OAuthProvider.BASIC,
        is_admin=False,
    )
    clean_session.add(new_user)
    await clean_session.commit()
    await clean_session.refresh(new_user)
    return new_user
