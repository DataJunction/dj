"""
Fixtures for testing DJ client.
"""

# pylint: disable=redefined-outer-name, invalid-name, W0611
import asyncio
from contextlib import ExitStack, asynccontextmanager, contextmanager
from datetime import timedelta
import os
from http.client import HTTPException
from pathlib import Path
from typing import AsyncGenerator, Awaitable, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from cachelib import SimpleCache
from datajunction_server.api.main import create_app
from datajunction_server.config import Settings, DatabaseConfig
from datajunction_server.database.base import Base
from datajunction_server.database.column import Column
from datajunction_server.database.engine import Engine
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.models.materialization import MaterializationInfo
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.typing import QueryState
from datajunction_server.utils import (
    get_query_service_client,
    get_session,
    get_settings,
)
from fastapi import FastAPI, Request
from pytest_mock import MockerFixture
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool, NullPool
from starlette.testclient import TestClient
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from datajunction import DJBuilder
from tests.examples import COLUMN_MAPPINGS, EXAMPLES, QUERY_DATA_MAPPINGS


def post_and_raise_if_error(server: TestClient, endpoint: str, json: dict):
    """
    Post the payload to the client and raise if there's an error
    """
    response = server.post(endpoint, json=json)
    if not response.status_code < 400:
        raise HTTPException(response.text)


@pytest.fixture
def change_to_project_dir(request):
    """
    Returns a function that changes to a specified project directory
    only for a single test. At the end of the test, this will change back
    to the tests directory to prevent any side-effects.
    """

    def _change_to_project_dir(project: str):
        """
        Changes to the directory for a specific example project
        """
        os.chdir(os.path.join(request.fspath.dirname, "examples", project))

    try:
        yield _change_to_project_dir
    finally:
        os.chdir(request.config.invocation_params.dir)


@pytest.fixture
def change_to_package_root_dir(request):
    """
    Changes to the datajunction package root dir only for a single test
    At the end of the test, this will change back
    to the tests directory to prevent any side-effects.
    """
    try:
        os.chdir(Path(request.fspath.dirname).parent)
    finally:
        os.chdir(request.config.invocation_params.dir)


def pytest_addoption(parser):
    """
    Add flags
    """
    parser.addoption(
        "--integration",
        action="store_true",
        dest="integration",
        default=False,
        help="Run integration tests",
    )


#
# Module scope fixtures
#
@pytest.fixture(scope="module")
def module__settings(module_mocker: MockerFixture) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    """
    writer_db = DatabaseConfig(uri="sqlite://")
    settings = Settings(
        writer_db=writer_db,
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

    yield settings


@pytest.fixture(scope="module")
def event_loop():
    """
    This fixture is OK because we are pinning the pytest_asyncio to 0.21.x.
    When they fix https://github.com/pytest-dev/pytest-asyncio/issues/718
    we can remove the pytest_asyncio pin and remove this fixture.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="module")
async def module__session(
    module__postgres_container: PostgresContainer,
) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a Postgres session to test models.
    """
    engine = create_async_engine(
        url=module__postgres_container.get_connection_url(),
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        await conn.run_sync(Base.metadata.create_all)
    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )
    async with async_session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()


@pytest.fixture(scope="module")
def module__query_service_client(
    module_mocker: MockerFixture,
) -> Iterator[QueryServiceClient]:
    """
    Custom settings for unit tests.
    """
    qs_client = QueryServiceClient(uri="query_service:8001")
    qs_client.query_state = QueryState.RUNNING  # type: ignore

    def mock_get_columns_for_table(
        catalog: str,
        schema: str,
        table: str,
        engine: Optional[Engine] = None,  # pylint: disable=unused-argument
        request_headers: Optional[  # pylint: disable=unused-argument
            Dict[str, str]
        ] = None,
    ) -> List[Column]:
        return COLUMN_MAPPINGS[f"{catalog}.{schema}.{table}"]

    module_mocker.patch.object(
        qs_client,
        "get_columns_for_table",
        mock_get_columns_for_table,
    )

    def mock_submit_query(
        query_create: QueryCreate,
        request_headers: Optional[  # pylint: disable=unused-argument
            Dict[str, str]
        ] = None,
    ) -> QueryWithResults:
        results = QUERY_DATA_MAPPINGS[
            query_create.submitted_query.strip()
            .replace('"', "")
            .replace("\n", "")
            .replace(" ", "")
        ]
        if isinstance(results, Exception):
            raise results

        if results.state not in (QueryState.FAILED,):
            results.state = qs_client.query_state  # type: ignore
            qs_client.query_state = QueryState.FINISHED  # type: ignore
        return results

    module_mocker.patch.object(
        qs_client,
        "submit_query",
        mock_submit_query,
    )

    def mock_create_view(
        view_name: str,
        query_create: QueryCreate,  # pylint: disable=unused-argument
        request_headers: Optional[  # pylint: disable=unused-argument
            Dict[str, str]
        ] = None,
    ) -> str:
        return f"View {view_name} created successfully."

    module_mocker.patch.object(
        qs_client,
        "create_view",
        mock_create_view,
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
    yield qs_client


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
    asyncio.get_event_loop().run_until_complete(init_db())

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


@pytest.fixture(scope="module")
def module__session_factory(module__postgres_container) -> Awaitable[AsyncSession]:
    return create_session_factory(module__postgres_container)


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


@pytest.fixture(scope="module")
def module__server(  # pylint: disable=too-many-statements
    module__session: AsyncSession,
    module__settings: Settings,
    module__query_service_client: QueryServiceClient,
    module_mocker,
    module__session_factory,
    jwt_token,
) -> Iterator[TestClient]:
    """
    Create a mock server for testing APIs that contains a mock query service.
    """
    from datajunction_server.api.attributes import default_attribute_types
    from datajunction_server.internal.seed import seed_default_catalogs

    def get_query_service_client_override(
        request: Request = None,  # pylint: disable=unused-argument
    ) -> QueryServiceClient:
        return module__query_service_client

    async def get_session_override() -> AsyncSession:
        return module__session

    def get_settings_override() -> Settings:
        return module__settings

    @asynccontextmanager
    async def noop_lifespan(app: FastAPI):
        """
        Lifespan context for initializing and tearing down app-wide resources, like the FastAPI cache
        """
        await default_attribute_types(module__session)
        await seed_default_catalogs(module__session)
        await create_default_user(module__session)

        yield

    module_mocker.patch(
        "datajunction_server.api.materializations.get_query_service_client",
        get_query_service_client_override,
    )

    module_mocker.patch(
        "datajunction_server.internal.caching.query_cache_manager.session_context",
        return_value=module__session,
    )
    with patch_session_contexts(
        session_factory=module__session_factory,
        use_patch=True,
    ):
        app = create_app(lifespan=noop_lifespan)
        app.dependency_overrides[get_session] = get_session_override
        app.dependency_overrides[get_settings] = get_settings_override
        app.dependency_overrides[get_query_service_client] = (
            get_query_service_client_override
        )

        with TestClient(app) as test_client:
            test_client.headers.update({"Authorization": f"Bearer {jwt_token}"})
            yield test_client

    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def module__session_with_examples(module__server: TestClient) -> TestClient:
    """
    load examples
    """
    for endpoint, json in EXAMPLES:
        post_and_raise_if_error(server=module__server, endpoint=endpoint, json=json)  # type: ignore
    return module__server


@pytest.fixture(scope="module")
def module__postgres_container(request) -> PostgresContainer:
    """
    Setup postgres container
    """
    postgres = PostgresContainer(
        image="postgres:latest",
        username="dj",
        password="dj",
        dbname=request.module.__name__,
        port=5432,
        driver="psycopg",
    )
    with postgres:
        wait_for_logs(
            postgres,
            r"UTC \[1\] LOG:  database system is ready to accept connections",
            10,
        )
        yield postgres


@pytest.fixture(scope="module")
def builder_client(module__session_with_examples: TestClient):
    """
    Returns a DJ client instance
    """
    client = DJBuilder(requests_session=module__session_with_examples)  # type: ignore
    client.create_user(
        email="dj@datajunction.io",
        username="datajunction",
        password="datajunction",
    )
    client.basic_login(
        username="datajunction",
        password="datajunction",
    )
    client.create_tag(
        name="system-tag",
        description="some system tag",
        tag_type="system",
        tag_metadata={},
    )
    return client
