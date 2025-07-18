"""
Fixtures for testing.
"""

import asyncio
from collections import namedtuple
import os
import re
from http.client import HTTPException
from typing import (
    Any,
    AsyncGenerator,
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
import pytest
import pytest_asyncio
from cachelib.simple import SimpleCache
from fastapi import Request
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from httpx import AsyncClient
from pytest_mock import MockerFixture
from sqlalchemy import StaticPool, insert, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from datajunction_server.api.main import app
from datajunction_server.api.attributes import default_attribute_types
from datajunction_server.internal.seed import seed_default_catalogs
from datajunction_server.config import DatabaseConfig, Settings
from datajunction_server.database.base import Base
from datajunction_server.database.column import Column
from datajunction_server.database.engine import Engine
from datajunction_server.database.user import User
from datajunction_server.errors import DJQueryServiceClientEntityNotFound
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models.access import AccessControl, ValidateAccessFn
from datajunction_server.models.materialization import MaterializationInfo
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.models.user import OAuthProvider
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.typing import QueryState
from datajunction_server.utils import (
    DatabaseSessionManager,
    get_query_service_client,
    get_session,
    get_settings,
)

from .examples import COLUMN_MAPPINGS, EXAMPLES, QUERY_DATA_MAPPINGS, SERVICE_SETUP


EXAMPLE_TOKEN = (
    "eyJhbGciOiJkaXIiLCJlbmMiOiJBMTI4R0NNIn0..SxGbG0NRepMY4z9-2-ZZdg.ug"
    "0FvJUoybiGGpUItL4VbM1O_oinX7dMBUM1V3OYjv30fddn9m9UrrXxv3ERIyKu2zVJ"
    "xx1gSoM5k8petUHCjatFQqA-iqnvjloFKEuAmxLdCHKUDgfKzCIYtbkDcxtzXLuqlj"
    "B0-ConD6tpjMjFxNrp2KD4vwaS0oGsDJGqXlMo0MOhe9lHMLraXzOQ6xDgDFHiFert"
    "Fc0T_9jYkcpmVDPl9pgPf55R.sKF18rttq1OZ_EjZqw8Www"
)
PostgresCluster = namedtuple("PostgresCluster", ["writer", "reader"])


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


@pytest_asyncio.fixture
def settings_no_qs(
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


@pytest.fixture(scope="session")
def duckdb_conn() -> duckdb.DuckDBPyConnection:
    """
    DuckDB connection fixture with mock roads data loaded
    """
    with open(
        os.path.join(os.path.dirname(__file__), "duckdb.sql"),
    ) as mock_data:
        with duckdb.connect(
            ":memory:",
        ) as conn:
            conn.execute(mock_data.read())
            yield conn


@pytest.fixture(scope="session")
def postgres_container() -> PostgresContainer:
    """
    Setup postgres container
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


@pytest_asyncio.fixture
async def session(
    postgres_container: PostgresContainer,
) -> AsyncGenerator[AsyncSession, None]:
    """
    Create a Postgres session to test models.
    """
    engine = create_async_engine(
        url=postgres_container.get_connection_url(),
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
        result = duckdb_conn.sql(query_create.submitted_query)
        columns = [
            {"name": col, "type": str(type_).lower()}
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
        duckdb_conn.sql(query_create.submitted_query)
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


@pytest_asyncio.fixture
async def client(
    session: AsyncSession,
    settings_no_qs: Settings,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Create a client for testing APIs.
    """
    await default_attribute_types(session)
    await seed_default_catalogs(session)

    def get_session_override() -> AsyncSession:
        return session

    def get_settings_override() -> Settings:
        return settings_no_qs

    def default_validate_access() -> ValidateAccessFn:
        def _(access_control: AccessControl):
            access_control.approve_all()

        return _

    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override
    app.dependency_overrides[validate_access] = default_validate_access

    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as test_client:
        test_client.headers.update({"Authorization": f"Bearer {EXAMPLE_TOKEN}"})
        test_client.app = app
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
    Load the DJ client with examples
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
                await post_and_raise_if_error(
                    client=client,
                    endpoint=endpoint,
                    json=json,  # type: ignore
                )
        return client

    # Load all examples if none are specified
    for example_name, examples in EXAMPLES.items():
        for endpoint, json in examples:  # type: ignore
            await post_and_raise_if_error(
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
    """

    async def _load_examples(examples_to_load: Optional[List[str]] = None):
        return await load_examples_in_client(client, examples_to_load)

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
) -> AsyncGenerator[AsyncClient, None]:
    """
    Create a client for testing APIs.
    """
    statement = insert(User).values(
        username="dj",
        email=None,
        name=None,
        oauth_provider="basic",
        is_admin=False,
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

    def default_validate_access() -> ValidateAccessFn:
        def _(access_control: AccessControl):
            access_control.approve_all()

        return _

    def get_session_override() -> AsyncSession:
        return session

    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override
    app.dependency_overrides[validate_access] = default_validate_access
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
            test_client.headers.update(
                {
                    "Authorization": f"Bearer {EXAMPLE_TOKEN}",
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


#
# Module scope fixtures
#
@pytest_asyncio.fixture(autouse=True, scope="module")
async def mock_user_dj():
    """
    Mock a DJ user for tests
    """
    with patch(
        "datajunction_server.internal.access.authentication.http.get_user",
        return_value=User(
            id=1,
            username="dj",
            oauth_provider=OAuthProvider.BASIC,
            is_admin=False,
        ),
    ):
        yield


@pytest_asyncio.fixture(scope="module")
async def module__client_example_loader(
    module__client: AsyncClient,
) -> Callable[[list[str] | None], Coroutine[Any, Any, AsyncClient]]:
    """
    Provides a callable fixture for loading examples into a DJ client.
    """

    async def _load_examples(examples_to_load: Optional[List[str]] = None):
        return await load_examples_in_client(module__client, examples_to_load)

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


@pytest_asyncio.fixture(scope="module")
async def module__client(
    module__session: AsyncSession,
    module__settings: Settings,
    module__query_service_client: QueryServiceClient,
    module_mocker: MockerFixture,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Create a client for testing APIs.
    """
    statement = insert(User).values(
        username="dj",
        email=None,
        name=None,
        oauth_provider="basic",
        is_admin=False,
    )
    await module__session.execute(statement)
    await default_attribute_types(module__session)
    await seed_default_catalogs(module__session)

    def get_query_service_client_override(
        request: Request = None,
    ) -> QueryServiceClient:
        return module__query_service_client

    def get_session_override() -> AsyncSession:
        return module__session

    def get_settings_override() -> Settings:
        return module__settings

    def default_validate_access() -> ValidateAccessFn:
        def _(access_control: AccessControl):
            access_control.approve_all()

        return _

    module_mocker.patch(
        "datajunction_server.api.materializations.get_query_service_client",
        get_query_service_client_override,
    )

    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override
    app.dependency_overrides[validate_access] = default_validate_access
    app.dependency_overrides[get_query_service_client] = (
        get_query_service_client_override
    )

    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as test_client:
        test_client.headers.update(
            {
                "Authorization": f"Bearer {EXAMPLE_TOKEN}",
            },
        )
        test_client.app = app
        yield test_client

    app.dependency_overrides.clear()


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
        session.remove = AsyncMock(return_value=None)
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

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
    from datajunction_server.transpilation import SQLTranspilationPlugin

    register_dialect_plugin("spark", SQLTranspilationPlugin)
    register_dialect_plugin("trino", SQLTranspilationPlugin)
    register_dialect_plugin("druid", SQLTranspilationPlugin)

    module_mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

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
        create_readonly_user(postgres)
        yield postgres


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
        result = duckdb_conn.sql(query_create.submitted_query)
        columns = [
            {"name": col, "type": str(type_).lower()}
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
        duckdb_conn.sql(query_create.submitted_query)
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


@pytest_asyncio.fixture(scope="module")
async def module__current_user(module__session: AsyncSession) -> User:
    """
    A user fixture.
    """

    new_user = User(
        username="datajunction",
        password="datajunction",
        email="dj@datajunction.io",
        name="DJ",
        oauth_provider=OAuthProvider.BASIC,
        is_admin=False,
    )
    existing_user = await module__session.get(User, new_user.id)
    if not existing_user:
        module__session.add(new_user)
        await module__session.commit()
        user = new_user
    else:
        user = existing_user
    return user


@pytest_asyncio.fixture
async def current_user(session: AsyncSession) -> User:
    """
    A user fixture.
    """

    new_user = User(
        username="datajunction",
        password="datajunction",
        email="dj@datajunction.io",
        name="DJ",
        oauth_provider=OAuthProvider.BASIC,
        is_admin=False,
    )
    existing_user = await session.get(User, new_user.id)
    if not existing_user:
        session.add(new_user)
        await session.commit()
        user = new_user
    else:
        user = existing_user
    return user
