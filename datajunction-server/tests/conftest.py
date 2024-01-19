"""
Fixtures for testing.
"""
import os
import re
from http.client import HTTPException
from typing import Callable, Collection, Generator, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from cachelib.simple import SimpleCache
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from datajunction_server.api.main import app
from datajunction_server.config import Settings
from datajunction_server.database.base import Base
from datajunction_server.database.column import Column
from datajunction_server.database.engine import Engine
from datajunction_server.database.user import User
from datajunction_server.errors import DJQueryServiceClientException
from datajunction_server.models.materialization import MaterializationInfo
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.models.user import OAuthProvider
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.typing import QueryState
from datajunction_server.utils import (
    get_query_service_client,
    get_session,
    get_settings,
)

from .construction.fixtures import (  # pylint: disable=unused-import
    build_expectation,
    construction_session,
)
from .examples import COLUMN_MAPPINGS, EXAMPLES, QUERY_DATA_MAPPINGS, SERVICE_SETUP

# pylint: disable=redefined-outer-name, invalid-name, W0611


EXAMPLE_TOKEN = (
    "eyJhbGciOiJkaXIiLCJlbmMiOiJBMTI4R0NNIn0..pMoQFVS0VMSAFsG5X0itfw.Lc"
    "8mo22qxeD1NQROlHkjFnmLiDXJGuhlSPcBOoQVlpQGbovHRHT7EJ9_vFGBqDGihul1"
    "BcABiJT7kJtO6cZCJNkykHx-Cbz7GS_6ZQs1_kR5FzsvrJt5_X-dqehVxCFATjv64-"
    "Lokgj9ciOudO2YoBW61UWoLdpmzX1A_OPgv9PlAX23owZrFbPcptcXSJPJQVwvvy8h"
    "DgZ1M6YtqZt_T7o0G2QmFukk.e0ZFTP0H5zP4_wZA3sIrxw"
)


@pytest.fixture
def settings(mocker: MockerFixture) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    """
    settings = Settings(
        index="sqlite://",
        repository="/path/to/repository",
        results_backend=SimpleCache(default_timeout=0),
        celery_broker=None,
        redis_cache=None,
        query_service=None,
        secret="a-fake-secretkey",
    )

    mocker.patch(
        "datajunction_server.utils.get_settings",
        return_value=settings,
    )

    yield settings


@pytest.fixture(scope="session")
def duckdb_conn() -> duckdb.DuckDBPyConnection:  # pylint: disable=c-extension-no-member
    """
    DuckDB connection fixture with mock roads data loaded
    """
    with open(  # pylint: disable=unspecified-encoding
        os.path.join(os.path.dirname(__file__), "duckdb.sql"),
    ) as mock_data:
        with duckdb.connect(  # pylint: disable=c-extension-no-member
            ":memory:",
        ) as conn:  # pylint: disable=c-extension-no-member
            conn.execute(mock_data.read())
            yield conn


@pytest.fixture(scope="session")
def postgres_container() -> PostgresContainer:
    """
    Setup postgres container
    """
    postgres = PostgresContainer(
        image="postgres:latest",
        user="dj",
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
        yield postgres


@pytest.fixture
def session(postgres_container: PostgresContainer) -> Iterator[Session]:
    """
    Create a Postgres session to test models.
    """
    url = postgres_container.get_connection_url()
    engine = create_engine(
        url=url,
    )
    Base.metadata.create_all(engine)
    with Session(engine, autoflush=False) as session:
        yield session
    Base.metadata.drop_all(engine)


@pytest.fixture
def query_service_client(
    mocker: MockerFixture,
    duckdb_conn: duckdb.DuckDBPyConnection,  # pylint: disable=c-extension-no-member
) -> Iterator[QueryServiceClient]:
    """
    Custom settings for unit tests.
    """
    qs_client = QueryServiceClient(uri="query_service:8001")

    def mock_get_columns_for_table(
        catalog: str,
        schema: str,
        table: str,
        engine: Optional[Engine] = None,  # pylint: disable=unused-argument
    ) -> List[Column]:
        return COLUMN_MAPPINGS[f"{catalog}.{schema}.{table}"]

    mocker.patch.object(
        qs_client,
        "get_columns_for_table",
        mock_get_columns_for_table,
    )

    def mock_submit_query(
        query_create: QueryCreate,
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

    def mock_get_query(
        query_id: str,
    ) -> Collection[Collection[str]]:
        if query_id == "foo-bar-baz":
            raise DJQueryServiceClientException("Query foo-bar-baz not found.")
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
def client(  # pylint: disable=too-many-statements
    session: Session,
    settings: Settings,
) -> Iterator[TestClient]:
    """
    Create a client for testing APIs.
    """

    def get_session_override() -> Session:
        return session

    def get_settings_override() -> Settings:
        return settings

    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override

    with TestClient(app) as client:
        client.headers.update(
            {
                "Authorization": f"Bearer {EXAMPLE_TOKEN}",
            },
        )
        yield client

    app.dependency_overrides.clear()


def post_and_raise_if_error(client: TestClient, endpoint: str, json: dict):
    """
    Post the payload to the client and raise if there's an error
    """
    response = client.post(endpoint, json=json)
    if not response.ok:
        raise HTTPException(response.text)


def load_examples_in_client(
    client: TestClient,
    examples_to_load: Optional[List[str]] = None,
):
    """
    Load the DJ client with examples
    """
    # Basic service setup always has to be done (i.e., create catalogs, engines, namespaces etc)
    for endpoint, json in SERVICE_SETUP:
        post_and_raise_if_error(client=client, endpoint=endpoint, json=json)  # type: ignore

    # Load only the selected examples if any are specified
    if examples_to_load is not None:
        for example_name in examples_to_load:
            for endpoint, json in EXAMPLES[example_name]:  # type: ignore
                post_and_raise_if_error(client=client, endpoint=endpoint, json=json)  # type: ignore
        return client

    # Load all examples if none are specified
    for example_name, examples in EXAMPLES.items():
        for endpoint, json in examples:  # type: ignore
            post_and_raise_if_error(client=client, endpoint=endpoint, json=json)  # type: ignore
    return client


@pytest.fixture
def client_example_loader(
    client: TestClient,
) -> Callable[[Optional[List[str]]], TestClient]:
    """
    Provides a callable fixture for loading examples into a DJ client.
    """

    def _load_examples(examples_to_load: Optional[List[str]] = None):
        return load_examples_in_client(client, examples_to_load)

    return _load_examples


@pytest.fixture
def client_with_examples(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> TestClient:
    """
    Provides a DJ client fixture with all examples
    """
    return client_example_loader(None)


@pytest.fixture
def client_with_service_setup(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> TestClient:
    """
    Provides a DJ client fixture with just the service setup
    """
    return client_example_loader([])


@pytest.fixture
def client_with_roads(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> TestClient:
    """
    Provides a DJ client fixture with roads examples
    """
    return client_example_loader(["ROADS"])


@pytest.fixture
def client_with_namespaced_roads(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> TestClient:
    """
    Provides a DJ client fixture with namespaced roads examples
    """
    return client_example_loader(["NAMESPACED_ROADS"])


@pytest.fixture
def client_with_basic(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> TestClient:
    """
    Provides a DJ client fixture with basic examples
    """
    return client_example_loader(["BASIC"])


@pytest.fixture
def client_with_account_revenue(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> TestClient:
    """
    Provides a DJ client fixture with account revenue examples
    """
    return client_example_loader(["ACCOUNT_REVENUE"])


@pytest.fixture
def client_with_event(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> TestClient:
    """
    Provides a DJ client fixture with event examples
    """
    return client_example_loader(["EVENT"])


@pytest.fixture
def client_with_dbt(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> TestClient:
    """
    Provides a DJ client fixture with dbt examples
    """
    return client_example_loader(["DBT"])


def compare_parse_trees(tree1, tree2):
    """
    Recursively compare two ANTLR parse trees for equality.
    """
    # Check if the node types are the same
    if type(tree1) != type(tree2):  # pylint: disable=unidiomatic-typecheck
        return False

    # Check if the node texts are the same
    if tree1.getText() != tree2.getText():
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


@pytest.fixture
def client_with_query_service_example_loader(  # pylint: disable=too-many-statements
    session: Session,
    settings: Settings,
    query_service_client: QueryServiceClient,
) -> Generator[Callable[[Optional[List[str]]], TestClient], None, None]:
    """
    Provides a callable fixture for loading examples into a test client
    fixture that additionally has a mocked query service.
    """

    def get_query_service_client_override() -> QueryServiceClient:
        return query_service_client

    def get_session_override() -> Session:
        return session

    def get_settings_override() -> Settings:
        return settings

    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override
    app.dependency_overrides[
        get_query_service_client
    ] = get_query_service_client_override

    with TestClient(app) as client:
        # The test client includes a signed and encrypted JWT in the authorization headers.
        # Even though the user is mocked to always return a "dj" user, this allows for the
        # JWT logic to be tested on all requests.
        client.headers.update(
            {
                "Authorization": f"Bearer {EXAMPLE_TOKEN}",
            },
        )

        def _load_examples(examples_to_load: Optional[List[str]] = None):
            return load_examples_in_client(client, examples_to_load)

        yield _load_examples

    app.dependency_overrides.clear()


@pytest.fixture
def client_with_query_service(  # pylint: disable=too-many-statements
    client_with_query_service_example_loader: Callable[
        [Optional[List[str]]],
        TestClient,
    ],
) -> TestClient:
    """
    Client with query service and all examples loaded.
    """
    return client_with_query_service_example_loader(None)


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


@pytest.fixture(scope="session", autouse=True)
def mock_user_dj() -> Iterator[None]:
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
