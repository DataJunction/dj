"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name, W0611

import re
from http.client import HTTPException
from typing import Collection, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from cachelib.simple import SimpleCache
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from datajunction_server.api.main import app
from datajunction_server.config import Settings
from datajunction_server.errors import DJQueryServiceClientException
from datajunction_server.models import Column, Engine
from datajunction_server.models.materialization import MaterializationInfo
from datajunction_server.models.query import QueryCreate
from datajunction_server.models.user import OAuthProvider, User
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import (
    get_query_service_client,
    get_session,
    get_settings,
)

from .construction.fixtures import build_expectation, construction_session
from .examples import COLUMN_MAPPINGS, EXAMPLES, QUERY_DATA_MAPPINGS

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


@pytest.fixture
def session() -> Iterator[Session]:
    """
    Create an in-memory SQLite session to test models.
    """
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine, autoflush=False) as session:
        yield session


@pytest.fixture
def query_service_client(mocker: MockerFixture) -> Iterator[QueryServiceClient]:
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
    ) -> Collection[Collection[str]]:
        return QUERY_DATA_MAPPINGS[
            query_create.submitted_query.strip()
            .replace('"', "")
            .replace("\n", "")
            .replace(" ", "")
            .replace("\t", "")
        ]

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


@pytest.fixture
def client_with_examples(client: TestClient) -> TestClient:
    """
    load examples
    """
    for endpoint, json in EXAMPLES:
        post_and_raise_if_error(client=client, endpoint=endpoint, json=json)  # type: ignore
    return client


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
def client_with_query_service(  # pylint: disable=too-many-statements
    session: Session,
    settings: Settings,
    query_service_client: QueryServiceClient,
) -> TestClient:
    """
    Add a mock query service to the test client.
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
        for endpoint, json in EXAMPLES:
            post_and_raise_if_error(client=client, endpoint=endpoint, json=json)  # type: ignore
        yield client

    app.dependency_overrides.clear()


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
        "datajunction_server.internal.authentication.http.get_user",
        return_value=User(id=1, username="dj", oauth_provider=OAuthProvider.BASIC),
    ):
        yield
