"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name, W0611

import re
from http.client import HTTPException
from typing import Collection, Iterator, List, Optional

import pytest
from cachelib.simple import SimpleCache
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from dj.api.main import app
from dj.config import Settings
from dj.models import Column, Engine
from dj.models.query import QueryCreate
from dj.service_clients import QueryServiceClient
from dj.utils import get_query_service_client, get_session, get_settings

from .construction.fixtures import build_expectation, construction_session
from .examples import COLUMN_MAPPINGS, EXAMPLES, QUERY_DATA_MAPPINGS


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
    )

    mocker.patch(
        "dj.utils.get_settings",
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
        ]

    mocker.patch.object(
        qs_client,
        "submit_query",
        mock_submit_query,
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
        for endpoint, json in EXAMPLES:
            post_and_raise_if_error(client=client, endpoint=endpoint, json=json)  # type: ignore
        yield client

    app.dependency_overrides.clear()


def pytest_addoption(parser):
    """
    Add a --tpcds flag that enables tpcds query parsing tests
    """
    parser.addoption(
        "--tpcds",
        action="store_true",
        dest="tpcds",
        default=False,
        help="include tests for parsing TPC-DS queries",
    )
