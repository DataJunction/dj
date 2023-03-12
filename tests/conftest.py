"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name, W0611

from typing import Iterator

import pytest
from cachelib.simple import SimpleCache
from fastapi.testclient import TestClient
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from dj.api.main import app
from dj.config import Settings
from dj.utils import get_session, get_settings

from .construction.fixtures import build_expectation, construction_session
from .examples import EXAMPLES
from .sql.parsing.queries import (
    case_when_null,
    cte_query,
    derived_subquery,
    derived_subquery_unaliased,
    tpcds_q01,
    tpcds_q99,
    trivial_query,
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
        raise Exception(response.text)


@pytest.fixture
def client_with_examples(client: TestClient) -> TestClient:
    """
    load examples
    """
    for endpoint, json in EXAMPLES:
        post_and_raise_if_error(client=client, endpoint=endpoint, json=json)  # type: ignore
    return client


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
