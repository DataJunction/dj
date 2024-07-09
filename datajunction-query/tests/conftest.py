"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name

from typing import Iterator

import duckdb
import pytest
from cachelib.simple import SimpleCache
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from djqs.api.main import app
from djqs.config import Settings
from djqs.utils import get_session, get_settings


@pytest.fixture
def settings(mocker: MockerFixture) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    """
    settings = Settings(
        index="sqlite://",
        results_backend=SimpleCache(default_timeout=0),
        configuration_file="./config.djqs.yml",
        enable_dynamic_config=True,
    )

    mocker.patch(
        "djqs.utils.get_settings",
        return_value=settings,
    )

    yield settings


@pytest.fixture
def settings_no_config_file(mocker: MockerFixture) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    """
    settings = Settings(
        index="sqlite://",
        results_backend=SimpleCache(default_timeout=0),
    )

    mocker.patch(
        "djqs.utils.get_settings",
        return_value=settings,
    )

    yield settings


@pytest.fixture()
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


@pytest.fixture()
def client(session: Session, settings: Settings) -> Iterator[TestClient]:
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


@pytest.fixture()
def client_no_config_file(
    session: Session,
    settings_no_config_file: Settings,
) -> Iterator[TestClient]:
    """
    Create a client for testing APIs.
    """

    def get_session_override() -> Session:
        return session

    def get_settings_override() -> Settings:
        return settings_no_config_file

    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_settings] = get_settings_override

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(scope="session")
def duckdb_conn():
    """
    A duckdb connection to a roads database
    """
    return duckdb.connect(
        database="docker/default.duckdb",
    )
