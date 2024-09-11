"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name

from typing import Iterator
from unittest.mock import patch

import pytest
from cachelib.simple import SimpleCache
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from pytest_postgresql import factories

from djqs.api.main import app
from djqs.config import Settings
from djqs.utils import get_settings

# Define a PostgreSQL process fixture that will be shared across tests
postgresql_my_proc = factories.postgresql_proc(
    port=4321,
    dbname="djqs",
    user="dj",
    password="dj",
)
import time


@pytest.fixture(scope="session")
def postgresql_my(postgresql_my_proc):
    print("Starting PostgreSQL instance...")
    start_time = time.time()
    proc = postgresql_my_proc
    end_time = time.time()
    print(f"PostgreSQL instance started in {end_time - start_time:.2f} seconds")
    return proc


@pytest.fixture(scope="session")
def postgresql_connection_string(postgresql_my):
    # Gather connection details from the PostgreSQL instance
    host = postgresql_my.host
    port = postgresql_my.port
    dbname = "postgres"
    user = "postgres"
    password = ""

    # Create the connection string
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return connection_string


@pytest.fixture(scope="session", autouse=True)
def mock_get_settings():
    with patch("djqs.utils.get_settings") as mocked_get_settings:
        mocked_get_settings.return_value = Settings(
            index="sqlite://",
            results_backend=SimpleCache(default_timeout=0),
            configuration_file="./config.djqs.yml",
        )
        yield mocked_get_settings


pytest_plugins = ["djqs.api.main"]


@pytest.fixture
def settings(mocker: MockerFixture) -> Iterator[Settings]:
    """
    Custom settings for unit tests.
    """
    settings = Settings(
        index="sqlite://",
        results_backend=SimpleCache(default_timeout=0),
        configuration_file="./tests/config.djqs.yml",
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
def client(settings: Settings) -> Iterator[TestClient]:
    """
    Create a client for testing APIs.
    """

    def get_settings_override() -> Settings:
        return settings

    app.dependency_overrides[get_settings] = get_settings_override

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture()
def client_no_config_file(
    settings_no_config_file: Settings,
) -> Iterator[TestClient]:
    """
    Create a client for testing APIs.
    """

    def get_settings_override() -> Settings:
        return settings_no_config_file

    app.dependency_overrides[get_settings] = get_settings_override

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()
