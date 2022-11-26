"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name

from pathlib import Path
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
from dj.utils import get_project_repository, get_session, get_settings

from .sql.parsing.queries import (  # pylint: disable=W0611
    case_when_null,
    derived_subquery,
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
    )

    mocker.patch(
        "dj.utils.get_settings",
        return_value=settings,
    )

    yield settings


@pytest.fixture
def repository(fs: FakeFilesystem) -> Iterator[Path]:
    """
    Create the main repository.
    """
    # add the examples repository to the fake filesystem
    repository = get_project_repository()
    fs.add_real_directory(
        repository / "tests/configs",
        target_path="/path/to/repository",
    )

    path = Path("/path/to/repository")
    yield path


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
