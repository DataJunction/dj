"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name

from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from datajunction.app import app, get_session
from datajunction.config import Settings
from datajunction.utils import get_project_repository


@pytest.fixture
def repository(mocker: MockerFixture, fs: FakeFilesystem) -> Iterator[Path]:
    """
    Create the main repository.
    """
    # add the examples repository to the fake filesystem
    repository = get_project_repository()
    fs.add_real_directory(
        repository / "examples/configs",
        target_path="/path/to/repository",
    )

    mocker.patch(
        "datajunction.utils.get_settings",
        return_value=Settings(index="sqlite://", repository="/path/to/repository"),
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

    with Session(engine) as session:
        yield session


@pytest.fixture()
def client(session: Session) -> Iterator[TestClient]:
    """
    Create a client for testing APIs.
    """

    def get_session_override() -> Session:
        return session

    app.dependency_overrides[get_session] = get_session_override

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()
