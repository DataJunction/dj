"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name

from pathlib import Path
from typing import Iterator

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem
from sqlmodel import Session, SQLModel, create_engine

from datajunction.models import Config
from datajunction.utils import get_project_repository, load_config


@pytest.fixture
def repository(fs: FakeFilesystem) -> Iterator[Path]:
    """
    Create the main repository.
    """
    # add the examples repository to the fake filesystem
    repository = get_project_repository()
    fs.add_real_directory(
        repository / "examples/configs",
        target_path="/path/to/repository",
    )

    path = Path("/path/to/repository")
    yield path


@pytest.fixture
def config(repository: Path) -> Iterator[Config]:
    """
    Load the configuration for a given repository.
    """
    yield load_config(repository)


@pytest.fixture()
def session() -> Iterator[Session]:
    """
    Create an in-memory SQLite session to test models.
    """
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        yield session
