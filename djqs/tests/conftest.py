"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name

from pathlib import Path
from typing import Iterator

import duckdb
import pytest
from cachelib.simple import SimpleCache
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession
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


@pytest.fixture(scope="session")
def spark():
    """
    A spark session fixture preloaded with the roads example database
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("djqs-tests")
        .enableHiveSupport()
        .getOrCreate()
    )
    for filepath in Path("tests/resources").glob("*"):
        spark.read.parquet(
            str(filepath),
            header=True,
            inferSchema=True,
        ).createOrReplaceTempView(Path(filepath).stem)
    yield spark


@pytest.fixture(scope="session")
def duckdb_conn():
    """
    A duckdb connection to a roads database
    """
    return duckdb.connect(database="docker/default.duckdb")
