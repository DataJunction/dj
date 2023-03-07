"""
Fixtures for testing.
"""
# pylint: disable=redefined-outer-name, invalid-name, W0611

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

from .construction.fixtures import build_expectation, construction_session
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
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "repair_order_id": {"type": "INT"},
                    "municipality_id": {"type": "STR"},
                    "hard_hat_id": {"type": "INT"},
                    "order_date": {"type": "DATETIME"},
                    "required_date": {"type": "DATETIME"},
                    "dispatched_date": {"type": "DATETIME"},
                    "dispatcher_id": {"type": "INT"},
                },
                "description": "Repair orders",
                "mode": "published",
                "name": "repair_orders",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "repair_order_id": {"type": "INT"},
                    "repair_type_id": {"type": "INT"},
                    "price": {"type": "FLOAT"},
                    "quantity": {"type": "INT"},
                    "discount": {"type": "FLOAT"},
                },
                "description": "Details on repair orders",
                "mode": "published",
                "name": "repair_order_details",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "repair_type_id": {"type": "INT"},
                    "repair_type_name": {"type": "STR"},
                    "contractor_id": {"type": "INT"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "repair_type",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "contractor_id": {"type": "INT"},
                    "company_name": {"type": "STR"},
                    "contact_name": {"type": "STR"},
                    "contact_title": {"type": "STR"},
                    "address": {"type": "STR"},
                    "city": {"type": "STR"},
                    "state": {"type": "STR"},
                    "postal_code": {"type": "STR"},
                    "country": {"type": "STR"},
                    "phone": {"type": "STR"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "contractors",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "municipality_id": {"type": "STR"},
                    "municipality_type_id": {"type": "STR"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "municipality_municipality_type",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "municipality_type_id": {"type": "STR"},
                    "municipality_type_desc": {"type": "STR"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "municipality_type",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "municipality_id": {"type": "STR"},
                    "contact_name": {"type": "STR"},
                    "contact_title": {"type": "STR"},
                    "local_region": {"type": "STR"},
                    "phone": {"type": "STR"},
                    "state_id": {"type": "INT"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "municipality",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "dispatcher_id": {"type": "INT"},
                    "company_name": {"type": "STR"},
                    "phone": {"type": "STR"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "dispatchers",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "hard_hat_id": {"type": "INT"},
                    "last_name": {"type": "STR"},
                    "first_name": {"type": "STR"},
                    "title": {"type": "STR"},
                    "birth_date": {"type": "DATETIME"},
                    "hire_date": {"type": "DATETIME"},
                    "address": {"type": "STR"},
                    "city": {"type": "STR"},
                    "state": {"type": "STR"},
                    "postal_code": {"type": "STR"},
                    "country": {"type": "STR"},
                    "manager": {"type": "INT"},
                    "contractor_id": {"type": "INT"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "hard_hats",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "hard_hat_id": {"type": "INT"},
                    "state_id": {"type": "STR"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "hard_hat_state",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "state_id": {"type": "INT"},
                    "state_name": {"type": "STR"},
                    "state_abbr": {"type": "STR"},
                    "state_region": {"type": "INT"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "us_states",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "columns": {
                    "us_region_id": {"type": "INT"},
                    "us_region_description": {"type": "STR"},
                },
                "description": "Information on different types of repairs",
                "mode": "published",
                "name": "us_region",
                "type": "source",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Repair order dimension",
                "query": """
                    SELECT
                    repair_order_id,
                    municipality_id,
                    hard_hat_id,
                    order_date,
                    required_date,
                    dispatched_date,
                    dispatcher_id
                    FROM repair_orders
                """,
                "mode": "published",
                "name": "repair_order",
                "type": "dimension",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Contractor dimension",
                "query": """
                    SELECT
                    contractor_id,
                    company_name,
                    contact_name,
                    contact_title,
                    address,
                    city,
                    state,
                    postal_code,
                    country,
                    phone
                    FROM contractors
                """,
                "mode": "published",
                "name": "contractor",
                "type": "dimension",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Hard hat dimension",
                "query": """
                    SELECT
                    hard_hat_id,
                    last_name,
                    first_name,
                    title,
                    birth_date,
                    hire_date,
                    address,
                    city,
                    state,
                    postal_code,
                    country,
                    manager,
                    contractor_id
                    FROM hard_hats
                """,
                "mode": "published",
                "name": "hard_hat",
                "type": "dimension",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Hard hat dimension",
                "query": """
                    SELECT
                    hh.hard_hat_id,
                    last_name,
                    first_name,
                    title,
                    birth_date,
                    hire_date,
                    address,
                    city,
                    state,
                    postal_code,
                    country,
                    manager,
                    contractor_id,
                    hhs.state_id AS state_id
                    FROM hard_hats hh
                    LEFT JOIN hard_hat_state hhs
                    ON hh.hard_hat_id = hhs.hard_hat_id
                    WHERE hh.state_id = 'NY'
                """,
                "mode": "published",
                "name": "local_hard_hats",
                "type": "dimension",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "US state dimension",
                "query": """
                    SELECT
                    state_id,
                    state_name,
                    state_abbr,
                    state_region,
                    r.us_region_description AS state_region_description
                    FROM us_states s
                    LEFT JOIN us_region r
                    ON s.state_region = r.us_region_id
                """,
                "mode": "published",
                "name": "us_state",
                "type": "dimension",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Dispatcher dimension",
                "query": """
                    SELECT
                    dispatcher_id,
                    company_name,
                    phone
                    FROM dispatchers
                """,
                "mode": "published",
                "name": "dispatcher",
                "type": "dimension",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Municipality dimension",
                "query": """
                    SELECT
                    m.municipality_id,
                    contact_name,
                    contact_title,
                    local_region,
                    phone,
                    state_id,
                    mmt.municipality_type_id,
                    mt.municipality_type_desc
                    FROM municipality AS m
                    LEFT JOIN municipality_municipality_type AS mmt
                    ON m.municipality_id = mmt.municipality_id
                    LEFT JOIN municipality_type AS mt
                    ON mmt.municipality_type_id = mt.municipality_type_desc
                """,
                "mode": "published",
                "name": "municipality_dim",
                "type": "dimension",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Number of repair orders",
                "query": "SELECT count(repair_order_id) as num_repair_orders FROM repair_orders",
                "mode": "published",
                "name": "num_repair_orders",
                "type": "metric",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Average repair price",
                "query": "SELECT avg(price) as avg_repair_price FROM repair_order_details",
                "mode": "published",
                "name": "avg_repair_price",
                "type": "metric",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Total repair cost",
                "query": "SELECT sum(price) as total_repair_cost FROM repair_order_details",
                "mode": "published",
                "name": "total_repair_cost",
                "type": "metric",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Average length of employment",
                "query": "SELECT avg(NOW() - hire_date) as avg_length_of_employment FROM hard_hats",
                "mode": "published",
                "name": "avg_length_of_employment",
                "type": "metric",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Total repair order discounts",
                "query": "SELECT sum(price * discount) as total_discount FROM repair_order_details",
                "mode": "published",
                "name": "total_repair_order_discounts",
                "type": "metric",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Total repair order discounts",
                "query": (
                    "SELECT avg(price * discount) as avg_repair_order_discount "
                    "FROM repair_order_details"
                ),
                "mode": "published",
                "name": "avg_repair_order_discounts",
                "type": "metric",
            },
        )
        client.post(
            "/nodes/",
            json={
                "description": "Average time to dispatch a repair order",
                "query": (
                    "SELECT avg(dispatched_date - order_date) as avg_time_to_dispatch "
                    "FROM repair_orders"
                ),
                "mode": "published",
                "name": "avg_time_to_dispatch",
                "type": "metric",
            },
        )
        client.post(  # Add a catalog named public
            "/catalogs/",
            json={"name": "public"},
        )
        client.post(  # Add spark as an engine
            "/engines/",
            json={"name": "spark", "version": "3.1.1"},
        )
        client.post(  # Attach the spark engine to the public catalog
            "/catalogs/public/engines/",
            json=[{"name": "spark", "version": "3.1.1"}],
        )
        client.post(
            "/nodes/repair_orders/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "INT"},
                    {"name": "municipality_id", "type": "STR"},
                    {"name": "hard_hat_id", "type": "INT"},
                    {"name": "order_date", "type": "DATETIME"},
                    {"name": "required_date", "type": "DATETIME"},
                    {"name": "dispatched_date", "type": "DATETIME"},
                    {"name": "dispatcher_id", "type": "INT"},
                ],
            },
        )
        client.post(
            "/nodes/repair_order_details/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "repair_order_details",
                "columns": [
                    {"name": "repair_order_id", "type": "INT"},
                    {"name": "repair_type_id", "type": "INT"},
                    {"name": "price", "type": "FLOAT"},
                    {"name": "quantity", "type": "INT"},
                    {"name": "discount", "type": "FLOAT"},
                ],
            },
        )
        client.post(
            "/nodes/repair_type/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "repair_type",
                "columns": [
                    {"name": "repair_type_id", "type": "INT"},
                    {"name": "repair_type_name", "type": "STR"},
                    {"name": "contractor_id", "type": "INT"},
                ],
            },
        )
        client.post(
            "/nodes/municipality/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "municipality",
                "columns": [
                    {"name": "municipality_id", "type": "STR"},
                    {"name": "contact_name", "type": "STR"},
                    {"name": "contact_title", "type": "STR"},
                    {"name": "local_region", "type": "STR"},
                    {"name": "phone", "type": "STR"},
                    {"name": "state_id", "type": "INT"},
                ],
            },
        )
        client.post(
            "/nodes/municipality_municipality_type/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "municipality_municipality_type",
                "columns": [
                    {"name": "municipality_id", "type": "STR"},
                    {"name": "municipality_type_id", "type": "STR"},
                ],
            },
        )
        client.post(
            "/nodes/municipality_type/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "municipality_type",
                "columns": [
                    {"name": "municipality_type_id", "type": "STR"},
                    {"name": "municipality_type_desc", "type": "STR"},
                ],
            },
        )
        client.post(
            "/nodes/dispatchers/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "dispatchers",
                "columns": [
                    {"name": "dispatcher_id", "type": "INT"},
                    {"name": "company_name", "type": "STR"},
                    {"name": "phone", "type": "STR"},
                ],
            },
        )
        client.post(
            "/nodes/us_region/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "us_region",
                "columns": [
                    {"name": "us_region_id", "type": "INT"},
                    {"name": "us_region_description", "type": "STR"},
                ],
            },
        )
        client.post(
            "/nodes/us_states/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "us_states",
                "columns": [
                    {"name": "state_id", "type": "INT"},
                    {"name": "state_name", "type": "STR"},
                    {"name": "state_abbr", "type": "STR"},
                    {"name": "state_region", "type": "INT"},
                ],
            },
        )
        client.post(
            "/nodes/hard_hat_state/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "hard_hat_state",
                "columns": [
                    {"name": "hard_hat_id", "type": "INT"},
                    {"name": "state_id", "type": "STR"},
                ],
            },
        )
        client.post(
            "/nodes/hard_hats/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "hard_hats",
                "columns": [
                    {"name": "hard_hat_id", "type": "INT"},
                    {"name": "last_name", "type": "STR"},
                    {"name": "first_name", "type": "STR"},
                    {"name": "title", "type": "STR"},
                    {"name": "birth_date", "type": "DATETIME"},
                    {"name": "hire_date", "type": "DATETIME"},
                    {"name": "address", "type": "STR"},
                    {"name": "city", "type": "STR"},
                    {"name": "state", "type": "STR"},
                    {"name": "postal_code", "type": "STR"},
                    {"name": "country", "type": "STR"},
                    {"name": "manager", "type": "INT"},
                    {"name": "contractor_id", "type": "INT"},
                ],
            },
        )
        client.post(
            "/nodes/contractors/table/",
            json={
                "database_name": "roads",
                "catalog_name": "public",
                "cost": 0.1,
                "schema": "roads",
                "table": "contractors",
                "columns": [
                    {"name": "contractor_id", "type": "INT"},
                    {"name": "company_name", "type": "STR"},
                    {"name": "contact_name", "type": "STR"},
                    {"name": "contact_title", "type": "STR"},
                    {"name": "address", "type": "STR"},
                    {"name": "city", "type": "STR"},
                    {"name": "state", "type": "STR"},
                    {"name": "postal_code", "type": "STR"},
                    {"name": "country", "type": "STR"},
                    {"name": "phone", "type": "STR"},
                ],
            },
        )
        client.post(
            (
                "/nodes/repair_order_details/columns/repair_order_id/"
                "?dimension=repair_order&dimension_column=repair_order_id"
            ),
        )
        client.post(
            (
                "/nodes/repair_orders/columns/municipality_id/"
                "?dimension=municipality_dim&dimension_column=municipality_id"
            ),
        )
        client.post(
            (
                "/nodes/repair_type/columns/contractor_id/"
                "?dimension=contractor&dimension_column=contractor_id"
            ),
        )
        client.post(
            (
                "/nodes/repair_orders/columns/hard_hat_id/"
                "?dimension=hard_hat&dimension_column=hard_hat_id"
            ),
        )
        client.post(
            (
                "/nodes/repair_orders/columns/dispatcher_id/"
                "?dimension=dispatcher&dimension_column=dispatcher_id"
            ),
        )
        client.post(
            (
                "/nodes/local_hard_hats/columns/state_id/"
                "?dimension=us_state&dimension_column=state_id"
            ),
        )
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
