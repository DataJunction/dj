"""
Fixtures for testing.
"""

# pylint: disable=redefined-outer-name, invalid-name
import logging
from typing import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from djqs.api.main import app

_logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def postgres_container() -> PostgresContainer:
    """
    Setup postgres container
    """
    localhost_port = 4321  # The test container will be bound to localhost port 4321
    postgres = PostgresContainer(
        image="postgres:latest",
        username="dj",
        password="dj",
        dbname="dj",
        port=5432,
        driver="psycopg",
    ).with_bind_ports(5432, localhost_port)
    with postgres:
        wait_for_logs(
            postgres,
            r"UTC \[1\] LOG:  database system is ready to accept connections",
            10,
        )

        # Manually build the connection string
        username = postgres.username
        password = postgres.password
        host = postgres.get_container_host_ip()
        port = postgres.get_exposed_port(postgres.port)
        dbname = postgres.dbname

        connection_url = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"

        with psycopg.connect(  # pylint: disable=not-context-manager
            connection_url,
        ) as conn:
            with conn.cursor() as cur:
                _logger.info("Creating query table")
                cur.execute(
                    """
                    CREATE TABLE query (
                        id UUID PRIMARY KEY,
                        catalog_name VARCHAR NOT NULL,
                        engine_name VARCHAR NOT NULL,
                        engine_version VARCHAR NOT NULL,
                        submitted_query VARCHAR NOT NULL,
                        async_ BOOLEAN NOT NULL,
                        executed_query VARCHAR,
                        scheduled TIMESTAMP,
                        started TIMESTAMP,
                        finished TIMESTAMP,
                        state VARCHAR NOT NULL,
                        progress FLOAT NOT NULL
                    )
                """,
                )
                conn.commit()
                _logger.info("Creating table created")
        yield postgres


@pytest.fixture(scope="session")
def client(  # pylint: disable=unused-argument
    postgres_container,
) -> Iterator[TestClient]:
    """
    Create a client for testing APIs.
    """
    with TestClient(app) as client:
        yield client
