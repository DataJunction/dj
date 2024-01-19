"""Verify alembic migrations."""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
from testcontainers.postgres import PostgresContainer

from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from datajunction_server.database.base import Base


@pytest.fixture(scope="function", name="connection")
def connection(postgres_container: PostgresContainer) -> Connection:
    """
    Create a Postgres connection for verifying models.
    """
    url = postgres_container.get_connection_url()
    engine = create_engine(
        url=url,
    )
    with engine.connect() as conn:
        transaction = conn.begin()
        yield conn
        transaction.rollback()


def test_migrations_are_current(connection):  # pylint: disable=redefined-outer-name
    """
    Verify that the alembic migrations are in line with the models.
    """
    target_metadata = Base.metadata

    config = Config("alembic.ini")
    config.set_main_option("script_location", "alembic")
    script = ScriptDirectory.from_config(config)

    context = EnvironmentContext(
        config,
        script,
        fn=lambda rev, _: script._upgrade_revs("head", rev),  # pylint: disable=W0212
    )
    context.configure(connection=connection)
    context.run_migrations()

    # Don't use compare_type due to false positives.
    migrations_state = MigrationContext.configure(
        connection,
        opts={"compare_type": False},
    )
    diff = compare_metadata(migrations_state, target_metadata)
    assert diff == [], "The alembic migrations do not match the models."
