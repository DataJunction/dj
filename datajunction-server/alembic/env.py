"""
Environment for Alembic migrations.
"""
# pylint: disable=no-member, unused-import, no-name-in-module, import-error

from logging.config import fileConfig

from sqlalchemy import create_engine

from alembic import context
from datajunction_server.database import (
    AttributeType,
    Catalog,
    Collection,
    ColumnAttribute,
    Database,
    DimensionLink,
    Engine,
    History,
    Measure,
    Node,
    NodeNamespace,
    NodeRevision,
    Partition,
    Table,
    Tag,
    User,
)
from datajunction_server.database.base import Base
from datajunction_server.database.column import Column

DEFAULT_URI = "postgresql+psycopg://dj:dj@postgres_metadata:5432/dj"

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    x_args = context.get_x_argument(as_dictionary=True)
    context.configure(
        url=x_args.get("uri") or DEFAULT_URI,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    x_args = context.get_x_argument(as_dictionary=True)
    connectable = create_engine(x_args.get("uri") or DEFAULT_URI)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
