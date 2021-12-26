"""
Compile a metrics repository.

This will:

    1. Build graph of nodes.
    2. Retrieve the schema of source nodes.
    3. Infer the schema of downstream nodes.
    4. Save everything to the DB.

"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml
from sqlalchemy import inspect
from sqlmodel import create_engine, select, Session, SQLModel

from datajunction.models import (
    Column,
    Database,
    get_name_from_path,
    Representation,
    Node,
)
from datajunction.utils import load_config

_logger = logging.getLogger(__name__)


def load_data(path: Path) -> Dict[str, Any]:
    """
    Load data from a YAML file.
    """
    with open(path, encoding="utf-8") as input_:
        data = yaml.safe_load(input_)

    return data


def update_config(path: Path, **kwargs: Any) -> None:
    """
    Update a YAML file with new keys.
    """
    data = load_data(path)
    data.update(kwargs)

    with open(path, "w", encoding="utf-8") as output:
        yaml.dump(data, output)


async def index_databases(repository: Path, session: Session) -> None:
    """
    Index all the databases.
    """
    directory = repository / "databases"

    async def add_from_path(path: Path) -> None:
        name = get_name_from_path(repository, path)
        _logger.info("Processing database %s", name)

        # check if the database was already indexed and if it's up-to-date
        query = select(Database).where(Database.name == name)
        database = session.exec(query).one_or_none()
        if database:
            # compare file modification time with timestamp on DB
            mtime = path.stat().st_mtime
            updated_at = database.updated_at

            # some DBs like SQLite will drop the timezone info; in that case
            # we assume it's UTC
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)

            if updated_at > datetime.fromtimestamp(mtime, tz=timezone.utc):
                _logger.info("Database %s is up-to-date, skipping", name)
                return

        _logger.info("Loading database from config %s", path)

        data = load_data(path)
        if database:
            _logger.info("Updating database %s", name)
            for key, value in data.items():
                setattr(database, key, value)
        else:
            _logger.info("Creating database %s", name)
            data["name"] = name
            database = Database(**data)

        session.add(database)

    tasks = [add_from_path(path) for path in directory.glob("**/*.yaml")]
    await asyncio.gather(*tasks)


async def get_columns(representations: List[Representation]) -> List[Column]:
    """
    Fetch all columns from a list of representations.
    """
    columns = {}
    for representation in representations:
        engine = create_engine(representation.database.URI)
        inspector = inspect(engine)
        for column in inspector.get_columns(
            representation.table, schema=representation.schema_
        ):
            # XXX type hierarchy
            columns[column["name"]] = Column(
                name=column["name"], type=column["type"].python_type.__name__
            )

    return list(columns.values())


async def index_nodes(repository: Path, session: Session) -> None:
    """
    Index all the nodes, computing their schema.
    """
    # load all databases
    databases = {
        database.name: database for database in session.exec(select(Database)).all()
    }

    directory = repository / "nodes"

    async def add_from_path(path: Path) -> None:
        name = get_name_from_path(repository, path)
        _logger.info("Processing node %s", name)

        # check if the node was already indexed and if it's up-to-date
        query = select(Node).where(Node.name == name)
        node = session.exec(query).one_or_none()
        if node:
            # compare file modification time with timestamp on DB
            mtime = path.stat().st_mtime
            updated_at = node.updated_at

            # some DBs like SQLite will drop the timezone info; in that case
            # we assume it's UTC
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)

            if updated_at > datetime.fromtimestamp(mtime, tz=timezone.utc):
                _logger.info("Node %s is up-do-date, skipping", name)
                return

        _logger.info("Loading node from config %s", path)
        data = load_data(path)
        data["name"] = name

        # delete existing representations
        if node:
            for representation in node.representations:
                session.delete(representation)
            session.flush()

        # create representations
        representations = []
        for database_name, representation_data in data["representations"].items():
            representation_data["database"] = databases[database_name]
            representation = Representation(**representation_data)
            representations.append(representation)
        data["representations"] = representations

        data["columns"] = await get_columns(representations)

        if node:
            _logger.info("Updating node %s", name)
            for key, value in data.items():
                setattr(node, key, value)
        else:
            _logger.info("Creating node %s", name)
            node = Node(**data)

        session.add(node)
        session.flush()

        # update config with column information
        columns = [
            column.dict(include={"name": True, "type": True})
            for column in data["columns"]
        ]
        update_config(path, columns=columns)

    tasks = [add_from_path(path) for path in directory.glob("**/*.yaml")]
    await asyncio.gather(*tasks)


async def run(repository: Path) -> None:
    """
    Compile the metrics repository.
    """
    config = load_config(repository)

    engine = create_engine(config.index)
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        await index_databases(repository, session)
        await index_nodes(repository, session)

        session.commit()
