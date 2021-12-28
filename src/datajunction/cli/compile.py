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
from typing import Any, Dict, List, Optional

import yaml
from sqlalchemy import inspect
from sqlmodel import Session, create_engine, select

from datajunction.models import (
    Column,
    Database,
    Node,
    Representation,
    get_name_from_path,
)
from datajunction.utils import create_db_and_tables, get_session

_logger = logging.getLogger(__name__)


def load_data(path: Path) -> Dict[str, Any]:
    """
    Load data from a YAML file.
    """
    with open(path, encoding="utf-8") as input_:
        data = yaml.safe_load(input_)

    return data


def get_more_specific_type(current_type: Optional[str], new_type: str) -> str:
    """
    Given two types, return the most specific one.

    Different databases might store the same column as different types. For example, Hive
    might store timestamps as strings, while Postgres would store the same data as a
    datetime.

        >>> get_more_specific_type('str',  'datetime')
        'datetime'
        >>> get_more_specific_type('str',  'int')
        'int'

    """
    if current_type is None:
        return new_type

    hierarchy = [
        "bytes",
        "str",
        "float",
        "int",
        "Decimal",
        "bool",
        "datetime",
        "date",
        "time",
        "timedelta",
        "list",
        "dict",
    ]

    return sorted([current_type, new_type], key=hierarchy.index)[1]


async def index_databases(repository: Path, session: Session) -> List[Database]:
    """
    Index all the databases.
    """
    directory = repository / "databases"

    async def add_from_path(path: Path) -> Database:
        name = get_name_from_path(repository, path)
        _logger.info("Processing database %s", name)

        # check if the database was already indexed and if it's up-to-date
        query = select(Database).where(Database.name == name)
        database = session.exec(query).one_or_none()
        if database:
            # compare file modification time with timestamp on DB
            mtime = path.stat().st_mtime

            # some DBs like SQLite will drop the timezone info; in that case
            # we assume it's UTC
            if database.updated_at.tzinfo is None:
                database.updated_at = database.updated_at.replace(tzinfo=timezone.utc)

            if database.updated_at > datetime.fromtimestamp(mtime, tz=timezone.utc):
                _logger.info("Database %s is up-to-date, skipping", name)
                return database

            # delete existing database
            created_at = database.created_at
            session.delete(database)
            session.flush()
        else:
            created_at = None

        _logger.info("Loading database from config %s", path)
        data = load_data(path)

        _logger.info("Creating database %s", name)
        data["name"] = name
        data["created_at"] = created_at or datetime.now(timezone.utc)
        data["updated_at"] = datetime.now(timezone.utc)
        database = Database(**data)

        session.add(database)
        session.flush()

        return database

    tasks = [add_from_path(path) for path in directory.glob("**/*.yaml")]
    databases = await asyncio.gather(*tasks)

    return databases


def get_columns(representations: List[Representation]) -> List[Column]:
    """
    Fetch all columns from a list of representations.
    """
    columns: Dict[str, Column] = {}
    for representation in representations:
        engine = create_engine(representation.database.URI)
        inspector = inspect(engine)
        for column in inspector.get_columns(
            representation.table,
            schema=representation.schema_,
        ):
            name = column["name"]
            type_ = column["type"].python_type.__name__

            columns[name] = Column(
                name=name,
                type=get_more_specific_type(columns[name].type, type_)
                if name in columns
                else type_,
            )

    return list(columns.values())


async def index_nodes(repository: Path, session: Session) -> List[Node]:
    """
    Index all the nodes, computing their schema.
    """
    # load all databases
    databases = {
        database.name: database for database in session.exec(select(Database)).all()
    }

    directory = repository / "nodes"

    async def add_from_path(path: Path) -> Node:
        name = get_name_from_path(repository, path)
        _logger.info("Processing node %s", name)

        # check if the node was already indexed and if it's up-to-date
        query = select(Node).where(Node.name == name)
        node = session.exec(query).one_or_none()
        if node:
            # compare file modification time with timestamp on DB
            mtime = path.stat().st_mtime

            # some DBs like SQLite will drop the timezone info; in that case
            # we assume it's UTC
            if node.updated_at.tzinfo is None:
                node.updated_at = node.updated_at.replace(tzinfo=timezone.utc)

            if node.updated_at > datetime.fromtimestamp(mtime, tz=timezone.utc):
                _logger.info("Node %s is up-do-date, skipping", name)
                return node

            # delete existing node
            created_at = node.created_at
            session.delete(node)
            session.flush()
        else:
            created_at = None

        _logger.info("Loading node from config %s", path)
        data = load_data(path)

        # create representations and columns
        representations = []
        for database_name, representation_data in data["representations"].items():
            representation_data["database"] = databases[database_name]
            representation = Representation(**representation_data)
            representations.append(representation)
        data["representations"] = representations
        data["columns"] = get_columns(representations)

        _logger.info("Creating node %s", name)
        data["name"] = name
        data["created_at"] = created_at or datetime.now(timezone.utc)
        data["updated_at"] = datetime.now(timezone.utc)
        node = Node(**data)

        session.add(node)
        session.flush()

        return node

    tasks = [add_from_path(path) for path in directory.glob("**/*.yaml")]
    nodes = await asyncio.gather(*tasks)

    return nodes


async def run(repository: Path) -> None:
    """
    Compile the metrics repository.
    """
    create_db_and_tables()

    session = next(get_session())

    await index_databases(repository, session)
    await index_nodes(repository, session)

    session.commit()
