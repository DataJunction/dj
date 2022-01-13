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
from typing import Any, Dict, List, Set

import yaml
from rich.text import Text
from sqlalchemy import inspect
from sqlmodel import Session, create_engine, select
from sqloxide import parse_sql

from datajunction.models import Column, Database, Node, Table
from datajunction.utils import (
    create_db_and_tables,
    find_nodes_by_key,
    get_name_from_path,
    get_session,
    render_dag,
)

_logger = logging.getLogger(__name__)


async def load_data(repository: Path, path: Path) -> Dict[str, Any]:
    """
    Load data from a YAML file.
    """
    with open(path, encoding="utf-8") as input_:
        data = yaml.safe_load(input_)

    data["name"] = get_name_from_path(repository, path)
    data["path"] = path

    return data


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

            # SQLite will drop the timezone info; in that case we assume it's UTC
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
        data = await load_data(repository, path)

        _logger.info("Creating database %s", name)
        data["created_at"] = created_at or datetime.now(timezone.utc)
        data["updated_at"] = datetime.now(timezone.utc)
        database = Database(**data)

        session.add(database)
        session.flush()

        return database

    tasks = [add_from_path(path) for path in directory.glob("**/*.yaml")]
    databases = await asyncio.gather(*tasks)

    return databases


def get_columns(table: Table) -> List[Column]:
    """
    Return all columns in a given table.
    """
    engine = create_engine(table.database.URI)
    try:
        inspector = inspect(engine)
        column_metadata = inspector.get_columns(
            table.table,
            schema=table.schema_,
        )
    except Exception:  # pylint: disable=broad-except
        # Druid currently doesn't work with SQLAlchemy 1.4, and raises an exception. Once
        # we've merged https://github.com/druid-io/pydruid/pull/275 we can modify this to
        # re-raise the exception.
        _logger.exception("Unable to get table metadata")
        return []

    return [
        Column(
            name=column["name"],
            type=column["type"].python_type.__name__,
        )
        for column in column_metadata
    ]


def get_dependencies(expression: str) -> Set[str]:
    """
    Return all the dependencies from a SQL expression.
    """
    tree = parse_sql(expression, dialect="ansi")

    return {
        ".".join(part["value"] for part in table["name"])
        for table in find_nodes_by_key(tree, "Table")
    }


async def index_nodes(  # pylint: disable=too-many-locals
    repository: Path,
    session: Session,
) -> List[Node]:
    """
    Index all the nodes, computing their schema.

    We first compute the schema of source nodes, since they are simply fetched from the
    database using SQLAlchemy. After that we compute the schema of downstream nodes, as
    the schema of source nodes become available.
    """
    directory = repository / "nodes"

    # load all databases
    databases = {
        database.name: database for database in session.exec(select(Database)).all()
    }

    # load all nodes and their dependencies
    tasks = [load_data(repository, path) for path in directory.glob("**/*.yaml")]
    configs = await asyncio.gather(*tasks)

    dependencies: Dict[str, Set[str]] = {}
    for config in configs:
        if "expression" in config:
            dependencies[config["name"]] = get_dependencies(config["expression"])
        else:
            dependencies[config["name"]] = set()
    _logger.info("DAG:\n%s", Text.from_ansi(render_dag(dependencies)))

    # compute the schema of nodes with upstream nodes already indexed
    nodes: Dict[str, Node] = {}
    started: Set[str] = set()
    finished: Set[str] = set()
    pending_tasks: Set[asyncio.Task] = set()
    while True:
        to_process = [
            config
            for config in configs
            if dependencies[config["name"]] <= finished
            and config["name"] not in started
        ]
        if not to_process and not pending_tasks:
            break
        started |= {config["name"] for config in to_process}
        new_tasks = {add_node(session, databases, config) for config in to_process}

        done, pending_tasks = await asyncio.wait(
            pending_tasks | new_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for future in done:
            node = future.result()
            node.parents = [nodes[parent] for parent in dependencies[node.name]]
            nodes[node.name] = node
            finished.add(node.name)

    return list(nodes.values())


async def add_node(
    session: Session,
    databases: Dict[str, Database],
    data: Dict[str, Any],
) -> Node:
    """
    Index a node given its YAML config.
    """
    path = data["path"]
    name = data["name"]
    _logger.info("Processing node %s", name)

    # check if the node was already indexed and if it's up-to-date
    query = select(Node).where(Node.name == name)
    node = session.exec(query).one_or_none()
    if node:
        # compare file modification time with timestamp on DB
        mtime = path.stat().st_mtime

        # SQLite will drop the timezone info; in that case we assume it's UTC
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

    # create tables and columns
    tables = []
    for database_name, table_data in data.get("tables", {}).items():
        table_data["database"] = databases[database_name]
        table = Table(**table_data)
        table.columns = get_columns(table)
        tables.append(table)
    data["tables"] = tables

    _logger.info("Creating node %s", name)
    data["name"] = name
    data["created_at"] = created_at or datetime.now(timezone.utc)
    data["updated_at"] = datetime.now(timezone.utc)
    node = Node(**data)

    session.add(node)
    session.flush()

    return node


async def run(repository: Path) -> None:
    """
    Compile the metrics repository.
    """
    create_db_and_tables()

    session = next(get_session())

    await index_databases(repository, session)
    await index_nodes(repository, session)

    session.commit()
