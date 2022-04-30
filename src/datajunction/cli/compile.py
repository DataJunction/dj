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
from typing import Any, Dict, List, Optional, Set, TypedDict, Union

import yaml
from rich.text import Text
from sqlalchemy import inspect
from sqlalchemy.engine.url import URL
from sqlmodel import Session, create_engine, select
from watchfiles import Change, awatch

from datajunction.constants import (
    DEFAULT_DIMENSION_COLUMN,
    DJ_DATABASE_ID,
    SQLITE_DATABASE_ID,
)
from datajunction.fixes import patch_druid_get_columns
from datajunction.models.column import Column
from datajunction.models.database import Database
from datajunction.models.node import Node, NodeType, NodeYAML
from datajunction.models.query import Query  # pylint: disable=unused-import
from datajunction.models.table import Table
from datajunction.sql.dag import render_dag
from datajunction.sql.inference import infer_columns
from datajunction.sql.parse import get_dependencies
from datajunction.typing import ColumnType
from datajunction.utils import get_more_specific_type, get_name_from_path, get_session

_logger = logging.getLogger(__name__)


# Database YAML with added information for processing
EnrichedDatabaseYAML = TypedDict(
    "EnrichedDatabaseYAML",
    {
        "description": str,
        "URI": str,
        "read-only": bool,
        "async_": bool,
        "cost": float,
        # this is added:
        "name": str,
        "path": Path,
    },
    total=False,
)


class EnrichedNodeYAML(NodeYAML):
    """
    Node YAML with extra information.
    """

    name: str
    path: Path


async def load_data(
    repository: Path,
    path: Path,
) -> Union[EnrichedDatabaseYAML, EnrichedNodeYAML]:
    """
    Load data from a YAML file.
    """
    with open(path, encoding="utf-8") as input_:
        data = yaml.safe_load(input_)

    data["name"] = get_name_from_path(repository, path)
    data["path"] = path

    return data


async def add_special_databases(session: Session) -> None:
    """
    Add two special databases to the index.

    This function adds two databases that are not defined in YAML files. The first one is
    a pseudo-database used to run SQL queries against metrics, and the second one is an in
    memory SQLite database for quickly running tableless queries like ``SELECT 1``.
    """
    if not session.get(Database, DJ_DATABASE_ID):
        session.add(
            Database(
                id=DJ_DATABASE_ID,
                name="dj",
                description="The DJ meta database",
                URI=str(
                    URL(
                        "dj",
                        host="localhost",
                        port=8000,
                        database=str(DJ_DATABASE_ID),
                    ),
                ),
                read_only=True,
            ),
        )

    if not session.get(Database, SQLITE_DATABASE_ID):
        session.add(
            Database(
                id=SQLITE_DATABASE_ID,
                name="in-memory",
                description="An in memory SQLite database for tableless queries",
                URI="sqlite://",
                read_only=True,
                cost=0,
            ),
        )


async def index_databases(
    repository: Path,
    session: Session,
    force: bool = False,
) -> List[Database]:
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
            updated_at = database.updated_at

            # SQLite will drop the timezone info; in that case we assume it's UTC
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)

            if not force and updated_at > datetime.fromtimestamp(
                mtime,
                tz=timezone.utc,
            ):
                _logger.info("Database %s is up-to-date, skipping", name)
                return database

        _logger.info("Loading database from config %s", path)
        data = await load_data(repository, path)

        if database:
            _logger.info("Updating database %s", name)
            for attr, value in data.items():
                if attr not in {"name", "path"}:
                    setattr(database, attr, value)
        else:
            _logger.info("Creating database %s", name)
            database = Database(
                created_at=datetime.now(timezone.utc),
                **data,
            )

        database.updated_at = datetime.now(timezone.utc)
        session.add(database)
        session.flush()
        session.refresh(database)

        return database

    tasks = [add_from_path(path) for path in directory.glob("**/*.yaml")]
    databases = await asyncio.gather(*tasks)

    return databases


def get_table_columns(
    uri: str,
    extra_params: Dict[str, Any],
    schema: Optional[str],
    table: str,
) -> List[Column]:
    """
    Return all columns in a given table.
    """
    engine = create_engine(uri, **extra_params)
    try:
        inspector = inspect(engine)
        column_metadata = inspector.get_columns(
            table,
            schema=schema,
        )
    except Exception:  # pylint: disable=broad-except
        _logger.exception("Unable to get table metadata")
        return []

    return [
        Column(
            name=column["name"],
            type=ColumnType[column["type"].python_type.__name__.upper()],
        )
        for column in column_metadata
    ]


async def load_node_configs(
    repository: Path,
) -> List[EnrichedNodeYAML]:
    """
    Load all configs from a repository.
    """
    directory = repository / "nodes"

    # load all nodes and their dependencies
    tasks = [load_data(repository, path) for path in directory.glob("**/*.yaml")]
    return await asyncio.gather(*tasks)


async def index_nodes(  # pylint: disable=too-many-locals
    repository: Path,
    session: Session,
    force: bool = False,
) -> List[Node]:
    """
    Index all the nodes, computing their schema.

    We first compute the schema of source nodes, since they are simply fetched from the
    database using SQLAlchemy. After that we compute the schema of downstream nodes, as
    the schemas of source nodes become available.
    """
    # load all databases
    databases = {
        database.name: database
        for database in session.exec(
            select(Database).where(Database.id != DJ_DATABASE_ID),
        ).all()
    }

    configs = await load_node_configs(repository)
    dependencies: Dict[str, Set[str]] = {}
    for config in configs:
        if "expression" in config:
            dependencies[config["name"]] = get_dependencies(config["expression"])
        else:
            dependencies[config["name"]] = set()
        # add dimensions to dependencies
        for column in config.get("columns", {}).values():
            if "dimension" in column:
                dependencies[config["name"]].add(column["dimension"])
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
        new_tasks = {
            add_node(
                session,
                databases,
                config,
                parents=[nodes[parent] for parent in dependencies[config["name"]]],
                force=force,
            )
            for config in to_process
        }

        done, pending_tasks = await asyncio.wait(
            pending_tasks | new_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for future in done:
            node = future.result()
            nodes[node.name] = node
            finished.add(node.name)

    return list(nodes.values())


async def add_node(  # pylint: disable=too-many-locals
    session: Session,
    databases: Dict[str, Database],
    data: EnrichedNodeYAML,
    parents: List[Node],
    force: bool = False,
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
        updated_at = node.updated_at

        # SQLite will drop the timezone info; in that case we assume it's UTC
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        if not force and updated_at > datetime.fromtimestamp(mtime, tz=timezone.utc):
            _logger.info("Node %s is up-do-date, skipping", name)
            return node

    config = {
        "name": name,
        "description": data["description"],
        "created_at": node.created_at if node else datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "type": data["type"],
        "expression": data.get("expression"),
        "tables": [],
        "parents": parents,
    }

    # create tables and columns
    for database_name, tables_data in data.get("tables", {}).items():
        for table_data in tables_data:
            config["tables"].append(
                Table(
                    database=databases[database_name],
                    columns=get_table_columns(
                        databases[database_name].URI,
                        databases[database_name].extra_params,
                        table_data["schema"],
                        table_data["table"],
                    ),
                    **table_data,
                ),
            )

    config["columns"] = (
        infer_columns(data["expression"], parents)
        if data.get("expression")
        else get_columns_from_tables(config["tables"])
    )

    # enrich columns with dimensions
    add_dimensions_to_columns(session, data, config["columns"])

    if node:
        _logger.info("Updating node %s", name)

        # delete old tables, otherwise they're left behind without a node id
        for table in node.tables:
            session.delete(table)

        for attr, value in config.items():
            if attr not in {"name", "path"}:
                setattr(node, attr, value)
    else:
        _logger.info("Creating node %s", name)
        node = Node(**config)

    node.extra_validation()

    session.add(node)
    session.flush()
    session.refresh(node)

    # write node back to YAML, with column information
    await update_node_config(node, path)

    return node


def add_dimensions_to_columns(
    session: Session,
    data: EnrichedNodeYAML,
    columns: List[Column],
) -> None:
    """
    Add dimension information to columns.

    While the columns are determined by reflecting the table via SQLAlchemy, the
    information about dimensions is added manually by the user, so we need to extract it
    from the YAML file.
    """
    original_columns = data.get("columns", {})
    for column in columns:
        if (
            column.name in original_columns
            and "dimension" in original_columns[column.name]
        ):
            # this can be a node (``core.users``) or include a column (``core.users.id``)
            dimension_target = original_columns[column.name]["dimension"]

            query = (
                select(Node)
                .where(Node.name == dimension_target)
                .where(Node.type == NodeType.DIMENSION)
            )
            dimension = session.exec(query).one_or_none()
            if dimension:
                dimension_column = DEFAULT_DIMENSION_COLUMN
            elif "." in dimension_target:
                dimension_target, dimension_column = dimension_target.rsplit(".", 1)
                query = (
                    select(Node)
                    .where(Node.name == dimension_target)
                    .where(Node.type == NodeType.DIMENSION)
                )
                dimension = session.exec(query).one()
            else:
                raise Exception(f"Invalid dimension: {dimension_target}")

            column.dimension = dimension
            column.dimension_column = dimension_column


def get_columns_from_tables(tables: List[Table]) -> List[Column]:
    """
    Return the superset of columns from a list of tables.
    """
    columns: Dict[str, Column] = {}
    for table in tables:
        for column in table.columns:
            name = column.name
            columns[name] = Column(
                name=name,
                type=get_more_specific_type(columns[name].type, column.type)
                if name in columns
                else column.type,
            )

    return list(columns.values())


async def update_node_config(node: Node, path: Path) -> None:
    """
    Update the node config with information about columns.
    """
    with open(path, encoding="utf-8") as input_:
        original = yaml.safe_load(input_)
    updated = node.to_yaml()

    # preserve column attributes entered by the user
    if "columns" in original:
        for name, column in original["columns"].items():
            if name not in updated["columns"]:
                continue
            for key, value in column.items():
                if key not in updated["columns"][name]:
                    updated["columns"][name][key] = value  # type: ignore

    if updated == original:
        _logger.info("Node %s is up-do-date, skipping", node.name)
        return

    _logger.info("Updating node %s config with column information", node.name)
    with open(path, "w", encoding="utf-8") as output:
        yaml.safe_dump(updated, output, sort_keys=False)


def yaml_file_changed(_: Change, path: str) -> bool:
    """
    Return if the modified file is a YAML file.

    Used for the watchdog.
    """
    return Path(path).suffix in {".yaml", ".yml"}


async def run(repository: Path, force: bool = False, reload: bool = False) -> None:
    """
    Compile the metrics repository.
    """
    patch_druid_get_columns()

    session = next(get_session())

    await add_special_databases(session)
    await index_databases(repository, session, force)
    await index_nodes(repository, session, force)
    session.commit()

    if not reload:
        return

    async for _ in awatch(  # pragma: no cover
        repository,
        watch_filter=yaml_file_changed,
    ):
        await index_databases(repository, session, force)
        await index_nodes(repository, session, force)
        session.commit()
