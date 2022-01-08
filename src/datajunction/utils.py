"""
Utility functions.
"""

import logging
import os
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Set

import asciidag.graph
import asciidag.node
from dotenv import load_dotenv
from rich.logging import RichHandler
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine

from datajunction.config import Settings


def setup_logging(loglevel: str) -> None:
    """
    Setup basic logging.
    """
    level = getattr(logging, loglevel.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid log level: {loglevel}")

    logformat = "[%(asctime)s] %(levelname)s: %(name)s: %(message)s"
    logging.basicConfig(
        level=level,
        format=logformat,
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
        force=True,
    )


def get_project_repository() -> Path:
    """
    Return the project repository.

    This is used for unit tests.
    """
    return Path(__file__).parent.parent.parent


@lru_cache
def get_settings() -> Settings:
    """
    Return a cached settings object.
    """
    load_dotenv()
    return Settings()


def get_engine() -> Engine:
    """
    Create the metadata engine.
    """
    settings = get_settings()
    connect_args = {"check_same_thread": False}
    engine = create_engine(settings.index, echo=False, connect_args=connect_args)

    return engine


def create_db_and_tables() -> None:
    """
    Create the database and tables.
    """
    engine = get_engine()
    SQLModel.metadata.create_all(engine)


def get_session() -> Iterator[Session]:
    """
    Per-request session.
    """
    engine = get_engine()

    with Session(engine) as session:
        yield session


def render_dag(dependencies: Dict[str, Set[str]], **kwargs: Any) -> str:
    """
    Render the DAG of dependencies.
    """
    out = StringIO()
    graph = asciidag.graph.Graph(out, **kwargs)

    asciidag_nodes: Dict[str, asciidag.node.Node] = {}
    tips = sorted(
        [build_asciidag(name, dependencies, asciidag_nodes) for name in dependencies],
        key=lambda n: n.item,
    )

    graph.show_nodes(tips)
    out.seek(0)
    return out.getvalue()


def build_asciidag(
    name: str,
    dependencies: Dict[str, Set[str]],
    asciidag_nodes: Dict[str, asciidag.node.Node],
) -> asciidag.node.Node:
    """
    Build the nodes for ``asciidag``.
    """
    if name in asciidag_nodes:
        asciidag_node = asciidag_nodes[name]
    else:
        asciidag_node = asciidag.node.Node(name)
        asciidag_nodes[name] = asciidag_node

    asciidag_node.parents = sorted(
        [
            build_asciidag(child, dependencies, asciidag_nodes)
            for child in dependencies[name]
        ],
        key=lambda n: n.item,
    )

    return asciidag_node


def get_name_from_path(repository: Path, path: Path) -> str:
    """
    Compute the name of a node given its path and the repository path.
    """
    # strip anything before the repository
    relative_path = path.relative_to(repository)

    if len(relative_path.parts) < 2 or relative_path.parts[0] not in {
        "nodes",
        "databases",
    }:
        raise Exception(f"Invalid path: {path}")

    # remove the "nodes" directory from the path
    relative_path = relative_path.relative_to(relative_path.parts[0])

    # remove extension
    relative_path = relative_path.with_suffix("")

    # encode percent symbols and periods
    encoded = (
        str(relative_path)
        .replace("%", "%25")
        .replace(".", "%2E")
        .replace(os.path.sep, ".")
    )

    return encoded


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
