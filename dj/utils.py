"""
Utility functions.
"""
# pylint: disable=line-too-long

import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Iterator, List, Optional

import sqlparse
import yaml
from dotenv import load_dotenv
from rich.logging import RichHandler
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine
from yarl import URL

from dj.config import Settings
from dj.typing import ColumnType


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
    return Path(__file__).parent.parent


@lru_cache
def get_settings() -> Settings:
    """
    Return a cached settings object.
    """
    dotenv_file = os.environ.get("DOTENV_FILE", ".env")
    load_dotenv(dotenv_file)
    return Settings()


def get_engine() -> Engine:
    """
    Create the metadata engine.
    """
    settings = get_settings()
    engine = create_engine(settings.index)

    return engine


def get_session() -> Iterator[Session]:
    """
    Per-request session.
    """
    engine = get_engine()

    with Session(engine, autoflush=False) as session:  # pragma: no cover
        yield session


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


def get_more_specific_type(
    current_type: Optional[ColumnType],
    new_type: ColumnType,
) -> ColumnType:
    """
    Given two types, return the most specific one.

    Different databases might store the same column as different types. For example, Hive
    might store timestamps as strings, while Postgres would store the same data as a
    datetime.

        >>> get_more_specific_type(ColumnType.STR, ColumnType.DATETIME)
        <ColumnType.DATETIME: 'DATETIME'>
        >>> get_more_specific_type(ColumnType.STR, ColumnType.INT)
        <ColumnType.INT: 'INT'>

    """
    if current_type is None:
        return new_type

    hierarchy = [
        ColumnType.BYTES,
        ColumnType.STR,
        ColumnType.FLOAT,
        ColumnType.INT,
        ColumnType.DECIMAL,
        ColumnType.BOOL,
        ColumnType.DATETIME,
        ColumnType.DATE,
        ColumnType.TIME,
        ColumnType.TIMEDELTA,
        ColumnType.LIST,
        ColumnType.DICT,
    ]

    return sorted([current_type, new_type], key=hierarchy.index)[1]


def get_issue_url(
    baseurl: URL = URL("https://github.com/DataJunction/dj/issues/new"),
    title: Optional[str] = None,
    body: Optional[str] = None,
    labels: Optional[List[str]] = None,
) -> URL:
    """
    Return the URL to file an issue on GitHub.

    https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-an-issue#creating-an-issue-from-a-url-query
    """
    query_arguments = {
        "title": title,
        "body": body,
        "labels": ",".join(label.strip() for label in labels) if labels else None,
    }
    query_arguments = {k: v for k, v in query_arguments.items() if v is not None}

    return baseurl % query_arguments


def str_representer(dumper: yaml.representer.SafeRepresenter, data: str):
    """
    Multiline string presenter for Node yaml printing.

    Source: https://stackoverflow.com/questions/8640959/how-can-i-control-what-scalar-form-pyyaml-uses-for-my-data
    """
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


def sql_format(sql: str) -> str:
    """
    Let's pick one way to format SQL strings.
    """
    return sqlparse.format(sql, reindent=True, keyword_case="upper")
