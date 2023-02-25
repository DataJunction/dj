"""
Utility functions.
"""
import datetime
import logging
import os
import re
from enum import Enum
from functools import lru_cache

# pylint: disable=line-too-long
from pathlib import Path
from typing import Iterator, List, Optional

import sqlparse
import yaml
from dotenv import load_dotenv
from pydantic.datetime_parse import parse_datetime
from rich.logging import RichHandler
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine
from yarl import URL

from dj.config import Settings
from dj.errors import DJException
from dj.service_clients import QueryServiceClient
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


def get_query_service_client() -> Optional[QueryServiceClient]:
    """
    Return query service client
    """
    settings = get_settings()
    if not settings.query_service:
        return None
    return QueryServiceClient(settings.query_service)


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

        >>> get_more_specific_type(ColumnType.STR, ColumnType.TIMESTAMP)
        'TIMESTAMP'
        >>> get_more_specific_type(ColumnType.STR, ColumnType.INT)
        'INT'

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
        ColumnType.TIMESTAMP,
        ColumnType.DATE,
        ColumnType.TIME,
        ColumnType.TIMEDELTA,
        ColumnType.ARRAY,
        ColumnType.MAP,
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


class UTCDatetime(datetime.datetime):
    """
    A UTC extension of pydantic's normal datetime handling
    """

    @classmethod
    def __get_validators__(cls):
        """
        Extend the builtin pydantic datetime parser with a custom validate method
        """
        yield parse_datetime
        yield cls.validate

    @classmethod
    def validate(cls, value) -> str:
        """
        Convert to UTC
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)

        return value.astimezone(datetime.timezone.utc)


class VersionUpgrade(str, Enum):
    """
    The version upgrade type
    """

    MAJOR = "major"
    MINOR = "minor"


class Version:
    """
    Represents a basic semantic version with only major & minor parts.
    Used for tracking node versioning.
    """

    def __init__(self, major, minor):
        self.major = major
        self.minor = minor

    def __str__(self) -> str:
        return f"v{self.major}.{self.minor}"

    @classmethod
    def parse(cls, version_string) -> "Version":
        """
        Parse a version string.
        """
        version_regex = re.compile(r"^v(?P<major>[0-9]+)\.(?P<minor>[0-9]+)")
        matcher = version_regex.search(version_string)
        if not matcher:
            raise DJException(
                http_status_code=500,
                message=f"Unparseable version {version_string}!",
            )
        results = matcher.groupdict()
        return Version(int(results["major"]), int(results["minor"]))

    def next_minor_version(self) -> "Version":
        """
        Returns the next minor version
        """
        return Version(self.major, self.minor + 1)

    def next_major_version(self) -> "Version":
        """
        Returns the next major version
        """
        return Version(self.major + 1, 0)
