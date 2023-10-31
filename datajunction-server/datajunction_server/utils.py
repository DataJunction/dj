"""
Utility functions.
"""
import logging
import os
import re
from enum import Enum
from functools import lru_cache
from string import ascii_letters, digits

# pylint: disable=line-too-long
from typing import Iterator, List, Optional

from dotenv import load_dotenv
from rich.logging import RichHandler
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine
from starlette.requests import Request
from yarl import URL

from datajunction_server.config import Settings
from datajunction_server.errors import DJException
from datajunction_server.models.user import User
from datajunction_server.service_clients import QueryServiceClient


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
    if not settings.query_service:  # pragma: no cover
        return None
    return QueryServiceClient(settings.query_service)


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


def get_namespace_from_name(name: str) -> str:
    """
    Splits a qualified node name into it's namespace and name parts
    """
    if "." in name:
        node_namespace, _ = name.rsplit(".", 1)
    else:  # pragma: no cover
        raise DJException(f"No namespace provided: {name}")
    return node_namespace


ACCEPTABLE_CHARS = set(ascii_letters + digits + "_")
LOOKUP_CHARS = {
    ".": "DOT",
    "'": "QUOTE",
    '"': "DQUOTE",
    "`": "BTICK",
    "!": "EXCL",
    "@": "AT",
    "#": "HASH",
    "$": "DOLLAR",
    "%": "PERC",
    "^": "CARAT",
    "&": "AMP",
    "*": "STAR",
    "(": "LPAREN",
    ")": "RPAREN",
    "[": "LBRACK",
    "]": "RBRACK",
    "-": "MINUS",
    "+": "PLUS",
    "=": "EQ",
    "/": "FSLSH",
    "\\": "BSLSH",
    "|": "PIPE",
    "~": "TILDE",
}


def amenable_name(name: str) -> str:
    """Takes a string and makes it have only alphanumerics"""
    ret: List[str] = []
    cont: List[str] = []
    for char in name:
        if char in ACCEPTABLE_CHARS:
            cont.append(char)
        else:
            ret.append("".join(cont))
            ret.append(LOOKUP_CHARS.get(char, "UNK"))
            cont = []

    return ("_".join(ret) + "_" + "".join(cont)).strip("_")


def from_amenable_name(name: str) -> str:
    """
    Takes a string and converts it back to a namespaced name
    """
    to_replace = f"_{LOOKUP_CHARS[SEPARATOR]}_"
    return name.replace(to_replace, SEPARATOR)


async def get_current_user(request: Request) -> Optional[User]:
    """
    Returns the current authenticated user
    """
    if hasattr(request.state, "user"):
        return request.state.user
    return None  # pragma: no cover


SEPARATOR = "."
