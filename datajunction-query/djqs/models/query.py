"""
Models for queries.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import msgpack

from djqs.enum import IntEnum
from djqs.typing import QueryState, Row


@dataclass
class BaseQuery:
    """
    Base class for query models.
    """

    catalog_name: str | None = None
    engine_name: str | None = None
    engine_version: str | None = None


@dataclass
class Query(BaseQuery):  # pylint: disable=too-many-instance-attributes
    """
    A query.
    """

    id: UUID = field(default_factory=uuid4)  # pylint: disable=invalid-name
    submitted_query: str = ""
    catalog_name: str = ""
    engine_name: str = ""
    engine_version: str = ""
    async_: bool = False
    executed_query: str | None = None
    scheduled: datetime | None = None
    started: datetime | None = None
    finished: datetime | None = None
    state: QueryState = QueryState.UNKNOWN
    progress: float = 0.0


@dataclass
class QueryCreate(BaseQuery):
    """
    Model for submitted queries.
    """

    submitted_query: str = ""
    async_: bool = False


@dataclass
class ColumnMetadata:
    """
    A simple model for column metadata.
    """

    name: str
    type: str


@dataclass
class StatementResults:
    """
    Results for a given statement.

    This contains the SQL, column names and types, and rows
    """

    sql: str
    columns: list[ColumnMetadata] = field(default_factory=list)
    rows: list[Row] = field(default_factory=list)
    row_count: int = 0  # used for pagination


@dataclass
class QueryResults(BaseQuery):  # pylint: disable=too-many-instance-attributes
    """
    Model for query with results.
    """

    id: uuid.UUID = field(default_factory=uuid4)  # pylint: disable=invalid-name
    engine_name: str | None = None
    engine_version: str | None = None
    submitted_query: str = ""
    executed_query: str | None = None
    scheduled: datetime | None = None
    started: datetime | None = None
    finished: datetime | None = None
    state: QueryState = QueryState.UNKNOWN
    async_: bool = False
    progress: float = 0.0
    results: list[StatementResults] = field(default_factory=list)
    next: str | None = None  # Changed to str, as AnyHttpUrl was from pydantic
    previous: str | None = None  # Changed to str, as AnyHttpUrl was from pydantic
    errors: list[str] = field(default_factory=list)


class QueryExtType(IntEnum):
    """
    Custom ext type for msgpack.
    """

    UUID = 1
    DATETIME = 2


def encode_results(obj: Any) -> Any:
    """
    Custom msgpack encoder for ``QueryWithResults``.
    """
    if isinstance(obj, uuid.UUID):
        return msgpack.ExtType(QueryExtType.UUID, str(obj).encode("utf-8"))

    if isinstance(obj, datetime):
        return msgpack.ExtType(QueryExtType.DATETIME, obj.isoformat().encode("utf-8"))

    return obj  # pragma: no cover


def decode_results(code: int, data: bytes) -> Any:
    """
    Custom msgpack decoder for ``QueryWithResults``.
    """
    if code == QueryExtType.UUID:
        return uuid.UUID(data.decode())

    if code == QueryExtType.DATETIME:
        return datetime.fromisoformat(data.decode())

    return msgpack.ExtType(code, data)  # pragma: no cover
