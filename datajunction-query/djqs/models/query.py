"""
Models for queries.
"""

import uuid
from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID, uuid4

import msgpack
from pydantic import AnyHttpUrl
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy_utils import UUIDType
from sqlmodel import Field, SQLModel

from djqs.enum import IntEnum
from djqs.typing import QueryState, Row


class BaseQuery(SQLModel):
    """
    Base class for query models.
    """

    catalog_name: Optional[str]
    engine_name: Optional[str] = None
    engine_version: Optional[str] = None

    class Config:  # pylint: disable=too-few-public-methods, missing-class-docstring
        allow_population_by_field_name = True


class Query(BaseQuery, table=True):  # type: ignore
    """
    A query.
    """

    id: UUID = Field(
        default_factory=uuid4,
        sa_column=SqlaColumn(UUIDType(), primary_key=True),
    )
    submitted_query: str
    catalog_name: str
    engine_name: str
    engine_version: str
    async_: bool
    executed_query: Optional[str] = None
    scheduled: Optional[datetime] = None
    started: Optional[datetime] = None
    finished: Optional[datetime] = None

    state: QueryState = QueryState.UNKNOWN
    progress: float = 0.0


class QueryCreate(BaseQuery):
    """
    Model for submitted queries.
    """

    submitted_query: str
    async_: bool = False


class ColumnMetadata(SQLModel):
    """
    A simple model for column metadata.
    """

    name: str
    type: str


class StatementResults(SQLModel):
    """
    Results for a given statement.

    This contains the SQL, column names and types, and rows
    """

    sql: str
    columns: List[ColumnMetadata]
    rows: List[Row]

    # this indicates the total number of rows, and is useful for paginated requests
    row_count: int = 0


class Results(SQLModel):
    """
    Results for a given query.
    """

    __root__: List[StatementResults]


class QueryResults(BaseQuery):
    """
    Model for query with results.
    """

    id: uuid.UUID
    engine_name: Optional[str] = None
    engine_version: Optional[str] = None
    submitted_query: str
    executed_query: Optional[str] = None

    scheduled: Optional[datetime] = None
    started: Optional[datetime] = None
    finished: Optional[datetime] = None

    state: QueryState = QueryState.UNKNOWN
    progress: float = 0.0

    results: Results
    next: Optional[AnyHttpUrl] = None
    previous: Optional[AnyHttpUrl] = None
    errors: List[str]


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
