"""
Models for queries.
"""

from datetime import datetime
from typing import Any, List, Optional

import msgpack
from pydantic import AnyHttpUrl, validator
from pydantic.fields import Field
from pydantic.main import BaseModel

from datajunction_server.enum import IntEnum
from datajunction_server.typing import QueryState, Row


class BaseQuery(BaseModel):
    """
    Base class for query models.
    """

    catalog_name: Optional[str]
    engine_name: Optional[str] = None
    engine_version: Optional[str] = None

    class Config:
        allow_population_by_field_name = True


class QueryCreate(BaseQuery):
    """
    Model for submitted queries.
    """

    engine_name: str
    engine_version: str
    catalog_name: str
    submitted_query: str
    async_: bool = False


class ColumnMetadata(BaseModel):
    """
    A simple model for column metadata.
    """

    name: str
    type: str
    column: Optional[str]
    node: Optional[str]
    semantic_entity: Optional[str]
    semantic_type: Optional[str]

    def __hash__(self):
        return hash((self.name, self.type))  # pragma: no cover


class StatementResults(BaseModel):
    """
    Results for a given statement.

    This contains the SQL, column names and types, and rows
    """

    sql: str
    columns: List[ColumnMetadata]
    rows: List[Row]

    # this indicates the total number of rows, and is useful for paginated requests
    row_count: int = 0


class QueryResults(BaseModel):
    """
    Results for a given query.
    """

    __root__: List[StatementResults]


class TableRef(BaseModel):
    """
    Table reference
    """

    catalog: str
    schema_: str = Field(alias="schema")
    table: str


class QueryWithResults(BaseModel):
    """
    Model for query with results.
    """

    id: str
    engine_name: Optional[str] = None
    engine_version: Optional[str] = None
    submitted_query: str
    executed_query: Optional[str] = None

    scheduled: Optional[datetime] = None
    started: Optional[datetime] = None
    finished: Optional[datetime] = None

    state: QueryState = QueryState.UNKNOWN
    progress: float = 0.0

    output_table: Optional[TableRef]
    results: QueryResults
    next: Optional[AnyHttpUrl] = None
    previous: Optional[AnyHttpUrl] = None
    errors: List[str]
    links: Optional[List[AnyHttpUrl]] = None

    @validator("scheduled", pre=True)
    def parse_scheduled_date_string(cls, value):
        """
        Convert string date values to datetime
        """
        return datetime.fromisoformat(value) if isinstance(value, str) else value

    @validator("started", pre=True)
    def parse_started_date_string(cls, value):
        """
        Convert string date values to datetime
        """
        return datetime.fromisoformat(value) if isinstance(value, str) else value

    @validator("finished", pre=True)
    def parse_finisheddate_string(cls, value):
        """
        Convert string date values to datetime
        """
        return datetime.fromisoformat(value) if isinstance(value, str) else value


class QueryExtType(IntEnum):
    """
    Custom ext type for msgpack.
    """

    UUID = 1
    TIMESTAMP = 2


def encode_results(obj: Any) -> Any:
    """
    Custom msgpack encoder for ``QueryWithResults``.
    """
    if isinstance(obj, datetime):
        return msgpack.ExtType(QueryExtType.TIMESTAMP, obj.isoformat().encode("utf-8"))

    return obj


def decode_results(code: int, data: bytes) -> Any:
    """
    Custom msgpack decoder for ``QueryWithResults``.
    """
    if code == QueryExtType.TIMESTAMP:
        return datetime.fromisoformat(data.decode())

    return msgpack.ExtType(code, data)
