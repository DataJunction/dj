"""
Models for queries.
"""

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, List, Optional, Tuple
from uuid import UUID, uuid4

from pydantic import AnyHttpUrl
from sqlalchemy import Column as SqlaColumn
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import UUIDType
from sqlmodel import Field, Relationship, SQLModel

from datajunction.models.database import Database

Base = declarative_base()


class QueryState(str, Enum):
    """
    Different states of a query.
    """

    UNKNOWN = "UNKNOWN"
    ACCEPTED = "ACCEPTED"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"


class BaseQuery(SQLModel):
    """
    Base class for query models.
    """

    database_id: int = Field(foreign_key="database.id")
    catalog: Optional[str] = None
    schema_: Optional[str] = None  # XXX use alias  # pylint: disable=fixme


class Query(BaseQuery, table=True):  # type: ignore
    """
    A query.
    """

    id: UUID = Field(
        default_factory=uuid4,
        sa_column=SqlaColumn(UUIDType(), primary_key=True),
    )
    database: Database = Relationship(back_populates="queries")

    submitted_query: str
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


class TypeEnum(Enum):
    """
    PEP 249 basic types.

    Unfortunately SQLAlchemy doesn't seem to offer an API for determining the types of the
    columns in a (SQL Core) query, and the DB API 2.0 cursor only offers very coarse
    types.
    """

    STRING = "STRING"
    BINARY = "BINARY"
    NUMBER = "NUMBER"
    DATETIME = "DATETIME"
    UNKNOWN = "UNKNOWN"


class ColumnMetadata(SQLModel):
    """
    A simple model for column metadata.
    """

    name: str
    type: TypeEnum


class StatementResults(SQLModel):
    """
    Results for a given statement.

    This contains the SQL, column names and types, and rows
    """

    sql: str
    columns: List[ColumnMetadata]
    rows: List[Tuple[Any, ...]]

    # this indicates the total number of rows, and is useful for paginated requests
    row_count: int = 0


class QueryResults(SQLModel):
    """
    Results for a given query.
    """

    __root__: List[StatementResults]


class QueryWithResults(BaseQuery):
    """
    Model for query with results.
    """

    id: uuid.UUID

    submitted_query: str
    executed_query: Optional[str] = None

    scheduled: Optional[datetime] = None
    started: Optional[datetime] = None
    finished: Optional[datetime] = None

    state: QueryState = QueryState.UNKNOWN
    progress: float = 0.0

    results: QueryResults
    next: Optional[AnyHttpUrl] = None
    previous: Optional[AnyHttpUrl] = None
    errors: List[str]
