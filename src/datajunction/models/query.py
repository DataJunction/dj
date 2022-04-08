"""
Models for queries.
"""

import uuid
from datetime import datetime
from typing import List, Optional
from uuid import UUID, uuid4

from pydantic import AnyHttpUrl
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy_utils import UUIDType
from sqlmodel import Field, Relationship, SQLModel

from datajunction.models.database import Database
from datajunction.typing import ColumnType, QueryState, Row


class BaseQuery(SQLModel):
    """
    Base class for query models.
    """

    database_id: int = Field(foreign_key="database.id")
    catalog: Optional[str] = None
    schema_: Optional[str] = None


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


class ColumnMetadata(SQLModel):
    """
    A simple model for column metadata.
    """

    name: str
    type: ColumnType


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
