"""
Models for nodes.
"""

import os
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from pathlib import Path
from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import String
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy_utils import UUIDType
from sqlmodel import Field, Relationship, SQLModel


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


class Database(SQLModel, table=True):  # type: ignore
    """
    A database.

    A simple example::

        name: druid
        description: An Apache Druid database
        URI: druid://localhost:8082/druid/v2/sql/
        read-only: true

    """

    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))
    updated_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))
    name: str
    description: str = ""
    URI: str
    read_only: bool = True
    async_: bool = Field(default=False, sa_column_kwargs={"name": "async"})

    representations: List["Representation"] = Relationship(
        back_populates="database",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )

    queries: List["Query"] = Relationship(
        back_populates="database",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )


class Node(SQLModel, table=True):  # type: ignore
    """
    A node.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(sa_column=SqlaColumn("name", String, unique=True))
    description: str = ""

    created_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))
    updated_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))

    expression: Optional[str] = None

    # schema
    columns: List["Column"] = Relationship(
        back_populates="node",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )

    # storages
    representations: List["Representation"] = Relationship(
        back_populates="node",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )


class Representation(SQLModel, table=True):  # type: ignore
    """
    A representation of data.

    Nodes can have multiple representations of data, in different databases.
    """

    id: Optional[int] = Field(default=None, primary_key=True)

    node_id: int = Field(foreign_key="node.id")
    node: Node = Relationship(back_populates="representations")

    database_id: int = Field(foreign_key="database.id")
    database: Database = Relationship(back_populates="representations")
    catalog: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    table: str

    cost: float = 1.0

    # aggregation_level => for materialized metrics?


class Column(SQLModel, table=True):  # type: ignore
    """
    A column.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    type: str

    # only-on => for columns that are present in only a few DBs

    node_id: int = Field(foreign_key="node.id")
    node: Node = Relationship(back_populates="columns")


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
