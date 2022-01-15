"""
Models for databases, tables, and columns.
"""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, List, Optional

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from datajunction.models.node import Node
    from datajunction.models.query import Query


class Database(SQLModel, table=True):  # type: ignore
    """
    A database.

    A simple example:

        name: druid
        description: An Apache Druid database
        URI: druid://localhost:8082/druid/v2/sql/
        read-only: true
        async_: false
        cost: 1.0

    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    description: str = ""
    URI: str
    read_only: bool = True
    async_: bool = Field(default=False, sa_column_kwargs={"name": "async"})
    cost: float = 1.0

    created_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))
    updated_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))

    tables: List["Table"] = Relationship(
        back_populates="database",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )

    queries: List["Query"] = Relationship(
        back_populates="database",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )

    def __hash__(self):
        return hash(self.id)


class Table(SQLModel, table=True):  # type: ignore
    """
    A table with data.

    Nodes can data in multiple tables, in different databases.
    """

    id: Optional[int] = Field(default=None, primary_key=True)

    node_id: int = Field(foreign_key="node.id")
    node: "Node" = Relationship(back_populates="tables")

    database_id: int = Field(foreign_key="database.id")
    database: Database = Relationship(back_populates="tables")
    catalog: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    table: str

    cost: float = 1.0

    columns: List["Column"] = Relationship(
        back_populates="table",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )


class Column(SQLModel, table=True):  # type: ignore
    """
    A column.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    type: str

    table_id: int = Field(foreign_key="table.id")
    table: Table = Relationship(back_populates="columns")
