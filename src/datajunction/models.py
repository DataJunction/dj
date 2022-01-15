"""
Models for nodes and everything else.
"""

from datetime import datetime, timezone
from enum import Enum
from functools import partial
from typing import Dict, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import Column as SqlaColumn
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import UUIDType
from sqlmodel import Field, Relationship, SQLModel
from sqloxide import parse_sql

from datajunction.sql.inference import get_column_from_expression
from datajunction.sql.parse import find_nodes_by_key
from datajunction.utils import get_more_specific_type

Base = declarative_base()


class Database(SQLModel, table=True):  # type: ignore
    """
    A database.

    A simple example:

        name: druid
        description: An Apache Druid database
        URI: druid://localhost:8082/druid/v2/sql/
        read-only: true
        async_: false

    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    description: str = ""
    URI: str
    read_only: bool = True
    async_: bool = Field(default=False, sa_column_kwargs={"name": "async"})

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


class NodeRelationship(SQLModel, table=True):  # type: ignore
    """
    Join table for self-referential many-to-many relationships between nodes.
    """

    parent_id: Optional[int] = Field(
        default=None,
        foreign_key="node.id",
        primary_key=True,
    )
    child_id: Optional[int] = Field(
        default=None,
        foreign_key="node.id",
        primary_key=True,
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

    tables: List["Table"] = Relationship(
        back_populates="node",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )

    parents: List["Node"] = Relationship(
        back_populates="children",
        link_model=NodeRelationship,
        sa_relationship_kwargs=dict(
            primaryjoin="Node.id==NodeRelationship.child_id",
            secondaryjoin="Node.id==NodeRelationship.parent_id",
        ),
    )

    children: List["Node"] = Relationship(
        back_populates="parents",
        link_model=NodeRelationship,
        sa_relationship_kwargs=dict(
            primaryjoin="Node.id==NodeRelationship.parent_id",
            secondaryjoin="Node.id==NodeRelationship.child_id",
        ),
    )

    @property
    def columns(self) -> List["Column"]:
        """
        Return the node schema.

        The schema is the superset of the columns across all tables, with the strictest
        type. Eg, if a node has a table with these types:

            timestamp: str
            user_id: int

        And another table with a single column:

            timestamp: datetime

        The node will have these columns:

            timestamp: datetime
            user_id: int

        """
        if self.expression:
            return self._infer_columns()

        columns: Dict[str, "Column"] = {}
        for table in self.tables:  # pylint: disable=not-an-iterable
            for column in table.columns:
                name = column.name
                columns[name] = Column(
                    name=name,
                    type=get_more_specific_type(columns[name].type, column.type)
                    if name in columns
                    else column.type,
                )

        return list(columns.values())

    def _infer_columns(self) -> List["Column"]:  # pylint: disable=too-many-branches
        """
        Infer columns based on parent nodes.
        """
        tree = parse_sql(self.expression, dialect="ansi")

        # Use the first projection. We actually want to check that all the projections
        # produce the same columns, and raise an error if not.
        projection = next(find_nodes_by_key(tree, "projection"))

        columns = []
        for expression in projection:
            alias: Optional[str] = None
            if "UnnamedExpr" in expression:
                expression = expression["UnnamedExpr"]
            elif "ExprWithAlias" in expression:
                alias = expression["ExprWithAlias"]["alias"]["value"]
                expression = expression["ExprWithAlias"]["expr"]
            else:
                raise NotImplementedError(f"Unable to handle expression: {expression}")

            columns.append(get_column_from_expression(self.parents, expression, alias))

        # name nameless columns
        i = 0
        for column in columns:
            if column.name is None:
                column.name = f"_col{i}"
                i += 1

        return columns


class Table(SQLModel, table=True):  # type: ignore
    """
    A table with data.

    Nodes can data in multiple tables, in different databases.
    """

    id: Optional[int] = Field(default=None, primary_key=True)

    node_id: int = Field(foreign_key="node.id")
    node: Node = Relationship(back_populates="tables")

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
