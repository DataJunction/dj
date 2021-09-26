"""
Models for nodes.
"""

from typing import List, Optional

from sqlmodel import Field, Relationship, SQLModel


class Database(SQLModel, table=True):
    """
    A database.

    A simple example::

        name: druid
        description: An Apache Druid database
        URI: druid://localhost:8082/druid/v2/sql/
        mode: read-only

    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    description: str = ""
    URI: str
    read_only: bool = True


class Node(SQLModel):
    """
    A node.

    DataJunction has 5 types of nodes:

        - source
        - transform
        - metric
        - dimension
        - population

    """


class Source(Node, table=True):
    """
    A source node.

    Source nodes point to 1+ database sources (either a table or view).
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    description: str = ""

    columns: List["Column"] = Relationship(back_populates="source")
    representations: List["Representation"] = Relationship(back_populates="source")


class Representation(SQLModel, table=True):
    """
    A representation of data.

    Source nodes can have multiple representations of data, in different databases.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    catalog: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    table: str
    cost: float = 1

    source_id: int = Field(foreign_key="source.id")
    source: Source = Relationship(back_populates="representations")


class Column(SQLModel, table=True):
    """
    A column.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    type: str

    # only-on

    source_id: int = Field(foreign_key="source.id")
    source: Source = Relationship(back_populates="columns")
