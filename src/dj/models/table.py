"""
Models for tables.
"""

from typing import TYPE_CHECKING, List, Optional, TypedDict

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from dj.models.column import Column
    from dj.models.database import Database
    from dj.models.node import Node


class TableYAML(TypedDict, total=False):
    """
    Schema of a table in the YAML file.
    """

    catalog: Optional[str]
    schema: Optional[str]
    table: str
    cost: float


class TableColumns(SQLModel, table=True):  # type: ignore
    """
    Join table for table columns.
    """

    table_id: Optional[int] = Field(
        default=None,
        foreign_key="table.id",
        primary_key=True,
    )
    column_id: Optional[int] = Field(
        default=None,
        foreign_key="column.id",
        primary_key=True,
    )


class Table(SQLModel, table=True):  # type: ignore
    """
    A table with data.

    Nodes can data in multiple tables, in different databases.
    """

    id: Optional[int] = Field(default=None, primary_key=True)

    node_id: int = Field(foreign_key="node.id")
    node: "Node" = Relationship(back_populates="tables")

    database_id: int = Field(foreign_key="database.id")
    database: "Database" = Relationship(back_populates="tables")
    catalog: Optional[str] = None
    schema_: Optional[str] = Field(default=None, alias="schema")
    table: str

    cost: float = 1.0

    columns: List["Column"] = Relationship(
        link_model=TableColumns,
        sa_relationship_kwargs={
            "primaryjoin": "Table.id==TableColumns.table_id",
            "secondaryjoin": "Column.id==TableColumns.column_id",
            "cascade": "all, delete",
        },
    )

    def to_yaml(self) -> TableYAML:
        """
        Serialize the table for YAML.
        """
        return {
            "catalog": self.catalog,
            "schema": self.schema_,
            "table": self.table,
            "cost": self.cost,
        }

    def __hash__(self):
        return hash(self.id)
