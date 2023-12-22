"""
Models for tables.
"""

from typing import TYPE_CHECKING, List, Optional, Tuple, TypedDict

from pydantic import Field
from pydantic.main import BaseModel
from sqlalchemy import BigInteger, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.schema import ForeignKey

from datajunction_server.database.connection import Base

if TYPE_CHECKING:
    from datajunction_server.models.column import Column
    from datajunction_server.models.database import Database


class TableYAML(TypedDict, total=False):
    """
    Schema of a table in the YAML file.
    """

    catalog: Optional[str]
    schema: Optional[str]
    table: str
    cost: float


class TableColumns(Base):  # type: ignore  # pylint: disable=too-few-public-methods
    """
    Join table for table columns.
    """

    __tablename__ = "tablecolumns"

    table_id: Mapped[int] = mapped_column(
        ForeignKey("table.id"),
        primary_key=True,
    )
    column_id: Mapped[int] = mapped_column(
        ForeignKey("column.id"),
        primary_key=True,
    )


class TableBase(BaseModel):  # pylint: disable=too-few-public-methods
    """
    A base table.
    """

    schema_: Optional[str] = Field(default=None, alias="schema")
    table: str
    cost: float = 1.0


class Table(Base):  # type: ignore
    """
    A table with data.

    Nodes can have data in multiple tables, in different databases.
    """

    __tablename__ = "table"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )

    schema_: Mapped[Optional[str]] = mapped_column(default=None, name="schema")
    table: Mapped[str]
    cost: Mapped[float] = mapped_column(default=1.0)

    database_id: Mapped[int] = mapped_column(ForeignKey("database.id"))
    database: Mapped["Database"] = relationship("Database", back_populates="tables")

    columns: Mapped[List["Column"]] = relationship(
        secondary="tablecolumns",
        primaryjoin="Table.id==TableColumns.table_id",
        secondaryjoin="Column.id==TableColumns.column_id",
        cascade="all, delete",
    )

    def identifier(
        self,
    ) -> Tuple[Optional[str], Optional[str], str]:  # pragma: no cover
        """
        Unique identifier for this table.
        """
        # Catalogs will soon be required and this return can be simplified
        return (
            self.catalog.name if self.catalog else None,  # pylint: disable=no-member
            self.schema_,
            self.table,
        )

    def __hash__(self):
        return hash(self.id)


class CreateColumn(BaseModel):
    """
    A column creation request
    """

    name: str
    type: str


class CreateTable(TableBase):
    """
    Create table input
    """

    database_name: str
    catalog_name: str
    columns: List[CreateColumn]
