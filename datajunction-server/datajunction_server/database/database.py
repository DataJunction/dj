"""Database schema"""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from sqlalchemy import JSON, BigInteger, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import UUIDType

from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.database.column import Column


class Database(Base):
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

    __tablename__ = "database"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    uuid: Mapped[UUID] = mapped_column(UUIDType(), default=uuid4)

    name: Mapped[str] = mapped_column(String, unique=True)
    description: Mapped[Optional[str]] = mapped_column(String, default="")
    URI: Mapped[str]
    extra_params: Mapped[Optional[Dict]] = mapped_column(JSON, default={})
    read_only: Mapped[bool] = mapped_column(default=True)
    async_: Mapped[bool] = mapped_column(default=False, name="async")
    cost: Mapped[float] = mapped_column(default=1.0)

    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
    )

    tables: Mapped[List["Table"]] = relationship(
        back_populates="database",
        cascade="all, delete",
    )

    def __hash__(self) -> int:
        return hash(self.id)


class Table(Base):
    """
    A table with data.

    Nodes can have data in multiple tables, in different databases.
    """

    __tablename__ = "table"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )

    schema: Mapped[Optional[str]] = mapped_column(default=None, name="schema_")
    table: Mapped[str]
    cost: Mapped[float] = mapped_column(default=1.0)

    database_id: Mapped[int] = mapped_column(
        ForeignKey("database.id", name="fk_table_database_id_database"),
    )
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
            self.catalog.name if self.catalog else None,
            self.schema_,
            self.table,
        )

    def __hash__(self):
        return hash(self.id)


class TableColumns(Base):
    """
    Join table for table columns.
    """

    __tablename__ = "tablecolumns"

    table_id: Mapped[int] = mapped_column(
        ForeignKey("table.id", name="fk_tablecolumns_table_id_table"),
        primary_key=True,
    )
    column_id: Mapped[int] = mapped_column(
        ForeignKey("column.id", name="fk_tablecolumns_column_id_column"),
        primary_key=True,
    )
