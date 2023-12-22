"""
Models for databases.
"""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Dict, List, TypedDict
from uuid import UUID, uuid4

from pydantic.main import BaseModel
from sqlalchemy import JSON, BigInteger, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import UUIDType

from datajunction_server.database.connection import Base
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.models.table import Table


# Schema of a database in the YAML file.
DatabaseYAML = TypedDict(
    "DatabaseYAML",
    {"description": str, "URI": str, "read-only": bool, "async_": bool, "cost": float},
    total=False,
)


class DatabaseOutput(BaseModel):
    """
    Output for database information.
    """

    uuid: UUID
    name: str
    description: str
    URI: str
    async_: bool
    cost: float


class Database(Base):  # pylint: disable=too-few-public-methods
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
    description: Mapped[str] = mapped_column(String, default="")
    URI: Mapped[str]
    extra_params: Mapped[Dict] = mapped_column(JSON, default={})
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
