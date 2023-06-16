"""
Models for databases.
"""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Optional, TypedDict
from uuid import UUID, uuid4

from sqlalchemy import DateTime, String
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy_utils import UUIDType
from sqlmodel import JSON, Field, Relationship

from datajunction_server.models.base import BaseSQLModel
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.models.catalog import Catalog
    from datajunction_server.models.table import Table


# Schema of a database in the YAML file.
DatabaseYAML = TypedDict(
    "DatabaseYAML",
    {"description": str, "URI": str, "read-only": bool, "async_": bool, "cost": float},
    total=False,
)


class Database(BaseSQLModel, table=True):  # type: ignore
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
    uuid: UUID = Field(default_factory=uuid4, sa_column=SqlaColumn(UUIDType()))

    name: str = Field(sa_column=SqlaColumn("name", String, unique=True))
    description: str = ""
    URI: str
    extra_params: Dict = Field(default={}, sa_column=SqlaColumn(JSON))
    read_only: bool = True
    async_: bool = Field(default=False, sa_column_kwargs={"name": "async"})
    cost: float = 1.0

    created_at: UTCDatetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )
    updated_at: UTCDatetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )

    tables: List["Table"] = Relationship(
        back_populates="database",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )

    def __hash__(self) -> int:
        return hash(self.id)
