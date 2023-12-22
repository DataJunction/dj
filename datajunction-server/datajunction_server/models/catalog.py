"""
Models for columns.
"""
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Optional
from uuid import UUID, uuid4

from pydantic.main import BaseModel
from sqlalchemy import JSON, BigInteger, DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy_utils import UUIDType

from datajunction_server.database.connection import Base
from datajunction_server.models.engine import Engine, EngineInfo
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.models import NodeRevision


class CatalogEngines(Base):  # type: ignore  # pylint: disable=too-few-public-methods
    """
    Join table for catalogs and engines.
    """

    __tablename__ = "catalogengines"

    catalog_id: Mapped[int] = mapped_column(
        ForeignKey("catalog.id"),
        primary_key=True,
    )
    engine_id: Mapped[int] = mapped_column(
        ForeignKey("engine.id"),
        primary_key=True,
    )


class Catalog(Base):  # type: ignore
    """
    A catalog.
    """

    __tablename__ = "catalog"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    uuid: Mapped[UUID] = mapped_column(UUIDType(), default=uuid4)
    name: Mapped[str]
    engines: Mapped[List[Engine]] = relationship(
        secondary="catalogengines",
        primaryjoin="Catalog.id==CatalogEngines.catalog_id",
        secondaryjoin="Engine.id==CatalogEngines.engine_id",
    )
    node_revisions: Mapped[List["NodeRevision"]] = relationship(
        back_populates="catalog",
    )
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )
    extra_params: Mapped[Dict] = mapped_column(JSON, default={})

    def __str__(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(self.id)


class CatalogInfo(BaseModel):
    """
    Class for catalog creation
    """

    name: str
    engines: Optional[List[EngineInfo]] = []

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True
