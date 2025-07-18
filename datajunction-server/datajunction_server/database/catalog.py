"""Catalog database schema."""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import UUIDType
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.base import Base
from datajunction_server.database.engine import Engine
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import get_settings

if TYPE_CHECKING:
    from datajunction_server.database.node import NodeRevision


class Catalog(Base):
    """
    A catalog.
    """

    __tablename__ = "catalog"
    __table_args__ = (UniqueConstraint("name", name="uq_catalog_name"),)

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    uuid: Mapped[UUID] = mapped_column(UUIDType(), default=uuid4)
    name: Mapped[str] = mapped_column(String)
    engines: Mapped[List[Engine]] = relationship(
        secondary="catalogengines",
        primaryjoin="Catalog.id==CatalogEngines.catalog_id",
        secondaryjoin="Engine.id==CatalogEngines.engine_id",
        lazy="selectin",
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
    extra_params: Mapped[Optional[Dict]] = mapped_column(JSON, default={})

    def __str__(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(self.id)

    async def get_by_names(session: AsyncSession, names: list[str]) -> list["Catalog"]:
        """
        Get catalogs by their names.
        """
        statement = select(Catalog).filter(Catalog.name.in_(names))
        return (await session.execute(statement)).scalars().all()

    async def get_by_name(session: AsyncSession, name: str) -> Optional["Catalog"]:
        """
        Get catalog by its name.
        """
        statement = select(Catalog).where(Catalog.name == name)
        return (await session.execute(statement)).scalar_one_or_none()

    async def get_virtual_catalog(session: AsyncSession) -> "Catalog":
        """
        Get the virtual catalog
        """
        settings = get_settings()
        catalog = await Catalog.get_by_name(
            session,
            settings.seed_setup.virtual_catalog_name,
        )
        if not catalog:
            raise DJDoesNotExistException(  # pragma: no cover
                f"Virtual catalog {settings.seed_setup.virtual_catalog_name} does not exist.",
            )
        return catalog


class CatalogEngines(Base):  # type: ignore
    """
    Join table for catalogs and engines.
    """

    __tablename__ = "catalogengines"

    catalog_id: Mapped[int] = mapped_column(
        ForeignKey("catalog.id", name="fk_catalogengines_catalog_id_catalog"),
        primary_key=True,
    )
    engine_id: Mapped[int] = mapped_column(
        ForeignKey("engine.id", name="fk_catalogengines_engine_id_engine"),
        primary_key=True,
    )
