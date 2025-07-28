"""Engine database schema."""

from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import UniqueConstraint, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.base import Base
from datajunction_server.models.dialect import Dialect


class DialectType(sa.TypeDecorator):
    impl = sa.String

    def process_bind_param(self, value, dialect):
        if isinstance(value, Dialect):
            return value.name
        return value

    def process_result_value(self, value, dialect):
        return Dialect(value) if value else None


class Engine(Base):
    """
    A query engine.
    """

    __tablename__ = "engine"
    __table_args__ = (
        UniqueConstraint("name", "version", name="uq_engine_name_version"),
    )

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str]
    version: Mapped[str]
    uri: Mapped[Optional[str]]
    dialect: Mapped[Optional[Dialect]] = mapped_column(DialectType())

    async def get_by_names(session: AsyncSession, names: list[str]) -> list["Engine"]:
        """
        Get engines by their names.
        """
        statement = select(Engine).filter(Engine.name.in_(names))
        return (await session.execute(statement)).scalars().all()

    async def get_by_name(session: AsyncSession, name: str) -> "Engine":
        """
        Get an engine by its name.
        """
        statement = select(Engine).where(Engine.name == name)
        return (await session.execute(statement)).scalar_one_or_none()
