"""Engine database schema."""

from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

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

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str]
    version: Mapped[str]
    uri: Mapped[Optional[str]]
    dialect: Mapped[Optional[Dialect]] = mapped_column(DialectType())
