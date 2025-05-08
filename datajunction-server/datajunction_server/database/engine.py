"""Engine database schema."""

from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base


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
    dialect: Mapped[Optional[str]] = mapped_column(sa.String)
