"""Engine database schema."""
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import Enum
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.models.engine import Dialect


class Engine(Base):  # pylint: disable=too-few-public-methods
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
    dialect: Mapped[Optional[Dialect]] = mapped_column(
        Enum(Dialect),
    )
