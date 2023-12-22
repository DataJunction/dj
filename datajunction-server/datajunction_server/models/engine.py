"""
Models for columns.
"""
from typing import Optional

import sqlalchemy as sa
from pydantic.main import BaseModel
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Enum

from datajunction_server.database.connection import Base
from datajunction_server.enum import StrEnum


class Dialect(StrEnum):
    """
    SQL dialect
    """

    SPARK = "spark"
    TRINO = "trino"
    DRUID = "druid"


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
    dialect: Mapped[Optional[Dialect]] = mapped_column(Enum(Dialect))


class EngineInfo(BaseModel):
    """
    Class for engine creation
    """

    name: str
    version: str
    uri: Optional[str]
    dialect: Optional[Dialect]

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class EngineRef(BaseModel):
    """
    Basic reference to an engine
    """

    name: str
    version: str
