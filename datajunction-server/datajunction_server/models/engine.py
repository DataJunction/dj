"""
Models for columns.
"""
import enum
from typing import Optional

from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.types import Enum
from sqlmodel import Field, SQLModel

from datajunction_server.models.base import BaseSQLModel


class Dialect(str, enum.Enum):
    """
    SQL dialect
    """

    SPARK = "spark"
    TRINO = "trino"
    DRUID = "druid"


class Engine(BaseSQLModel, table=True):  # type: ignore
    """
    A query engine.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    version: str
    uri: Optional[str]
    dialect: Optional[Dialect] = Field(sa_column=SqlaColumn(Enum(Dialect)))


class EngineInfo(SQLModel):
    """
    Class for engine creation
    """

    name: str
    version: str
    uri: Optional[str]
    dialect: Optional[Dialect]


class EngineRef(SQLModel):
    """
    Basic reference to an engine
    """

    name: str
    version: str
