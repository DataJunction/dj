"""
Models for columns.
"""
from typing import Optional

from pydantic.main import BaseModel

from datajunction_server.enum import StrEnum


class Dialect(StrEnum):
    """
    SQL dialect
    """

    SPARK = "spark"
    TRINO = "trino"
    DRUID = "druid"


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
