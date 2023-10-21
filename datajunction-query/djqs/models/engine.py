"""
Models for columns.
"""
from enum import Enum
from typing import Dict, Optional

from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlmodel import JSON, Field, SQLModel


class EngineType(Enum):
    """
    Supported engine types
    """

    DUCKDB = "duckdb"
    SQLALCHEMY = "sqlalchemy"
    SNOWFLAKE = "snowflake"


class Engine(SQLModel, table=True):  # type: ignore
    """
    A query engine.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    type: EngineType
    version: str
    uri: Optional[str]
    extra_params: Dict = Field(default={}, sa_column=SqlaColumn(JSON))


class BaseEngineInfo(SQLModel):
    """
    Class for engine creation
    """

    name: str
    version: str
    type: EngineType
    extra_params: Dict = {}


class EngineInfo(BaseEngineInfo):
    """
    Class for engine creation
    """

    uri: str
