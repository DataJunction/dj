"""
Models for columns.
"""

from typing import Optional

from sqlmodel import Field, SQLModel


class Engine(SQLModel, table=True):  # type: ignore
    """
    A query engine.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    version: str
    uri: Optional[str]


class BaseEngineInfo(SQLModel):
    """
    Class for engine creation
    """

    name: str
    version: str


class EngineInfo(BaseEngineInfo):
    """
    Class for engine creation
    """

    uri: str
