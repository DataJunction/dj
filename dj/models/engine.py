"""
Models for columns.
"""

from typing import Optional

from sqlmodel import Field, SQLModel

from dj.models.base import BaseSQLModel


class Engine(BaseSQLModel, table=True):  # type: ignore
    """
    A query engine.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    version: str
    uri: Optional[str]


class EngineInfo(SQLModel):
    """
    Class for engine creation
    """

    name: str
    version: str
    uri: Optional[str]
