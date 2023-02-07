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
