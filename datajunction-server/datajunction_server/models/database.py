"""
Models for databases.
"""

from typing import TypedDict
from uuid import UUID

from pydantic.main import BaseModel

# Schema of a database in the YAML file.
DatabaseYAML = TypedDict(
    "DatabaseYAML",
    {"description": str, "URI": str, "read-only": bool, "async_": bool, "cost": float},
    total=False,
)


class DatabaseOutput(BaseModel):
    """
    Output for database information.
    """

    uuid: UUID
    name: str
    description: str
    URI: str
    async_: bool
    cost: float
