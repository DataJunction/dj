"""
Models for columns.
"""

from typing import Optional

from pydantic.main import BaseModel
from datajunction_server.models.dialect import Dialect


class EngineInfo(BaseModel):
    """
    Class for engine creation
    """

    name: str
    version: str
    uri: Optional[str]
    dialect: Optional[Dialect]

    class Config:
        orm_mode = True


class EngineRef(BaseModel):
    """
    Basic reference to an engine
    """

    name: str
    version: str
