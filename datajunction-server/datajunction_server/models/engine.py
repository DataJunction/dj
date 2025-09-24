"""
Models for columns.
"""

from typing import Optional

from pydantic.main import BaseModel
from pydantic import ConfigDict
from datajunction_server.models.dialect import Dialect


class EngineInfo(BaseModel):
    """
    Class for engine creation
    """

    name: str
    version: str
    uri: Optional[str] = None
    dialect: Optional[Dialect] = None

    model_config = ConfigDict(from_attributes=True)


class EngineRef(BaseModel):
    """
    Basic reference to an engine
    """

    name: str
    version: str
