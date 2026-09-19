"""
Models for columns.
"""

from pydantic import ConfigDict
from pydantic.main import BaseModel

from datajunction_server.models.dialect import Dialect


class EngineInfo(BaseModel):
    """
    Class for engine creation
    """

    name: str
    version: str
    uri: str | None = None
    dialect: Dialect | None = None

    model_config = ConfigDict(from_attributes=True)


class EngineRef(BaseModel):
    """
    Basic reference to an engine
    """

    name: str
    version: str
