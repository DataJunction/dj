"""
Models for columns.
"""

from pydantic import ConfigDict, Field
from pydantic.main import BaseModel

from datajunction_server.models.engine import EngineInfo


class CatalogInfo(BaseModel):
    """
    Class for catalog creation
    """

    name: str
    engines: list[EngineInfo] | None = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)
