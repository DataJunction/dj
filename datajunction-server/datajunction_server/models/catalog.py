"""
Models for columns.
"""

from typing import TYPE_CHECKING, List, Optional

from pydantic.main import BaseModel
from pydantic import ConfigDict, Field

from datajunction_server.models.engine import EngineInfo

if TYPE_CHECKING:
    pass


class CatalogInfo(BaseModel):
    """
    Class for catalog creation
    """

    name: str
    engines: Optional[List[EngineInfo]] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)
