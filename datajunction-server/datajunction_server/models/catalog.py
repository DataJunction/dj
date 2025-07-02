"""
Models for columns.
"""

from typing import TYPE_CHECKING, List, Optional

from pydantic.main import BaseModel

from datajunction_server.models.engine import EngineInfo
from pydantic import ConfigDict

if TYPE_CHECKING:
    pass


class CatalogInfo(BaseModel):
    """
    Class for catalog creation
    """

    name: str
    engines: Optional[List[EngineInfo]] = []
    model_config = ConfigDict(from_attributes=True)
