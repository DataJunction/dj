"""
Models for columns.
"""
from typing import TYPE_CHECKING, List, Optional

from pydantic.main import BaseModel

from datajunction_server.models.engine import EngineInfo

if TYPE_CHECKING:
    pass


class CatalogInfo(BaseModel):
    """
    Class for catalog creation
    """

    name: str
    engines: Optional[List[EngineInfo]] = []

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True
