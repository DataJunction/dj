"""
Models for collections
"""

from typing import Optional

from pydantic.main import BaseModel
from pydantic import ConfigDict

from datajunction_server.models.node import NodeNameOutput


class CollectionInfo(BaseModel):
    """
    Class for a collection information
    """

    id: Optional[int] = None
    name: str
    description: str

    model_config = ConfigDict(from_attributes=True)


class CollectionDetails(CollectionInfo):
    """
    Collection information with details
    """

    id: Optional[int] = None
    name: str
    description: str
    nodes: list[NodeNameOutput]

    model_config = ConfigDict(from_attributes=True)
