"""
Models for collections
"""

from pydantic import ConfigDict
from pydantic.main import BaseModel

from datajunction_server.models.node import NodeNameOutput


class CollectionInfo(BaseModel):
    """
    Class for a collection information
    """

    id: int | None = None
    name: str
    description: str

    model_config = ConfigDict(from_attributes=True)


class CollectionDetails(CollectionInfo):
    """
    Collection information with details
    """

    id: int | None = None
    name: str
    description: str
    nodes: list[NodeNameOutput]

    model_config = ConfigDict(from_attributes=True)
