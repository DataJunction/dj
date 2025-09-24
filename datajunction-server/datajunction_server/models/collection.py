"""
Models for collections
"""

from typing import Optional

from pydantic.main import BaseModel

from datajunction_server.models.node import NodeNameOutput


class CollectionInfo(BaseModel):
    """
    Class for a collection information
    """

    id: Optional[int]
    name: str
    description: str

    class Config:
        model_config = {"from_attributes": True}


class CollectionDetails(CollectionInfo):
    """
    Collection information with details
    """

    id: Optional[int]
    name: str
    description: str
    nodes: list[NodeNameOutput]

    class Config:
        model_config = {"from_attributes": True}
