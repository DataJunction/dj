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

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class CollectionDetails(CollectionInfo):
    """
    Collection information with details
    """

    id: Optional[int]
    name: str
    description: str
    nodes: list[NodeNameOutput]

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True
