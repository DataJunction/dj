from typing import Optional, List

from datajunction_server.models.node import NodeOutput

from pydantic.main import BaseModel

class CollectionInfo(BaseModel):
    """
    Class for a collection information
    """

    name: str
    description: str

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True

class CollectionInfoWithNodes(BaseModel):
    """
    Class for collection information including node information
    """

    name: str
    description: str
    nodes: Optional[List[NodeOutput]]

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True
