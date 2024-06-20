from typing import Optional, List

from datajunction_server.models.node import NodeOutput

from pydantic.main import BaseModel

class CreateCollection(BaseModel):
    """
    Class for a collection creation request
    """

    name: str
    description: str

class CollectionInfo(BaseModel):
    """
    Class for collection information
    """

    name: str
    description: str
    nodes: Optional[List[NodeOutput]]

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True
