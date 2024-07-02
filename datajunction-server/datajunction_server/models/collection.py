"""
Models for collections
"""

from pydantic.main import BaseModel


class CollectionInfo(BaseModel):
    """
    Class for a collection information
    """

    name: str
    description: str

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True
