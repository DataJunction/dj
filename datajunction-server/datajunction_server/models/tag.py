"""
Models for tags.
"""
from typing import TYPE_CHECKING, Any, Dict, Optional

from pydantic import Extra
from pydantic.main import BaseModel

if TYPE_CHECKING:
    pass


class MutableTagFields(BaseModel):
    """
    Tag fields that can be changed.
    """

    description: Optional[str]
    display_name: Optional[str]
    tag_metadata: Optional[Dict[str, Any]] = {}

    class Config:  # pylint: disable=too-few-public-methods
        """
        Allow types for tag metadata.
        """

        arbitrary_types_allowed = True


class ImmutableTagFields(BaseModel):
    """
    Tag fields that cannot be changed.
    """

    name: str
    tag_type: str


class CreateTag(ImmutableTagFields, MutableTagFields):
    """
    Create tag model.
    """


class TagMinimum(BaseModel):
    """
    Output tag model.
    """

    name: str

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class TagOutput(ImmutableTagFields, MutableTagFields):
    """
    Output tag model.
    """

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class UpdateTag(MutableTagFields):
    """
    Update tag model. Only works on mutable fields.
    """

    __annotations__ = {
        k: Optional[v]
        for k, v in {
            **MutableTagFields.__annotations__,  # pylint: disable=E1101
        }.items()
    }

    class Config:  # pylint: disable=too-few-public-methods
        """
        Do not allow fields other than the ones defined here.
        """

        extra = Extra.forbid
