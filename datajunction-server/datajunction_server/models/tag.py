"""
Models for tags.
"""

from typing import TYPE_CHECKING, Any, Dict, Optional

from pydantic import ConfigDict, Field
from pydantic.main import BaseModel

if TYPE_CHECKING:
    pass


class MutableTagFields(BaseModel):
    """
    Tag fields that can be changed.
    """

    description: Optional[str] = None
    display_name: Optional[str] = None
    tag_metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)


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
    model_config = ConfigDict(from_attributes=True)


class TagOutput(ImmutableTagFields, MutableTagFields):
    """
    Output tag model.
    """

    model_config = ConfigDict(from_attributes=True)


class UpdateTag(MutableTagFields):
    """
    Update tag model. Only works on mutable fields.
    """

    model_config = ConfigDict(extra="forbid")
