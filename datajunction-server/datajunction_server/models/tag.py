"""
Models for tags.
"""
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pydantic import Extra
from sqlalchemy import String
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlmodel import JSON, Field, Relationship

from datajunction_server.models.base import BaseSQLModel, generate_display_name

if TYPE_CHECKING:
    from datajunction_server.models.node import Node


class MutableTagFields(BaseSQLModel):
    """
    Tag fields that can be changed.
    """

    description: str
    display_name: Optional[str] = Field(
        sa_column=SqlaColumn(
            "display_name",
            String,
            default=generate_display_name("name"),
        ),
    )
    tag_metadata: Dict[str, Any] = Field(default={}, sa_column=SqlaColumn(JSON))

    class Config:  # pylint: disable=too-few-public-methods
        """
        Allow types for tag metadata.
        """

        arbitrary_types_allowed = True


class ImmutableTagFields(BaseSQLModel):
    """
    Tag fields that cannot be changed.
    """

    name: str = Field(sa_column=SqlaColumn("name", String, unique=True))
    tag_type: str


class TagNodeRelationship(BaseSQLModel, table=True):  # type: ignore
    """
    Join table between tags and nodes
    """

    tag_id: Optional[int] = Field(
        default=None,
        foreign_key="tag.id",
        primary_key=True,
    )
    node_id: Optional[int] = Field(
        default=None,
        foreign_key="node.id",
        primary_key=True,
    )


class Tag(ImmutableTagFields, MutableTagFields, table=True):  # type: ignore
    """
    A tag.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    nodes: List["Node"] = Relationship(
        back_populates="tags",
        link_model=TagNodeRelationship,
        sa_relationship_kwargs={
            "primaryjoin": "TagNodeRelationship.tag_id==Tag.id",
            "secondaryjoin": "TagNodeRelationship.node_id==Node.id",
        },
    )


class CreateTag(ImmutableTagFields, MutableTagFields):
    """
    Create tag model.
    """


class TagOutput(ImmutableTagFields, MutableTagFields):
    """
    Output tag model.
    """


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
