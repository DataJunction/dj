"""
Models for tags.
"""
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import sqlalchemy as sa
from pydantic import Extra
from pydantic.main import BaseModel
from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.schema import ForeignKey

from datajunction_server.database.connection import Base
from datajunction_server.models.base import labelize

if TYPE_CHECKING:
    from datajunction_server.models.node import Node


class MutableTagFields(BaseModel):
    """
    Tag fields that can be changed.
    """

    description: str
    display_name: Optional[str]
    tag_metadata: Dict[str, Any]

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


class TagNodeRelationship(Base):  # pylint: disable=too-few-public-methods
    """
    Join table between tags and nodes
    """

    __tablename__ = "tagnoderelationship"

    tag_id: Mapped[int] = mapped_column(
        ForeignKey("tag.id"),
        primary_key=True,
    )
    node_id: Mapped[int] = mapped_column(
        ForeignKey("node.id"),
        primary_key=True,
    )


class Tag(Base):  # pylint: disable=too-few-public-methods
    """
    A tag.
    """

    __tablename__ = "tag"

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    tag_type: Mapped[str]
    description: Mapped[str] = mapped_column(nullable=True)
    display_name: Mapped[str] = mapped_column(
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    tag_metadata: Mapped[Dict[str, Any]] = mapped_column(JSON, default={})

    nodes: Mapped[List["Node"]] = relationship(
        back_populates="tags",
        secondary="tagnoderelationship",
        primaryjoin="TagNodeRelationship.tag_id==Tag.id",
        secondaryjoin="TagNodeRelationship.node_id==Node.id",
    )


class CreateTag(ImmutableTagFields, MutableTagFields):
    """
    Create tag model.
    """


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
