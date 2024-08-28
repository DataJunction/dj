"""Tag database schema."""
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import sqlalchemy as sa
from sqlalchemy import JSON, Column, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.database.user import User
from datajunction_server.models.base import labelize

if TYPE_CHECKING:
    from datajunction_server.database.node import Node


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
    description: Mapped[Optional[str]]
    display_name: Mapped[str] = mapped_column(  # pragma: no cover
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    created_by_id: Mapped[int] = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_by: Mapped[User] = relationship(
        "User",
        back_populates="created_tags",
        foreign_keys=[created_by_id],
        lazy="selectin",
    )
    tag_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, default={})

    nodes: Mapped[List["Node"]] = relationship(
        back_populates="tags",
        secondary="tagnoderelationship",
        primaryjoin="TagNodeRelationship.tag_id==Tag.id",
        secondaryjoin="TagNodeRelationship.node_id==Node.id",
    )


class TagNodeRelationship(Base):  # pylint: disable=too-few-public-methods
    """
    Join table between tags and nodes
    """

    __tablename__ = "tagnoderelationship"

    tag_id: Mapped[int] = mapped_column(
        ForeignKey("tag.id", name="fk_tagnoderelationship_tag_id_tag"),
        primary_key=True,
    )
    node_id: Mapped[int] = mapped_column(
        ForeignKey("node.id", name="fk_tagnoderelationship_node_id_node"),
        primary_key=True,
    )
