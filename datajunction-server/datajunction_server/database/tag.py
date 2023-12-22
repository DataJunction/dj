"""Tag database schema."""
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import sqlalchemy as sa
from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.connection import Base
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
    display_name: Mapped[str] = mapped_column(
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    tag_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, default={})

    nodes: Mapped[List["Node"]] = relationship(
        back_populates="tags",
        secondary="tagnoderelationship",
        primaryjoin="TagNodeRelationship.tag_id==Tag.id",
        secondaryjoin="TagNodeRelationship.node_id==Node.id",
    )
