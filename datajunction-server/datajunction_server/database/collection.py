"""Collection database schema."""
from datetime import datetime, timezone
from functools import partial
from typing import List, Optional, TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, Integer, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.database.node import Node
from datajunction_server.typing import UTCDatetime

class Collection(Base):  # pylint: disable=too-few-public-methods
    """
    A collection of nodes
    """

    __tablename__ = "collection"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[Optional[str]] = mapped_column(String, default=None)
    description: Mapped[Optional[str]] = mapped_column(String, default=None)
    nodes: Mapped[List[Node]] = relationship(
        secondary="collectionnodes",
        primaryjoin="Collection.id==CollectionNodes.collection_id",
        secondaryjoin="Node.id==CollectionNodes.node_id",
        lazy="selectin",
    )
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )
    deactivated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )

    def __hash__(self) -> int:
        return hash(self.id)


class CollectionNodes(Base):  # type: ignore  # pylint: disable=too-few-public-methods
    """
    Join table for collections and nodes.
    """

    __tablename__ = "collectionnodes"

    collection_id: Mapped[int] = mapped_column(
        ForeignKey("collection.id", name="fk_collectionnodes_collection_id_collection"),
        primary_key=True,
    )
    node_id: Mapped[int] = mapped_column(
        ForeignKey("node.id", name="fk_collectionnodes_node_id_node"),
        primary_key=True,
    )