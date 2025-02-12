"""Collection database schema."""

from datetime import datetime, timezone
from functools import partial
from typing import List, Optional

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.typing import UTCDatetime


class Collection(Base):
    """
    A collection of nodes
    """

    __tablename__ = "collection"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String, default=None, unique=True)
    description: Mapped[Optional[str]] = mapped_column(String, default=None)
    created_by_id: Mapped[int] = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_by: Mapped[User] = relationship(
        "User",
        back_populates="created_collections",
        foreign_keys=[created_by_id],
        lazy="selectin",
    )
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
        return hash(self.id)  # pragma: no cover

    @classmethod
    async def get_by_name(
        cls,
        session: AsyncSession,
        name: str,
        raise_if_not_exists: bool = False,
    ) -> Optional["Collection"]:
        """
        Get a collection by name
        """
        statement = select(Collection).where(Collection.name == name)
        collection = (await session.execute(statement)).scalar()
        if not collection and raise_if_not_exists:
            raise DJDoesNotExistException(
                message=f"Collection with name `{name}` does not exist.",
                http_status_code=404,
            )
        return collection


class CollectionNodes(Base):  # type: ignore
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
