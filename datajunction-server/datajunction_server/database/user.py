"""User database schema."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import BigInteger, Enum, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.enum import StrEnum

if TYPE_CHECKING:
    from datajunction_server.database.collection import Collection
    from datajunction_server.database.node import Node, NodeRevision
    from datajunction_server.database.notification_preference import (
        NotificationPreference,
    )
    from datajunction_server.database.tag import Tag


class OAuthProvider(StrEnum):
    """
    Support oauth providers
    """

    BASIC = "basic"
    GITHUB = "github"
    GOOGLE = "google"


class User(Base):
    """Class for a user."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    username: Mapped[str] = mapped_column(String, unique=True)
    password: Mapped[Optional[str]]
    email: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    oauth_provider: Mapped[OAuthProvider] = mapped_column(
        Enum(OAuthProvider),
    )
    is_admin: Mapped[bool] = mapped_column(default=False)
    created_collections: Mapped[list["Collection"]] = relationship(
        "Collection",
        back_populates="created_by",
        foreign_keys="Collection.created_by_id",
        lazy="joined",
    )
    created_nodes: Mapped[list["Node"]] = relationship(
        "Node",
        back_populates="created_by",
        foreign_keys="Node.created_by_id",
        lazy="selectin",
    )
    created_node_revisions: Mapped[list["NodeRevision"]] = relationship(
        "NodeRevision",
        back_populates="created_by",
        foreign_keys="NodeRevision.created_by_id",
    )
    created_tags: Mapped[list["Tag"]] = relationship(
        "Tag",
        back_populates="created_by",
        foreign_keys="Tag.created_by_id",
        lazy="joined",
    )
    notification_preferences: Mapped[list["NotificationPreference"]] = relationship(
        "NotificationPreference",
        back_populates="user",
    )

    @classmethod
    async def get_by_username(
        cls,
        session: AsyncSession,
        username: str,
    ) -> Optional["User"]:
        """
        Find a user by username
        """
        statement = select(User).where(User.username == username)
        result = await session.execute(statement)
        return result.unique().scalar_one_or_none()
