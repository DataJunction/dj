"""User database schema."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import BigInteger, Enum, Integer, String, case, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.base import ExecutableOption
from sqlalchemy.orm import selectinload
from datajunction_server.database.base import Base
from datajunction_server.database.nodeowner import NodeOwner
from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJDoesNotExistException

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
        lazy="selectin",
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
        lazy="selectin",
    )
    notification_preferences: Mapped[list["NotificationPreference"]] = relationship(
        "NotificationPreference",
        back_populates="user",
    )
    owned_associations: Mapped[list[NodeOwner]] = relationship(
        "NodeOwner",
        back_populates="user",
        cascade="all, delete-orphan",
        viewonly=True,
    )
    owned_nodes = relationship(
        "Node",
        secondary="node_owners",
        back_populates="owners",
        overlaps="owned_associations,user",
        lazy="selectin",
        viewonly=True,
    )

    @classmethod
    async def get_by_username(
        cls,
        session: AsyncSession,
        username: str,
        options: list[ExecutableOption] = None,
    ) -> Optional["User"]:
        """
        Find a user by username
        """
        options = options or [
            selectinload(User.created_nodes),
            selectinload(User.created_collections),
            selectinload(User.created_tags),
            selectinload(User.owned_nodes),
        ]
        statement = select(User).where(User.username == username).options(*options)
        result = await session.execute(statement)
        return result.unique().scalar_one_or_none()

    @classmethod
    async def get_by_usernames(
        cls,
        session: AsyncSession,
        usernames: list[str],
    ) -> list["User"]:
        """
        Find users by username, preserving the order of the input usernames list.
        """
        if not usernames:
            return []

        order_case = case(
            {username: index for index, username in enumerate(usernames)},
            value=User.username,
        )

        statement = (
            select(User).where(User.username.in_(usernames)).order_by(order_case)
        )
        result = await session.execute(statement)
        users = result.unique().scalars().all()
        if len(users) != len(usernames):
            missing_usernames = set(usernames) - {user.username for user in users}
            raise DJDoesNotExistException(
                f"Users not found: {', '.join(missing_usernames)}",
            )
        return users
