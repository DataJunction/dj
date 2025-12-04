"""User database schema."""

import logging
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    BigInteger,
    Enum,
    Integer,
    String,
    ForeignKey,
    case,
    select,
    DateTime,
    and_,
)
from datetime import datetime, timezone
from functools import partial

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.base import ExecutableOption
from sqlalchemy.orm import selectinload
from datajunction_server.database.base import Base
from datajunction_server.database.nodeowner import NodeOwner
from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.database.collection import Collection
    from datajunction_server.database.group_member import GroupMember
    from datajunction_server.database.node import Node, NodeRevision
    from datajunction_server.database.notification_preference import (
        NotificationPreference,
    )
    from datajunction_server.database.tag import Tag

logger = logging.getLogger(__name__)


class OAuthProvider(StrEnum):
    """
    Support oauth providers
    """

    BASIC = "basic"
    GITHUB = "github"
    GOOGLE = "google"


class PrincipalKind(StrEnum):
    """
    Principal kinds: users, service accounts, and groups
    """

    USER = "user"
    SERVICE_ACCOUNT = "service_account"
    GROUP = "group"


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
    kind: Mapped[PrincipalKind] = mapped_column(
        Enum(PrincipalKind),
        default=PrincipalKind.USER,
    )

    created_by_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id"),
        nullable=True,
    )

    created_at: Mapped[UTCDatetime | None] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
        nullable=True,
    )

    # Timestamp when user last viewed notifications (for unread badge)
    last_viewed_notifications_at: Mapped[UTCDatetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )

    created_by: Mapped["User"] = relationship("User")
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
        lazy="selectin",
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

    # Group membership relationships (for kind=GROUP)
    group_members: Mapped[list["GroupMember"]] = relationship(
        "GroupMember",
        foreign_keys="GroupMember.group_id",
        viewonly=True,
    )
    # Memberships where this user is a member of groups (for kind=USER or SERVICE_ACCOUNT)
    member_of: Mapped[list["GroupMember"]] = relationship(
        "GroupMember",
        foreign_keys="GroupMember.member_id",
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
        options = (
            options
            if options is not None
            else [
                selectinload(User.created_nodes),
                selectinload(User.created_collections),
                selectinload(User.created_tags),
                selectinload(User.owned_nodes),
                selectinload(User.notification_preferences),
            ]
        )
        statement = select(User).where(User.username == username).options(*options)
        result = await session.execute(statement)
        return result.unique().scalar_one_or_none()

    @classmethod
    async def get_by_usernames(
        cls,
        session: AsyncSession,
        usernames: list[str],
        raise_if_not_exists: bool = True,
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
        if len(users) != len(usernames) and raise_if_not_exists:
            missing_usernames = set(usernames) - {user.username for user in users}
            raise DJDoesNotExistException(
                f"Users not found: {', '.join(missing_usernames)}",
            )
        return users

    @classmethod
    async def get_service_accounts_for_user_id(
        cls,
        session: AsyncSession,
        user_id: int,
        options: list[ExecutableOption] = None,
    ) -> list["User"]:
        """
        Find service accounts created by a user
        """
        logger.info("Getting service accounts for user_id=%s", user_id)
        options = options or [
            selectinload(User.created_nodes),
            selectinload(User.created_collections),
            selectinload(User.created_tags),
            selectinload(User.owned_nodes),
        ]

        statement = (
            select(User)
            .where(
                and_(
                    User.created_by_id == user_id,
                    User.kind == PrincipalKind.SERVICE_ACCOUNT,
                ),
            )
            .options(*options)
        )

        result = await session.execute(statement)
        service_accounts = result.unique().scalars().all()
        logger.info(
            "Found %d service accounts for user_id=%s",
            len(service_accounts),
            user_id,
        )
        return service_accounts
