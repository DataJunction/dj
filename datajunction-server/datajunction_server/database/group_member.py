"""Group membership database schema."""

from datetime import datetime, timezone
from functools import partial

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime


class GroupMember(Base):
    """
    Join table for groups and their members (users or service accounts).

    Note: This table is used by the Postgres group membership provider.
    Deployments using external identity systems (LDAP, SAML etc.)
    may leave this table empty and resolve membership externally.
    """

    __tablename__ = "group_members"
    __table_args__ = (
        Index("idx_group_members_group_id", "group_id"),
        Index("idx_group_members_member_id", "member_id"),
        CheckConstraint(
            "group_id != member_id",
            name="chk_no_self_membership",
        ),
    )

    group_id: Mapped[int] = mapped_column(
        ForeignKey(
            "users.id",
            name="fk_group_members_group_id",
            ondelete="CASCADE",
        ),
        primary_key=True,
    )
    member_id: Mapped[int] = mapped_column(
        ForeignKey(
            "users.id",
            name="fk_group_members_member_id",
            ondelete="CASCADE",
        ),
        primary_key=True,
    )
    added_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    group = relationship(
        "User",
        foreign_keys=[group_id],
        viewonly=True,
    )
    member = relationship(
        "User",
        foreign_keys=[member_id],
        viewonly=True,
    )
