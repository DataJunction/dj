"""RBAC database schema."""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.models.access import ResourceType, ResourceAction
from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.database.user import User


class Role(Base):
    """
    A named collection of permissions (scopes).

    Unlike traditional RBAC where roles have fixed meanings,
    DJ roles are flexible labels for custom permission sets.

    Examples:
    - "growth-data-eng": read+write on growth.*, read on member.*
    - "finance-owners": read+write+manage on finance.*
    - "ci-bot-staging": read+write+execute on staging.*
    """

    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    # Relationships
    scopes: Mapped[list["RoleScope"]] = relationship(
        "RoleScope",
        back_populates="role",
        cascade="all, delete-orphan",
    )
    assignments: Mapped[list["RoleAssignment"]] = relationship(
        "RoleAssignment",
        back_populates="role",
        cascade="all, delete-orphan",
    )

    __table_args__ = (Index("idx_roles_name", "name"),)


class RoleScope(Base):
    """
    Individual permission within a role.

    Defines: what action, on what type of resource, with what pattern.

    Examples:
    - action="read", scope_type="namespace", scope_value="finance.*"
    - action="write", scope_type="node", scope_value="finance.revenue"
    - action="manage", scope_type="namespace", scope_value="*"
    """

    __tablename__ = "role_scopes"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    role_id: Mapped[int] = mapped_column(
        ForeignKey("roles.id", ondelete="CASCADE"),
        nullable=False,
    )

    action: Mapped[ResourceAction] = mapped_column(Enum(ResourceAction), nullable=False)
    scope_type: Mapped[ResourceType] = mapped_column(
        Enum(ResourceType),
        nullable=False,
    )
    scope_value: Mapped[str] = mapped_column(String(500), nullable=False)

    # Relationships
    role: Mapped[Role] = relationship("Role", back_populates="scopes")

    __table_args__ = (
        # Prevent duplicate scopes within a role
        Index(
            "idx_unique_role_scope",
            "role_id",
            "action",
            "scope_type",
            "scope_value",
            unique=True,
        ),
        Index("idx_role_scopes_role_id", "role_id"),
    )


class RoleAssignment(Base):
    """
    Assigns a role to a principal (user/service account/group).

    Examples:
    - User alice (id=42) has role "growth-editors" (id=1)
    - Group finance-team (id=101) has role "finance-owners" (id=2)
    - Service account ci-bot (id=99) has role "staging-deployer" (id=3)
    """

    __tablename__ = "role_assignments"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )

    # WHO: Principal (user, service account, or group)
    principal_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )

    # WHAT: Role
    role_id: Mapped[int] = mapped_column(
        ForeignKey("roles.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Metadata
    granted_by_id: Mapped[int] = mapped_column(
        ForeignKey("users.id"),
        nullable=False,
    )
    granted_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )
    expires_at: Mapped[Optional[UTCDatetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    principal: Mapped["User"] = relationship(
        "User",
        foreign_keys=[principal_id],
        lazy="selectin",
    )
    role: Mapped[Role] = relationship(
        "Role",
        back_populates="assignments",
        lazy="selectin",
    )
    granted_by: Mapped["User"] = relationship(
        "User",
        foreign_keys=[granted_by_id],
        lazy="selectin",
    )

    __table_args__ = (
        # Prevent assigning the same role twice to the same principal
        Index(
            "idx_unique_role_assignment",
            "principal_id",
            "role_id",
            unique=True,
        ),
        Index("idx_role_assignments_principal", "principal_id"),
        Index("idx_role_assignments_role", "role_id"),
    )
