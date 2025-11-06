"""
RBAC database models for DataJunction
"""

from datetime import datetime, timezone
from functools import partial
from typing import List, Optional

from sqlalchemy import (
    ARRAY,
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.models.access import ResourceType
from datajunction_server.typing import UTCDatetime


class Role(Base):
    """
    Defines a role with associated permissions
    """

    __tablename__ = "roles"

    name: Mapped[str] = mapped_column(String(50), primary_key=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    permissions: Mapped[List[str]] = mapped_column(ARRAY(String))
    is_system_role: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    # Relationships
    assignments: Mapped[List["RoleAssignment"]] = relationship(
        "RoleAssignment",
        back_populates="role",
        cascade="all, delete-orphan",
    )

    @classmethod
    async def get_by_name(
        cls,
        session: AsyncSession,
        name: str,
    ) -> Optional["Role"]:
        """Get role by name"""
        result = await session.execute(select(Role).where(Role.name == name))
        return result.scalar_one_or_none()

    @classmethod
    async def get_all(cls, session: AsyncSession) -> List["Role"]:
        """Get all roles"""
        result = await session.execute(select(Role))
        return result.scalars().all()


class RoleAssignment(Base):
    """
    Assigns a role to a principal (user, service account, or group) within a scope
    """

    __tablename__ = "role_assignments"
    __table_args__ = (
        UniqueConstraint(
            "principal_id",
            "role_name",
            "scope_type",
            "scope_value",
            name="unique_role_assignment",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )

    # Principal (can be user, service account, or group)
    principal_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
    )

    # Role
    role_name: Mapped[str] = mapped_column(
        ForeignKey("roles.name", ondelete="CASCADE"),
    )

    # Scope - where this role applies
    scope_type: Mapped[ResourceType] = mapped_column(Enum(ResourceType))
    scope_value: Mapped[Optional[str]] = mapped_column(
        String(500),
    )  # For NAMESPACE: namespace path (empty = all), For NODE: node name

    # Metadata
    granted_by_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    granted_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )
    expires_at: Mapped[Optional[UTCDatetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # External sync tracking
    synced_from_external: Mapped[bool] = mapped_column(default=False)
    external_system: Mapped[Optional[str]] = mapped_column(String(50))

    # Relationships
    principal = relationship("User", foreign_keys=[principal_id], lazy="selectin")
    role: Mapped[Role] = relationship(
        "Role",
        back_populates="assignments",
        lazy="selectin",
    )
    granted_by = relationship("User", foreign_keys=[granted_by_id], lazy="selectin")

    @classmethod
    async def get_assignments_for_principal(
        cls,
        session: AsyncSession,
        principal_id: int,
    ) -> List["RoleAssignment"]:
        """Get all role assignments for a principal"""
        result = await session.execute(
            select(RoleAssignment)
            .where(RoleAssignment.principal_id == principal_id)
            .where(
                (RoleAssignment.expires_at.is_(None))
                | (RoleAssignment.expires_at > func.now()),
            ),
        )
        return result.scalars().all()

    @classmethod
    async def get_assignments_for_resource(
        cls,
        session: AsyncSession,
        resource_type: str,
        resource_name: str,
    ) -> List["RoleAssignment"]:
        """Get all role assignments that apply to a resource"""
        result = await session.execute(
            select(RoleAssignment)
            .where(
                (
                    # Global assignments
                    (RoleAssignment.scope_type == "global")
                    # Namespace assignments
                    | (
                        (RoleAssignment.scope_type == "namespace")
                        & (resource_name.startswith(RoleAssignment.scope_value))
                    )
                    # Node assignments
                    | (
                        (RoleAssignment.scope_type == "node")
                        & (RoleAssignment.scope_value == resource_name)
                    )
                ),
            )
            .where(
                (RoleAssignment.expires_at.is_(None))
                | (RoleAssignment.expires_at > func.now()),
            ),
        )
        return result.scalars().all()


class PrincipalMembership(Base):
    """
    Local group membership for principals (when not using external group resolution)
    """

    __tablename__ = "principal_memberships"

    member_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    group_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )

    # Metadata
    added_by_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    added_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    # Relationships
    member = relationship("User", foreign_keys=[member_id], lazy="selectin")
    group = relationship("User", foreign_keys=[group_id], lazy="selectin")
    added_by = relationship("User", foreign_keys=[added_by_id], lazy="selectin")

    @classmethod
    async def get_user_groups(
        cls,
        session: AsyncSession,
        user_id: int,
    ) -> List[int]:
        """Get all groups a user belongs to"""
        result = await session.execute(
            select(PrincipalMembership.group_id).where(
                PrincipalMembership.member_id == user_id,
            ),
        )
        return [row[0] for row in result]

    @classmethod
    async def get_group_members(
        cls,
        session: AsyncSession,
        group_id: int,
    ) -> List[int]:
        """Get all members of a group"""
        result = await session.execute(
            select(PrincipalMembership.member_id).where(
                PrincipalMembership.group_id == group_id,
            ),
        )
        return [row[0] for row in result]
