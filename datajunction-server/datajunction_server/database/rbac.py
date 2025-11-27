"""RBAC database schema."""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship, selectinload

from datajunction_server.errors import DJDoesNotExistException
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

    created_by_id: Mapped[int] = mapped_column(
        ForeignKey("users.id"),
        nullable=False,
    )
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    # Soft delete for audit trail (who deleted is in History table)
    deleted_at: Mapped[Optional[UTCDatetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    created_by: Mapped["User"] = relationship(
        "User",
        foreign_keys=[created_by_id],
        lazy="selectin",
    )
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

    __table_args__ = (
        Index("idx_roles_name", "name"),
        Index("idx_roles_created_by", "created_by_id"),
        Index("idx_roles_deleted_at", "deleted_at"),
    )

    @classmethod
    async def get_by_name(
        cls,
        session: AsyncSession,
        name: str,
        include_deleted: bool = False,
        options: Optional[List] = None,
    ) -> Optional["Role"]:
        """
        Get a role by name.

        Args:
            session: Database session
            name: Role name
            include_deleted: Whether to include soft-deleted roles
            options: Additional query options

        Returns:
            Role if found, None otherwise
        """
        statement = select(Role).where(Role.name == name)

        if not include_deleted:
            statement = statement.where(Role.deleted_at.is_(None))

        if options:
            statement = statement.options(*options)
        else:
            statement = statement.options(
                selectinload(Role.scopes),
                selectinload(Role.created_by),
            )

        result = await session.execute(statement)
        return result.scalar_one_or_none()

    @classmethod
    async def get_by_name_or_raise(
        cls,
        session: AsyncSession,
        name: str,
        include_deleted: bool = False,
        options: Optional[List] = None,
    ) -> "Role":
        """
        Get a role by name, raising an exception if not found.

        Args:
            session: Database session
            name: Role name
            include_deleted: Whether to include soft-deleted roles
            options: Additional query options

        Returns:
            Role (never None)

        Raises:
            DJDoesNotExistException: If role not found
        """
        role = await cls.get_by_name(session, name, include_deleted, options)
        if not role:
            raise DJDoesNotExistException(
                message=f"A role with name `{name}` does not exist.",
            )
        return role

    @classmethod
    async def find(
        cls,
        session: AsyncSession,
        include_deleted: bool = False,
        created_by_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List["Role"]:
        """
        Find roles with optional filters.

        Args:
            session: Database session
            include_deleted: Whether to include soft-deleted roles
            created_by_id: Filter by creator
            limit: Maximum number of results
            offset: Number of results to skip

        Returns:
            List of roles
        """
        statement = select(Role)

        if not include_deleted:  # pragma: no cover
            statement = statement.where(Role.deleted_at.is_(None))

        if created_by_id is not None:
            statement = statement.where(Role.created_by_id == created_by_id)

        statement = (
            statement.options(
                selectinload(Role.scopes),
                selectinload(Role.created_by),
            )
            .order_by(Role.name)
            .limit(limit)
            .offset(offset)
        )

        result = await session.execute(statement)
        return list(result.scalars().all())


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

    @classmethod
    async def find(
        cls,
        session: AsyncSession,
        principal_id: Optional[int] = None,
        role_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List["RoleAssignment"]:
        """
        Find role assignments with optional filters.

        Args:
            session: Database session
            principal_id: Filter by principal
            role_id: Filter by role
            limit: Maximum number of results
            offset: Number of results to skip

        Returns:
            List of role assignments
        """
        statement = select(RoleAssignment)

        if principal_id is not None:
            statement = statement.where(RoleAssignment.principal_id == principal_id)

        if role_id is not None:  # pragma: no cover
            statement = statement.where(RoleAssignment.role_id == role_id)

        statement = (
            statement.options(
                selectinload(RoleAssignment.principal),
                selectinload(RoleAssignment.role).selectinload(Role.scopes),
                selectinload(RoleAssignment.granted_by),
            )
            .order_by(RoleAssignment.granted_at.desc())
            .limit(limit)
            .offset(offset)
        )

        result = await session.execute(statement)
        return list(result.scalars().all())
