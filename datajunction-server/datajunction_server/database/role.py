"""RBAC database schema."""

from functools import partial
from typing import TYPE_CHECKING, Optional
from datetime import datetime, timezone

from sqlalchemy import BigInteger, ForeignKey, DateTime, String, select, Enum
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.base import ExecutableOption

from datajunction_server.database.base import Base
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.models.resources import ResourceType, ResourceRequestVerb
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.database.user import User


class Role(Base):
    """
    Role that can be assigned to users or service accounts.
    """

    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(BigInteger(), primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    description: Mapped[Optional[str]]

    created_by_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "users.id",
            name="fk_roles_created_by_id_users",
            ondelete="SET NULL",
        ),
    )
    created_by: Mapped[Optional["User"]] = relationship(
        "User",
        lazy="joined",
        foreign_keys=[created_by_id],
    )
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    access_rules: Mapped[list["AccessRule"]] = relationship(
        "AccessRule",
        back_populates="role",
        lazy="selectin",
        cascade="all,delete",
    )


class RoleAssignment(Base):
    """
    Assignment of a role to a user or service account.
    """

    __tablename__ = "role_assignments"

    id: Mapped[int] = mapped_column(BigInteger(), primary_key=True)
    role_id: Mapped[int] = mapped_column(
        ForeignKey(
            "roles.id",
            name="fk_role_assignments_role_id_roles",
            ondelete="CASCADE",
        ),
    )
    role: Mapped["Role"] = relationship("Role", lazy="joined")
    principal_id: Mapped[int] = mapped_column(
        ForeignKey(
            "users.id",
            name="fk_role_assignments_principal_id_users",
            ondelete="CASCADE",
        ),
    )
    principal: Mapped["User"] = relationship("User", lazy="joined")

    @classmethod
    async def get_by_name(
        cls,
        session: AsyncSession,
        name: str,
        options: list[ExecutableOption] = None,
    ) -> "RoleAssignment":
        """
        Get a role assignment by name.

        Args:
            session: The database session.
            name: The name of the role assignment.
            options: Optional SQLAlchemy loader options.

        Returns:
            The role assignment.

        Raises:
            DJDoesNotExistException: If the role assignment does not exist.
        """
        query = select(cls).join(cls.role).where(Role.name == name)
        if options:
            query = query.options(*options)
        result = await session.execute(query)
        role_assignment = result.scalars().first()
        if not role_assignment:
            raise DJDoesNotExistException(
                f"Role assignment with name {name} does not exist.",
            )
        return role_assignment


class AccessRule(Base):
    """
    Access rules that define permissions for roles on resources.
    """

    __tablename__ = "access_rules"
    id: Mapped[int] = mapped_column(BigInteger(), primary_key=True)
    role_id: Mapped[int] = mapped_column(
        ForeignKey(
            "roles.id",
            name="fk_access_rules_role_id_roles",
            ondelete="CASCADE",
        ),
    )
    role: Mapped[Role] = relationship(
        "Role",
        lazy="joined",
        back_populates="access_rules",
    )
    action: Mapped[ResourceRequestVerb] = mapped_column(Enum(ResourceRequestVerb))
    resource_type: Mapped[ResourceType] = mapped_column(Enum(ResourceType))
    resource_name: Mapped[str] = mapped_column(String)

    created_by_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "users.id",
            name="fk_access_rules_created_by_id_users",
            ondelete="SET NULL",
        ),
    )
    created_by: Mapped[Optional["User"]] = relationship(
        "User",
        lazy="joined",
        foreign_keys=[created_by_id],
    )
    updated_by_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "users.id",
            name="fk_access_rules_updated_by_id_users",
            ondelete="SET NULL",
        ),
    )
    updated_by: Mapped[Optional["User"]] = relationship(
        "User",
        lazy="joined",
        foreign_keys=[updated_by_id],
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
