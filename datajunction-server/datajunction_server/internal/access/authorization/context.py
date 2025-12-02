"""
Authorization context for a user, pre-loaded with all roles.
"""

from fastapi import Depends
from dataclasses import dataclass
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datajunction_server.internal.access.group_membership import (
    GroupMembershipService,
    get_group_membership_service,
)
from datajunction_server.database.rbac import RoleAssignment
from datajunction_server.database.user import User
from datajunction_server.utils import (
    get_current_user,
    get_session,
    get_settings,
)

settings = get_settings()


@dataclass(frozen=True)
class AuthContext:
    """
    Authorization context for a user.

    Contains all data needed to make authorization decisions,
    pre-loaded and ready for fast in-memory checks.

    This separates authorization data from the full User model,
    allowing for clean caching, testing, and type safety.
    """

    user_id: int
    username: str
    oauth_provider: Optional[str]
    role_assignments: List[RoleAssignment]  # Direct + groups, flattened

    @classmethod
    async def from_user(
        cls,
        session: AsyncSession,
        user: User,
        group_membership_service: GroupMembershipService | None = None,
    ) -> "AuthContext":
        """
        Build authorization context from a User object.

        This loads all effective role assignments (direct + group-based)
        using the configured GroupMembershipService.

        Args:
            session: Database session
            user: User to build context for
            group_membership_service: Optional service override

        Returns:
            AuthContext ready for authorization checks
        """
        assignments = await cls.get_effective_assignments(
            session=session,
            user=user,
            group_membership_service=group_membership_service,
        )

        return cls(
            user_id=user.id,
            username=user.username,
            oauth_provider=user.oauth_provider,
            role_assignments=assignments,
        )

    @classmethod
    async def get_effective_assignments(
        cls,
        session: AsyncSession,
        user: User,
        group_membership_service: GroupMembershipService | None = None,
    ) -> List[RoleAssignment]:
        """
        Get all effective role assignments for a user (direct + group-based).

        This function:
        1. Takes user's direct role_assignments
        2. Calls GroupMembershipService to get groups (LDAP/local/etc.)
        3. Loads those groups' role_assignments from DJ database
        4. Returns flattened list

        Args:
            session: Database session
            user: User to get assignments for
            group_membership_service: Optional service override

        Returns:
            Flat list of all role assignments that apply to this user
        """
        from datajunction_server.database.rbac import Role as RoleModel

        # Start with user's direct assignments
        assignments = list(user.role_assignments)

        # Get group membership service
        if group_membership_service is None:
            group_membership_service = get_group_membership_service()

        # Get groups from service (could be LDAP, local DB, etc.)
        group_usernames = await group_membership_service.get_user_groups(
            session,
            user.username,
        )

        if not group_usernames:
            return assignments  # No groups

        # Load groups from DJ database with their role_assignments
        stmt = (
            select(User)
            .where(User.username.in_(group_usernames))
            .options(
                selectinload(User.role_assignments)
                .selectinload(RoleAssignment.role)
                .selectinload(RoleModel.scopes),
            )
        )
        result = await session.execute(stmt)
        groups = result.scalars().all()

        # Flatten group assignments into the list
        for group in groups:
            assignments.extend(group.role_assignments)

        return assignments


async def get_auth_context(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> AuthContext:
    """Build authorization context with user + group assignments."""
    return await AuthContext.from_user(session, current_user)
