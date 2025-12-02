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
    get_group_membership_service,
)
from datajunction_server.database.rbac import RoleAssignment, Role
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
    ) -> "AuthContext":
        """
        Build authorization context from a User object.

        This loads all effective role assignments (direct + group-based)
        for the user using the configured GroupMembershipService.

        Args:
            session: db session
            user: user to build context for

        Returns:
            AuthContext ready for authorization checks
        """
        assignments = await cls.get_effective_assignments(
            session=session,
            user=user,
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
    ) -> List[RoleAssignment]:
        """
        Get all effective role assignments for a user (direct + group-based).

        Args:
            session: db session
            user: user to get assignments for
        Returns:
            list of all role assignments that apply to this user
        """
        group_membership_service = get_group_membership_service()

        # Start with user's direct assignments
        assignments = list(user.role_assignments)

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
                .selectinload(Role.scopes),
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
