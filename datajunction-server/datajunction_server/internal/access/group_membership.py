"""
Group membership resolution.

This module provides pluggable group membership resolution to support
different deployment scenarios:

- **Postgres (Default)**: Uses group_members table for self-contained deployments
- **Static**: No-op implementation (groups exist but have no members)
- **External**: Custom implementations can query LDAP, SAML etc.

Example custom implementation:

```python
class LDAPGroupMembershipService(GroupMembershipService):
    name = "ldap"  # Register with this name

    async def is_user_in_group(self, session, username, group_name):
        # Query LDAP server
        return ldap_client.check_membership(username, group_name)
```

Configure via:
```bash
GROUP_MEMBERSHIP_PROVIDER=ldap
```

The factory will automatically discover all subclasses of GroupMembershipService.
"""

import logging
from abc import ABC, abstractmethod

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from datajunction_server.database.group_member import GroupMember
from datajunction_server.database.user import PrincipalKind, User

logger = logging.getLogger(__name__)


class GroupMembershipService(ABC):
    """
    Abstract base class for group membership resolution.

    Implementations decide WHERE to look for membership data:
    - PostgresGroupMembershipService: Queries group_members table
    - StaticGroupMembershipService: Returns empty (no membership)
    - Custom implementations: LDAP, SAML etc.

    Each implementation should define a `name` class attribute to register itself.
    """

    name: str  # Subclasses must define this

    @abstractmethod
    async def is_user_in_group(
        self,
        session: AsyncSession,
        username: str,
        group_name: str,
    ) -> bool:
        """
        Check if a user is a member of a group.

        Args:
            session: Database session (may not be used by external providers)
            username: User's username (unique identifier)
            group_name: Group's username (unique identifier)

        Returns:
            True if user is in the group, False otherwise
        """

    @abstractmethod
    async def get_user_groups(
        self,
        session: AsyncSession,
        username: str,
    ) -> list[str]:
        """
        Get all groups a user belongs to.

        Args:
            session: Database session
            username: User's username (unique identifier)

        Returns:
            List of group usernames the user is a member of
        """


class PostgresGroupMembershipService(GroupMembershipService):
    """
    Default implementation using group_members table.

    Used by OSS deployments without external identity systems.
    Membership is stored directly in the DJ database.
    """

    name = "postgres"

    async def is_user_in_group(
        self,
        session: AsyncSession,
        username: str,
        group_name: str,
    ) -> bool:
        """Check membership via group_members table."""
        # Create aliases to distinguish between member and group users
        member_user = aliased(User)
        group_user = aliased(User)

        # Query: Check if there's a group_members entry linking them
        statement = (
            select(GroupMember.group_id)
            .join(member_user, GroupMember.member_id == member_user.id)
            .join(group_user, GroupMember.group_id == group_user.id)
            .where(
                member_user.username == username,
                group_user.username == group_name,
                group_user.kind == PrincipalKind.GROUP,
            )
            .limit(1)
        )

        result = await session.execute(statement)
        return result.scalar_one_or_none() is not None

    async def get_user_groups(
        self,
        session: AsyncSession,
        username: str,
    ) -> list[str]:
        """Get user's groups from group_members table."""
        member_user = aliased(User)
        group_user = aliased(User)

        # Query: Get all groups where user is a member
        statement = (
            select(group_user.username)
            .join(GroupMember, GroupMember.group_id == group_user.id)
            .join(member_user, GroupMember.member_id == member_user.id)
            .where(
                member_user.username == username,
                group_user.kind == PrincipalKind.GROUP,
            )
        )

        result = await session.execute(statement)
        return list(result.scalars().all())


class StaticGroupMembershipService(GroupMembershipService):
    """
    No-op implementation that returns empty results.

    Useful for:
    - Testing without setting up membership
    - Deployments where groups exist but membership isn't tracked
    - Gradual rollout (groups registered, membership added later)
    """

    name = "static"

    async def is_user_in_group(
        self,
        session: AsyncSession,
        username: str,
        group_name: str,
    ) -> bool:
        """Always returns False - no membership tracked."""
        return False

    async def get_user_groups(
        self,
        session: AsyncSession,
        username: str,
    ) -> list[str]:
        """Always returns empty list - no membership tracked."""
        return []


def get_group_membership_service() -> GroupMembershipService:
    """
    Factory to get the configured membership service.

    Automatically discovers all GroupMembershipService subclasses
    and returns the one matching GROUP_MEMBERSHIP_PROVIDER setting.

    Built-in providers:
    - "postgres": Uses group_members table
    - "static": No membership tracking

    Custom providers can be added by:
    1. Subclassing GroupMembershipService
    2. Defining a `name` class attribute
    3. Importing the class before calling this function

    Example:
    ```python
    class LDAPGroupMembershipService(GroupMembershipService):
        name = "ldap"
        async def is_user_in_group(...): ...
    ```

    Returns:
        GroupMembershipService implementation

    Raises:
        ValueError: If provider is unknown
    """
    from datajunction_server.utils import get_settings

    settings = get_settings()
    provider = settings.group_membership_provider

    logger.debug(f"Loading group membership provider: {provider}")

    # Discover all subclasses
    providers = {}
    for subclass in GroupMembershipService.__subclasses__():
        if hasattr(subclass, "name"):  # pragma: no cover
            providers[subclass.name] = subclass
            logger.debug(f"Discovered provider: {subclass.name}")
            if subclass.name == provider:
                logger.info(f"Using group membership provider: {provider}")
                return subclass()  # type: ignore[abstract]

    available = ", ".join(sorted(providers.keys()))
    raise ValueError(
        f"Unknown group_membership_provider: '{provider}'. "
        f"Available providers: {available}",
    )
