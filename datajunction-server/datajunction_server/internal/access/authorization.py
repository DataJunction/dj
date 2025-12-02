"""
Authorization related functionality using pluggable services.

This module defines an abstract base class `AuthorizationService` for implementing different
authorization strategies. It includes built-in implementations such as
`RBACAuthorizationService` for role-based access control and `PassthroughAuthorizationService`
for permissive access.

Example custom implementation:
```python
class CustomAuthService(AuthorizationService):
    name = "custom"

    def authorize(self, auth_context, requests):
        # Sync in-memory authorization logic
        return requests
```
"""

from fastapi import Depends
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datajunction_server.internal.access.group_membership import (
    GroupMembershipService,
    get_group_membership_service,
)
from datajunction_server.database.node import Node
from datajunction_server.database.rbac import RoleAssignment
from datajunction_server.database.user import User
from datajunction_server.models.access import (
    AccessDecision,
    Resource,
    ResourceAction,
    ResourceRequest,
    ResourceType,
)
from datajunction_server.utils import (
    SEPARATOR,
    get_current_user,
    get_session,
    get_settings,
)

settings = get_settings()


# ============================================================================
# Access Check Modes
# ============================================================================


class AccessDenialMode(Enum):
    """
    How to handle denied access requests.
    """

    FILTER = "filter"  # Return only approved requests
    RAISE = "raise"  # Raise exception if any denied
    RETURN = "return"  # Return all requests with approved field set


# ============================================================================
# Authorization Context
# ============================================================================


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


# ============================================================================
# New FastAPI-style Authorization Service
# ============================================================================


class AuthorizationService(ABC):
    """
    Abstract base class for authorization strategies.

    Authorization is performed on a pre-loaded authorization context.

    Implementations of this base class decide exactly how to authorize requests:
    - RBACAuthorizationService: Uses pre-loaded roles/scopes (default)
    - PassthroughAuthorizationService: Always approve (testing/permissive)
    - Custom: Your own authorization logic

    Each implementation should define a `name` class attribute to register itself.
    """

    name: str  # Subclasses must define this

    @abstractmethod
    def authorize(
        self,
        auth_context: AuthContext,
        requests: list[ResourceRequest],
    ) -> list[AccessDecision]:
        """
        Authorize resource requests for a user.

        This method should mutate the `approved` field on each request
        to indicate whether access is granted.

        Args:
            auth_context: Pre-loaded authorization context with all needed data
            requests: List of resource requests to authorize

        Returns:
            The same list of requests with approved=True/False set on each
        """


class RBACAuthorizationService(AuthorizationService):
    """
    Default RBAC implementation using pre-loaded roles and scopes.

    This implementation:
    1. Works on AuthContext with pre-loaded role_assignments (direct + groups)
    2. Falls back to default_access_policy if no explicit rule exists
    3. Respects role expiration
    4. Synchronous - works on eagerly loaded data

    Group Membership Integration:
    - Supports pluggable GroupMembershipService (LDAP, local DB, etc.)
    - Groups are loaded when building AuthContext via from_user()
    - No DB queries during authorization - all data pre-loaded
    """

    name = "rbac"

    PERMISSION_HIERARCHY = {
        ResourceAction.MANAGE: {
            ResourceAction.MANAGE,
            ResourceAction.DELETE,
            ResourceAction.WRITE,
            ResourceAction.EXECUTE,
            ResourceAction.READ,
        },
        ResourceAction.DELETE: {
            ResourceAction.DELETE,
            ResourceAction.WRITE,
            ResourceAction.READ,
        },
        ResourceAction.WRITE: {
            ResourceAction.WRITE,
            ResourceAction.READ,
        },
        ResourceAction.EXECUTE: {
            ResourceAction.EXECUTE,
            ResourceAction.READ,
        },
        ResourceAction.READ: {
            ResourceAction.READ,
        },
    }

    def authorize(
        self,
        auth_context: AuthContext,
        requests: list[ResourceRequest],
    ) -> list[AccessDecision]:
        """
        Authorize using pre-loaded RBAC roles and scopes (sync).

        Args:
            auth_context: Pre-loaded authorization context with role assignments
            requests: Resource requests to authorize

        Returns:
            Same list of requests with approved=True/False set
        """
        return [self._make_decision(auth_context, request) for request in requests]

    def _make_decision(
        self,
        auth_context: AuthContext,
        request: ResourceRequest,
    ) -> AccessDecision:
        """
        Convert ResourceRequest to AccessDecision.
        """
        has_grant = self.has_permission(
            assignments=auth_context.role_assignments,
            action=request.verb,
            resource_type=request.access_object.resource_type,
            resource_name=request.access_object.name,
        )
        return AccessDecision(
            request=request,
            approved=(has_grant or settings.default_access_policy == "permissive"),
        )

    @classmethod
    def resource_matches_pattern(cls, resource_name: str, pattern: str) -> bool:
        """
        Check if resource name matches a pattern with wildcard support.

        resource_matches_pattern("finance.revenue", "finance.*") --> True
        resource_matches_pattern("finance.quarterly.revenue", "finance.*") --> True
        resource_matches_pattern("users.alice.dashboard", "users.alice.*") --> True
        resource_matches_pattern("marketing.revenue", "finance.*") --> False
        resource_matches_pattern("anything", "*") --> True
        resource_matches_pattern("finance", "finance.*") --> False
        """
        if pattern == "*":
            return True  # Match everything

        if "*" not in pattern:
            return resource_name == pattern  # Exact match

        # Wildcard pattern: finance.* matches finance.revenue and finance.quarterly.revenue
        # But NOT just "finance" (must have something after the dot)
        pattern_prefix = pattern.rstrip("*").rstrip(SEPARATOR)

        if not pattern_prefix:
            return True  # Pattern was just "*"

        # Resource must start with pattern_prefix followed by a dot
        # (not an exact match to pattern_prefix, that would be handled by exact pattern)
        return resource_name.startswith(pattern_prefix + SEPARATOR)

    @classmethod
    def has_permission(
        cls,
        assignments: List,
        action: ResourceAction,
        resource_type: ResourceType,
        resource_name: str,
    ) -> bool:
        """
        Determine if a list of role assignments grants the requested permission.

        This method iterates through all provided role assignments, checking if any
        grant the specified action on the given resource. Expired assignments are
        automatically skipped. Returns True if at least one valid assignment grants
        access, False otherwise.

        Args:
            assignments: List of role assignments to check
            action: The action being requested (READ, WRITE, etc.)
            resource_type: Type of resource (NODE, NAMESPACE, etc.)
            resource_name: Full name/identifier of the resource

        Returns:
            True if permission is granted, False otherwise
        """
        for assignment in assignments:
            # Skip expired assignments
            if assignment.expires_at and assignment.expires_at < datetime.now(
                timezone.utc,
            ):
                continue

            # Check each scope in the role
            for scope in assignment.role.scopes:
                # Check if scope grants permission for this resource
                if cls._scope_grants_permission(
                    scope,
                    action,
                    resource_type,
                    resource_name,
                ):
                    return True

        return False

    @classmethod
    def _scope_grants_permission(
        cls,
        scope,
        action: ResourceAction,
        resource_type: ResourceType,
        resource_name: str,
    ) -> bool:
        """
        Check if a scope grants permission for a resource.

        Handles:
        1. Permission hierarchy (MANAGE > DELETE > WRITE > READ, EXECUTE > READ)
        2. Empty/None scope_value or "*" = global access
        3. Wildcard pattern matching (finance.*)
        4. Cross-resource-type: namespace scope covers nodes in that namespace
        """
        # Check permission hierarchy: does scope.action grant the requested action?
        granted_actions = cls.PERMISSION_HIERARCHY.get(scope.action, {scope.action})
        if action not in granted_actions:
            return False

        # Handle global access (empty string, None, or "*" scope_value)
        if not scope.scope_value or scope.scope_value == "" or scope.scope_value == "*":
            # Global scope matches any resource of the same type
            return scope.scope_type == resource_type

        # Same resource type - use pattern matching
        if scope.scope_type == resource_type:
            return cls.resource_matches_pattern(resource_name, scope.scope_value)

        # Cross-resource-type: namespace scope can cover nodes
        if (
            scope.scope_type == ResourceType.NAMESPACE
            and resource_type == ResourceType.NODE
        ):
            # Check if node name matches the namespace pattern
            return cls.resource_matches_pattern(resource_name, scope.scope_value)

        # No match
        return False


class PassthroughAuthorizationService(AuthorizationService):
    """
    Always approves all requests without checking permissions.

    Useful for:
    - Local development
    - Testing
    - Fully permissive deployments
    - Gradual RBAC rollout (start permissive, add rules incrementally)
    """

    name = "passthrough"

    def authorize(
        self,
        auth_context: AuthContext,
        requests: list[ResourceRequest],
    ) -> list[AccessDecision]:
        """Approve all requests without checks (sync)."""
        return [AccessDecision(request=request, approved=True) for request in requests]


@lru_cache(maxsize=None)
def get_authorization_service() -> AuthorizationService:
    """
    Factory function to get the configured authorization service.

    This is used as a FastAPI dependency. The service can be overridden
    via app.dependency_overrides for testing or custom deployments.

    Built-in providers:
    - "rbac": Role-based access control using roles/scopes tables (default)
    - "passthrough": Always approve all requests

    Configure via environment variable:
    ```bash
    AUTHORIZATION_PROVIDER=rbac  # or passthrough
    ```

    Custom providers can be added by:
    1. Subclassing AuthorizationService
    2. Defining a `name` class attribute
    3. Importing the class before app starts

    Example:
    ```python
    class ExampleAuthService(AuthorizationService):
        name = "example"

        def authorize(self, user, requests):
            # Your sync authorization logic
            return requests
    ```

    Returns:
        AuthorizationService implementation

    Raises:
        ValueError: If the configured provider is unknown
    """
    provider = getattr(settings, "authorization_provider", "rbac")

    # Discover all subclasses
    providers = {}
    for subclass in AuthorizationService.__subclasses__():
        if hasattr(subclass, "name"):
            providers[subclass.name] = subclass
            if subclass.name == provider:
                return subclass()  # type: ignore[abstract]

    available = ", ".join(sorted(providers.keys()))
    raise ValueError(
        f"Unknown authorization_provider: '{provider}'. "
        f"Available providers: {available}",
    )


async def get_auth_context(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> AuthContext:
    """Build authorization context with user + group assignments."""
    return await AuthContext.from_user(session, current_user)


class AccessChecker:
    """Collects authorization requests and validates them."""

    def __init__(self, auth_context: AuthContext):
        self.auth_context = auth_context
        self.requests: list[ResourceRequest] = []

    def add_request(self, request: ResourceRequest):
        """Add a request to check."""
        self.requests.append(request)

    def add_requests(self, requests: list[ResourceRequest]):
        """Add requests to check."""
        self.requests.extend(requests)

    @classmethod
    def resource_request_from_node(
        cls,
        node: Node,
        action: ResourceAction,
    ) -> ResourceRequest:
        """Create ResourceRequest from a Node."""
        return ResourceRequest(
            verb=action,
            access_object=Resource.from_node(node),
        )

    def add_request_by_node_name(self, node_name: str, action: ResourceAction):
        """Add request by node name."""
        self.requests.append(
            ResourceRequest(
                verb=action,
                access_object=Resource(name=node_name, resource_type=ResourceType.NODE),
            ),
        )

    def add_node(self, node: Node, action: ResourceAction):
        """Add request for a node."""
        node_request = self.resource_request_from_node(node, action)
        self.add_request(node_request)

    def add_nodes(self, nodes: list[Node], action: ResourceAction):
        """Add requests for multiple nodes."""
        self.requests.extend(
            self.resource_request_from_node(node, action) for node in nodes
        )

    @classmethod
    def resource_request_from_namespace(
        cls,
        namespace: str,
        action: ResourceAction,
    ) -> ResourceRequest:
        """Create ResourceRequest from a namespace."""
        return ResourceRequest(
            verb=action,
            access_object=Resource.from_namespace(namespace),
        )

    def add_namespace(self, namespace: str, action: ResourceAction):
        """Add request for a namespace."""
        namespace_request = self.resource_request_from_namespace(namespace, action)
        self.add_request(namespace_request)

    def add_namespaces(self, namespaces: list[str], action: ResourceAction):
        """Add requests for multiple namespaces."""
        self.requests.extend(
            self.resource_request_from_namespace(namespace, action)
            for namespace in namespaces
        )

    async def check(
        self,
        on_denied: AccessDenialMode = AccessDenialMode.FILTER,
    ) -> list[AccessDecision]:
        """
        Validate all requests using AuthorizationService.

        Args:
            on_denied: How to handle denied requests
                - FILTER: Return only approved (default)
                - RAISE: Raise exception if any denied
                - RETURN_ALL: Return all with approved field set
        """
        auth_service = get_authorization_service()
        access_decisions = auth_service.authorize(self.auth_context, self.requests)

        if on_denied == AccessDenialMode.RETURN:
            return access_decisions
        elif on_denied == AccessDenialMode.RAISE:
            denied: list[AccessDecision] = [
                decision for decision in access_decisions if not decision.approved
            ]
            if denied:
                from datajunction_server.errors import DJAuthorizationException

                # Show first 5 denied resources
                denied_names = [d.request.access_object.name for d in denied[:5]]
                more_count = max(0, len(denied) - 5)

                raise DJAuthorizationException(
                    message=(
                        f"Access denied to {len(denied)} resource(s): "
                        f"{', '.join(denied_names)}"
                        + (f" and {more_count} more" if more_count else "")
                    ),
                )
            return access_decisions
        # Default: FILTER
        return [decision for decision in access_decisions if decision.approved]

    async def approved_resource_names(self) -> list[str]:
        """Get approved resource names."""
        return [
            decision.request.access_object.name
            for decision in await self.check(on_denied=AccessDenialMode.FILTER)
        ]


def get_access_checker(
    auth_context: AuthContext = Depends(get_auth_context),
) -> AccessChecker:
    """Provide AccessChecker with pre-loaded context."""
    return AccessChecker(auth_context)
