"""
Authorization service implementations for access control.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import lru_cache
from typing import List


from datajunction_server.models.access import (
    AccessDecision,
    ResourceAction,
    ResourceRequest,
    ResourceType,
)
from datajunction_server.internal.access.authorization.context import (
    AuthContext,
)
from datajunction_server.utils import (
    SEPARATOR,
    get_settings,
)

settings = get_settings()


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
    Always approves all requests (for testing or permissive environments).
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
        providers[subclass.name] = subclass
        if subclass.name == provider:
            return subclass()  # type: ignore[abstract]

    available = ", ".join(sorted(providers.keys()))
    raise ValueError(
        f"Unknown authorization_provider: '{provider}'. "
        f"Available providers: {available}",
    )
