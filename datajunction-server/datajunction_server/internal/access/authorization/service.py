"""
Authorization service implementations for access control.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import lru_cache
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from datajunction_server.database.rbac import RoleScope


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

logger = logging.getLogger(__name__)

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
        # Break-glass: admins bypass all RBAC checks. Kept as a single explicit
        # check (and logged for audit) so the bypass is easy to find and, if
        # ever needed, to scope down to "admin bypasses grants but still
        # respects X".
        if auth_context.is_admin:
            logger.info(
                "Admin access bypass: user=%s (id=%s) approved %d request(s): %s",
                auth_context.username,
                auth_context.user_id,
                len(requests),
                ", ".join(str(request) for request in requests),
            )
            return [
                AccessDecision(request=request, approved=True, reason="admin")
                for request in requests
            ]
        return [self._make_decision(auth_context, request) for request in requests]

    @classmethod
    def candidate_scopes(cls, auth_context: AuthContext) -> List["RoleScope"]:
        """
        Collect every scope that could grant a request for this context.

        This is the union of the principal's own (non-expired) role scopes and
        the configured default-access role's scopes. Collecting all candidates
        up front (rather than short-circuiting source by source) keeps the
        decision a single resolve step, leaving room for future deny/precedence
        rules without restructuring.
        """
        scopes: List["RoleScope"] = []
        now = datetime.now(timezone.utc)
        for assignment in auth_context.role_assignments:
            if assignment.expires_at and assignment.expires_at < now:
                continue
            scopes.extend(assignment.role.scopes)
        scopes.extend(auth_context.default_scopes)
        return scopes

    def _make_decision(
        self,
        auth_context: AuthContext,
        request: ResourceRequest,
    ) -> AccessDecision:
        """
        Convert ResourceRequest to AccessDecision.

        Gathers all applicable scopes (explicit grants + default-access role)
        and approves if any grants the request. With no explicit grant, a
        request under a configured restrictive scope is denied; otherwise it
        falls back to the configured default_access_policy.
        """
        granted = any(
            self._scope_grants_permission(
                scope,
                request.verb,
                request.access_object.resource_type,
                request.access_object.name,
            )
            for scope in self.candidate_scopes(auth_context)
        )
        if granted:
            return AccessDecision(request=request, approved=True)
        if self.request_is_restricted(request):
            return AccessDecision(
                request=request,
                approved=False,
                reason="restrictive scope (explicit grant required)",
            )
        return AccessDecision(
            request=request,
            approved=(settings.default_access_policy == "permissive"),
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

        return cls._resource_in_scope(
            scope.scope_type,
            scope.scope_value,
            resource_type,
            resource_name,
        )

    @classmethod
    def _resource_in_scope(
        cls,
        scope_type: ResourceType,
        scope_value: str,
        resource_type: ResourceType,
        resource_name: str,
    ) -> bool:
        """
        Check if a resource falls within a (scope_type, scope_value) boundary,
        ignoring actions. Handles global ("*"/empty), same-type pattern matching,
        and the namespace-covers-node cross-type case.
        """
        # Handle global access (empty string, None, or "*" scope_value)
        if not scope_value or scope_value == "*":
            return scope_type == resource_type

        # Same resource type - use pattern matching
        if scope_type == resource_type:
            return cls.resource_matches_pattern(resource_name, scope_value)

        # Cross-resource-type: namespace scope can cover nodes
        if scope_type == ResourceType.NAMESPACE and resource_type == ResourceType.NODE:
            return cls.resource_matches_pattern(resource_name, scope_value)

        # No match
        return False

    @classmethod
    def _restrictive_rules(cls) -> List[tuple]:
        """
        Parse settings.restrictive_scopes into (action, scope_type, scope_value)
        tuples. Malformed entries are ignored.
        """
        rules: List[tuple] = []
        for raw in getattr(settings, "restrictive_scopes", []) or []:
            parts = raw.split(":")
            if len(parts) != 3:  # pragma: no cover
                continue
            action_str, type_str, scope_value = parts
            try:
                rules.append(
                    (
                        ResourceAction(action_str),
                        ResourceType(type_str),
                        scope_value,
                    ),
                )
            except ValueError:  # pragma: no cover
                continue
        return rules

    @classmethod
    def request_is_restricted(cls, request: ResourceRequest) -> bool:
        """
        Whether a request falls under a configured restrictive scope, meaning it
        is denied by default (requires an explicit grant). Action match is exact
        here (no hierarchy), so each restricted action must be listed explicitly.
        """
        for action, scope_type, scope_value in cls._restrictive_rules():
            if action != request.verb:
                continue
            if cls._resource_in_scope(
                scope_type,
                scope_value,
                request.access_object.resource_type,
                request.access_object.name,
            ):
                return True
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
