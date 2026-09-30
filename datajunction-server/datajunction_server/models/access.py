"""
Models for authorization
"""

from dataclasses import dataclass

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.naming import parse_scope_pattern
from datajunction_server.typing import StrEnum


class ResourceType(StrEnum):
    """
    Types of resources
    """

    NODE = "node"
    NAMESPACE = "namespace"


class ResourceAction(StrEnum):
    """
    Actions that can be performed on resources
    """

    READ = "read"  # View details + list/browse (merge BROWSE into READ)
    WRITE = "write"  # Create/update resources
    EXECUTE = "execute"  # Run queries against nodes
    DELETE = "delete"  # Delete resources (keep for safety/auditability)
    MANAGE = "manage"  # Grant/revoke permissions (RBAC-specific)


def namespace_boundary_scope_targets(
    namespace: str,
) -> tuple[tuple[ResourceType, str], ...]:
    """Return every scope governed by a namespace boundary."""
    return (
        (ResourceType.NAMESPACE, namespace),
        (ResourceType.NAMESPACE, f"{namespace}.*"),
        (ResourceType.NODE, f"{namespace}.*"),
    )


@dataclass(frozen=True)
class RestrictiveScopeRule:
    """A configured action and resource scope that requires an explicit grant."""

    action: ResourceAction
    scope_type: ResourceType
    scope_value: str

    def __str__(self) -> str:
        return f"{self.action.value}:{self.scope_type.value}:{self.scope_value}"


def parse_restrictive_scope_rule(value: str) -> RestrictiveScopeRule:
    """Parse ``action:scope_type:scope_value`` using the RBAC scope grammar."""
    parts = value.split(":")
    if len(parts) != 3:
        raise ValueError(
            "restrictive scope must be 'action:scope_type:scope_value'",
        )
    action_value, scope_type_value, scope_value = parts
    try:
        action = ResourceAction(action_value)
        scope_type = ResourceType(scope_type_value)
    except ValueError as exc:
        raise ValueError(
            "restrictive scope action and scope type must be supported values",
        ) from exc
    if scope_value != scope_value.strip() or parse_scope_pattern(scope_value) is None:
        raise ValueError(
            "restrictive scope value must be '*', an exact scope, "
            "or a subtree ending in '.*'",
        )
    return RestrictiveScopeRule(
        action=action,
        scope_type=scope_type,
        scope_value=scope_value,
    )


@dataclass(frozen=True)
class Resource:
    """
    Base class for resource objects
    that are passed to injected validation logic
    """

    name: str
    resource_type: ResourceType

    def __hash__(self) -> int:
        return hash((self.name, self.resource_type))

    @classmethod
    def from_node(cls, node: NodeRevision | Node) -> "Resource":
        """
        Create a resource object from a DJ Node
        """
        return cls(name=node.name, resource_type=ResourceType.NODE)

    @classmethod
    def from_namespace(cls, namespace: str) -> "Resource":
        """
        Create a resource object from a namespace
        """
        return cls(name=namespace, resource_type=ResourceType.NAMESPACE)


@dataclass(frozen=True)
class ResourceRequest:
    """
    Resource Requests provide the information
    that is available to grant access to a resource

    ``scope_target`` marks a delegated scope pattern so authorization can
    prove containment instead of treating it as one concrete resource.
    """

    verb: ResourceAction
    access_object: Resource
    scope_target: bool = False

    def __hash__(self) -> int:
        return hash((self.verb, self.access_object, self.scope_target))

    def __eq__(self, other) -> bool:
        return (
            self.verb == other.verb
            and self.access_object == other.access_object
            and self.scope_target == other.scope_target
        )

    def __str__(self) -> str:
        return (
            f"{self.verb.value}:"
            f"{self.access_object.resource_type.value}/"
            f"{self.access_object.name}"
        )


@dataclass(frozen=True)
class AccessDecision:
    """
    The result of an access control check for a resource request.

    Attributes:
        request: The resource request that was checked
        approved: Whether access was granted
        reason: Optional explanation if access was denied
    """

    request: ResourceRequest
    approved: bool
    reason: str | None = None
