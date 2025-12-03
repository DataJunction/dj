"""
Models for authorization
"""

from dataclasses import dataclass

from datajunction_server.typing import StrEnum
from datajunction_server.database.node import Node, NodeRevision


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
    """

    verb: ResourceAction
    access_object: Resource

    def __hash__(self) -> int:
        return hash((self.verb, self.access_object))

    def __eq__(self, other) -> bool:
        return self.verb == other.verb and self.access_object == other.access_object

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
