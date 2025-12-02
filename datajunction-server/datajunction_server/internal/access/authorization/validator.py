"""
Access validation collection and helper functions.
"""

from fastapi import Depends
from enum import Enum


from datajunction_server.internal.access.authorization.service import (
    get_authorization_service,
)
from datajunction_server.database.node import Node
from datajunction_server.models.access import (
    AccessDecision,
    Resource,
    ResourceAction,
    ResourceRequest,
    ResourceType,
)
from datajunction_server.internal.access.authorization.context import (
    AuthContext,
    get_auth_context,
)
from datajunction_server.utils import (
    get_settings,
)

settings = get_settings()


class AccessDenialMode(Enum):
    """
    How to handle denied access requests.
    """

    FILTER = "filter"  # Return only approved requests
    RAISE = "raise"  # Raise exception if any denied
    RETURN = "return"  # Return all requests with approved field set


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
