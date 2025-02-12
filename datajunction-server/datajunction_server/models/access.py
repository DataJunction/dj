"""
Models for authorization
"""

from copy import deepcopy
from enum import Enum
from typing import TYPE_CHECKING, Callable, Iterable, Optional, Set, Union

from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.utils import try_get_dj_node
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJAuthorizationException, DJError, ErrorCode
from datajunction_server.models.user import UserOutput

if TYPE_CHECKING:
    from datajunction_server.sql.parsing.ast import Column


class ResourceType(Enum):
    """
    Types of resources
    """

    NODE = "node"
    NAMESPACE = "namespace"


class ResourceRequestVerb(Enum):
    """
    Types of actions for a request
    """

    BROWSE = "browse"
    READ = "read"
    WRITE = "write"
    USE = "use"
    EXECUTE = "execute"
    DELETE = "delete"


class Resource(BaseModel):
    """
    Base class for resource objects
    that are passed to injected validation logic
    """

    name: str  # name of the node
    resource_type: ResourceType
    owner: str

    def __hash__(self) -> int:
        return hash((self.name, self.resource_type, self.owner))

    @classmethod
    def from_node(cls, node: Union[NodeRevision, Node]) -> "Resource":
        """
        Create a resource object from a DJ Node
        """
        return cls(name=node.name, resource_type=ResourceType.NODE, owner="")

    @classmethod
    def from_namespace(cls, namespace: str) -> "Resource":
        """
        Create a resource object from a namespace
        """
        return cls(
            name=namespace,
            resource_type=ResourceType.NAMESPACE,
            owner="",
        )


class ResourceRequest(BaseModel):
    """
    Resource Requests provide the information
    that is available to grant access to a resource
    """

    verb: ResourceRequestVerb
    access_object: Resource
    approved: Optional[bool] = None

    def approve(self):
        """
        Approve the request
        """
        self.approved = True

    def deny(self):
        """
        Deny the request
        """
        self.approved = False

    def __hash__(self) -> int:
        return hash((self.verb, self.access_object))

    def __eq__(self, other) -> bool:
        return self.verb == other.verb and self.access_object == other.access_object

    def __str__(self) -> str:
        return (  # pragma: no cover
            f"{self.verb.value}:"
            f"{self.access_object.resource_type.value}/"
            f"{self.access_object.name}"
        )


class AccessControlState(Enum):
    """
    State values used by the ACS function to track when
    """

    DIRECT = "direct"
    INDIRECT = "indirect"


class AccessControl(BaseModel):
    """
    An access control provides all the information
    necessary to deny or approve a request
    """

    user: str
    state: AccessControlState
    direct_requests: Set[ResourceRequest]
    indirect_requests: Set[ResourceRequest]
    validation_request_count: int

    @property
    def requests(self) -> Set[ResourceRequest]:
        """
        Get all direct and indirect requests as a single set
        """
        return self.direct_requests | self.indirect_requests

    def approve_all(self):
        """
        Approve all requests
        """
        for request in self.requests:
            request.approve()

    def deny_all(self):
        """
        Deny all requests
        """
        for request in self.requests:
            request.deny()


ValidateAccessFn = Callable[[AccessControl], None]


class AccessControlStore(BaseModel):
    """
    An access control store tracks all ResourceRequests
    """

    validate_access: Callable[["AccessControl"], bool]
    user: Optional[UserOutput]
    base_verb: Optional[ResourceRequestVerb] = None
    state: AccessControlState = AccessControlState.DIRECT
    direct_requests: Set[ResourceRequest] = Field(default_factory=set)
    indirect_requests: Set[ResourceRequest] = Field(default_factory=set)
    validation_request_count: int = 0
    validation_results: Set[ResourceRequest] = Field(default_factory=set)

    def add_request(self, request: ResourceRequest):
        """
        Add a resource request to the store
        """
        if self.state == AccessControlState.DIRECT:
            self.direct_requests.add(request)
        else:
            self.indirect_requests.add(request)  # pragma: no cover

    async def add_request_by_node_name(
        self,
        session: AsyncSession,
        node_name: Union[str, "Column"],
        verb: Optional[ResourceRequestVerb] = None,
    ):
        """
        Add a request using a node's name
        """
        node = await try_get_dj_node(session, node_name)
        if node is not None:
            self.add_request_by_node(node, verb)
        return node

    def add_request_by_node(
        self,
        node: Union[NodeRevision, Node],
        verb: Optional[ResourceRequestVerb] = None,
    ):
        """
        Add a request using a node
        """
        self.add_request(
            ResourceRequest(
                verb=verb or self.base_verb,
                access_object=Resource.from_node(node),
            ),
        )

    def add_request_by_nodes(
        self,
        nodes: Iterable[Union[NodeRevision, Node]],
        verb: Optional[ResourceRequestVerb] = None,
    ):
        """
        Add a request using a node
        """
        for node in nodes:  # pragma: no cover
            self.add_request(  # pragma: no cover
                ResourceRequest(
                    verb=verb or self.base_verb,
                    access_object=Resource.from_node(node),
                ),
            )

    def raise_if_invalid_requests(self, show_denials: bool = True):
        """
        Raises if validate has ever given any invalid requests
        """
        denied = ", ".join(
            [
                str(request)
                for request in self.validation_results
                if not request.approved
            ],
        )
        if denied:
            message = (
                f"Authorization of User `{self.user.username if self.user else 'no user'}` "
                "for this request failed."
                f"\nThe following requests were denied:\n{denied}."
                if show_denials
                else ""
            )
            raise DJAuthorizationException(
                errors=[
                    DJError(
                        code=ErrorCode.UNAUTHORIZED_ACCESS,
                        message=message,
                    ),
                ],
            )

    def validate(self) -> Set[ResourceRequest]:
        """
        Checks with ACS and stores any returned invalid requests
        """
        self.validation_request_count += 1

        access_control = AccessControl(
            user=self.user.username if self.user is not None else "",
            state=self.state,
            direct_requests=deepcopy(self.direct_requests),
            indirect_requests=deepcopy(self.indirect_requests),
            validation_request_count=self.validation_request_count,
        )

        self.validate_access(access_control)  # type: ignore

        self.validation_results = access_control.requests

        if any((result.approved is None for result in self.validation_results)):
            raise DJAuthorizationException(
                errors=[
                    DJError(
                        code=ErrorCode.INCOMPLETE_AUTHORIZATION,
                        message="Injected `validate_access` must approve or deny all requests.",
                    ),
                ],
            )

        return self.validation_results

    def validate_and_raise(self):
        """
        Validates with ACS and raises if any resources were denied
        """
        self.validate()
        self.raise_if_invalid_requests()
