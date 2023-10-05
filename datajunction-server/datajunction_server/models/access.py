"""
Models for authorization
"""
from enum import Enum
from typing import Union, List, Set, Callable, Optional, Dict, FrozenSet, Iterable
from http import HTTPStatus
from pydantic import BaseModel, Field
from datajunction_server.models.node import NodeRevision, Node
from datajunction_server.models.user import User
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.construction.utils import get_dj_node
from sqlmodel import Session
from copy import deepcopy


class ResourceRequestVerb(Enum):
    """
    Types of actions for a request
    """

    VIEW = "view"
    READ = "read"
    WRITE = "write"


class ResourceObjectBase(BaseModel):
    """
    Base class for resource objects
    that are passed to injected validation logic
    """

    name: str  # name of the node
    created_by: str

    def __hash__(self) -> int:
        return hash((self.name, self.created_by))

    @staticmethod
    def from_node(node: NodeRevision|Node) -> "DJNode":
        if isinstance(node, Node):
            return DJNode(
                id=node.id,
                revision_id=node.current.id,
                name=node.name,
                namespace=node.namespace,
                created_by="",
                tags=frozenset({tag.name for tag in node.tags}),
            )
        return DJNode(
            id=node.node_id,
            revision_id=node.id,
            name=node.name,
            namespace=node.node.namespace,
            created_by="",
            tags=frozenset({tag.name for tag in node.node.tags}),
        )

    @staticmethod
    def from_namespace(namespace: str) -> "DJNode":
        return DJNamespace(name=namespace, created_by="")


class DJNode(ResourceObjectBase):
    """
    Resource Object for DJ Node
    """

    id: int
    revision_id: int
    namespace: str  # namespace the node belongs to
    tags: FrozenSet[str]

    def __hash__(self) -> int:
        return hash((self.name, self.created_by, self.namespace, self.tags))


class DJNamespace(ResourceObjectBase):
    """
    Resource Object for DJ Node
    """


class ResourceRequest(BaseModel):
    """
    Resource Requests provide the information
    that is available to grant access to a resource
    """

    verb: ResourceRequestVerb
    access_object: ResourceObjectBase
    approved: Optional[bool] = None

    def approve(self):
        self.approved = True

    def deny(self):
        self.approved = False

    def __hash__(self) -> int:
        """
        hash an ResourceRequestInternal
        """
        return hash((self.verb, self.access_object, self.approved))

    def __str__(self) -> str:
        return f"{self.verb}:{self.access_object.__class__.__name__.lower()}/{self.access_object.name}"


class AccessControlState(Enum):
    """
    State values used by the ACS function to track when
    """

    DIRECT = "direct"
    INDIRECT = "INDIRECT"


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
        return self.direct_requests | self.indirect_requests


class AccessControlStore(BaseModel):
    """
    An access control store tracks all ResourceRequests
    """

    validate_access: Callable[["AccessControl"], bool]
    user: User
    state: AccessControlState = AccessControlState.DIRECT
    direct_requests: Set[ResourceRequest] = Field(default_factory=set)
    indirect_requests: Set[ResourceRequest] = Field(default_factory=set)
    validation_request_count: int = 0
    validation_results: Set[ResourceRequest] = Field(default_factory=set)

    def add_request(self, request: ResourceRequest):
        if self.state == AccessControlState.DIRECT:
            self.direct_requests.add(request)
        else:
            self.indirect_requests.add(request)

    def add_request_by_node_name(
        self, session: Session, verb: ResourceRequestVerb, node_name: str
    ):
        """
        Add a request using a node's name
        """
        node = get_dj_node(session, node_name, current=True)
        self.add_request_by_node(verb, node)

    def add_request_by_node(self, verb: ResourceRequestVerb, node: NodeRevision|Node):
        """
        Add a request using a node
        """
        self.add_request(
            ResourceRequest(verb=verb, access_object=ResourceObjectBase.from_node(node))
        )

    def add_request_by_nodes(
        self, verb: ResourceRequestVerb, nodes: Iterable[NodeRevision|Node]
    ):
        """
        Add a request using a node
        """
        for node in nodes:
            self.add_request(
                ResourceRequest(
                    verb=verb, access_object=ResourceObjectBase.from_node(node)
                )
            )

    def add_request_by_namespace(self, verb: ResourceRequestVerb, namespace: str):
        """
        Add a request using a namespace
        """
        self.add_request(
            ResourceRequest(
                verb=verb, access_object=ResourceObjectBase.from_namespace(namespace)
            )
        )

    def raise_if_invalid_requests(self, show_denials: bool = True):
        """
        Raises if validate has ever given any invalid requests
        """
        denied = [
            request for request in self.validation_results if not request.approved
        ]
        if denied:
            message = (
                f"Authorization of User `{self.user.username}` for this request failed."
                f"\nThe following requests were denied:\n{denied}."
                if show_denials
                else ""
            )
            raise DJException(
                http_status_code=HTTPStatus.FORBIDDEN,
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
            user=self.user.username,
            state=self.state,
            direct_requests=deepcopy(self.direct_requests),
            indirect_requests=deepcopy(self.indirect_requests),
            validation_request_count=self.validation_request_count,
        )
        self.validate_access(access_control)
        self.validation_results = access_control.requests

        if any((result.approved is None for result in self.validation_results)):
            raise DJException(
                http_status_code=HTTPStatus.FORBIDDEN,
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


def validate_nodes(
    validate_access: "ValidateAccessFn",
    user: User,
    nodes: Iterable[Node|NodeRevision],
    raise_: bool = False,
) -> List[NodeRevision]:
    
    access_control = AccessControlStore(
        validate_access=validate_access,
        user=user,
    )

    access_control.add_request_by_nodes(ResourceRequestVerb.VIEW, nodes)

    validation_results = access_control.validate()
    if raise_:
        access_control.raise_if_invalid_requests()

    return [
        node
        for node in nodes
        if node.id
        in {
            request.access_object.revision_id
            for request in validation_results
            if request.approved
        }
    ]


ValidateAccessFn = Callable[[AccessControl], None]


# Dummy default if not dependency injected
def validate_access()->ValidateAccessFn:
    def _validate_access(access_control: AccessControl):
        """
        Examines all requests in the AccessControl
        and approves or denies each

        Args:
            access_control (AccessControl): The access control object
                containing the access control state and requests.

        Example:
            for request in access_control.requests:
                if ...:
                    request.approve()
                else:
                    request.deny()
        """
        for request in access_control.requests:
            request.approved = "dj" == access_control.user

    return _validate_access
