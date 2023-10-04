"""
Models for authorization
"""
from enum import Enum
from typing import Union, List, Set, Callable, Optional, Dict
from http import HTTPStatus
from pydantic import BaseModel, Field
from datajunction_server.models.node import NodeRevision
from datajunction_server.models.user import User
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.construction.utils import get_dj_node
from sqlmodel import Session


class ResourceRequestVerb(Enum):
    """
    Types of actions for a request
    """

    READ = "read"
    WRITE = "write"


class ResourceObjectKind(Enum):
    """
    Types of objects for a request
    """

    NODE = "node"
    NAMESPACE = "namespace"


class ResourceRequest(BaseModel):
    """
    Resource Requests provide the information
    that is available to grant access to a resource
    """

    verb: ResourceRequestVerb
    object_kind: ResourceObjectKind
    access_object: str
    approved: bool = True

    def approve(self):
        self.approved = True

    def deny(self):
        self.approved = False

    def __hash__(self) -> int:
        """
        hash an ResourceRequestInternal
        """
        return hash((self.verb, self.object_kind, self.access_object))

    def __str__(self) -> str:
        return f"{self.verb}:{self.object_kind}/{self.access_object}"


class ResourceRequestInternal(BaseModel):
    """
    An Access request specifying what action
    is being attempted
    on what kind of object
    and the specific object
    """

    verb: ResourceRequestVerb
    access_object: Union[str, NodeRevision]

    def __hash__(self) -> int:
        """
        hash an ResourceRequestInternal
        """
        return hash((self.verb, self.access_object, self.access_object))

    def to_resource_request(self) -> ResourceRequest:
        return ResourceRequest(
            verb=self.verb, object_kind=self.object_kind, access_object=self.obj_str
        )

    @property
    def object_kind(self) -> ResourceObjectKind:
        if isinstance(self.access_object, str):
            return ResourceObjectKind.NAMESPACE
        return ResourceObjectKind.NODE

    @property
    def obj_str(self) -> str:
        return (
            self.access_object
            if self.object_kind == ResourceObjectKind.NAMESPACE
            else self.access_object.name
        )


class AccessControlState(Enum):
    """
    State values used by the ACS function to track when
    """

    IMMEDIATE = "immediate"
    INTERMEDIATE = "intermediate"


class AccessControl(BaseModel):
    """
    An access control provides all the information
    necessary to deny or approve a request
    """

    state: AccessControlState
    requests: Set[ResourceRequest]
    validation_request_count: int


class AccessControlStore(BaseModel):
    """
    An access control store tracks all ResourceRequests
    """

    validate_access: Callable[["AccessControl"], bool]
    user: User
    state: AccessControlState = AccessControlState.IMMEDIATE
    immediate_requests: Set[ResourceRequestInternal] = Field(default_factory=set)
    intermediate_requests: Set[ResourceRequestInternal] = Field(default_factory=set)
    validation_request_count: int = 0
    validation_results: Set[ResourceRequestInternal] = Field(default_factory=set)

    def add_request_by_node_name(
        self, session: Session, verb: ResourceRequestVerb, node_name: str
    ):
        node = get_dj_node(session, node_name, current=True)
        if self.state == AccessControlState.IMMEDIATE:
            self.immediate_requests.add(
                ResourceRequestInternal(verb=verb, access_object=node)
            )
        else:
            self.intermediate_requests.add(
                ResourceRequestInternal(verb=verb, access_object=node)
            )

    def add_request_by_node(self, verb: ResourceRequestVerb, node: NodeRevision):
        if self.state == AccessControlState.IMMEDIATE:
            self.immediate_requests.add(
                ResourceRequestInternal(verb=verb, access_object=node)
            )
        else:
            self.intermediate_requests.add(
                ResourceRequestInternal(verb=verb, access_object=node)
            )

    def raise_if_invalid_requests(self, show_denials: bool = True):
        """
        Raises if validate has ever given any invalid requests
        """
        if self.validation_results is None: return
        denied = [request for request in self.validation_results if self.validation_results]
        if denied:
            message = (
                f"Authorization of User `{self.user.username}` for this request failed."
                f"\nThe following requests were denied:\n{denied}." if show_denials else ""
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

    def validate(self) -> Dict[ResourceRequest]:
        """
        Checks with ACS and stores any returned invalid requests
        """
        self.validation_request_count += 1
        request_map = {
            rr.to_resource_request(): rr
            for rr in self.immediate_requests | self.intermediate_requests
        }
        request_set = set(request_map.keys())
        access_control = AccessControl(
            state=self.state,
            requests=request_set,
            validation_request_count=self.validation_request_count,
        )
        self.validate_access(access_control)
        self.validation_results = {
            request_map[request]
            for request in access_control.requests
            if request in request_map
        }
        return self.validation_results

    def validate_and_raise(self):
        """
        Validates with ACS and raises if any resources were denied
        """
        self.validate()
        self.raise_if_invalid_requests()


ValidateAccessFn = Callable[[AccessControl], None]


# Dummy default if not dependency injected
def validate_access(access_control: AccessControl):
    """
    Return a set of denied ResourceRequests.
    An empty set signals approval of all requests.

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
