"""
Models for users and auth
"""
from enum import Enum
from typing import Union, List, Set, Callable, Optional
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

class ResourceObjectKind(Enum):add
    """
    Types of objects for a request
    """

    NODE = "node"
    NAMESPACE = "namespace"

class ResourceRequest(BaseModel):
    """
    An Access request specifying what action is being attempted
    on what kind of object
    and the specific object
    """

    verb: ResourceRequestVerb
    access_object: Union[str, NodeRevision]

    def __hash__(self)->int:
        """
        hash an ResourceRequest
        """
        return hash((self.verb, self.access_object))

    @property
    def object_kind(self) ->ResourceObjectKind:
        if isinstance(self.access_object, str):
            return ResourceObjectKind.NAMESPACE
        return ResourceObjectKind.NODE

class AccessControlState(Enum):
    """
    State values used by the ACS function to track when 
    """

    IMMEDIATE = "immediate"
    INTERMEDIATE = "intermediate"



class AccessControl(BaseModel):
    """
    An access control stores a callback to validate a user 
    """

    _validate_access: Optional[Callable[["AccessControl"], bool]]
    _user: User
    _state: AccessControlState = AccessControlState.IMMEDIATE
    _immediate_requests: Set[ResourceRequest] = Field(default_factory=set)
    _intermediate_requests: Set[ResourceRequest] = Field(default_factory=set)
    _validation_request_count: int = 0
    
    @property
    def state(self) -> AccessControlState:
        return self._state

    @property
    def user(self) -> User:
        return self._user

    @property
    def immediate_requests(self) -> Set[ResourceRequest]:
        return self._immediate_requests

    @property
    def intermediate_requests(self) -> Set[ResourceRequest]:
        return self._intermediate_requests

    @property
    def requests(self)->Set[ResourceRequest]:
        return self._immediate_requests|self._intermediate_requests
        
    @property
    def validation_request_count(self) -> int:
        return self._validation_request_count

    def _add_request_by_node_name(self, session: Session, verb: AccessVerb, node_name: str):
        node = get_dj_node(session, node_name, current=True)
        if self.state == AccessControlState.IMMEDIATE:
            self._immediate_requests.add(ResourceRequest(verb=verb, access_object=node))
        else:
            self._intermediate_requests.add(ResourceRequest(verb=verb, access_object=node))

    def _add_request_by_node(self, session: Session, verb: AccessVerb, node: NodeRevision):
        if self.state == AccessControlState.IMMEDIATE:
            self._immediate_requests.add(ResourceRequest(verb=verb, access_object=node))
        else:
            self._intermediate_requests.add(ResourceRequest(verb=verb, access_object=node))

    def _validate(self):
        self._validation_request_count += 1
        valid = False
        message = f"Authorization of User `{self.user.username}` for this request failed."
        try:
            valid = self._validate_access(self)
        except Exception as exc:
            message += f"\n{exc}"

        if not valid:
            raise DJException(
                http_status_code=HTTPStatus.FORBIDDEN,
                errors=[
                    DJError(
                        code=ErrorCode.UNAUTHORIZED_ACCESS,
                        message=message,
                    ),
                ],
            )

def validate_access(access_control: AccessControl)->bool: 
    """
    Determine whether a request from a user is allowed on a set of .
    """
    return True

