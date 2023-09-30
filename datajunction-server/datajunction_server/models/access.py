"""
Models for users and auth
"""
from enum import Enum
from typing import Union, List, Set
from http import HTTPStatus
from pydantic import BaseModel, Field
from datajunction_server.models.node import NodeRevision
from datajunction_server.models.user import User
from datajunction_server.config import Settings
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.construction.utils import get_dj_node
from sqlmodel import Session

SETTINGS = Settings()

class AccessVerb(Enum):
    """
    Types of actions for a request
    """

    READ = "read"
    WRITE = "write"

class AccessObjectKind(Enum):add
    """
    Types of objects for a request
    """

    NODE = "node"
    NAMESPACE = "namespace"

class AccessRequest(BaseModel):
    """
    An Access request specifying what action is being attempted
    on what kind of object
    and the specific object
    """

    verb: AccessVerb
    access_object: Union[str, NodeRevision]

    def __hash__(self)->int:
        """
        hash an AccessRequest
        """
        return hash((self.verb, self.access_object))

    @property
    def object_kind(self) ->AccessObjectKind:
        if isinstance(self.access_object, str):
            return AccessObjectKind.NAMESPACE
        return AccessObjectKind.NODE

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

    state: AccessControlState
    user: "User"
    immediate_requests: Set[AccessRequest]
    intermediate_requests: Set[AccessRequest] = Field(default_factory=set)
    validation_request_count: int = 0

    def set_state(self, state: AccessControlState):
        self.state = state

    def add_request_by_node_name(self, session: Session, verb: AccessVerb, node_name: str):
        node = get_dj_node(session, node_name, current=True)
        if self.state == AccessControlState.IMMEDIATE:
            self.immediate_requests.add(AccessRequest(verb=verb, access_object=node))
        else:
            self.intermediate_requests.add(AccessRequest(verb=verb, access_object=node))

    def add_request_by_node(self, session: Session, verb: AccessVerb, node: NodeRevision):
        if self.state == AccessControlState.IMMEDIATE:
            self.immediate_requests.add(AccessRequest(verb=verb, access_object=node))
        else:
            self.intermediate_requests.add(AccessRequest(verb=verb, access_object=node))

    def validate(self):
        validation_request_count+=1
        if not SETTINGS.validate_access(self):
            raise DJException(
                http_status_code=HTTPStatus.FORBIDDEN,
                errors=[
                    DJError(
                        code=ErrorCode.UNAUTHORIZED_ACCESS,
                        message=f"User {self.user.username} is not authorized to make this request.",
                    ),
                ],
            )




