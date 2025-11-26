"""Pydantic models for RBAC."""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from datajunction_server.models.access import ResourceAction, ResourceType
from datajunction_server.typing import UTCDatetime


class RoleScopeInput(BaseModel):
    """Input for creating a role scope."""

    action: ResourceAction
    scope_type: ResourceType
    scope_value: str = Field(..., max_length=500)


class RoleScopeOutput(BaseModel):
    """Output for role scope."""

    id: int
    role_id: int
    action: ResourceAction
    scope_type: ResourceType
    scope_value: str

    model_config = ConfigDict(from_attributes=True)


class RoleCreate(BaseModel):
    """Input for creating a role."""

    name: str = Field(..., max_length=255, min_length=1)
    description: Optional[str] = None
    scopes: list[RoleScopeInput] = Field(default_factory=list)


class RoleUpdate(BaseModel):
    """Input for updating a role."""

    name: Optional[str] = Field(None, max_length=255, min_length=1)
    description: Optional[str] = None


class RoleOutput(BaseModel):
    """Output for role."""

    id: int
    name: str
    description: Optional[str]
    created_by_id: int
    created_at: UTCDatetime
    deleted_at: Optional[UTCDatetime] = None
    scopes: list[RoleScopeOutput] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class RoleAssignmentCreate(BaseModel):
    """Input for creating a role assignment."""

    principal_id: int
    role_id: int
    expires_at: Optional[UTCDatetime] = None


class RoleAssignmentOutput(BaseModel):
    """Output for role assignment."""

    id: int
    principal_id: int
    role_id: int
    granted_by_id: int
    granted_at: UTCDatetime
    expires_at: Optional[UTCDatetime]

    model_config = ConfigDict(from_attributes=True)
