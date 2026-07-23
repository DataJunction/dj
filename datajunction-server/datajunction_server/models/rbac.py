"""Pydantic models for RBAC."""

from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StringConstraints

from datajunction_server.models.access import (
    ResourceAction,
    ResourceType,
    parse_scope_pattern,
)
from datajunction_server.typing import UTCDatetime


def validate_scope_value(value: str) -> str:
    """Reject scope values outside the supported containment grammar."""
    if not value.strip():
        raise ValueError("scope_value cannot be blank; use '*' for a global scope")
    if value != value.strip() or parse_scope_pattern(value) is None:
        raise ValueError(
            "scope_value must be '*', an exact scope, or a subtree ending in '.*'",
        )
    return value


ScopeValue = Annotated[
    str,
    StringConstraints(max_length=500),
    AfterValidator(validate_scope_value),
]


class PrincipalOutput(BaseModel):
    """Output for a principal (user, service account, or group)."""

    username: str
    email: str | None = None

    model_config = ConfigDict(from_attributes=True)


class RoleScopeInput(BaseModel):
    """Input for creating a role scope."""

    action: ResourceAction
    scope_type: ResourceType
    scope_value: ScopeValue


class RoleScopeOutput(BaseModel):
    """Output for role scope."""

    action: ResourceAction
    scope_type: ResourceType
    scope_value: str

    model_config = ConfigDict(from_attributes=True)


class RoleCreate(BaseModel):
    """Input for creating a role."""

    name: str = Field(..., max_length=255, min_length=1)
    description: str | None = None
    scopes: list[RoleScopeInput] = Field(default_factory=list)


class RoleUpdate(BaseModel):
    """Input for updating a role."""

    name: str | None = Field(None, max_length=255, min_length=1)
    description: str | None = None


class RoleOutput(BaseModel):
    """Output for role."""

    id: int
    name: str
    description: str | None
    created_by: PrincipalOutput
    created_at: UTCDatetime
    deleted_at: UTCDatetime | None = None
    scopes: list[RoleScopeOutput] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class RoleAssignmentCreate(BaseModel):
    """Input for assigning a role to a principal."""

    principal_username: str
    expires_at: UTCDatetime | None = None


class RoleAssignmentOutput(BaseModel):
    """Output for role assignment."""

    principal: PrincipalOutput
    role: RoleOutput
    granted_by: PrincipalOutput
    granted_at: UTCDatetime
    expires_at: UTCDatetime | None

    model_config = ConfigDict(from_attributes=True)
