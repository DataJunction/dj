from pydantic import BaseModel
from datajunction_server.models.access import ResourceRequestVerb, ResourceType
from datajunction_server.database.role import AccessRule


class RoleBase(BaseModel):
    name: str
    description: str | None


class RoleCreate(RoleBase):
    pass


class RoleUpdate(RoleBase):
    pass


class BasicUserOut(BaseModel):
    """User information to be included in responses"""

    username: str
    email: str | None
    name: str | None

    class Config:
        orm_mode = True


class RoleOut(RoleBase):
    id: int
    name: str
    description: str | None
    created_by: BasicUserOut | None

    class Config:
        orm_mode = True


class AccessRuleBase(BaseModel):
    resource_type: ResourceType
    resource_name: str
    action: ResourceRequestVerb

    class Config:
        orm_mode = True


class AccessRuleCreate(AccessRuleBase):
    def equivalent(self, other: AccessRule) -> bool:
        return (
            self.action == other.action
            and self.resource_name == other.resource_name
            and self.resource_type == other.resource_type
        )


class AccessRuleOutput(AccessRuleBase):
    pass
