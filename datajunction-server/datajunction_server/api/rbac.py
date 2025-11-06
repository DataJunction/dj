"""
RBAC (Role-Based Access Control) API endpoints
"""

from http import HTTPStatus
from typing import List, Optional

from fastapi import Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.models.access import ResourceType
from datajunction_server.database.rbac import Role, RoleAssignment
from datajunction_server.database.user import PrincipalKind, User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.rbac import RBACService
from datajunction_server.utils import get_current_user, get_session

router = SecureAPIRouter(tags=["rbac"])


# Pydantic models for API requests/responses
class RoleResponse(BaseModel):
    """Response model for roles"""

    name: str
    description: Optional[str] = None
    permissions: List[str]
    is_system_role: bool
    created_at: str

    class Config:
        from_attributes = True


class RoleCreate(BaseModel):
    """Request model for creating a custom role"""

    name: str
    description: Optional[str] = None
    permissions: List[str]


class RoleAssignmentCreate(BaseModel):
    """Request model for creating a role assignment"""

    principal_id: int
    role_name: str
    scope_type: ResourceType
    scope_value: Optional[str] = None


class RoleAssignmentResponse(BaseModel):
    """Response model for role assignments"""

    id: int
    principal_id: int
    principal_name: str
    principal_kind: str
    role_name: str
    scope_type: ResourceType
    scope_value: Optional[str]
    granted_by_id: int
    granted_by_name: str
    granted_at: str


class UserResponse(BaseModel):
    """Response model for users/groups"""

    id: int
    username: str
    name: Optional[str] = None
    kind: str
    created_at: Optional[str] = None

    class Config:
        from_attributes = True


class GroupCreate(BaseModel):
    """Request model for creating a group"""

    name: str
    display_name: str


class GroupMembershipCreate(BaseModel):
    """Request model for adding members to a group"""

    member_ids: List[int]


class PrincipalMembershipResponse(BaseModel):
    """Response model for group memberships"""

    member_id: int
    group_id: int
    added_by_id: int
    added_at: str

    class Config:
        from_attributes = True


class PermissionCheck(BaseModel):
    """Response model for permission checks"""

    user: str
    action: str
    resource: str
    resource_type: str
    allowed: bool
    reason: str


class NamespaceOwnership(BaseModel):
    """Request model for namespace ownership assignment"""

    usernames: List[str] = []
    group_names: List[str] = []


# Role Management APIs
@router.get("/roles", response_model=List[RoleResponse])
async def list_roles(
    session: AsyncSession = Depends(get_session),
) -> List[RoleResponse]:
    """List all available roles"""
    roles = await Role.get_all(session)
    return [
        RoleResponse(
            name=role.name,
            description=role.description,
            permissions=role.permissions,
            is_system_role=role.is_system_role,
            created_at=role.created_at.isoformat(),
        )
        for role in roles
    ]


@router.post("/roles", status_code=HTTPStatus.CREATED, response_model=RoleResponse)
async def create_role(
    role_data: RoleCreate,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> RoleResponse:
    """Create a custom role (admin only for now)"""

    # TODO: Add admin check

    role = Role(
        name=role_data.name,
        description=role_data.description,
        permissions=role_data.permissions,
        is_system_role=False,
    )

    session.add(role)
    await session.commit()

    return RoleResponse(
        name=role.name,
        description=role.description,
        permissions=role.permissions,
        is_system_role=role.is_system_role,
        created_at=role.created_at.isoformat(),
    )


# Role Assignment APIs
@router.post("/role-assignments", status_code=HTTPStatus.CREATED)
async def assign_role(
    assignment_data: RoleAssignmentCreate,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> RoleAssignmentResponse:
    """Assign a role to a principal"""

    rbac_service = RBACService(session)

    assignment = await rbac_service.assign_role(
        principal_id=assignment_data.principal_id,
        role_name=assignment_data.role_name,
        scope_type=assignment_data.scope_type,
        scope_value=assignment_data.scope_value,
        granted_by_id=current_user.id,
    )

    # Load relationships for response
    await session.refresh(assignment)

    return RoleAssignmentResponse(
        id=assignment.id,
        principal_id=assignment.principal_id,
        principal_name=assignment.principal.username,
        principal_kind=assignment.principal.kind.value,
        role_name=assignment.role_name,
        scope_type=assignment.scope_type,
        scope_value=assignment.scope_value,
        granted_by_id=assignment.granted_by_id,
        granted_by_name=assignment.granted_by.username,
        granted_at=assignment.granted_at.isoformat(),
    )


@router.get("/role-assignments")
async def list_role_assignments(
    principal_id: Optional[int] = Query(None),
    role_name: Optional[str] = Query(None),
    scope_type: Optional[ResourceType] = Query(None),
    session: AsyncSession = Depends(get_session),
) -> List[RoleAssignmentResponse]:
    """List role assignments with optional filtering"""

    # TODO: Implement filtering and proper response formatting
    raise HTTPException(
        status_code=HTTPStatus.NOT_IMPLEMENTED,
        detail="Filtering not yet implemented",
    )


# Namespace/Node Ownership APIs
@router.post("/namespaces/{namespace}/owners", status_code=HTTPStatus.CREATED)
async def assign_namespace_owners(
    namespace: str,
    ownership_data: NamespaceOwnership,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> List[RoleAssignmentResponse]:
    """Assign owners to a namespace"""

    rbac_service = RBACService(session)
    assignments = []

    # Assign to users
    for username in ownership_data.usernames:
        user = await User.get_by_username(session, username)
        if not user:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f"User '{username}' not found",
            )

        assignment = await rbac_service.assign_role(
            principal_id=user.id,
            role_name="owner",
            scope_type="namespace",
            scope_value=namespace,
            granted_by_id=current_user.id,
        )
        assignments.append(assignment)

    # Assign to groups
    for group_name in ownership_data.group_names:
        group = await User.get_by_username(session, group_name)
        if not group or group.kind != PrincipalKind.GROUP:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f"Group '{group_name}' not found",
            )

        assignment = await rbac_service.assign_role(
            principal_id=group.id,
            role_name="owner",
            scope_type="namespace",
            scope_value=namespace,
            granted_by_id=current_user.id,
        )
        assignments.append(assignment)

    # Format responses
    responses = []
    for assignment in assignments:
        await session.refresh(assignment)
        responses.append(
            RoleAssignmentResponse(
                id=assignment.id,
                principal_id=assignment.principal_id,
                principal_name=assignment.principal.username,
                principal_kind=assignment.principal.kind.value,
                role_name=assignment.role_name,
                scope_type=assignment.scope_type,
                scope_value=assignment.scope_value,
                granted_by_id=assignment.granted_by_id,
                granted_by_name=assignment.granted_by.username,
                granted_at=assignment.granted_at.isoformat(),
            ),
        )

    return responses


@router.get("/namespaces/{namespace}/owners")
async def get_namespace_owners(
    namespace: str,
    session: AsyncSession = Depends(get_session),
) -> List[RoleAssignmentResponse]:
    """Get owners of a namespace"""

    rbac_service = RBACService(session)
    owners = await rbac_service.get_resource_owners("namespace", namespace)

    # Convert to response format
    responses = []
    for owner in owners:
        # Get the assignment details
        assignments = await RoleAssignment.get_assignments_for_principal(
            session,
            owner.id,
        )
        for assignment in assignments:
            if (
                assignment.role_name == "owner"
                and assignment.scope_type == "namespace"
                and assignment.scope_value == namespace
            ):
                responses.append(
                    RoleAssignmentResponse(
                        id=assignment.id,
                        principal_id=assignment.principal_id,
                        principal_name=assignment.principal.username,
                        principal_kind=assignment.principal.kind.value,
                        role_name=assignment.role_name,
                        scope_type=assignment.scope_type,
                        scope_value=assignment.scope_value,
                        granted_by_id=assignment.granted_by_id,
                        granted_by_name=assignment.granted_by.username,
                        granted_at=assignment.granted_at.isoformat(),
                    ),
                )

    return responses


# Group Management APIs
@router.post("/groups", status_code=HTTPStatus.CREATED, response_model=UserResponse)
async def create_group(
    group_data: GroupCreate,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> UserResponse:
    """Create a new group"""

    rbac_service = RBACService(session)

    group = await rbac_service.create_group(
        name=group_data.name,
        display_name=group_data.display_name,
        created_by_id=current_user.id,
    )

    return UserResponse(
        id=group.id,
        username=group.username,
        name=group.name,
        kind=group.kind.value,
        created_at=group.created_at.isoformat() if group.created_at else None,
    )


@router.post(
    "/groups/{group_name}/members",
    status_code=HTTPStatus.CREATED,
    response_model=List[PrincipalMembershipResponse],
)
async def add_group_members(
    group_name: str,
    membership_data: GroupMembershipCreate,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> List[PrincipalMembershipResponse]:
    """Add members to a group"""

    # Get group
    group = await User.get_by_username(session, group_name)
    if not group or group.kind != PrincipalKind.GROUP:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Group '{group_name}' not found",
        )

    rbac_service = RBACService(session)
    membership_responses = []

    for member_id in membership_data.member_ids:
        membership = await rbac_service.add_to_group(
            member_id=member_id,
            group_id=group.id,
            added_by_id=current_user.id,
        )

        membership_responses.append(
            PrincipalMembershipResponse(
                member_id=membership.member_id,
                group_id=membership.group_id,
                added_by_id=membership.added_by_id,
                added_at=membership.added_at.isoformat(),
            ),
        )

    return membership_responses


# Permission Checking APIs
@router.get("/me/permissions/check")
async def check_my_permissions(
    action: str = Query(
        ...,
        description="Action: READ, WRITE, DELETE, EXECUTE, MANAGE",
    ),
    resource: str = Query(..., description="Resource name (node or namespace)"),
    resource_type: str = Query(
        default="node",
        description="Resource type: node or namespace",
    ),
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> PermissionCheck:
    """Check if current user can perform action on resource"""

    from datajunction_server.internal.access.rbac import RBACValidator
    from datajunction_server.models.access import Resource, ResourceType

    validator = RBACValidator(session)

    # Create resource object
    if resource_type == "namespace":
        resource_obj = Resource.from_namespace(resource)
    else:
        resource_obj = Resource(
            name=resource,
            resource_type=ResourceType.NODE,
            owner="",  # Not used in validation
        )

    allowed = await validator.check_permission(current_user, action, resource_obj)

    reason = "Permission granted" if allowed else "Permission denied"

    return PermissionCheck(
        user=current_user.username,
        action=action,
        resource=resource,
        resource_type=resource_type,
        allowed=allowed,
        reason=reason,
    )


@router.get("/me/role-assignments")
async def get_my_role_assignments(
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> List[RoleAssignmentResponse]:
    """Get all role assignments for current user"""

    from datajunction_server.internal.access.rbac import RBACValidator

    validator = RBACValidator(session)
    assignments = await validator._get_effective_assignments(current_user.id)

    responses = []
    for assignment in assignments:
        responses.append(
            RoleAssignmentResponse(
                id=assignment.id,
                principal_id=assignment.principal_id,
                principal_name=assignment.principal.username,
                principal_kind=assignment.principal.kind.value,
                role_name=assignment.role_name,
                scope_type=assignment.scope_type,
                scope_value=assignment.scope_value,
                granted_by_id=assignment.granted_by_id,
                granted_by_name=assignment.granted_by.username,
                granted_at=assignment.granted_at.isoformat(),
            ),
        )

    return responses


# Migration API (admin only)
@router.post("/migrate-ownership", status_code=HTTPStatus.OK)
async def migrate_existing_ownership(
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Migrate existing ownership to RBAC system (admin only)"""

    # TODO: Add admin check

    rbac_service = RBACService(session)
    await rbac_service.migrate_existing_ownership()

    return {"message": "Migration completed successfully"}
