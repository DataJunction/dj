"""RBAC API endpoints."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datajunction_server.database.history import History
from datajunction_server.database.rbac import Role, RoleScope
from datajunction_server.errors import (
    DJAlreadyExistsException,
    DJDoesNotExistException,
    DJException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.models.access import ResourceAction, ResourceType
from datajunction_server.models.rbac import (
    RoleCreate,
    RoleOutput,
    RoleScopeInput,
    RoleScopeOutput,
    RoleUpdate,
)
from datajunction_server.utils import get_session, get_current_user
from datajunction_server.models.user import UserOutput

router = SecureAPIRouter(tags=["rbac"])


async def log_activity(
    session: AsyncSession,
    entity_type: EntityType,
    entity_name: str,
    activity_type: ActivityType,
    user: str,
    pre: Optional[Dict[str, Any]] = None,
    post: Optional[Dict[str, Any]] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log activity to history table.
    """
    history_entry = History(
        entity_type=entity_type,
        entity_name=entity_name,
        activity_type=activity_type,
        user=user,
        pre=pre or {},
        post=post or {},
        details=details or {},
    )
    session.add(history_entry)
    await session.flush()


@router.post("/roles/", response_model=RoleOutput, status_code=201)
async def create_role(
    role_data: RoleCreate,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: UserOutput = Depends(get_current_user),
) -> Role:
    """
    Create a new role with optional scopes.

    Roles are named collections of permissions that can be assigned to principals.
    """
    # Check if role with this name already exists
    existing = await Role.get_by_name(
        session=session,
        name=role_data.name,
        include_deleted=False,
    )
    if existing:
        raise DJAlreadyExistsException(
            message=f"Role with name '{role_data.name}' already exists",
        )

    # Create role
    role = Role(
        name=role_data.name,
        description=role_data.description,
        created_by_id=current_user.id,
    )
    session.add(role)
    await session.flush()  # Get role.id

    # Create scopes
    for scope_data in role_data.scopes:
        scope = RoleScope(
            role_id=role.id,
            action=scope_data.action,
            scope_type=scope_data.scope_type,
            scope_value=scope_data.scope_value,
        )
        session.add(scope)

    await session.commit()
    await session.refresh(role)

    # Load scopes relationship
    await session.refresh(role, ["scopes"])

    # Log activity for audit trail
    await log_activity(
        session=session,
        entity_type=EntityType.ROLE,
        entity_name=role.name,
        activity_type=ActivityType.CREATE,
        user=current_user.username,
        post={
            "id": role.id,
            "name": role.name,
            "description": role.description,
            "scopes": [
                {
                    "action": s.action.value,
                    "scope_type": s.scope_type.value,
                    "scope_value": s.scope_value,
                }
                for s in role.scopes
            ],
        },
    )
    await session.commit()

    return role


@router.get("/roles/", response_model=List[RoleOutput])
async def list_roles(
    *,
    session: AsyncSession = Depends(get_session),
    current_user: UserOutput = Depends(get_current_user),
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    include_deleted: bool = Query(default=False),
    created_by_id: Optional[int] = Query(default=None),
) -> List[Role]:
    """
    List all roles with their scopes.

    By default, excludes soft-deleted roles. Set include_deleted=true to see all.
    """
    return await Role.find(
        session=session,
        include_deleted=include_deleted,
        created_by_id=created_by_id,
        limit=limit,
        offset=offset,
    )


@router.get("/roles/{role_name}", response_model=RoleOutput)
async def get_role(
    role_name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: UserOutput = Depends(get_current_user),
    include_deleted: bool = Query(default=False),
) -> Role:
    """
    Get a specific role with its scopes.

    By default, returns 404 for deleted roles. Set include_deleted=true to see deleted roles.
    """
    role = await Role.get_by_name_or_raise(
        session=session,
        name=role_name,
        include_deleted=include_deleted,
    )

    return role


@router.patch("/roles/{role_name}", response_model=RoleOutput)
async def update_role(
    role_name: str,
    role_update: RoleUpdate,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: UserOutput = Depends(get_current_user),
) -> Role:
    """
    Update a role's name or description.

    Note: Use /roles/{role_name}/scopes/ endpoints to manage scopes.
    """
    # Get existing role
    role = await Role.get_by_name_or_raise(
        session=session,
        name=role_name,
        include_deleted=False,
    )

    # Capture pre-state for audit
    pre_state = {
        "name": role.name,
        "description": role.description,
    }

    # Check if new name conflicts with existing role
    if role_update.name and role_update.name != role.name:
        existing = await Role.get_by_name(
            session=session,
            name=role_update.name,
            include_deleted=False,
        )
        if existing:
            raise DJAlreadyExistsException(
                message=f"Role with name '{role_update.name}' already exists",
            )
        role.name = role_update.name

    # Update description
    if role_update.description is not None:
        role.description = role_update.description

    await session.commit()
    await session.refresh(role, ["scopes"])

    # Log activity for audit trail
    await log_activity(
        session=session,
        entity_type=EntityType.ROLE,
        entity_name=role.name,
        activity_type=ActivityType.UPDATE,
        user=current_user.username,
        pre=pre_state,
        post={
            "name": role.name,
            "description": role.description,
        },
    )
    await session.commit()

    return role


@router.delete("/roles/{role_name}", status_code=204)
async def delete_role(
    role_name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: UserOutput = Depends(get_current_user),
) -> None:
    """
    Soft delete a role.

    Roles that have ever been assigned cannot be deleted (for SOX compliance).
    This ensures a complete audit trail. Instead, roles are marked as deleted
    and hidden from normal queries.
    """
    role = await Role.get_by_name_or_raise(
        session=session,
        name=role_name,
        include_deleted=False,
        options=[
            selectinload(Role.scopes),
            selectinload(Role.assignments),
        ],
    )

    # Check if role has any assignments (current or past)
    if role.assignments:
        raise DJException(
            message=(
                f"Cannot delete role '{role.name}' because it has been assigned to principals. "
                "Roles with assignments must be retained for audit compliance. "
                "The role will remain in the system but can be hidden from active use."
            ),
            http_status_code=400,
        )

    # Soft delete
    role.deleted_at = datetime.now(timezone.utc)

    # Log activity for audit trail (who deleted is captured in History.user)
    await log_activity(
        session=session,
        entity_type=EntityType.ROLE,
        entity_name=role.name,
        activity_type=ActivityType.DELETE,
        user=current_user.username,
        pre={
            "id": role.id,
            "name": role.name,
            "description": role.description,
            "scopes": [
                {
                    "action": s.action.value,
                    "scope_type": s.scope_type.value,
                    "scope_value": s.scope_value,
                }
                for s in role.scopes
            ],
        },
    )

    await session.commit()


@router.post(
    "/roles/{role_name}/scopes/",
    response_model=RoleScopeOutput,
    status_code=201,
)
async def add_scope_to_role(
    role_name: str,
    scope_data: RoleScopeInput,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: UserOutput = Depends(get_current_user),
) -> RoleScope:
    """
    Add a scope (permission) to a role.
    """
    # Check role exists
    role = await Role.get_by_name_or_raise(
        session=session,
        name=role_name,
        include_deleted=False,
    )

    # Check if scope already exists (duplicate check)
    existing_scope = [
        scope
        for scope in role.scopes
        if scope.action == scope_data.action
        and scope.scope_type == scope_data.scope_type
        and scope.scope_value == scope_data.scope_value
    ]
    if existing_scope:
        raise DJAlreadyExistsException(
            message=f"Scope already exists for role '{role_name}'",
        )

    # Create scope
    scope = RoleScope(
        role_id=role.id,
        action=scope_data.action,
        scope_type=scope_data.scope_type,
        scope_value=scope_data.scope_value,
    )
    session.add(scope)
    await session.commit()
    await session.refresh(scope)

    # Log activity for audit trail
    await log_activity(
        session=session,
        entity_type=EntityType.ROLE_SCOPE,
        entity_name=f"{role.name}:{scope.action.value}:{scope.scope_type.value}:{scope.scope_value}",
        activity_type=ActivityType.CREATE,
        user=current_user.username,
        post={
            "role_id": role.id,
            "role_name": role.name,
            "action": scope.action.value,
            "scope_type": scope.scope_type.value,
            "scope_value": scope.scope_value,
        },
    )
    await session.commit()

    return scope


@router.get("/roles/{role_name}/scopes/", response_model=List[RoleScopeOutput])
async def list_role_scopes(
    role_name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: UserOutput = Depends(get_current_user),
) -> List[RoleScope]:
    """
    List all scopes for a role.
    """
    # Check role exists
    role = await Role.get_by_name_or_raise(
        session=session,
        name=role_name,
        include_deleted=False,
    )
    return role.scopes


@router.delete(
    "/roles/{role_name}/scopes/{action}/{scope_type}/{scope_value:path}",
    status_code=204,
)
async def delete_scope_from_role(
    role_name: str,
    action: ResourceAction,
    scope_type: ResourceType,
    scope_value: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: UserOutput = Depends(get_current_user),
) -> None:
    """
    Remove a scope from a role using its composite key.

    Example: DELETE /roles/finance-editor/scopes/read/namespace/finance.*
    """
    # Get role
    role = await Role.get_by_name_or_raise(
        session=session,
        name=role_name,
        include_deleted=False,
    )

    # Find the scope by composite key
    from sqlalchemy import delete as sql_delete

    delete_stmt = (
        sql_delete(RoleScope)
        .where(RoleScope.role_id == role.id)
        .where(RoleScope.action == action)
        .where(RoleScope.scope_type == scope_type)
        .where(RoleScope.scope_value == scope_value)
    )

    result = await session.execute(delete_stmt)

    if result.rowcount == 0:
        raise DJDoesNotExistException(
            message=f"Scope {action.value}:{scope_type.value}:{scope_value} not found for role '{role_name}'",
        )

    # Capture pre-state for audit
    pre_state = {
        "role_id": role.id,
        "role_name": role.name,
        "action": action.value,
        "scope_type": scope_type.value,
        "scope_value": scope_value,
    }

    # Log activity for audit trail
    await log_activity(
        session=session,
        entity_type=EntityType.ROLE_SCOPE,
        entity_name=f"{role.name}:{action.value}:{scope_type.value}:{scope_value}",
        activity_type=ActivityType.DELETE,
        user=current_user.username,
        pre=pre_state,
    )

    await session.commit()
