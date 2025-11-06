"""
Convenience functions for common RBAC operations
These provide simple, high-level interfaces for the most common use cases
"""

from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.user import User
from datajunction_server.internal.access.rbac import RBACService, RBACValidator
from datajunction_server.models.access import Resource, ResourceType


async def can_user_edit_node(user: User, node_name: str, session: AsyncSession) -> bool:
    """
    Simple check: Can user edit this node?

    Args:
        user: User to check
        node_name: Full node name (e.g., "finance.metrics.revenue")
        session: Database session

    Returns:
        True if user can edit, False otherwise
    """
    validator = RBACValidator(session)
    resource = Resource(name=node_name, resource_type=ResourceType.NODE, owner="")
    return await validator.check_permission(user, "WRITE", resource)


async def can_user_manage_namespace(
    user: User,
    namespace: str,
    session: AsyncSession,
) -> bool:
    """
    Simple check: Can user manage this namespace?

    Args:
        user: User to check
        namespace: Namespace path (e.g., "finance")
        session: Database session

    Returns:
        True if user can manage, False otherwise
    """
    validator = RBACValidator(session)
    resource = Resource.from_namespace(namespace)
    return await validator.check_permission(user, "MANAGE", resource)


async def make_namespace_owners(
    namespace: str,
    usernames: List[str],
    granted_by: User,
    session: AsyncSession,
) -> int:
    """
    Make users owners of a namespace

    Args:
        namespace: Namespace to assign ownership to
        usernames: List of usernames to make owners
        granted_by: User granting the permissions
        session: Database session

    Returns:
        Number of assignments created
    """
    rbac_service = RBACService(session)

    assignments_created = 0
    for username in usernames:
        user = await User.get_by_username(session, username)
        if user:
            await rbac_service.assign_role(
                principal_id=user.id,
                role_name="owner",
                scope_type="namespace",
                scope_value=namespace,
                granted_by_id=granted_by.id,
            )
            assignments_created += 1

    return assignments_created


async def make_team_namespace_owners(
    namespace: str,
    team_name: str,
    team_members: List[str],
    created_by: User,
    session: AsyncSession,
) -> User:
    """
    Create a team group and make it owner of a namespace

    Args:
        namespace: Namespace to assign to team
        team_name: Name of the team/group to create
        team_members: List of usernames to add to team
        created_by: User creating the team
        session: Database session

    Returns:
        The created group
    """
    rbac_service = RBACService(session)

    # Create team group
    team_group = await rbac_service.create_group(
        name=team_name,
        display_name=team_name.replace("-", " ").title(),
        created_by_id=created_by.id,
    )

    # Add members to team
    for username in team_members:
        user = await User.get_by_username(session, username)
        if user:
            await rbac_service.add_to_group(
                member_id=user.id,
                group_id=team_group.id,
                added_by_id=created_by.id,
            )

    # Make team owner of namespace
    await rbac_service.assign_role(
        principal_id=team_group.id,
        role_name="owner",
        scope_type="namespace",
        scope_value=namespace,
        granted_by_id=created_by.id,
    )

    return team_group


async def get_my_accessible_namespaces(
    user: User,
    session: AsyncSession,
) -> List[str]:
    """
    Get list of namespaces the user has any access to

    Args:
        user: User to check
        session: Database session

    Returns:
        List of namespace names user can access
    """

    validator = RBACValidator(session)
    assignments = await validator._get_effective_assignments(user.id)

    namespaces = set()
    for assignment in assignments:
        if assignment.scope_type == "namespace" and assignment.scope_value:
            namespaces.add(assignment.scope_value)
        elif assignment.scope_type == "global":
            namespaces.add("*")  # Global access

    return sorted(list(namespaces))


async def get_my_owned_nodes(
    user: User,
    session: AsyncSession,
) -> List[str]:
    """
    Get list of nodes the user owns (has owner role on)

    Args:
        user: User to check
        session: Database session

    Returns:
        List of node names user owns
    """
    validator = RBACValidator(session)
    assignments = await validator._get_effective_assignments(user.id)

    owned_nodes = []
    for assignment in assignments:
        if assignment.role_name == "owner":
            if assignment.scope_type == "node" and assignment.scope_value:
                owned_nodes.append(assignment.scope_value)
            elif assignment.scope_type == "namespace" and assignment.scope_value:
                # TODO: Query actual nodes in namespace
                # For now, just indicate they own the namespace
                owned_nodes.append(f"{assignment.scope_value}.*")

    return owned_nodes


async def setup_basic_namespace_structure(
    admin_user: User,
    session: AsyncSession,
) -> dict:
    """
    Set up a basic namespace structure for a fresh DJ installation
    Creates some common namespaces and makes admin the owner

    Args:
        admin_user: Admin user to make owner of namespaces
        session: Database session

    Returns:
        Dictionary with created namespaces
    """
    rbac_service = RBACService(session)

    # Common namespace structure
    namespaces = ["finance", "marketing", "product", "engineering", "analytics"]

    created = []
    for namespace in namespaces:
        await rbac_service.assign_role(
            principal_id=admin_user.id,
            role_name="owner",
            scope_type="namespace",
            scope_value=namespace,
            granted_by_id=admin_user.id,
        )
        created.append(namespace)

    return {
        "created_namespaces": created,
        "admin_user": admin_user.username,
    }


# Decorator for API endpoints that require specific permissions
def require_permission(action: str, resource_type: str = "node"):
    """
    Decorator to require specific permission for API endpoints

    Usage:
        @require_permission("WRITE", "node")
        async def update_node(node_name: str, ...):
            # This endpoint requires WRITE permission on the node
    """

    def decorator(func):
        # This would be implemented to check permissions before calling the function
        # For now, just return the original function
        return func

    return decorator


# Simple API endpoint helpers
async def assign_role_by_names(
    principal_name: str,
    role_name: str,
    scope_type: str,
    scope_value: Optional[str],
    granted_by: User,
    session: AsyncSession,
) -> bool:
    """
    Assign role using names instead of IDs

    Args:
        principal_name: Username or group name
        role_name: Name of role to assign
        scope_type: Scope type (global, namespace, node)
        scope_value: Scope value
        granted_by: User granting the role
        session: Database session

    Returns:
        True if successful, False if principal not found
    """
    principal = await User.get_by_username(session, principal_name)
    if not principal:
        return False

    rbac_service = RBACService(session)
    await rbac_service.assign_role(
        principal_id=principal.id,
        role_name=role_name,
        scope_type=scope_type,
        scope_value=scope_value,
        granted_by_id=granted_by.id,
    )

    return True
