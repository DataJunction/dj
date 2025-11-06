"""
Core RBAC (Role-Based Access Control) implementation for DataJunction
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.rbac import PrincipalMembership, Role, RoleAssignment
from datajunction_server.database.user import PrincipalKind, User
from datajunction_server.models.access import (
    AccessControl,
    Resource,
    ResourceRequestVerb,
    ResourceType,
)
from datajunction_server.utils import get_namespace_from_name, is_namespace_under


class GroupResolver(ABC):
    """
    Abstract interface for resolving group memberships
    Can be implemented for external systems (Gandalf, LDAP) or local groups
    """

    @abstractmethod
    async def get_user_groups(self, session: AsyncSession, user_id: int) -> List[int]:
        """Get list of group IDs that user belongs to"""
        pass


class LocalGroupResolver(GroupResolver):
    """Default group resolver using local principal_memberships table"""

    async def get_user_groups(self, session: AsyncSession, user_id: int) -> List[int]:
        """Get groups from local principal_memberships table"""
        return await PrincipalMembership.get_user_groups(session, user_id)


class RBACValidator:
    """
    Core RBAC validator that checks permissions based on role assignments
    """

    def __init__(
        self,
        session: AsyncSession,
        group_resolver: Optional[GroupResolver] = None,
    ):
        self.session = session
        self.group_resolver = group_resolver or LocalGroupResolver()

    async def __call__(self, access_control: AccessControl) -> None:
        """
        Main entry point for RBAC validation
        Called by the existing access control framework
        """
        if not access_control.user:
            # No user, deny all requests
            access_control.deny_all()
            return

        user = await User.get_by_username(self.session, access_control.user)
        if not user:
            access_control.deny_all()
            return

        # Check each request
        for request in access_control.requests:
            has_permission = await self.check_permission(
                user,
                request.verb,
                request.access_object,
            )

            if has_permission:
                request.approve()
            else:
                request.deny()

    async def check_permission(
        self,
        user: User,
        verb: ResourceRequestVerb,
        resource: Resource,
    ) -> bool:
        """
        Check if user has permission to perform verb on resource

        Args:
            user: User requesting access
            verb: ResourceRequestVerb to perform (READ, WRITE, DELETE, EXECUTE, etc.)
            resource: Resource being accessed

        Returns:
            True if permission granted, False otherwise
        """
        # Get all effective role assignments for user
        effective_assignments = await self._get_effective_assignments(user.id)

        # Check if any assignment grants the permission
        for assignment in effective_assignments:
            if await self._assignment_grants_permission(assignment, verb, resource):
                return True

        return False

    async def _get_effective_assignments(self, user_id: int) -> List[RoleAssignment]:
        """
        Get all role assignments that apply to this user
        Includes direct assignments and assignments through group membership
        """
        # Direct assignments to user
        direct_assignments = await RoleAssignment.get_assignments_for_principal(
            self.session,
            user_id,
        )

        # Group-based assignments
        user_groups = await self.group_resolver.get_user_groups(self.session, user_id)
        group_assignments = []

        for group_id in user_groups:
            group_assignments.extend(
                await RoleAssignment.get_assignments_for_principal(
                    self.session,
                    group_id,
                ),
            )

        return direct_assignments + group_assignments

    async def _assignment_grants_permission(
        self,
        assignment: RoleAssignment,
        verb: ResourceRequestVerb,
        resource: Resource,
    ) -> bool:
        """Check if a specific role assignment grants the requested permission"""

        # Get role definition
        role = await Role.get_by_name(self.session, assignment.role_name)
        if not role:
            return False

        # Check if role has this verb permission
        if verb.value not in role.permissions:
            return False

        # Check if assignment scope covers the resource
        return self._scope_covers_resource(
            assignment.scope_type,
            assignment.scope_value,
            resource,
        )

    def _scope_covers_resource(
        self,
        scope_type: ResourceType,
        scope_value: Optional[str],
        resource: Resource,
    ) -> bool:
        """Check if assignment scope covers the requested resource"""

        if scope_type == ResourceType.NAMESPACE:
            # Handle global namespace access (empty scope_value = all namespaces)
            if not scope_value:
                return True
            if resource.resource_type == ResourceType.NAMESPACE:
                # Namespace resource: check if it's under the scope namespace
                return is_namespace_under(resource.name, scope_value)
            elif resource.resource_type == ResourceType.NODE:
                # Node resource: check if node's namespace is under scope namespace
                node_namespace = get_namespace_from_name(resource.name)
                return is_namespace_under(node_namespace, scope_value)
        elif scope_type == ResourceType.NODE:
            # Node-specific assignment only covers exact node match
            return (
                resource.resource_type == ResourceType.NODE
                and resource.name == scope_value
            )
        return False


class RBACService:
    """
    Service class for RBAC operations like assigning roles, managing groups, etc.
    """

    def __init__(
        self,
        session: AsyncSession,
        group_resolver: Optional[GroupResolver] = None,
    ):
        self.session = session
        self.group_resolver = group_resolver or LocalGroupResolver()

    async def assign_role(
        self,
        principal_id: int,
        role_name: str,
        scope_type: ResourceType,
        scope_value: Optional[str] = None,
        granted_by_id: int = None,
    ) -> RoleAssignment:
        """Assign a role to a principal"""

        # Validate role exists
        role = await Role.get_by_name(self.session, role_name)
        if not role:
            raise ValueError(f"Role '{role_name}' does not exist")

        # Create assignment
        assignment = RoleAssignment(
            principal_id=principal_id,
            role_name=role_name,
            scope_type=scope_type,
            scope_value=scope_value,
            granted_by_id=granted_by_id,
        )

        self.session.add(assignment)
        await self.session.commit()
        return assignment

    async def assign_namespace_owners(
        self,
        namespace: str,
        principal_ids: List[int],
        granted_by_id: int,
    ) -> List[RoleAssignment]:
        """Convenient method to assign ownership of a namespace to multiple principals"""

        assignments = []
        for principal_id in principal_ids:
            assignment = await self.assign_role(
                principal_id=principal_id,
                role_name=f"owner-{namespace}",
                scope_type=ResourceType.NAMESPACE,
                scope_value=namespace,
                granted_by_id=granted_by_id,
            )
            assignments.append(assignment)

        return assignments

    async def create_group(
        self,
        name: str,
        display_name: str,
        created_by_id: int,
    ) -> User:
        """Create a new group principal"""

        group = User(
            username=name,
            name=display_name,
            kind=PrincipalKind.GROUP,
            created_by_id=created_by_id,
        )

        self.session.add(group)
        await self.session.commit()
        return group

    async def add_to_group(
        self,
        member_id: int,
        group_id: int,
        added_by_id: int,
    ) -> PrincipalMembership:
        """Add a principal to a group"""

        membership = PrincipalMembership(
            member_id=member_id,
            group_id=group_id,
            added_by_id=added_by_id,
        )

        self.session.add(membership)
        await self.session.commit()
        return membership

    async def get_resource_owners(
        self,
        resource_type: str,
        resource_name: str,
    ) -> List[User]:
        """Get all principals who have owner role on a resource"""

        assignments = await RoleAssignment.get_assignments_for_resource(
            self.session,
            resource_type,
            resource_name,
        )

        owners = []
        for assignment in assignments:
            if assignment.role_name == "owner":
                owners.append(assignment.principal)

        return owners

    async def auto_assign_ownership_on_create(
        self,
        resource_type: ResourceType,
        resource_name: str,
        creator: User,
    ) -> RoleAssignment:
        """
        Automatically assign ownership when a resource is created

        Logic:
        1. If creator is in groups, assign ownership to primary group
        2. Otherwise, assign ownership to creator
        3. Scope can be namespace or node level based on resource
        """

        # Get creator's groups
        user_groups = await self.group_resolver.get_user_groups(
            self.session,
            creator.id,
        )

        if user_groups:
            # Assign to primary group (first group)
            owner_id = user_groups[0]
        else:
            # Assign to creator
            owner_id = creator.id

        # Create ownership assignment
        return await self.assign_role(
            principal_id=owner_id,
            role_name=f"owner-{resource_name}",
            scope_type=resource_type,
            scope_value=resource_name,
            granted_by_id=creator.id,
        )

    async def setup_team_namespace_ownership(
        self,
        team_group: User,  # Group principal
        namespace: str,
        granted_by: User,
    ) -> RoleAssignment:
        """
        Set up a team to own an entire namespace

        Example: growth-dse team owns growth.* namespace
        """
        return await self.assign_role(
            principal_id=team_group.id,
            role_name=f"owner-{namespace}",
            scope_type=ResourceType.NAMESPACE,
            scope_value=namespace,
            granted_by_id=granted_by.id,
        )

    async def migrate_existing_ownership(self) -> None:
        """
        Migrate existing node ownership to RBAC system
        This should be run once when enabling RBAC
        """
        from sqlalchemy import select
        from datajunction_server.database.node import Node
        from datajunction_server.database.nodeowner import NodeOwner

        # Migrate node creators to owners
        nodes_with_creators = await self.session.execute(select(Node))

        for node in nodes_with_creators.scalars():
            try:
                await self.assign_role(
                    principal_id=node.created_by_id,
                    role_name=f"owner-{node.name}",
                    scope_type=ResourceType.NODE,
                    scope_value=node.name,
                    granted_by_id=node.created_by_id,  # Self-granted
                )
            except Exception:
                # Skip if assignment already exists
                pass

        # Migrate existing node_owners table if it exists
        try:
            existing_owners = await self.session.execute(select(NodeOwner))

            for owner_rel in existing_owners.scalars():
                try:
                    await self.assign_role(
                        principal_id=owner_rel.user_id,
                        role_name=f"owner-{owner_rel.node.name}",
                        scope_type=ResourceType.NODE,
                        scope_value=owner_rel.node.name,
                        granted_by_id=owner_rel.user_id,  # Self-granted
                    )
                except Exception:
                    # Skip if assignment already exists
                    pass
        except Exception:
            # NodeOwner table might not exist, skip
            pass


# Helper function to create RBAC validator for dependency injection
def get_rbac_validator(session: AsyncSession) -> RBACValidator:
    """Factory function for RBAC validator"""
    return RBACValidator(session)
