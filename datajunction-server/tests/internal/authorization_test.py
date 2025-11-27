"""Tests for RBAC authorization logic."""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.group_member import GroupMember
from datajunction_server.database.rbac import Role, RoleAssignment, RoleScope
from datajunction_server.database.user import PrincipalKind, User
from datajunction_server.internal.access.authorization import (
    AccessDenialMode,
    AuthContext,
    PassthroughAuthorizationService,
    RBACAuthorizationService,
    authorize,
    get_authorization_service,
)
from datajunction_server.errors import DJAuthorizationException
from datajunction_server.internal.access.authentication.basic import get_user
from datajunction_server.models.access import (
    Resource,
    ResourceAction,
    ResourceRequest,
    ResourceType,
)
from datajunction_server.internal.access.group_membership import (
    GroupMembershipService,
)


class TestResourceMatching:
    """Tests for wildcard pattern matching."""

    def test_exact_match(self):
        """Test exact string matching (no wildcards)."""
        assert RBACAuthorizationService.resource_matches_pattern(
            "finance.revenue",
            "finance.revenue",
        )
        assert not RBACAuthorizationService.resource_matches_pattern(
            "finance.revenue",
            "finance.cost",
        )

    def test_wildcard_all(self):
        """Test the universal wildcard *."""
        assert RBACAuthorizationService.resource_matches_pattern("anything", "*")
        assert RBACAuthorizationService.resource_matches_pattern(
            "finance.revenue.quarterly",
            "*",
        )
        assert RBACAuthorizationService.resource_matches_pattern("", "*")

    def test_namespace_wildcard(self):
        """Test namespace wildcard patterns."""
        # finance.* matches finance.revenue
        assert RBACAuthorizationService.resource_matches_pattern(
            "finance.revenue",
            "finance.*",
        )

        # finance.* matches finance.quarterly.revenue
        assert RBACAuthorizationService.resource_matches_pattern(
            "finance.quarterly.revenue",
            "finance.*",
        )

        # finance.* does NOT match finance (exact namespace)
        assert not RBACAuthorizationService.resource_matches_pattern(
            "finance",
            "finance.*",
        )

        # finance.* does NOT match marketing.revenue
        assert not RBACAuthorizationService.resource_matches_pattern(
            "marketing.revenue",
            "finance.*",
        )

    def test_nested_namespace_wildcard(self):
        """Test nested namespace patterns."""
        # users.alice.* matches users.alice.dashboard
        assert RBACAuthorizationService.resource_matches_pattern(
            "users.alice.dashboard",
            "users.alice.*",
        )

        # users.alice.* matches users.alice.metrics.revenue
        assert RBACAuthorizationService.resource_matches_pattern(
            "users.alice.metrics.revenue",
            "users.alice.*",
        )

        # users.alice.* does NOT match users.bob.dashboard
        assert not RBACAuthorizationService.resource_matches_pattern(
            "users.bob.dashboard",
            "users.alice.*",
        )


@pytest.mark.asyncio
class TestRBACPermissionChecks:
    """Tests for RBAC permission checking."""

    async def test_no_roles_returns_false(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """Test that user with no roles gets False (no explicit rule)."""
        user = await get_user(username=default_user.username, session=session)
        result = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )

        assert result is False

    async def test_explicit_grant_exact_match(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """Test explicit permission grant with exact resource match."""
        # Create role with exact scope
        role = Role(
            name="test-role",
            created_by_id=default_user.id,
        )
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.revenue",
        )
        session.add(scope)

        # Assign role to user
        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Check permission
        user = await get_user(username=default_user.username, session=session)
        result = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert result is True

    async def test_explicit_grant_wildcard_match(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """Test permission grant via wildcard pattern."""
        # Create role with wildcard scope
        role = Role(
            name="finance-reader",
            created_by_id=default_user.id,
        )
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",  # Wildcard
        )
        session.add(scope)

        # Assign role
        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Check permissions on various resources
        user = await get_user(username=default_user.username, session=session)
        result1 = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert result1 is True

        result2 = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.quarterly.revenue",
        )
        assert result2 is True

        # Different namespace - no match
        result3 = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="marketing.revenue",
        )
        assert result3 is False

    async def test_wrong_action_no_match(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """Test that wrong action doesn't grant permission."""
        role = Role(name="reader-role", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        # Only READ permission
        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Check READ - should be granted
        user = await get_user(username=default_user.username, session=session)
        result_read = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert result_read is True

        # Check WRITE - should be None (no explicit rule)
        result_write = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.WRITE,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert result_write is False

    async def test_expired_assignment_ignored(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """Test that expired role assignments are ignored."""
        role = Role(name="temp-role", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        # Assignment expired 1 hour ago
        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
            expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        session.add(assignment)
        await session.commit()

        # Should not grant permission (expired)
        user = await get_user(username=default_user.username, session=session)
        result = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert not result

    async def test_multiple_roles_any_grants(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """Test that having ANY role that grants permission is sufficient."""
        # Role 1: No matching scope
        role1 = Role(name="marketing-role", created_by_id=default_user.id)
        session.add(role1)
        await session.flush()

        scope1 = RoleScope(
            role_id=role1.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="marketing.*",
        )
        session.add(scope1)

        assignment1 = RoleAssignment(
            principal_id=default_user.id,
            role_id=role1.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment1)

        # Role 2: Matching scope
        role2 = Role(name="finance-role", created_by_id=default_user.id)
        session.add(role2)
        await session.flush()

        scope2 = RoleScope(
            role_id=role2.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope2)

        assignment2 = RoleAssignment(
            principal_id=default_user.id,
            role_id=role2.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment2)
        await session.commit()

        # Should grant because role2 matches
        user = await get_user(username=default_user.username, session=session)
        result = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert result is True

    async def test_universal_wildcard(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """Test that * wildcard grants access to everything."""
        role = Role(name="super-admin", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        # Universal wildcard
        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Should grant for anything
        user = await get_user(username=default_user.username, session=session)
        result1 = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert result1 is True

        result2 = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="anything.at.all",
        )
        assert result2 is True


@pytest.mark.asyncio
class TestAuthorizationService:
    """Tests for the synchronous AuthorizationService."""

    async def test_passthrough_service_approves_all(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """Test that PassthroughAuthorizationService approves everything."""
        # Get existing user
        user = await get_user(username=default_user.username, session=session)

        service = PassthroughAuthorizationService()

        requests = [
            ResourceRequest(
                verb=ResourceAction.WRITE,
                access_object=Resource(
                    name="finance.revenue",
                    resource_type=ResourceType.NAMESPACE,
                    owner="",
                ),
            ),
            ResourceRequest(
                verb=ResourceAction.DELETE,
                access_object=Resource(
                    name="secret.data",
                    resource_type=ResourceType.NODE,
                    owner="",
                ),
            ),
        ]

        result = service.authorize(user, requests)  # Now sync!

        assert len(result) == 2
        assert all(req.approved for req in result)

    async def test_rbac_service_with_permissions(
        self,
        session: AsyncSession,
        default_user: User,
        mocker,
    ):
        """Test RBACAuthorizationService with granted permissions."""
        mock_settings = mocker.patch(
            "datajunction_server.internal.access.authorization.settings",
        )
        mock_settings.authorization_provider = "rbac"
        mock_settings.default_access_policy = "restrictive"

        role = Role(name="test-role", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        user = await get_user(username=default_user.username, session=session)
        requests = [
            ResourceRequest(
                verb=ResourceAction.READ,
                access_object=Resource(
                    name="finance.revenue",
                    resource_type=ResourceType.NAMESPACE,
                    owner="",
                ),
            ),
            ResourceRequest(
                verb=ResourceAction.WRITE,  # Not granted
                access_object=Resource(
                    name="finance.revenue",
                    resource_type=ResourceType.NAMESPACE,
                    owner="",
                ),
            ),
        ]

        result = await authorize(
            session,
            user,
            requests,
            on_denied=AccessDenialMode.RETURN,
        )
        assert len(result) == 2
        assert result[0].approved is True  # READ granted
        assert result[1].approved is False  # WRITE not granted

    async def test_get_authorization_service_factory(self, mocker):
        """Test the factory function returns correct service."""
        mock_settings = mocker.patch(
            "datajunction_server.internal.access.authorization.settings",
        )
        mock_settings.authorization_provider = "rbac"
        mock_settings.default_access_policy = "restrictive"

        service = get_authorization_service()
        assert isinstance(service, RBACAuthorizationService)

        # Test passthrough provider
        mock_settings.authorization_provider = "passthrough"
        service = get_authorization_service()

        # Cached instance, so need to clear cache
        assert isinstance(service, RBACAuthorizationService)

        # Clear LRU cache to test different provider
        get_authorization_service.cache_clear()
        service = get_authorization_service()
        assert isinstance(service, PassthroughAuthorizationService)

        # Test unknown provider
        mock_settings.authorization_provider = "unknown"
        get_authorization_service.cache_clear()
        with pytest.raises(ValueError) as exc_info:
            get_authorization_service()
        assert "unknown" in str(exc_info.value).lower()
        assert "rbac" in str(exc_info.value).lower()
        assert "passthrough" in str(exc_info.value).lower()


@pytest.mark.asyncio
class TestGroupBasedPermissions:
    """Tests for group-based role assignments."""

    async def test_user_inherits_group_permissions(
        self,
        session: AsyncSession,
        default_user: User,
        mocker,
    ):
        """Test that users inherit permissions from groups they belong to."""
        # Create a group
        group = User(
            username="finance-team",
            kind=PrincipalKind.GROUP,
            oauth_provider="basic",
        )
        session.add(group)
        await session.flush()

        # Add user to group
        membership = GroupMember(
            group_id=group.id,
            member_id=default_user.id,
        )
        session.add(membership)
        await session.flush()

        # Create role and assign to group
        role = Role(name="finance-reader", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        # Assign role to group
        assignment = RoleAssignment(
            principal_id=group.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Expire the user object so we get a fresh load
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        mock_settings = mocker.patch(
            "datajunction_server.internal.access.authorization.settings",
        )
        mock_settings.authorization_provider = "rbac"
        mock_settings.default_access_policy = "restrictive"

        # Check permission - should be granted via group
        results = await authorize(
            session=session,
            user=user,
            resource_requests=[
                ResourceRequest(
                    verb=ResourceAction.READ,
                    access_object=Resource(
                        name="finance.revenue.something",
                        resource_type=ResourceType.NODE,
                        owner="",
                    ),
                ),
                ResourceRequest(
                    verb=ResourceAction.READ,
                    access_object=Resource(
                        resource_type=ResourceType.NAMESPACE,
                        name="finance.revenue",
                        owner="",
                    ),
                ),
            ],
        )
        assert results[0].approved is True
        assert results[1].approved is True

    async def test_user_no_permission_without_group(
        self,
        session: AsyncSession,
        default_user: User,
        mocker,
    ):
        """Test that user without group membership doesn't get permission."""
        # Create a group with permissions
        group = User(
            username="marketing-team",
            kind=PrincipalKind.GROUP,
            oauth_provider="basic",
        )
        session.add(group)
        await session.flush()

        # Create role and assign to GROUP
        role = Role(name="marketing-reader", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="marketing.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=group.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user without adding them to the group
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        mock_settings = mocker.patch(
            "datajunction_server.internal.access.authorization.settings",
        )
        mock_settings.authorization_provider = "rbac"
        mock_settings.default_access_policy = "restrictive"

        # Check permission - should NOT be granted (user not in group)
        results = await authorize(
            session=session,
            user=user,
            resource_requests=[
                ResourceRequest(
                    verb=ResourceAction.READ,
                    access_object=Resource(
                        name="marketing.revenue",
                        resource_type=ResourceType.NAMESPACE,
                        owner="",
                    ),
                ),
            ],
            on_denied=AccessDenialMode.RETURN,
        )
        assert results[0].approved is False


@pytest.mark.asyncio
class TestCrossResourceTypePermissions:
    """Tests for namespace scopes covering nodes."""

    async def test_namespace_scope_covers_nodes(
        self,
        # client_with_basic: AsyncClient,
        session: AsyncSession,
        default_user: User,
    ):
        """Test that namespace scope grants permission for nodes in that namespace."""
        # Create role with NAMESPACE scope
        role = Role(name="finance-ns-reader", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,  # Namespace scope
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with roles
        await session.refresh(default_user)

        # Reload user with member_of and group role_assignments eagerly loaded
        user = await get_user(username=default_user.username, session=session)

        # Check permission for NAMESPACE resource - should be granted
        result_namespace = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert result_namespace is True

        # Check permission for NODE resource in that namespace - should ALSO be granted!
        result_node = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NODE,
            resource_name="finance.revenue.total",  # Node in finance namespace
        )
        assert result_node is True

        # Node in different namespace - should NOT be granted
        result_other = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NODE,
            resource_name="marketing.revenue.total",
        )
        assert result_other is False

    async def test_namespace_scope_nested_namespaces(
        self,
        # client_with_basic: AsyncClient,
        session: AsyncSession,
        default_user: User,
    ):
        """Test namespace scope with nested namespaces."""
        # Create role with wildcard namespace scope
        role = Role(name="finance-all", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",  # finance.*
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with roles
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        # finance.quarterly.revenue node should match finance.* namespace
        result = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NODE,
            resource_name="finance.quarterly.revenue",
        )
        assert result is True

    async def test_node_scope_does_not_cover_namespace(
        self,
        # client_with_basic: AsyncClient,
        session: AsyncSession,
        default_user: User,
    ):
        """Test that NODE scope does NOT grant permission for NAMESPACE resources."""
        # Create role with NODE scope
        role = Role(name="specific-node-reader", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NODE,  # NODE scope
            scope_value="finance.revenue",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with roles
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        # Check permission for NODE - should be granted
        result_node = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NODE,
            resource_name="finance.revenue",
        )
        assert result_node is True

        # Check permission for NAMESPACE - should NOT be granted (cross-type only works one way)
        result_namespace = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert result_namespace is False


@pytest.mark.asyncio
class TestGlobalAccessScope:
    """Tests for global access (empty or * scope_value)."""

    async def test_empty_scope_grants_global_access(
        self,
        # client_with_basic: AsyncClient,
        session: AsyncSession,
        default_user: User,
    ):
        """Test that empty scope_value grants access to all resources of that type."""
        # Create role with empty scope_value (global)
        role = Role(name="global-reader", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="",  # Global! (empty string)
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with roles
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        # Should grant for any namespace
        result1 = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="finance.revenue",
        )
        assert result1 is True

        result2 = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NAMESPACE,
            resource_name="marketing.anything",
        )
        assert result2 is True

        # Should NOT grant for different resource type
        result3 = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NODE,  # Different type
            resource_name="finance.revenue",
        )
        assert result3 is False

    async def test_star_scope_grants_global_access(
        self,
        # client_with_basic: AsyncClient,
        session: AsyncSession,
        default_user: User,
    ):
        """Test that "*" scope_value grants access to all resources of that type."""
        # Create role with "*" scope_value
        role = Role(name="star-reader", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NODE,
            scope_value="*",  # Wildcard for all
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with roles
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        # Should grant for any node
        result = RBACAuthorizationService.has_permission(
            assignments=user.role_assignments,
            action=ResourceAction.READ,
            resource_type=ResourceType.NODE,
            resource_name="anything.anywhere.node",
        )
        assert result is True


@pytest.mark.asyncio
class TestPermissionHierarchy:
    """Tests for permission hierarchy (MANAGE > DELETE > WRITE > READ)."""

    async def test_manage_implies_all_permissions(
        self,
        # client_with_basic: AsyncClient,
        session: AsyncSession,
        default_user: User,
    ):
        """Test that MANAGE permission grants all other permissions."""
        # Create role with MANAGE permission
        role = Role(name="finance-manager", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.MANAGE,  # Top-level permission
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with roles
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        # MANAGE should grant READ
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.READ,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is True
        )

        # MANAGE should grant WRITE
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.WRITE,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is True
        )

        # MANAGE should grant DELETE
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.DELETE,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is True
        )

        # MANAGE should grant EXECUTE
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.EXECUTE,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is True
        )

        # MANAGE should grant MANAGE
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.MANAGE,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is True
        )

    async def test_write_implies_read(
        self,
        # client_with_basic: AsyncClient,
        session: AsyncSession,
        default_user: User,
    ):
        """Test that WRITE permission implies READ."""
        # Create role with WRITE permission
        role = Role(name="finance-writer", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.WRITE,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with roles
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        # WRITE should grant READ
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.READ,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is True
        )

        # WRITE should grant WRITE
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.WRITE,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is True
        )

        # WRITE should NOT grant DELETE
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.DELETE,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is False
        )

    async def test_read_does_not_imply_write(
        self,
        # client_with_basic: AsyncClient,
        session: AsyncSession,
        default_user: User,
    ):
        """Test that READ permission does NOT imply WRITE."""
        # Create role with only READ permission
        role = Role(name="readonly-role", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with roles
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        # READ should grant READ
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.READ,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is True
        )

        # READ should NOT grant WRITE
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.WRITE,
                resource_type=ResourceType.NAMESPACE,
                resource_name="finance.revenue",
            )
            is False
        )

    async def test_execute_implies_read(
        self,
        # client_with_basic: AsyncClient,
        session: AsyncSession,
        default_user: User,
    ):
        """Test that EXECUTE permission implies READ."""
        # Create role with EXECUTE permission
        role = Role(name="query-executor", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.EXECUTE,
            scope_type=ResourceType.NODE,
            scope_value="finance.revenue",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with roles
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        # EXECUTE should grant READ
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.READ,
                resource_type=ResourceType.NODE,
                resource_name="finance.revenue",
            )
            is True
        )

        # EXECUTE should grant EXECUTE
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.EXECUTE,
                resource_type=ResourceType.NODE,
                resource_name="finance.revenue",
            )
            is True
        )

        # EXECUTE should NOT grant WRITE
        assert (
            RBACAuthorizationService.has_permission(
                assignments=user.role_assignments,
                action=ResourceAction.WRITE,
                resource_type=ResourceType.NODE,
                resource_name="finance.revenue",
            )
            is False
        )


@pytest.mark.asyncio
class TestAuthContext:
    """Tests for AuthContext and effective assignments."""

    async def test_auth_context_from_user_direct_assignments_only(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """AuthContext includes user's direct role assignments."""
        # Create role and assign to user
        role = Role(name="test-role", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user with assignments
        user = await get_user(username=default_user.username, session=session)

        # Build AuthContext
        auth_context = await AuthContext.from_user(session, user)

        assert auth_context.user_id == user.id
        assert auth_context.username == user.username
        assert len(auth_context.role_assignments) == 1
        assert auth_context.role_assignments[0].role.name == "test-role"

    async def test_auth_context_includes_group_assignments(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """AuthContext flattens user's + groups' assignments."""
        # Create a group
        group = User(
            username="finance-team",
            kind=PrincipalKind.GROUP,
            oauth_provider="basic",
        )
        session.add(group)
        await session.flush()

        # Add user to group
        membership = GroupMember(
            group_id=group.id,
            member_id=default_user.id,
        )
        session.add(membership)

        # Create role for user (direct)
        user_role = Role(name="user-role", created_by_id=default_user.id)
        session.add(user_role)
        await session.flush()

        user_scope = RoleScope(
            role_id=user_role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="personal.*",
        )
        session.add(user_scope)

        user_assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=user_role.id,
            granted_by_id=default_user.id,
        )
        session.add(user_assignment)

        # Create role for group
        group_role = Role(name="group-role", created_by_id=default_user.id)
        session.add(group_role)
        await session.flush()

        group_scope = RoleScope(
            role_id=group_role.id,
            action=ResourceAction.WRITE,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(group_scope)

        group_assignment = RoleAssignment(
            principal_id=group.id,
            role_id=group_role.id,
            granted_by_id=default_user.id,
        )
        session.add(group_assignment)
        await session.commit()

        # Reload user
        user = await get_user(username=default_user.username, session=session)

        # Build AuthContext (should include both)
        auth_context = await AuthContext.from_user(session, user)

        assert auth_context.user_id == user.id
        assert len(auth_context.role_assignments) == 2  # User's + group's

        role_names = {a.role.name for a in auth_context.role_assignments}
        assert role_names == {"user-role", "group-role"}

    async def test_auth_context_with_multiple_groups(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """User in multiple groups gets all group assignments."""
        # Create two groups
        group1 = User(
            username="finance-team",
            kind=PrincipalKind.GROUP,
            oauth_provider="basic",
        )
        group2 = User(
            username="data-eng-team",
            kind=PrincipalKind.GROUP,
            oauth_provider="basic",
        )
        session.add_all([group1, group2])
        await session.flush()

        # Add user to both groups
        membership1 = GroupMember(group_id=group1.id, member_id=default_user.id)
        membership2 = GroupMember(group_id=group2.id, member_id=default_user.id)
        session.add_all([membership1, membership2])

        # Give each group a role
        role1 = Role(name="finance-role", created_by_id=default_user.id)
        role2 = Role(name="data-eng-role", created_by_id=default_user.id)
        session.add_all([role1, role2])
        await session.flush()

        scope1 = RoleScope(
            role_id=role1.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        scope2 = RoleScope(
            role_id=role2.id,
            action=ResourceAction.WRITE,
            scope_type=ResourceType.NAMESPACE,
            scope_value="analytics.*",
        )
        session.add_all([scope1, scope2])

        assignment1 = RoleAssignment(
            principal_id=group1.id,
            role_id=role1.id,
            granted_by_id=default_user.id,
        )
        assignment2 = RoleAssignment(
            principal_id=group2.id,
            role_id=role2.id,
            granted_by_id=default_user.id,
        )
        session.add_all([assignment1, assignment2])
        await session.commit()

        # Reload user
        user = await get_user(username=default_user.username, session=session)

        # Build AuthContext
        auth_context = await AuthContext.from_user(session, user)

        # Should have assignments from both groups
        assert len(auth_context.role_assignments) == 2
        role_names = {a.role.name for a in auth_context.role_assignments}
        assert role_names == {"finance-role", "data-eng-role"}


@pytest.mark.asyncio
class TestCheckAccess:
    """Tests for authorize() function with different denial modes."""

    async def test_check_access_filter_mode_returns_only_approved(
        self,
        default_user: User,
        session: AsyncSession,
        mocker,
    ):
        """FILTER mode returns only approved requests (default)."""
        # Give user access to finance.* but not marketing.*
        role = Role(name="finance-reader", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        # Reload user
        await session.refresh(default_user)
        user = await get_user(username=default_user.username, session=session)

        # Request access to 3 nodes: 2 accessible, 1 not
        requests = [
            ResourceRequest(
                verb=ResourceAction.READ,
                access_object=Resource(
                    name="finance.revenue",
                    resource_type=ResourceType.NODE,
                    owner="",
                ),
            ),
            ResourceRequest(
                verb=ResourceAction.READ,
                access_object=Resource(
                    name="finance.cost",
                    resource_type=ResourceType.NODE,
                    owner="",
                ),
            ),
            ResourceRequest(
                verb=ResourceAction.READ,
                access_object=Resource(
                    name="marketing.revenue",
                    resource_type=ResourceType.NODE,
                    owner="",
                ),
            ),
        ]

        mock_settings = mocker.patch(
            "datajunction_server.internal.access.authorization.settings",
        )
        mock_settings.authorization_provider = "rbac"
        mock_settings.default_access_policy = "restrictive"

        # Check access (default FILTER mode)
        approved = await authorize(
            session,
            user,
            requests,
            on_denied=AccessDenialMode.FILTER,
        )

        # Should only return the 2 approved (finance.* nodes)
        assert len(approved) == 2
        assert all(req.approved for req in approved)
        approved_names = {req.access_object.name for req in approved}
        assert approved_names == {"finance.revenue", "finance.cost"}

    async def test_check_access_raise_mode_throws_on_denial(
        self,
        default_user: User,
        session: AsyncSession,
        mocker,
    ):
        """
        Raise mode throws DJAuthorizationException when access denied
        for a user with no permissions.
        """
        user = await get_user(username=default_user.username, session=session)

        request = ResourceRequest(
            verb=ResourceAction.WRITE,
            access_object=Resource(
                name="finance.revenue",
                resource_type=ResourceType.NODE,
                owner="",
            ),
        )

        mock_settings = mocker.patch(
            "datajunction_server.internal.access.authorization.settings",
        )
        mock_settings.authorization_provider = "rbac"
        mock_settings.default_access_policy = "restrictive"

        with pytest.raises(DJAuthorizationException) as exc_info:
            await authorize(
                session,
                user,
                [request],
                on_denied=AccessDenialMode.RAISE,
            )

        # Check exception message
        assert "Access denied" in str(exc_info.value)
        assert "WRITE" in str(exc_info.value)
        assert "finance.revenue" in str(exc_info.value)

    async def test_check_access_raise_mode_succeeds_when_approved(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """RAISE mode succeeds without exception when all approved."""
        # Give user access
        role = Role(name="finance-writer", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.WRITE,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        user = await get_user(username=default_user.username, session=session)

        request = ResourceRequest(
            verb=ResourceAction.WRITE,
            access_object=Resource(
                name="finance.revenue",
                resource_type=ResourceType.NODE,
                owner="",
            ),
        )

        # Should NOT raise
        result = await authorize(
            session,
            user,
            [request],
            on_denied=AccessDenialMode.RAISE,
        )

        assert len(result) == 1
        assert result[0].approved is True

    async def test_check_access_return_mode(
        self,
        default_user: User,
        session: AsyncSession,
        mocker,
    ):
        """RETURN_ALL mode returns all requests with approved field set."""
        # Give user access to finance.* only
        role = Role(name="finance-reader", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        user = await get_user(username=default_user.username, session=session)

        # Request access to 3 nodes: 2 accessible, 1 not
        requests = [
            ResourceRequest(
                verb=ResourceAction.READ,
                access_object=Resource(
                    name="finance.revenue",
                    resource_type=ResourceType.NODE,
                    owner="",
                ),
            ),
            ResourceRequest(
                verb=ResourceAction.READ,
                access_object=Resource(
                    name="finance.cost",
                    resource_type=ResourceType.NODE,
                    owner="",
                ),
            ),
            ResourceRequest(
                verb=ResourceAction.READ,
                access_object=Resource(
                    name="marketing.revenue",
                    resource_type=ResourceType.NODE,
                    owner="",
                ),
            ),
        ]

        mock_settings = mocker.patch(
            "datajunction_server.internal.access.authorization.settings",
        )
        mock_settings.authorization_provider = "rbac"
        mock_settings.default_access_policy = "restrictive"

        # Check access with RETURN_ALL
        all_requests = await authorize(
            session,
            user,
            requests,
            on_denied=AccessDenialMode.RETURN,
        )

        # Should return all 3 requests
        assert len(all_requests) == 3

        # 2 approved, 1 denied
        approved = [r for r in all_requests if r.approved]
        denied = [r for r in all_requests if not r.approved]

        assert len(approved) == 2
        assert len(denied) == 1
        assert denied[0].access_object.name == "marketing.revenue"


@pytest.mark.asyncio
class TestGetEffectiveAssignments:
    """Tests for get_effective_assignments() with GroupMembershipService."""

    async def test_effective_assignments_user_only(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """User with no groups gets only direct assignments."""
        # Give user a direct assignment
        role = Role(name="personal-role", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="personal.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        user = await get_user(username=default_user.username, session=session)

        # Get effective assignments
        assignments = await AuthContext.get_effective_assignments(session, user)

        assert len(assignments) == 1
        assert assignments[0].role.name == "personal-role"

    async def test_effective_assignments_with_postgres_groups(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """Effective assignments includes groups from PostgresGroupMembershipService."""
        # Create group
        group = User(
            username="test-group",
            kind=PrincipalKind.GROUP,
            oauth_provider="basic",
        )
        session.add(group)
        await session.flush()

        # Add user to group via GroupMember table
        membership = GroupMember(
            group_id=group.id,
            member_id=default_user.id,
        )
        session.add(membership)

        # Create role and assign to GROUP
        role = Role(name="group-role", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.WRITE,
            scope_type=ResourceType.NAMESPACE,
            scope_value="shared.*",
        )
        session.add(scope)

        group_assignment = RoleAssignment(
            principal_id=group.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(group_assignment)
        await session.commit()

        user = await get_user(username=default_user.username, session=session)

        # Get effective assignments (should use PostgresGroupMembershipService by default)
        assignments = await AuthContext.get_effective_assignments(session, user)

        # Should include group's assignment
        assert len(assignments) >= 1
        role_names = {a.role.name for a in assignments}
        assert "group-role" in role_names

    async def test_effective_assignments_with_custom_service(
        self,
        default_user: User,
        session: AsyncSession,
        mocker,
    ):
        """Custom GroupMembershipService can be provided."""

        # Create a mock service that returns a specific group
        class MockGroupService(GroupMembershipService):
            name = "mock"

            async def is_user_in_group(self, session, username, group_name):
                return group_name == "mock-group"

            async def get_user_groups(self, session, username):
                return ["mock-group"]

            async def add_user_to_group(self, session, username, group_name):
                pass

            async def remove_user_from_group(self, session, username, group_name):
                pass

        # Create the mock group in DB
        group = User(
            username="mock-group",
            kind=PrincipalKind.GROUP,
            oauth_provider="basic",
        )
        session.add(group)
        await session.flush()

        # Assign role to mock group
        role = Role(name="mock-role", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.EXECUTE,
            scope_type=ResourceType.NAMESPACE,
            scope_value="special.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=group.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        user = await get_user(username=default_user.username, session=session)

        # Use custom service
        mock_service = MockGroupService()
        assignments = await AuthContext.get_effective_assignments(
            session,
            user,
            mock_service,
        )

        # Should include mock group's assignment
        role_names = {a.role.name for a in assignments}
        assert "mock-role" in role_names


@pytest.mark.asyncio
class TestCheckAccessIntegration:
    """Integration tests for authorize() with real authorization flow."""

    async def test_check_access_with_group_based_permissions(
        self,
        default_user: User,
        session: AsyncSession,
    ):
        """End-to-end: User gets access via group membership."""
        # Create group
        group = User(
            username="data-team",
            kind=PrincipalKind.GROUP,
            oauth_provider="basic",
        )
        session.add(group)
        await session.flush()

        # Add user to group
        membership = GroupMember(
            group_id=group.id,
            member_id=default_user.id,
        )
        session.add(membership)

        # Give group permission
        role = Role(name="data-team-role", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="data.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=group.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        user = await get_user(username=default_user.username, session=session)

        # Request access to data.* node
        request = ResourceRequest(
            verb=ResourceAction.READ,
            access_object=Resource(
                name="data.user_events",
                resource_type=ResourceType.NODE,
                owner="",
            ),
        )

        # Should be approved via group
        approved = await authorize(session, user, [request])

        assert len(approved) == 1
        assert approved[0].approved is True

    async def test_check_access_with_mixed_approval(
        self,
        default_user: User,
        session: AsyncSession,
        mocker,
    ):
        """Some requests approved, some denied."""
        # Give access to finance.* only
        role = Role(name="finance-reader", created_by_id=default_user.id)
        session.add(role)
        await session.flush()

        scope = RoleScope(
            role_id=role.id,
            action=ResourceAction.READ,
            scope_type=ResourceType.NAMESPACE,
            scope_value="finance.*",
        )
        session.add(scope)

        assignment = RoleAssignment(
            principal_id=default_user.id,
            role_id=role.id,
            granted_by_id=default_user.id,
        )
        session.add(assignment)
        await session.commit()

        user = await get_user(username=default_user.username, session=session)

        # Mix of accessible and inaccessible
        requests = [
            ResourceRequest(
                verb=ResourceAction.READ,
                access_object=Resource(
                    name="finance.revenue",
                    resource_type=ResourceType.NODE,
                    owner="",
                ),
            ),
            ResourceRequest(
                verb=ResourceAction.READ,
                access_object=Resource(
                    name="marketing.revenue",
                    resource_type=ResourceType.NODE,
                    owner="",
                ),
            ),
        ]
        mock_settings = mocker.patch(
            "datajunction_server.internal.access.authorization.settings",
        )
        mock_settings.authorization_provider = "rbac"
        mock_settings.default_access_policy = "restrictive"

        # FILTER mode - returns only approved
        filtered = await authorize(
            session,
            user,
            requests,
            on_denied=AccessDenialMode.FILTER,
        )
        assert len(filtered) == 1
        assert filtered[0].access_object.name == "finance.revenue"

        # RETURN_ALL mode - returns both
        all_results = await authorize(
            session,
            user,
            requests,
            on_denied=AccessDenialMode.RETURN,
        )
        assert len(all_results) == 2
        assert all_results[0].approved is True
        assert all_results[1].approved is False

        # RAISE mode - should raise
        with pytest.raises(DJAuthorizationException):
            await authorize(
                session,
                user,
                requests,
                on_denied=AccessDenialMode.RAISE,
            )
