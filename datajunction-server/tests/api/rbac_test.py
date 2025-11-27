"""Tests for RBAC API endpoints."""

from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.history import History
from datajunction_server.database.rbac import RoleScope, RoleAssignment
from datajunction_server.database.user import User
from datajunction_server.internal.history import ActivityType, EntityType


@pytest.mark.asyncio
async def test_create_role_basic(client_with_basic: AsyncClient):
    """Test creating a basic role without scopes."""
    response = await client_with_basic.post(
        "/roles/",
        json={
            "name": "test-role",
            "description": "A test role",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "test-role"
    assert data["description"] == "A test role"
    assert data["scopes"] == []
    assert "id" in data
    assert "created_at" in data
    assert "created_by" in data
    assert data["created_by"][
        "username"
    ]  # Should have the authenticated user's username


@pytest.mark.asyncio
async def test_create_role_with_scopes(client_with_basic: AsyncClient):
    """Test creating a role with scopes."""
    response = await client_with_basic.post(
        "/roles/",
        json={
            "name": "finance-editor",
            "description": "Can edit finance namespace",
            "scopes": [
                {
                    "action": "read",
                    "scope_type": "namespace",
                    "scope_value": "finance.*",
                },
                {
                    "action": "write",
                    "scope_type": "namespace",
                    "scope_value": "finance.*",
                },
            ],
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "finance-editor"
    assert len(data["scopes"]) == 2

    # Check scopes
    scopes = {(s["action"], s["scope_type"], s["scope_value"]) for s in data["scopes"]}
    assert ("read", "namespace", "finance.*") in scopes
    assert ("write", "namespace", "finance.*") in scopes


@pytest.mark.asyncio
async def test_create_role_duplicate_name(client_with_basic: AsyncClient):
    """Test that creating a role with duplicate name fails."""
    # Create first role
    response = await client_with_basic.post(
        "/roles/",
        json={"name": "duplicate-role", "description": "First"},
    )
    assert response.status_code == 201

    # Try to create with same name
    response = await client_with_basic.post(
        "/roles/",
        json={"name": "duplicate-role", "description": "Second"},
    )
    assert response.status_code == 409
    assert "already exists" in response.json()["message"]


@pytest.mark.asyncio
async def test_list_roles(client_with_basic: AsyncClient):
    """Test listing roles."""
    # Create several roles
    for i in range(3):
        await client_with_basic.post(
            "/roles/",
            json={"name": f"role-{i}", "description": f"Role {i}"},
        )

    # List roles
    response = await client_with_basic.get("/roles/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 3

    # Check they're ordered by name
    names = [role["name"] for role in data]
    assert sorted(names) == names


@pytest.mark.asyncio
async def test_list_roles_pagination(client_with_basic: AsyncClient):
    """Test role list pagination."""
    # Create several roles
    for i in range(5):
        await client_with_basic.post(
            "/roles/",
            json={"name": f"pag-role-{i:02d}"},
        )

    # Get first page
    response = await client_with_basic.get("/roles/?limit=2&offset=0")
    assert response.status_code == 200
    page1 = response.json()
    assert len(page1) == 2

    # Get second page
    response = await client_with_basic.get("/roles/?limit=2&offset=2")
    assert response.status_code == 200
    page2 = response.json()
    assert len(page2) == 2

    # Ensure no overlap
    page1_ids = {role["id"] for role in page1}
    page2_ids = {role["id"] for role in page2}
    assert page1_ids.isdisjoint(page2_ids)


@pytest.mark.asyncio
async def test_list_roles_by_creator(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test filtering roles by creator."""
    # Create a role (will be created by the authenticated user)
    response = await client_with_basic.post(
        "/roles/",
        json={"name": "creator-test-role"},
    )
    assert response.status_code == 201
    role_data = response.json()
    creator_username = role_data["created_by"]["username"]

    # Get the creator's user ID from database
    user_result = await session.execute(
        select(User).where(User.username == creator_username),
    )
    creator = user_result.scalar_one()

    # Filter by created_by_id - should return at least our role
    response = await client_with_basic.get(f"/roles/?created_by_id={creator.id}")
    assert response.status_code == 200
    filtered_roles = response.json()
    assert len(filtered_roles) >= 1
    assert all(r["created_by"]["username"] == creator_username for r in filtered_roles)
    assert any(r["name"] == "creator-test-role" for r in filtered_roles)


@pytest.mark.asyncio
async def test_get_role(client_with_basic: AsyncClient):
    """Test getting a specific role."""
    # Create role
    create_response = await client_with_basic.post(
        "/roles/",
        json={
            "name": "get-test-role",
            "description": "Test role for GET",
            "scopes": [
                {
                    "action": "read",
                    "scope_type": "namespace",
                    "scope_value": "test.*",
                },
            ],
        },
    )
    role_name = create_response.json()["name"]

    # Get role
    response = await client_with_basic.get(f"/roles/{role_name}")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "get-test-role"
    assert len(data["scopes"]) == 1


@pytest.mark.asyncio
async def test_get_role_not_found(client_with_basic: AsyncClient):
    """Test getting non-existent role."""
    response = await client_with_basic.get("/roles/nonexistent-role")
    assert response.status_code == 404
    assert "does not exist" in response.json()["message"]


@pytest.mark.asyncio
async def test_update_role_name(client_with_basic: AsyncClient):
    """Test updating role name."""
    # Create role
    create_response = await client_with_basic.post(
        "/roles/",
        json={"name": "old-name", "description": "Original"},
    )
    assert create_response.status_code == 201

    # Update name
    response = await client_with_basic.patch(
        "/roles/old-name",
        json={"name": "new-name"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "new-name"
    assert data["description"] == "Original"  # Unchanged


@pytest.mark.asyncio
async def test_update_role_description(client_with_basic: AsyncClient):
    """Test updating role description."""
    # Create role
    await client_with_basic.post(
        "/roles/",
        json={"name": "update-desc", "description": "Old description"},
    )

    # Update description
    response = await client_with_basic.patch(
        "/roles/update-desc",
        json={"description": "New description"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "update-desc"  # Unchanged
    assert data["description"] == "New description"


@pytest.mark.asyncio
async def test_update_role_name_conflict(client_with_basic: AsyncClient):
    """Test that updating to existing name fails."""
    # Create two roles
    await client_with_basic.post(
        "/roles/",
        json={"name": "existing-role"},
    )
    await client_with_basic.post(
        "/roles/",
        json={"name": "other-role"},
    )

    # Try to rename to existing name
    response = await client_with_basic.patch(
        "/roles/other-role",
        json={"name": "existing-role"},
    )
    assert response.status_code == 409
    assert "already exists" in response.json()["message"]


@pytest.mark.asyncio
async def test_delete_role(client_with_basic: AsyncClient):
    """Test soft deleting a role."""
    # Create role
    await client_with_basic.post(
        "/roles/",
        json={"name": "delete-me"},
    )

    # Delete role (soft delete)
    response = await client_with_basic.delete("/roles/delete-me")
    assert response.status_code == 204

    # Verify it's not in normal queries
    response = await client_with_basic.get("/roles/delete-me")
    assert response.status_code == 404

    # Verify it still exists with include_deleted=true
    response = await client_with_basic.get("/roles/delete-me?include_deleted=true")
    assert response.status_code == 200
    data = response.json()
    assert data["deleted_at"] is not None
    # Who deleted is in History table, not on the model


@pytest.mark.asyncio
async def test_delete_role_not_found(client_with_basic: AsyncClient):
    """Test deleting non-existent role."""
    response = await client_with_basic.delete("/roles/nonexistent-role")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_add_scope_to_role(client_with_basic: AsyncClient):
    """Test adding a scope to an existing role."""
    # Create role without scopes
    await client_with_basic.post(
        "/roles/",
        json={"name": "add-scope-role"},
    )

    # Add scope
    response = await client_with_basic.post(
        "/roles/add-scope-role/scopes/",
        json={
            "action": "execute",
            "scope_type": "namespace",
            "scope_value": "analytics.*",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["action"] == "execute"
    assert data["scope_type"] == "namespace"
    assert data["scope_value"] == "analytics.*"


@pytest.mark.asyncio
async def test_add_scope_duplicate(client_with_basic: AsyncClient):
    """Test that adding duplicate scope fails."""
    # Create role with scope
    await client_with_basic.post(
        "/roles/",
        json={
            "name": "dup-scope-role",
            "scopes": [
                {
                    "action": "read",
                    "scope_type": "namespace",
                    "scope_value": "test.*",
                },
            ],
        },
    )

    # Try to add same scope
    response = await client_with_basic.post(
        "/roles/dup-scope-role/scopes/",
        json={
            "action": "read",
            "scope_type": "namespace",
            "scope_value": "test.*",
        },
    )
    assert response.status_code == 409
    assert "already exists" in response.json()["message"]


@pytest.mark.asyncio
async def test_add_scope_to_nonexistent_role(client_with_basic: AsyncClient):
    """Test adding scope to non-existent role."""
    response = await client_with_basic.post(
        "/roles/nonexistent-role/scopes/",
        json={
            "action": "read",
            "scope_type": "namespace",
            "scope_value": "test.*",
        },
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_role_scopes(client_with_basic: AsyncClient):
    """Test listing scopes for a role."""
    # Create role with scopes
    await client_with_basic.post(
        "/roles/",
        json={
            "name": "list-scopes-role",
            "scopes": [
                {"action": "read", "scope_type": "namespace", "scope_value": "a.*"},
                {"action": "write", "scope_type": "namespace", "scope_value": "a.*"},
                {"action": "execute", "scope_type": "node", "scope_value": "a.b.c"},
            ],
        },
    )

    # List scopes
    response = await client_with_basic.get("/roles/list-scopes-role/scopes/")
    assert response.status_code == 200
    scopes = response.json()
    assert len(scopes) == 3


@pytest.mark.asyncio
async def test_delete_scope_from_role(client_with_basic: AsyncClient):
    """Test deleting a scope from a role."""
    from urllib.parse import quote

    # Create role with scopes
    await client_with_basic.post(
        "/roles/",
        json={
            "name": "delete-scope-role",
            "scopes": [
                {"action": "read", "scope_type": "namespace", "scope_value": "x.*"},
                {"action": "write", "scope_type": "namespace", "scope_value": "x.*"},
            ],
        },
    )

    # Delete scope using composite key (URL-encode the wildcard)
    scope_value = quote("x.*", safe="")
    response = await client_with_basic.delete(
        f"/roles/delete-scope-role/scopes/read/namespace/{scope_value}",
    )
    assert response.status_code == 204

    # Verify it's gone
    response = await client_with_basic.get("/roles/delete-scope-role/scopes/")
    assert response.status_code == 200
    remaining_scopes = response.json()
    assert len(remaining_scopes) == 1
    assert remaining_scopes[0]["action"] == "write"  # Only write scope remains


@pytest.mark.asyncio
async def test_delete_scope_not_found(client_with_basic: AsyncClient):
    """Test deleting non-existent scope."""
    from urllib.parse import quote

    # Create role
    await client_with_basic.post(
        "/roles/",
        json={"name": "test-role"},
    )

    # Try to delete non-existent scope (URL-encode the wildcard)
    scope_value = quote("nonexistent.*", safe="")
    response = await client_with_basic.delete(
        f"/roles/test-role/scopes/read/namespace/{scope_value}",
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_soft_delete_preserves_scopes(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test that soft deleting a role preserves its scopes for audit trail."""
    # Create role with scopes
    create_response = await client_with_basic.post(
        "/roles/",
        json={
            "name": "soft-delete-test-role",
            "scopes": [
                {"action": "read", "scope_type": "namespace", "scope_value": "test.*"},
                {"action": "write", "scope_type": "namespace", "scope_value": "test.*"},
            ],
        },
    )
    role_id = create_response.json()["id"]

    # Get scope IDs from database
    scope_result = await session.execute(
        select(RoleScope).where(RoleScope.role_id == role_id),
    )
    scope_ids = [scope.id for scope in scope_result.scalars().all()]
    assert len(scope_ids) == 2  # Should have 2 scopes

    # Soft delete role
    response = await client_with_basic.delete("/roles/soft-delete-test-role")
    assert response.status_code == 204

    # Verify scopes still exist in database (for audit)
    for scope_id in scope_ids:
        result = await session.execute(
            select(RoleScope).where(RoleScope.id == scope_id),
        )
        assert result.scalar_one_or_none() is not None  # Still exists!


@pytest.mark.asyncio
async def test_cannot_delete_role_with_assignments(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test that roles with assignments cannot be deleted (SOX compliance)."""
    # Create role
    create_response = await client_with_basic.post(
        "/roles/",
        json={"name": "assigned-role"},
    )
    role_id = create_response.json()["id"]

    # Get a user to assign the role to
    user_result = await session.execute(select(User).limit(1))
    user = user_result.scalar_one()

    # Create an assignment
    assignment = RoleAssignment(
        principal_id=user.id,
        role_id=role_id,
        granted_by_id=user.id,
    )
    session.add(assignment)
    await session.commit()

    # Try to delete role
    response = await client_with_basic.delete("/roles/assigned-role")
    assert response.status_code == 400
    assert "cannot delete" in response.json()["message"].lower()
    assert "audit compliance" in response.json()["message"].lower()


@pytest.mark.asyncio
async def test_audit_logging_for_role_operations(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test that all role operations are logged to history for SOX compliance."""
    # Create role
    await client_with_basic.post(
        "/roles/",
        json={
            "name": "audit-test-role",
            "description": "Test role for audit",
        },
    )

    # Check CREATE was logged
    history_result = await session.execute(
        select(History).where(
            History.entity_type == EntityType.ROLE,
            History.entity_name == "audit-test-role",
            History.activity_type == ActivityType.CREATE,
        ),
    )
    create_log = history_result.scalar_one_or_none()
    assert create_log is not None
    assert create_log.post["name"] == "audit-test-role"

    # Update role
    await client_with_basic.patch(
        "/roles/audit-test-role",
        json={"description": "Updated description"},
    )

    # Check UPDATE was logged
    history_result = await session.execute(
        select(History).where(
            History.entity_type == EntityType.ROLE,
            History.entity_name == "audit-test-role",
            History.activity_type == ActivityType.UPDATE,
        ),
    )
    update_log = history_result.scalar_one_or_none()
    assert update_log is not None
    assert update_log.pre["description"] == "Test role for audit"
    assert update_log.post["description"] == "Updated description"

    # Delete role
    await client_with_basic.delete("/roles/audit-test-role")

    # Check DELETE was logged
    history_result = await session.execute(
        select(History).where(
            History.entity_type == EntityType.ROLE,
            History.entity_name == "audit-test-role",
            History.activity_type == ActivityType.DELETE,
        ),
    )
    delete_log = history_result.scalar_one_or_none()
    assert delete_log is not None
    assert delete_log.pre["name"] == "audit-test-role"


# ============================================================================
# Role Assignment Tests
# ============================================================================


@pytest.mark.asyncio
async def test_create_role_assignment(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test assigning a role to a principal."""
    # Create role
    await client_with_basic.post(
        "/roles/",
        json={
            "name": "test-assignment-role",
            "description": "Role for assignment testing",
        },
    )

    # Get a user to assign the role to
    user_result = await session.execute(select(User).limit(1))
    user = user_result.scalar_one()

    # Create assignment
    response = await client_with_basic.post(
        "/roles/test-assignment-role/assign",
        json={
            "principal_username": user.username,
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data == {
        "expires_at": None,
        "granted_at": mock.ANY,
        "granted_by": {
            "email": "dj@datajunction.io",
            "username": "dj",
        },
        "principal": {
            "email": "dj@datajunction.io",
            "username": "dj",
        },
        "role": {
            "created_at": mock.ANY,
            "created_by": {
                "email": "dj@datajunction.io",
                "username": "dj",
            },
            "deleted_at": None,
            "description": "Role for assignment testing",
            "id": mock.ANY,
            "name": "test-assignment-role",
            "scopes": [],
        },
    }


@pytest.mark.asyncio
async def test_create_role_assignment_with_expiration(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test assigning a role with an expiration date."""
    # Create role
    await client_with_basic.post(
        "/roles/",
        json={"name": "temp-role"},
    )

    # Get a user
    user_result = await session.execute(select(User).limit(1))
    user = user_result.scalar_one()

    # Create assignment with expiration
    expires_at = datetime.now(timezone.utc) + timedelta(days=30)
    response = await client_with_basic.post(
        "/roles/temp-role/assign",
        json={
            "principal_username": user.username,
            "expires_at": expires_at.isoformat(),
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["expires_at"] is not None


@pytest.mark.asyncio
async def test_create_role_assignment_duplicate(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test that assigning the same role twice fails."""
    # Create role
    await client_with_basic.post(
        "/roles/",
        json={"name": "dup-assignment-role"},
    )

    # Get a user
    user_result = await session.execute(select(User).limit(1))
    user = user_result.scalar_one()

    # Create first assignment
    response = await client_with_basic.post(
        "/roles/dup-assignment-role/assign",
        json={
            "principal_username": user.username,
        },
    )
    assert response.status_code == 201

    # Try to create duplicate
    response = await client_with_basic.post(
        "/roles/dup-assignment-role/assign",
        json={
            "principal_username": user.username,
        },
    )
    assert response.status_code == 409
    assert "already has role" in response.json()["message"]


@pytest.mark.asyncio
async def test_create_role_assignment_nonexistent_role(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test assigning a non-existent role."""
    # Get a user
    user_result = await session.execute(select(User).limit(1))
    user = user_result.scalar_one()

    response = await client_with_basic.post(
        "/roles/nonexistent-role/assign",
        json={
            "principal_username": user.username,
        },
    )
    assert response.status_code == 404
    assert "does not exist" in response.json()["message"]


@pytest.mark.asyncio
async def test_create_role_assignment_nonexistent_principal(
    client_with_basic: AsyncClient,
):
    """Test assigning a role to a non-existent principal."""
    # Create role
    await client_with_basic.post(
        "/roles/",
        json={"name": "test-role"},
    )

    response = await client_with_basic.post(
        "/roles/test-role/assign",
        json={
            "principal_username": "nonexistent-user",
        },
    )
    assert response.status_code == 404
    assert "Principal" in response.json()["message"]


@pytest.mark.asyncio
async def test_list_role_assignments(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test listing role assignments."""
    # Create role
    await client_with_basic.post(
        "/roles/",
        json={"name": "list-test-role"},
    )

    # Get a user
    user_result = await session.execute(select(User).limit(1))
    user = user_result.scalar_one()

    # Create assignment
    await client_with_basic.post(
        "/roles/list-test-role/assign",
        json={
            "principal_username": user.username,
        },
    )

    # List assignments for this role
    response = await client_with_basic.get("/roles/list-test-role/assignments")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1  # At least the one we just created
    # Verify structure
    assignment = data[0]
    assert "principal" in assignment
    assert assignment["principal"]["username"] == user.username
    assert assignment["role"]["name"] == "list-test-role"


@pytest.mark.asyncio
async def test_list_role_assignments_by_role_name(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test listing assignments for a specific role."""
    # Create role
    await client_with_basic.post("/roles/", json={"name": "shared-role"})

    # Get a user
    user_result = await session.execute(select(User).limit(1))
    user = user_result.scalar_one()

    # Assign role to user
    await client_with_basic.post(
        "/roles/shared-role/assign",
        json={"principal_username": user.username},
    )

    # List assignments for this role
    response = await client_with_basic.get("/roles/shared-role/assignments")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert all(a["role"]["name"] == "shared-role" for a in data)


@pytest.mark.asyncio
async def test_revoke_role_assignment(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test revoking a role assignment."""
    # Create role
    await client_with_basic.post("/roles/", json={"name": "revoke-test-role"})

    # Get a user
    user_result = await session.execute(select(User).limit(1))
    user = user_result.scalar_one()

    # Create assignment
    create_response = await client_with_basic.post(
        "/roles/revoke-test-role/assign",
        json={"principal_username": user.username},
    )
    assert create_response.status_code == 201

    # Revoke assignment
    response = await client_with_basic.delete(
        f"/roles/revoke-test-role/assignments/{user.username}",
    )
    assert response.status_code == 204

    # Verify it's gone
    response = await client_with_basic.get("/roles/revoke-test-role/assignments")
    data = response.json()
    assert not any(a["principal"]["username"] == user.username for a in data)

    # Try revoking again to ensure idempotency
    response = await client_with_basic.delete(
        f"/roles/revoke-test-role/assignments/{user.username}",
    )
    assert response.status_code == 404
    assert (
        response.json()["message"]
        == f"Principal '{user.username}' does not have role 'revoke-test-role'"
    )


@pytest.mark.asyncio
async def test_revoke_role_assignment_not_found(client_with_basic: AsyncClient):
    """Test revoking non-existent assignment."""
    # Create role
    await client_with_basic.post("/roles/", json={"name": "test-role"})

    # Try to revoke assignment that doesn't exist
    response = await client_with_basic.delete(
        "/roles/test-role/assignments/nonexistent-user",
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_audit_logging_for_role_assignments(
    client_with_basic: AsyncClient,
    session: AsyncSession,
):
    """Test that role assignment operations are logged for SOX compliance."""
    # Create role
    await client_with_basic.post("/roles/", json={"name": "audit-assignment-role"})

    # Get a user
    user_result = await session.execute(select(User).limit(1))
    user = user_result.scalar_one()

    # Create assignment
    create_response = await client_with_basic.post(
        "/roles/audit-assignment-role/assign",
        json={"principal_username": user.username},
    )
    assert create_response.status_code == 201

    # Check CREATE was logged
    history_result = await session.execute(
        select(History)
        .where(
            History.entity_type == EntityType.ROLE_ASSIGNMENT,
            History.activity_type == ActivityType.CREATE,
        )
        .order_by(History.created_at.desc())
        .limit(1),
    )
    create_log = history_result.scalar_one_or_none()
    assert create_log is not None
    assert create_log.post["principal_id"] == user.id
    assert create_log.post["role_name"] == "audit-assignment-role"

    # Revoke assignment
    await client_with_basic.delete(
        f"/roles/audit-assignment-role/assignments/{user.username}",
    )

    # Check DELETE was logged
    history_result = await session.execute(
        select(History)
        .where(
            History.entity_type == EntityType.ROLE_ASSIGNMENT,
            History.activity_type == ActivityType.DELETE,
        )
        .order_by(History.created_at.desc())
        .limit(1),
    )
    delete_log = history_result.scalar_one_or_none()
    assert delete_log is not None
    assert delete_log.pre["principal_username"] == user.username
    assert delete_log.pre["role_name"] == "audit-assignment-role"
