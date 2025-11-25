"""
Tests for groups API endpoints.
"""

import pytest
from httpx import AsyncClient


# Group Registration Tests


@pytest.mark.asyncio
async def test_register_group(module__client: AsyncClient) -> None:
    """Test registering a new group."""
    response = await module__client.post(
        "/groups/",
        params={
            "username": "eng-team",
            "email": "eng-team@company.com",
            "name": "Engineering Team",
        },
    )

    assert response.status_code == 201
    data = response.json()
    assert data["username"] == "eng-team"
    assert data["email"] == "eng-team@company.com"
    assert data["name"] == "Engineering Team"


@pytest.mark.asyncio
async def test_register_group_minimal(module__client: AsyncClient) -> None:
    """Test registering a group with minimal info."""
    response = await module__client.post(
        "/groups/",
        params={"username": "data-team"},
    )

    assert response.status_code == 201
    data = response.json()
    assert data["username"] == "data-team"
    assert data["name"] == "data-team"  # Defaults to username


@pytest.mark.asyncio
async def test_register_duplicate_group(module__client: AsyncClient) -> None:
    """Test that registering a duplicate group fails."""
    # Register once
    await module__client.post(
        "/groups/",
        params={"username": "duplicate-group"},
    )

    # Try to register again
    response = await module__client.post(
        "/groups/",
        params={"username": "duplicate-group"},
    )

    assert response.status_code == 409
    assert response.json()["message"] == "Group duplicate-group already exists"


# List Groups Tests


@pytest.mark.asyncio
async def test_list_groups_empty(module__client: AsyncClient) -> None:
    """Test listing groups when none exist."""
    response = await module__client.get("/groups/")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_list_groups_with_data(module__client: AsyncClient) -> None:
    """Test listing groups returns registered groups."""
    # Register a few groups
    await module__client.post("/groups/", params={"username": "team-a"})
    await module__client.post("/groups/", params={"username": "team-b"})
    await module__client.post("/groups/", params={"username": "team-c"})

    response = await module__client.get("/groups/")

    assert response.status_code == 200
    groups = response.json()
    assert len(groups) >= 3

    usernames = [g["username"] for g in groups]
    assert "team-a" in usernames
    assert "team-b" in usernames
    assert "team-c" in usernames


# Get Group Tests


@pytest.mark.asyncio
async def test_get_group_success(module__client: AsyncClient) -> None:
    """Test getting a specific group."""
    # Register group
    await module__client.post(
        "/groups/",
        params={
            "username": "test-group",
            "name": "Test Group",
        },
    )

    # Get group
    response = await module__client.get("/groups/test-group")

    assert response.status_code == 200
    data = response.json()
    assert data["email"] is None
    assert data["username"] == "test-group"
    assert data["name"] == "Test Group"


@pytest.mark.asyncio
async def test_get_nonexistent_group(module__client: AsyncClient) -> None:
    """Test getting a group that doesn't exist."""
    response = await module__client.get("/groups/nonexistent")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


# Group Membership Tests (Postgres Provider)


@pytest.mark.asyncio
async def test_add_group_member_success(
    module__client: AsyncClient,
) -> None:
    """Test adding a member to a group (Postgres provider)."""
    # Register group
    await module__client.post("/groups/", params={"username": "eng"})

    # Register user (create via node to ensure user exists)
    # For now, just test the endpoint behavior
    response = await module__client.post(
        "/groups/eng/members/",
        params={"member_username": "dj"},
    )
    assert response.status_code == 201
    assert response.json()["message"] == "Added dj to eng"

    # Check user is in members
    response = await module__client.get("/groups/eng/members")
    assert [user["username"] for user in response.json()] == ["dj"]

    # Add non-existent user to group
    response = await module__client.post(
        "/groups/eng/members/",
        params={"member_username": "someuser"},
    )
    assert response.status_code == 404
    assert response.json()["message"] == "User someuser not found"

    # Try to add user to the group again
    response = await module__client.post(
        "/groups/eng/members/",
        params={"member_username": "dj"},
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "dj is already a member of eng"


@pytest.mark.asyncio
async def test_add_group_member_static_provider(
    module__client: AsyncClient,
    mocker,
) -> None:
    """Test that adding members fails with static provider."""
    # Mock static provider
    mock_settings = mocker.patch("datajunction_server.api.groups.settings")
    mock_settings.group_membership_provider = "static"

    # Register group
    await module__client.post("/groups/", params={"username": "test-group"})

    # Try to add member
    response = await module__client.post(
        "/groups/test-group/members/",
        params={"member_username": "dj"},
    )
    assert response.status_code == 400
    assert (
        response.json()["detail"]
        == "Membership management not supported for provider: static"
    )


@pytest.mark.asyncio
async def test_add_member_to_nonexistent_group(
    module__client: AsyncClient,
) -> None:
    """Test adding member to a group that doesn't exist."""
    response = await module__client.post(
        "/groups/nonexistent/members/",
        params={"member_username": "someuser"},
    )

    assert response.status_code == 404
    assert response.json()["message"] == "Group nonexistent not found"


@pytest.mark.asyncio
async def test_remove_group_member(
    module__client: AsyncClient,
) -> None:
    """Test removing a member from a group."""
    # Try removing a member from a non-existent group
    response = await module__client.delete("/groups/lifecycle-group/members/someuser")
    assert response.status_code == 404
    assert response.json()["message"] == "Group lifecycle-group not found"

    # Register group
    await module__client.post("/groups/", params={"username": "test-group"})

    # Add member to group
    response = await module__client.post(
        "/groups/test-group/members/",
        params={"member_username": "dj"},
    )

    # Try to remove member
    response = await module__client.delete("/groups/test-group/members/dj")
    assert response.status_code == 204

    # Try to remove member again (should raise)
    response = await module__client.delete("/groups/test-group/members/dj")
    assert response.status_code == 404
    assert response.json()["detail"] == "dj is not a member of test-group"

    # Try to remove non-existent user
    response = await module__client.delete("/groups/test-group/members/someuser")
    assert response.status_code == 404
    assert response.json()["message"] == "User someuser not found"


@pytest.mark.asyncio
async def test_remove_member_static_provider(
    module__client: AsyncClient,
    mocker,
) -> None:
    """Test that removing members fails with static provider."""
    # Mock static provider
    mock_settings = mocker.patch("datajunction_server.api.groups.settings")
    mock_settings.group_membership_provider = "static"

    # Register group
    await module__client.post("/groups/", params={"username": "test-group"})

    # Add user to group
    response = await module__client.post(
        "/groups/test-group/members/",
        params={"member_username": "dj"},
    )

    # Try to remove user from group
    response = await module__client.delete("/groups/test-group/members/dj")
    assert response.status_code == 400
    assert (
        response.json()["detail"]
        == "Membership management not supported for provider: static"
    )


@pytest.mark.asyncio
async def test_list_group_members_postgres(
    module__client: AsyncClient,
) -> None:
    """Test listing group members with Postgres provider."""
    # Register group
    await module__client.post("/groups/", params={"username": "test-group"})

    # List members
    response = await module__client.get("/groups/test-group/members/")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_list_group_members_static(
    module__client: AsyncClient,
    mocker,
) -> None:
    """Test listing group members with static provider returns empty."""
    # Mock static provider
    mock_settings = mocker.patch("datajunction_server.api.groups.settings")
    mock_settings.group_membership_provider = "static"

    # Register group
    await module__client.post("/groups/", params={"username": "test-group"})

    # List members
    response = await module__client.get("/groups/test-group/members/")

    assert response.status_code == 200
    assert response.json() == []  # Empty for static provider


@pytest.mark.asyncio
async def test_list_members_nonexistent_group(module__client: AsyncClient) -> None:
    """Test listing members of a nonexistent group."""
    response = await module__client.get("/groups/nonexistent/members/")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


@pytest.mark.asyncio
async def test_group_lifecycle(
    module__client: AsyncClient,
) -> None:
    """Test complete group lifecycle: register -> list -> get -> delete"""
    # Register group
    response = await module__client.post(
        "/groups/",
        params={
            "username": "lifecycle-group",
            "email": "lifecycle@test.com",
            "name": "Lifecycle Test Group",
        },
    )
    assert response.status_code == 201

    # Verify it appears in list
    response = await module__client.get("/groups/")
    assert response.status_code == 200
    usernames = [g["username"] for g in response.json()]
    assert "lifecycle-group" in usernames

    # Get specific group
    response = await module__client.get("/groups/lifecycle-group")
    assert response.status_code == 200
    assert response.json()["username"] == "lifecycle-group"
    assert response.json()["email"] == "lifecycle@test.com"
