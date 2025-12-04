from unittest import mock
import pytest
from httpx import AsyncClient
from datajunction_server.database.user import OAuthProvider, PrincipalKind, User
from datajunction_server.internal.access.authentication.basic import (
    get_password_hash,
    validate_password_hash,
)

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_create_service_account(
    module__client: AsyncClient,
    module__session: AsyncSession,
):
    """
    Test creating a service account
    """
    payload = {"name": "Test Service Account"}

    # Authenticated client should be used
    response = await module__client.post("/service-accounts", json=payload)
    assert response.status_code == 200, response.text

    data = response.json()
    assert "client_id" in data
    assert "client_secret" in data
    assert "id" in data

    # Verify it's stored in DB correctly
    sa = await User.get_by_username(module__session, data["client_id"])
    assert sa is not None
    assert sa.name == "Test Service Account"
    assert validate_password_hash(data["client_secret"], sa.password)

    response = await module__client.get("/service-accounts")
    assert response.status_code == 200, response.text
    sa_list = response.json()
    assert len(sa_list) == 1
    assert sa_list[0] == {
        "client_id": data["client_id"],
        "created_at": mock.ANY,
        "id": data["id"],
        "name": data["name"],
    }


@pytest.mark.asyncio
async def test_create_sa_with_non_user_identity(module__client: AsyncClient):
    """
    Test creating a service account with a non-user identity (should fail)
    """
    sa_response = await module__client.post(
        "/service-accounts",
        json={"name": "General SA"},
    )
    service_account = sa_response.json()
    token_response = await module__client.post(
        "/service-accounts/token",
        data={
            "client_id": service_account["client_id"],
            "client_secret": service_account["client_secret"],
        },
    )
    auth_token = token_response.json()
    create_resp = await module__client.post(
        "/service-accounts",
        headers={"Authorization": f"Bearer {auth_token['token']}"},
        json={"name": "Bogus"},
    )
    assert create_resp.status_code == 401
    error = create_resp.json()
    assert error["errors"][0] == {
        "code": 400,
        "context": "",
        "debug": None,
        "message": "Only users can create service accounts",
    }


@pytest.mark.asyncio
async def test_service_account_token_success(
    module__client: AsyncClient,
):
    """
    Test successful service account token retrieval
    """
    # Create a service account
    payload = {"name": "Login SA"}
    create_resp = await module__client.post("/service-accounts", json=payload)
    assert create_resp.status_code == 200
    sa_data = create_resp.json()

    # Use returned client_id + client_secret to get a token
    login_resp = await module__client.post(
        "/service-accounts/token",
        data={
            "client_id": sa_data["client_id"],
            "client_secret": sa_data["client_secret"],
        },
    )
    assert login_resp.status_code == 200, login_resp.text
    token_data = login_resp.json()
    assert token_data["token_type"] == "bearer"
    assert "token" in token_data
    assert isinstance(token_data["expires_in"], int)

    # Use the token to call a protected endpoint
    whoami_response = await module__client.get(
        "/whoami",
        headers={"Authorization": f"Bearer {token_data['token']}"},
    )
    assert whoami_response.status_code == 200
    assert whoami_response.json() == {
        "created_collections": [],
        "created_nodes": [],
        "created_tags": [],
        "email": None,
        "id": mock.ANY,
        "is_admin": False,
        "name": "Login SA",
        "oauth_provider": "basic",
        "owned_nodes": [],
        "username": sa_data["client_id"],
        "last_viewed_notifications_at": None,
        "notification_preferences": [],
    }


@pytest.mark.asyncio
async def test_service_account_login_invalid_client_id(module__client: AsyncClient):
    """
    Test login with non-existent client_id
    """
    resp = await module__client.post(
        "/service-accounts/token",
        data={
            "client_id": "non-existent-id",
            "client_secret": "whatever",
        },
    )
    assert resp.status_code == 401
    error = resp.json()
    assert error["errors"][0] == {
        "code": 403,
        "context": "",
        "debug": None,
        "message": "Service account `non-existent-id` not found",
    }


@pytest.mark.asyncio
async def test_service_account_login_wrong_kind(
    module__client: AsyncClient,
    module__session: AsyncSession,
):
    # Create a regular user
    user = User(
        username="normal-user",
        password=get_password_hash("secret"),
        kind=PrincipalKind.USER,
        oauth_provider=OAuthProvider.BASIC,
    )
    module__session.add(user)
    await module__session.commit()
    await module__session.refresh(user)

    resp = await module__client.post(
        "/service-accounts/token",
        data={"client_id": user.username, "client_secret": "secret"},
    )
    assert resp.status_code == 401
    error = resp.json()
    assert error["errors"][0]["message"] == "Not a service account"


@pytest.mark.asyncio
async def test_service_account_login_invalid_secret(module__client: AsyncClient):
    """
    Test login with incorrect client_secret
    """
    # Create a service account
    payload = {"name": "Bad Secret SA"}
    create_resp = await module__client.post("/service-accounts", json=payload)
    assert create_resp.status_code == 200
    sa_data = create_resp.json()

    # Try wrong secret
    resp = await module__client.post(
        "/service-accounts/token",
        data={
            "client_id": sa_data["client_id"],
            "client_secret": "wrong-secret",
        },
    )
    assert resp.status_code == 401
    error = resp.json()
    assert error["errors"][0] == {
        "code": 402,
        "context": "",
        "debug": None,
        "message": "Invalid service account credentials",
    }


@pytest.mark.asyncio
async def test_delete_service_account_success(
    module__client: AsyncClient,
    module__session: AsyncSession,
):
    """
    Test successfully deleting a service account
    """
    # Create a service account
    payload = {"name": "SA To Delete"}
    create_resp = await module__client.post("/service-accounts", json=payload)
    assert create_resp.status_code == 200
    sa_data = create_resp.json()

    # Verify it exists
    sa = await User.get_by_username(module__session, sa_data["client_id"])
    assert sa is not None

    # Delete it
    delete_resp = await module__client.delete(
        f"/service-accounts/{sa_data['client_id']}",
    )
    assert delete_resp.status_code == 200
    assert delete_resp.json() == {
        "message": f"Service account `{sa_data['client_id']}` deleted",
    }

    # Verify it's gone from the database
    await module__session.expire_all()
    sa_after = await User.get_by_username(module__session, sa_data["client_id"])
    assert sa_after is None

    # Verify it's gone from the list
    list_resp = await module__client.get("/service-accounts")
    assert list_resp.status_code == 200
    sa_list = list_resp.json()
    assert all(sa["client_id"] != sa_data["client_id"] for sa in sa_list)


@pytest.mark.asyncio
async def test_delete_service_account_not_found(module__client: AsyncClient):
    """
    Test deleting a service account that doesn't exist
    """
    resp = await module__client.delete("/service-accounts/non-existent-id")
    assert resp.status_code == 401
    error = resp.json()
    assert (
        error["errors"][0]["message"] == "Service account `non-existent-id` not found"
    )


@pytest.mark.asyncio
async def test_delete_service_account_wrong_kind(
    module__client: AsyncClient,
    module__session: AsyncSession,
):
    """
    Test deleting something that is not a service account (e.g., a regular user)
    """
    # Create a regular user
    user = User(
        username="regular-user-to-delete",
        password=get_password_hash("secret"),
        kind=PrincipalKind.USER,
        oauth_provider=OAuthProvider.BASIC,
    )
    module__session.add(user)
    await module__session.commit()

    # Try to delete it via service account endpoint
    resp = await module__client.delete(f"/service-accounts/{user.username}")
    assert resp.status_code == 401
    error = resp.json()
    assert error["errors"][0]["message"] == "Not a service account"


@pytest.mark.asyncio
async def test_delete_service_account_not_owner(
    module__client: AsyncClient,
    module__session: AsyncSession,
):
    """
    Test that a user cannot delete a service account they didn't create
    """
    # Create a service account owned by a different user
    other_user = User(
        username="other-user",
        password=get_password_hash("secret"),
        kind=PrincipalKind.USER,
        oauth_provider=OAuthProvider.BASIC,
    )
    module__session.add(other_user)
    await module__session.commit()
    await module__session.refresh(other_user)

    # Create a service account owned by the other user
    sa = User(
        name="Other User's SA",
        username="other-users-sa-id",
        password=get_password_hash("secret"),
        kind=PrincipalKind.SERVICE_ACCOUNT,
        oauth_provider=OAuthProvider.BASIC,
        created_by_id=other_user.id,
    )
    module__session.add(sa)
    await module__session.commit()

    # Try to delete it as the current user (dj)
    resp = await module__client.delete(f"/service-accounts/{sa.username}")
    assert resp.status_code == 401
    error = resp.json()
    assert (
        error["errors"][0]["message"]
        == "You can only delete service accounts you created"
    )
