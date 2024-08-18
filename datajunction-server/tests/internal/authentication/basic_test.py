"""
Tests for basic auth helper functions
"""
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.constants import AUTH_COOKIE
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication import basic


@pytest.mark.asyncio
async def test_login_with_username_and_password(client: AsyncClient):
    """
    Test validating a username and a password
    """
    await client.post(
        "/basic/user/",
        data={"email": "dj@datajunction.io", "username": "dj", "password": "dj"},
    )
    response = await client.post(
        "/basic/login/",
        data={"username": "dj", "password": "dj"},
    )
    assert response.status_code in (200, 201)
    assert response.cookies.get(AUTH_COOKIE)


@pytest.mark.asyncio
async def test_logout(client: AsyncClient):
    """
    Test validating logging out
    """
    await client.post("/logout/")


def test_hash_and_verify_password():
    """
    Test hashing a password and verifying a password against a hash
    """
    hashed_password = basic.get_password_hash(password="foo")
    assert basic.validate_password_hash(
        plain_password="foo",
        hashed_password=hashed_password,
    )


@pytest.mark.asyncio
async def test_validate_username_and_password(
    client: AsyncClient,
    session: AsyncSession,
):
    """
    Test validating a username and a password
    """
    await client.post(
        "/basic/user/",
        data={"email": "dj@datajunction.io", "username": "dj", "password": "dj"},
    )
    user = await basic.validate_user_password(
        username="dj",
        password="dj",
        session=session,
    )
    assert user.username == "dj"


@pytest.mark.asyncio
async def test_get_user(client: AsyncClient, session: AsyncSession):
    """
    Test getting a user
    """
    await client.post(
        "/basic/user/",
        data={"email": "dj@datajunction.io", "username": "dj", "password": "dj"},
    )
    user = await basic.get_user(username="dj", session=session)
    assert user.username == "dj"


@pytest.mark.asyncio
async def test_get_user_raise_on_user_not_found(session: AsyncSession):
    """
    Test raising when trying to get a user that doesn't exist
    """
    with pytest.raises(DJException) as exc_info:
        await basic.get_user(username="dj", session=session)
    assert "User dj not found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_login_raise_on_user_not_found(client: AsyncClient):
    """
    Test raising when trying to login as a user that doesn't exist
    """
    response = await client.post(
        "/basic/login/",
        data={"username": "foo", "password": "bar"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_fail_invalid_credentials(client: AsyncClient, session: AsyncSession):
    """
    Test failing on invalid user credentials
    """
    await client.post(
        "/basic/user/",
        data={"email": "dj@datajunction.io", "username": "dj", "password": "incorrect"},
    )
    with pytest.raises(DJException) as exc_info:
        await basic.validate_user_password(
            username="dj",
            password="dj",
            session=session,
        )
    assert "Invalid password for user dj" in str(exc_info.value)


@pytest.mark.asyncio
async def test_fail_on_user_already_exists(client: AsyncClient):
    """
    Test failing when creating a user that already exists
    """
    await client.post(
        "/basic/user/",
        data={"email": "dj@datajunction.io", "username": "dj", "password": "dj"},
    )
    response = await client.post(
        "/basic/user/",
        data={"email": "dj@datajunction.io", "username": "dj", "password": "dj"},
    )
    assert response.status_code == 409
    assert response.json() == {
        "message": "User dj already exists.",
        "errors": [
            {
                "code": 2,
                "message": "User dj already exists.",
                "debug": None,
                "context": "",
            },
        ],
        "warnings": [],
    }


@pytest.mark.asyncio
async def test_whoami(client: AsyncClient):
    """
    Test the /whoami/ endpoint
    """
    response = await client.get("/whoami/")
    assert response.status_code in (200, 201)
    assert response.json() == {
        "id": 1,
        "username": "dj",
        "email": None,
        "name": None,
        "oauth_provider": "basic",
        "is_admin": False,
        "created_collections": [],
        "created_nodes": [],
        "created_tags": [],
    }
