"""
Tests for basic auth helper functions
"""
import asyncio
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from requests import Request
from sqlmodel import Session

from datajunction_server.errors import DJException
from datajunction_server.internal.authentication import basic


def test_hash_and_verify_password():
    """
    Test hashing a password and verifying a password against a hash
    """
    hashed_password = basic.get_password_hash(password="foo")
    assert basic.verify_password(plain_password="foo", hashed_password=hashed_password)


def test_get_user_info(client: TestClient, session: Session):
    """
    Test getting user info given a username and a password
    """
    client.post("/basic/user/", data={"username": "dj", "password": "dj"})
    user = basic.get_user_info(username="dj", password="dj", session=session)
    assert user.username == "dj"


def test_fail_invalid_credentials(client: TestClient, session: Session):
    """
    Test failing on invalid user credentials
    """
    client.post("/basic/user/", data={"username": "dj", "password": "incorrect"})
    with pytest.raises(DJException) as exc_info:
        basic.get_user_info(username="dj", password="dj", session=session)
    assert "Invalid username or password" in str(exc_info.value)


def test_fail_on_user_already_exists(client: TestClient):
    """
    Test failing when creating a user that already exists
    """
    client.post("/basic/user/", data={"username": "dj", "password": "dj"})
    response = client.post("/basic/user/", data={"username": "dj", "password": "dj"})
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


def test_parse_basic_auth_cookie(client: TestClient, session: Session):
    """
    Test parsing a basic auth cookie
    """
    response = client.post("/basic/user/", data={"username": "dj", "password": "dj"})
    assert response.ok
    response = client.post("/basic/login/", data={"username": "dj", "password": "dj"})
    assert response.ok
    request = Request("GET", "/metrics/", cookies=response.cookies)
    request.url = MagicMock()
    request.state = MagicMock()  # type: ignore
    asyncio.run(basic.parse_basic_auth_cookie(request=request, session=session))
    assert request.state.user.username == "dj"  # type: ignore


def test_failing_parse_basic_auth_cookie(session: Session):
    """
    Test failing on parsing a basic auth cookie
    """
    request = Request("GET", "/metrics/", cookies={"__dj": "foo"})
    request.url = MagicMock()
    with pytest.raises(DJException) as exc_info:
        asyncio.run(basic.parse_basic_auth_cookie(request=request, session=session))
    assert "This endpoint requires authentication." in str(exc_info.value)


def test_basic_whoami(client: TestClient):
    """
    Test the /basic/whoami/ endpoint
    """
    response = client.get("/basic/whoami/")
    assert response.ok
    assert response.json() == {
        "id": None,
        "username": "dj",
        "email": None,
        "name": None,
        "oauth_provider": "basic",
        "is_admin": False,
    }
