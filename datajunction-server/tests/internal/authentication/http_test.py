"""
Test internal http authentication logic
"""

import asyncio
from unittest.mock import MagicMock

from sqlalchemy.ext.asyncio import AsyncSession
import pytest

from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.http import DJHTTPBearer
from datajunction_server.models.user import OAuthProvider, UserOutput


def test_dj_http_bearer_raise_when_unauthenticated():
    """
    Test raising when no cookie or auth headers are provided
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = None
    request.headers.get.return_value = None
    with pytest.raises(DJException) as exc_info:
        asyncio.run(bearer(request))
    assert "Not authenticated" in str(exc_info.value)


def test_dj_http_bearer_raise_with_empty_bearer_token():
    """
    Test raising when the authorization header has an empty bearer token
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = None
    request.headers.get.return_value = "Bearer "
    with pytest.raises(DJException) as exc_info:
        asyncio.run(bearer(request))
    assert "Not authenticated" in str(exc_info.value)


def test_dj_http_bearer_raise_with_unsupported_scheme(jwt_token):
    """
    Test raising when the authorization header scheme is not a supported one
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = None
    request.headers.get.return_value = f"Foo {jwt_token}"
    with pytest.raises(DJException) as exc_info:
        asyncio.run(bearer(request))
    assert "Invalid authentication credentials" in str(exc_info.value)


def test_dj_http_bearer_raise_with_non_jwt_token():
    """
    Test raising when the token can't be parsed as a JWT
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = None
    request.headers.get.return_value = "Foo NotAJWT"
    with pytest.raises(DJException) as exc_info:
        asyncio.run(bearer(request))
    assert "Invalid authentication credentials" in str(exc_info.value)


def test_dj_http_bearer_w_cookie(
    jwt_token: str,
    session: AsyncSession,
    current_user: User,
):
    """
    Test using the DJHTTPBearer middleware with a cookie
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = jwt_token

    asyncio.run(bearer(request, session))
    assert UserOutput.from_orm(request.state.user).dict() == {
        "id": 1,
        "username": "dj",
        "email": "dj@datajunction.io",
        "name": "DJ",
        "oauth_provider": OAuthProvider.BASIC,
        "is_admin": False,
        "created_collections": [],
        "created_nodes": [],
        "created_tags": [],
        "owned_nodes": [],
    }


def test_dj_http_bearer_w_auth_headers(
    jwt_token: str,
    session: AsyncSession,
    current_user: User,
):
    """
    Test using the DJHTTPBearer middleware with an authorization header
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = None
    request.headers.get.return_value = f"Bearer {jwt_token}"

    asyncio.run(bearer(request, session))
    assert UserOutput.from_orm(request.state.user).dict() == {
        "id": 1,
        "username": "dj",
        "email": "dj@datajunction.io",
        "name": "DJ",
        "oauth_provider": OAuthProvider.BASIC,
        "is_admin": False,
        "created_collections": [],
        "created_nodes": [],
        "created_tags": [],
        "owned_nodes": [],
    }


def test_raise_on_non_jwt_cookie():
    """
    Test using the DJHTTPBearer middleware with a cookie
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = "NotAJWT"
    with pytest.raises(DJException) as exc_info:
        asyncio.run(bearer(request))
    assert "Cannot decode authorization token" in str(exc_info.value)
