"""
Test internal http authentication logic
"""
import asyncio
from unittest.mock import MagicMock

import pytest

from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.http import DJHTTPBearer
from datajunction_server.models.user import OAuthProvider, UserOutput

EXAMPLE_TOKEN = (
    "eyJhbGciOiJkaXIiLCJlbmMiOiJBMTI4R0NNIn0..SxGbG0NRepMY4z9-2-ZZdg.ug"
    "0FvJUoybiGGpUItL4VbM1O_oinX7dMBUM1V3OYjv30fddn9m9UrrXxv3ERIyKu2zVJ"
    "xx1gSoM5k8petUHCjatFQqA-iqnvjloFKEuAmxLdCHKUDgfKzCIYtbkDcxtzXLuqlj"
    "B0-ConD6tpjMjFxNrp2KD4vwaS0oGsDJGqXlMo0MOhe9lHMLraXzOQ6xDgDFHiFert"
    "Fc0T_9jYkcpmVDPl9pgPf55R.sKF18rttq1OZ_EjZqw8Www"
)


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


def test_dj_http_bearer_raise_with_unsupported_scheme():
    """
    Test raising when the authorization header scheme is not a supported one
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = None
    request.headers.get.return_value = f"Foo {EXAMPLE_TOKEN}"
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


def test_dj_http_bearer_w_cookie():
    """
    Test using the DJHTTPBearer middleware with a cookie
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = EXAMPLE_TOKEN
    asyncio.run(bearer(request))
    assert UserOutput.from_orm(request.state.user).dict() == {
        "id": 1,
        "username": "dj",
        "email": None,
        "name": None,
        "oauth_provider": OAuthProvider.BASIC,
        "is_admin": False,
        "created_collections": [],
        "created_nodes": [],
        "created_tags": [],
    }


def test_dj_http_bearer_w_auth_headers():
    """
    Test using the DJHTTPBearer middleware with an authorization header
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = None
    request.headers.get.return_value = f"Bearer {EXAMPLE_TOKEN}"
    asyncio.run(bearer(request))
    assert UserOutput.from_orm(request.state.user).dict() == {
        "id": 1,
        "username": "dj",
        "email": None,
        "name": None,
        "oauth_provider": OAuthProvider.BASIC,
        "is_admin": False,
        "created_collections": [],
        "created_nodes": [],
        "created_tags": [],
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
