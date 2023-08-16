"""
Test internal http authentication logic
"""
import asyncio
from unittest.mock import MagicMock

import pytest

from datajunction_server.errors import DJException
from datajunction_server.internal.authentication.http import DJHTTPBearer
from datajunction_server.models.user import OAuthProvider, User

EXAMPLE_TOKEN = (
    "eyJhbGciOiJkaXIiLCJlbmMiOiJBMTI4R0NNIn0..-w7EJW7ZZzaX5pfW1dGcGQ.JcyKwLTOru1nVAre63kF1rw"
    "jTmv3sfshaujyhEeaY7Y_G-LUwSfarz2jM5S8oKYQsDLH4PSEcrY7APOLLpkjTIndCRnBz4UigCDAE9g-aLpw0ju4Fl4rz"
    "N1CUJ_Tas-VFxwN05B9sX5pA8m2pE2xhYStvIFDxQsrbqUVQejO3w.syaacHYmeNgo18xP2aqMhg"
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


def test_dj_http_bearer_w_cookie():
    """
    Test using the DJHTTPBearer middleware with a cookie
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = EXAMPLE_TOKEN
    asyncio.run(bearer(request))
    assert request.state.user == User(
        id=1,
        username="dj",
        password=None,
        email=None,
        name=None,
        oauth_provider=OAuthProvider.BASIC,
        is_admin=False,
    )


def test_dj_http_bearer_w_auth_headers():
    """
    Test using the DJHTTPBearer middleware with an authorization header
    """
    bearer = DJHTTPBearer()
    request = MagicMock()
    request.cookies.get.return_value = None
    request.headers.get.return_value = f"Bearer {EXAMPLE_TOKEN}"
    asyncio.run(bearer(request))
    assert request.state.user == User(
        id=1,
        username="dj",
        password=None,
        email=None,
        name=None,
        oauth_provider=OAuthProvider.BASIC,
        is_admin=False,
    )
