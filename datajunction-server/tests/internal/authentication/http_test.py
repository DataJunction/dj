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
    "eyJhbGciOiJkaXIiLCJlbmMiOiJBMTI4R0NNIn0..pMoQFVS0VMSAFsG5X0itfw.Lc"
    "8mo22qxeD1NQROlHkjFnmLiDXJGuhlSPcBOoQVlpQGbovHRHT7EJ9_vFGBqDGihul1"
    "BcABiJT7kJtO6cZCJNkykHx-Cbz7GS_6ZQs1_kR5FzsvrJt5_X-dqehVxCFATjv64-"
    "Lokgj9ciOudO2YoBW61UWoLdpmzX1A_OPgv9PlAX23owZrFbPcptcXSJPJQVwvvy8h"
    "DgZ1M6YtqZt_T7o0G2QmFukk.e0ZFTP0H5zP4_wZA3sIrxw"
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
