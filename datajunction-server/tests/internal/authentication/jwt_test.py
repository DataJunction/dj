"""
Test JWT helper functions
"""
import asyncio
from datetime import timedelta

from requests import Request

from datajunction_server.internal.authentication import jwt


def test_create_and_get_jwt():
    """
    Test creating a JWT and getting it back from a request
    """
    jwt_string = jwt.create_jwt(
        data={"foo": "bar"},
        expires_delta=timedelta(minutes=30),
    )
    request = Request(
        "GET",
        "/metrics/",
        cookies={"__dj": jwt_string},
        headers={"Authorization": f"Bearer {jwt_string}"},
    )
    data = asyncio.run(jwt.get_jwt(request=request))
    assert data["foo"] == "bar"


def test_encrypt_and_decrypt():
    """
    Test encrypting and decrypting a value
    """
    encrypted_string = jwt.encrypt("foo")
    assert jwt.decrypt(encrypted_string) == "foo"
