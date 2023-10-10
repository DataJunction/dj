"""
Test JWT helper functions
"""
import asyncio
from datetime import timedelta

from datajunction_server.internal.access.authentication import tokens


def test_create_and_get_token():
    """
    Test creating a JWT and getting it back from a request
    """
    jwe_string = tokens.create_token(
        data={"foo": "bar"},
        expires_delta=timedelta(minutes=30),
    )
    data = asyncio.run(tokens.decode_token(jwe_string))
    assert data["foo"] == "bar"


def test_encrypt_and_decrypt():
    """
    Test encrypting and decrypting a value
    """
    encrypted_string = tokens.encrypt("foo")
    assert tokens.decrypt(encrypted_string) == "foo"
