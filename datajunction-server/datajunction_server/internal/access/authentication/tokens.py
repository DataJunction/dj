"""
Helper functions for authentication tokens
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from jose import jwe, jwt
from passlib.context import CryptContext

from datajunction_server.utils import get_settings

_logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def encrypt(value: str) -> str:
    """
    Encrypt a string value using the configured SECRET
    """
    settings = get_settings()
    return jwe.encrypt(
        value,
        settings.secret,
        algorithm="dir",
        encryption="A128GCM",
    ).decode("utf-8")


def decrypt(value: str) -> str:
    """
    Decrypt a string value using the configured SECRET
    """
    settings = get_settings()
    return jwe.decrypt(value, settings.secret).decode("utf-8")


def create_token(
    data: dict,
    secret: str,
    iss: str,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    Encode data into a signed JWT that's then encrypted using JWE.

    Tokens are created by encoding data (typically a user's information) into
    a signed JWT that's then encrypted using JWE. The resulting string can
    then be stored in a cookie or authorization headers. When returning a token
    in any form other than an HTTP-only cookie, it's important that a reasonably
    small expires_delta is provided, such as 24 hours.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:  # pragma: no cover
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    to_encode.update({"iss": iss})
    encoded_jwt = jwt.encode(
        to_encode,
        secret,
        algorithm="HS256",
    )
    return encrypt(encoded_jwt)


def decode_token(token: str) -> dict:
    """
    Decodes a token by first decrypting the JWE and then decoding the signed JWT.
    """
    settings = get_settings()
    decrypted_token = decrypt(token)
    decoded_jwt = jwt.decode(
        decrypted_token,
        settings.secret,
        algorithms=["HS256"],
        issuer=settings.url,
    )
    return decoded_jwt
