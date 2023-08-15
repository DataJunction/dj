"""JWT related functions"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

from fastapi import Header, Request
from jose import jwe, jwt
from passlib.context import CryptContext

from datajunction_server.utils import get_settings

_logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def create_jwt(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Return an encoded JSON web token for a dictionary
    """
    settings = get_settings()
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode,
        settings.secret,
        algorithm="HS256",
    )
    return encoded_jwt


async def decode_jwt(token: str) -> dict:
    """
    Decodes a JWT token
    """
    settings = get_settings()
    return jwt.decode(
        token,
        settings.secret,
        algorithms=["HS256"],
    )


async def get_jwt(request: Request) -> Optional[Dict]:
    """
    Get a DJ user from a request object by parsing the "__dj" cookie

    Raise:
        JWTError: If the JWT token is malformed or the signature fails
        AttributeError: If no "__dj" cookie is found on the request
    """
    _logger.info(
        "Checking for an authorization headers",
    )
    token = await get_token(request.headers.get("Authorization"))
    return await decode_jwt(token)


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


async def get_token(
    authorization: str = Header(default="Bearer "),
) -> str:
    """
    Get an authorization bearer token from an authorization header
    """
    _, token = authorization.split(" ")
    return token
