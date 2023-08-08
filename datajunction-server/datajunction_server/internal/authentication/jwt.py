"""JWT related functions"""

from datetime import datetime, timedelta
from typing import Dict, Optional

from fastapi import Request
from jose import jwe, jwt
from passlib.context import CryptContext

from datajunction_server.utils import get_settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def create_jwt(data: dict, expires_delta: timedelta | None = None):
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


def get_jwt(request: Request) -> Optional[Dict]:
    """
    Get a DJ user from a request object by parsing the "__dj" cookie

    Raise:
        JWTError: If the JWT token is malformed or the signature fails
        AttributeError: If no "__dj" cookie is found on the request
    """
    settings = get_settings()
    token = request.cookies.get("__dj")
    data = jwt.decode(
        token,
        settings.secret,
        algorithms=["HS256"],
    )
    return data


def encrypt(value: str):
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


def decrypt(value: str):
    """
    Decrypt a string value using the configured SECRET
    """
    settings = get_settings()
    return jwe.decrypt(value, settings.secret).decode("utf-8")
