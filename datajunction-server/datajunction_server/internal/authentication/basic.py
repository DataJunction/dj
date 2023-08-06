"""
Basic OAuth and JWT helper functions
"""
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Dict, Optional

from fastapi import Request
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlmodel import Session, select

from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.models import User
from datajunction_server.utils import get_session, get_settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password, hashed_password):
    """
    Verify a plain-text password against a hashed password
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    """
    Returns a hashed version of a plain-text password
    """
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: timedelta | None = None):
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
        settings.basic_oauth_client_secret,
        algorithm="HS256",
    )
    return encoded_jwt


def get_user_info(username: str, password: str, session: Session) -> User:
    """
    Get a DJ user using basic auth
    """
    user = session.exec(select(User).where(User.username == username)).one_or_none()
    if not user or not verify_password(password, user.password):
        raise DJException(
            http_status_code=HTTPStatus.UNAUTHORIZED,
            errors=[
                DJError(
                    message="Invalid username or password",
                    code=ErrorCode.INVALID_LOGIN_CREDENTIALS,
                ),
            ],
        )
    return user


def get_basic_user_from_cookie(request: Request) -> Optional[Dict]:
    """
    Get a DJ user from a request object by parsing the "access_token" cookie
    """
    settings = get_settings()
    session = next(get_session())
    token = request.cookies.get("access_token")
    try:
        payload = jwt.decode(
            token,
            settings.basic_oauth_client_secret,
            algorithms=["HS256"],
        )
    except (JWTError, AttributeError):
        return None
    username: str = payload.get("sub")
    user = session.exec(select(User).where(User.username == username)).one_or_none()
    return user
