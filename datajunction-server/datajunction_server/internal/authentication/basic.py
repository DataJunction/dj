"""
Basic OAuth and JWT helper functions
"""
import logging
from http import HTTPStatus

from fastapi import Depends, Request
from jose import JWTError
from passlib.context import CryptContext
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from datajunction_server.constants import UNAUTHENTICATED_ENDPOINTS
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.internal.authentication.jwt import decrypt, get_jwt
from datajunction_server.models import User
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
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


async def parse_basic_auth_cookie(
    request: Request,
    session: Session = Depends(get_session),
):
    """
    Parse an "__dj" cookie for basic auth
    """
    settings = get_settings()
    github_oauth_configured = (
        all(
            [
                settings.secret,
                settings.github_oauth_client_id,
                settings.github_oauth_client_secret,
            ],
        )
        or False
    )
    _logger.info("Attempting to get basic authenticated user from request cookie")
    jwt = None
    try:
        jwt = get_jwt(request=request)
    except (JWTError, AttributeError):
        pass
    encrypted_username = jwt.get("sub") if jwt else None
    username = decrypt(encrypted_username) if encrypted_username else None
    user = None
    try:
        user = session.exec(select(User).where(User.username == username)).one()
    except NoResultFound:
        pass
    if (
        not user
        and not any([github_oauth_configured])
        and request.url.path not in UNAUTHENTICATED_ENDPOINTS
    ):
        # We must respond as unauthorized here if user is None
        # because there are no more layers of auth middleware
        raise DJException(
            http_status_code=HTTPStatus.UNAUTHORIZED,
            errors=[
                DJError(
                    code=ErrorCode.OAUTH_ERROR,
                    message="This endpoint requires authentication.",
                ),
            ],
        )
    request.state.user = user
