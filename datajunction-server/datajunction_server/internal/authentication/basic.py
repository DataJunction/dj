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

from datajunction_server.constants import SSE_ENDPOINTS, UNAUTHENTICATED_ENDPOINTS
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.internal.authentication.jwt import decrypt, get_token
from datajunction_server.models import User
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password, hashed_password) -> bool:
    """
    Verify a plain-text password against a hashed password
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password) -> str:
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
) -> None:
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
    auth_token = None
    try:
        if any(request.url.path.startswith(endpoint) for endpoint in SSE_ENDPOINTS):
            auth_token = request.query_params.get("token")
        else:
            auth_token = await get_token(request.headers.get("Authorization"))
    except (JWTError, AttributeError) as exc:
        _logger.error(str(exc))
    username = decrypt(auth_token) if auth_token else None
    _logger.info("User detected as %s", username)
    user = None
    try:
        user = session.exec(select(User).where(User.username == username)).one()
    except NoResultFound:
        _logger.error("Cannot find user %s", username)
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
