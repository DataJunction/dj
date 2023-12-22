"""
Basic OAuth and JWT helper functions
"""
import logging
from http import HTTPStatus

from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.orm import Session

from datajunction_server.database.user import User
from datajunction_server.errors import DJError, DJException, ErrorCode

_logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def validate_password_hash(plain_password, hashed_password) -> bool:
    """
    Verify a plain-text password against a hashed password
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password) -> str:
    """
    Returns a hashed version of a plain-text password
    """
    return pwd_context.hash(password)


def get_user(username: str, session: Session) -> User:
    """
    Get a DJ user
    """
    user = (
        session.execute(select(User).where(User.username == username))
        .scalars()
        .one_or_none()
    )
    if not user:
        raise DJException(
            http_status_code=HTTPStatus.UNAUTHORIZED,
            errors=[
                DJError(
                    message=f"User {username} not found",
                    code=ErrorCode.USER_NOT_FOUND,
                ),
            ],
        )
    return user


def validate_user_password(username: str, password: str, session: Session) -> User:
    """
    Get a DJ user and verify that the provided password matches the hashed password
    """
    user = get_user(username=username, session=session)
    if not validate_password_hash(password, user.password):
        raise DJException(
            http_status_code=HTTPStatus.UNAUTHORIZED,
            errors=[
                DJError(
                    message=f"Invalid password for user {username}",
                    code=ErrorCode.INVALID_LOGIN_CREDENTIALS,
                ),
            ],
        )
    return user
