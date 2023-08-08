"""
Basic OAuth and JWT helper functions
"""
from http import HTTPStatus

from passlib.context import CryptContext
from sqlmodel import Session, select

from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.models import User

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
