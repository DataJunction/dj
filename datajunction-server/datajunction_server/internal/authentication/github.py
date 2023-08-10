"""
GitHub OAuth helper functions
"""
import logging
import secrets
from http import HTTPStatus
from typing import Optional
from urllib.parse import urljoin

import requests
from fastapi import Request
from jose import JWTError
from sqlalchemy.exc import NoResultFound
from sqlmodel import select

from datajunction_server.constants import UNAUTHENTICATED_ENDPOINTS
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.internal.authentication.basic import get_password_hash
from datajunction_server.internal.authentication.jwt import decrypt, get_jwt
from datajunction_server.models.user import OAuthProvider, User
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)


def get_authorize_url(oauth_client_id: str) -> str:
    """
    Get the authorize url for a GitHub OAuth app
    """
    settings = get_settings()
    redirect_uri = urljoin(settings.url, "/github/token/")
    return (
        f"https://github.com/login/oauth/authorize?client_id={oauth_client_id}"
        f"&scope=read:user&redirect_uri={redirect_uri}"
    )


def get_github_user(encrypted_access_token: str) -> Optional[User]:  # pragma: no cover
    """
    Get the user for a request
    """
    access_token = decrypt(encrypted_access_token)
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
    user_data = requests.get(
        "https://api.github.com/user",
        headers=headers,
        timeout=10,
    ).json()
    if "message" in user_data and user_data["message"] == "Bad credentials":
        return None
    session = next(get_session())
    existing_user = None
    try:
        existing_user = session.exec(
            select(User).where(User.username == user_data["login"]),
        ).one()
    except NoResultFound:
        pass
    if existing_user:
        _logger.info("OAuth user found")
        user = existing_user
    else:
        _logger.info("OAuth user does not exist, creating a new user")
        new_user = User(
            username=user_data["login"],
            password=get_password_hash(secrets.token_urlsafe(13)),
            email=user_data["email"],
            name=user_data["name"],
            oauth_provider=OAuthProvider.GITHUB,
        )
        session.add(new_user)
        session.commit()
        session.refresh(new_user)
        user = new_user
    return user


async def parse_github_auth_cookie(request: Request) -> None:  # pragma: no cover
    """
    Middleware for parsing a "__dj" cookie for GitHub auth
    """
    if not hasattr(request.state, "user") or not request.state.user:
        _logger.info(
            "Attempting to get GitHub authenticated user from request cookie",
        )
        jwt = None
        try:
            jwt = get_jwt(request=request)
        except (JWTError, AttributeError):
            pass
        encrypted_access_token = jwt.get("sub") if jwt else None
        user = (
            get_github_user(encrypted_access_token=encrypted_access_token)
            if encrypted_access_token
            else None
        )
        if not user:
            if request.url.path not in UNAUTHENTICATED_ENDPOINTS:
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
    else:
        _logger.info(
            "GitHub authentication not checked, user already "
            "set through a higher ranked auth scheme",
        )
