"""
GitHub OAuth helper functions
"""
import logging
import secrets
from typing import Optional
from urllib.parse import urljoin

import requests
from sqlalchemy.exc import NoResultFound
from sqlmodel import select

from datajunction_server.internal.access.authentication.basic import get_password_hash
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


def get_github_user(access_token: str) -> Optional[User]:  # pragma: no cover
    """
    Get the user for a request
    """
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
    user_data = requests.get(
        "https://api.github.com/user",
        headers=headers,
        timeout=10,
    ).json()
    if "message" in user_data and user_data["message"] == "Bad credentials":
        return None
    session = next(get_session())
    existing_user: Optional[User] = None
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
