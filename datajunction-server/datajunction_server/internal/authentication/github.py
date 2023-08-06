"""
GitHub OAuth helper functions
"""
import logging
import secrets
from typing import Dict, Optional
from urllib.parse import urljoin

import requests
from fastapi import Request
from sqlmodel import select

from datajunction_server.internal.authentication.basic import get_password_hash
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
        f"&scope=user&redirect_uri={redirect_uri}"
    )


def get_github_user_from_cookie(request: Request) -> Optional[Dict]:
    """
    Get the user for a request
    """
    token = request.cookies.get("access_token")
    if not token:
        return None
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    user_data = requests.get(
        "https://api.github.com/user",
        headers=headers,
        timeout=10,
    ).json()
    if "message" in user_data and user_data["message"] == "Bad credentials":
        return None
    session = next(get_session())
    existing_user = session.exec(
        select(User).where(User.username == user_data["login"]),
    ).one_or_none()
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
