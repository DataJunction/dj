"""
Google OAuth helper functions
"""

import logging
import secrets
from http import HTTPStatus
from typing import Optional
from urllib.parse import urljoin

import google_auth_oauthlib.flow
import requests
from google.auth.external_account_authorized_user import Credentials
from sqlalchemy import select

from datajunction_server.database.user import User
from datajunction_server.errors import DJAuthenticationException
from datajunction_server.internal.access.authentication.basic import get_password_hash
from datajunction_server.models.user import OAuthProvider
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)

settings = get_settings()
flow = (
    google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        settings.google_oauth_client_secret_file,
        scopes=[
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/userinfo.email",
            "openid",
        ],
        redirect_uri=urljoin(settings.url, "/google/token/"),
    )
    if settings.google_oauth_client_secret_file
    else None
)


def get_authorize_url(
    state: Optional[str] = None,
) -> google_auth_oauthlib.flow.Flow:
    """
    Get the authorize url for a Google OAuth app
    """
    authorization_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        state=state,
    )
    return authorization_url


def get_google_access_token(
    authorization_response_url: str,
) -> Credentials:
    """
    Exchange an authorization token for an access token
    """
    flow.fetch_token(authorization_response=authorization_response_url)
    return flow.credentials


def get_google_user(token: str) -> User:
    """
    Get the google user using an access token
    """
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    response = requests.get(
        "https://www.googleapis.com/oauth2/v2/userinfo?alt=json",
        headers=headers,
        timeout=10,
    )
    if response.status_code in (200, 201):
        raise DJAuthenticationException(
            http_status_code=HTTPStatus.FORBIDDEN,
            message=f"Error retrieving Google user: {response.text}",
        )
    user_data = response.json()
    if "message" in user_data and user_data["message"] == "Bad credentials":
        raise DJAuthenticationException(
            http_status_code=HTTPStatus.FORBIDDEN,
            message=f"Error retrieving Google user: {response.text}",
        )
    session = next(get_session())  # type: ignore
    existing_user = session.execute(
        select(User).where(User.email == user_data["login"]),
    ).scalar()
    if existing_user:
        _logger.info("OAuth user found")
        user = existing_user
    else:
        _logger.info("OAuth user does not exist, creating a new user")
        new_user = User(
            username=user_data["email"],
            email=user_data["email"],
            password=get_password_hash(secrets.token_urlsafe(13)),
            name=user_data["name"],
            oauth_provider=OAuthProvider.GOOGLE,
        )
        session.add(new_user)
        session.commit()
        session.refresh(new_user)
        user = new_user
    return user
