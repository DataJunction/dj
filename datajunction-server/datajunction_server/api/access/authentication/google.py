"""
Google OAuth Router
"""
import logging
import secrets
from datetime import timedelta
from http import HTTPStatus
from typing import Optional
from urllib.parse import urljoin, urlparse

import google.auth.transport.requests
import google.oauth2.credentials
import requests
from fastapi import APIRouter, Depends, Request
from google.oauth2 import id_token
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import RedirectResponse

from datajunction_server.constants import AUTH_COOKIE, LOGGED_IN_FLAG_COOKIE
from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.basic import get_password_hash
from datajunction_server.internal.access.authentication.google import (
    flow,
    get_authorize_url,
)
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.models.user import OAuthProvider
from datajunction_server.utils import Settings, get_session, get_settings

_logger = logging.getLogger(__name__)
router = APIRouter(tags=["Google OAuth"])
settings = get_settings()


@router.get("/google/login/", status_code=HTTPStatus.FOUND)
def login(target: Optional[str] = None):
    """
    Login using Google OAuth
    """
    return RedirectResponse(
        url=get_authorize_url(state=target),
        status_code=HTTPStatus.FOUND,
    )


@router.get("/google/token/")
async def get_access_token(
    request: Request,
    state: Optional[str] = None,
    error: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    setting: Settings = Depends(get_settings),
):
    """
    Perform a token exchange, exchanging a google auth code for a google access token.
    The google access token is then used to request user information and return a JWT
    cookie. If the user does not already exist, a new user is created.
    """
    if error:
        raise DJException(
            http_status_code=HTTPStatus.UNAUTHORIZED,
            message="Ran into an error during Google auth: {error}",
        )
    hostname = urlparse(settings.url).hostname
    url = str(request.url)
    flow.fetch_token(authorization_response=url)
    credentials = flow.credentials
    request_session = requests.session()
    token_request = google.auth.transport.requests.Request(session=request_session)
    user_data = id_token.verify_oauth2_token(
        id_token=credentials._id_token,  # pylint: disable=protected-access
        request=token_request,
        audience=setting.google_oauth_client_id,
    )

    existing_user = (
        await session.execute(
            select(User).where(User.email == user_data["email"]),
        )
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
        await session.commit()
        await session.refresh(new_user)
        user = new_user
    response = RedirectResponse(url=urljoin(settings.frontend_host, state))  # type: ignore
    response.set_cookie(
        AUTH_COOKIE,
        create_token(
            {"username": user.email},
            secret=settings.secret,
            iss=settings.url,
            expires_delta=timedelta(days=365),
        ),
        httponly=True,
        samesite="none",
        secure=True,
        domain=hostname,
    )
    response.set_cookie(
        LOGGED_IN_FLAG_COOKIE,
        "true",
        samesite="none",
        secure=True,
        domain=hostname,
    )
    return response
