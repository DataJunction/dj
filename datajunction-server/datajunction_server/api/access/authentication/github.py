"""
GitHub OAuth Authentication Router
"""

import logging
from datetime import timedelta
from http import HTTPStatus

import requests
from fastapi import APIRouter, Depends, Response
from fastapi.responses import JSONResponse, RedirectResponse

from datajunction_server.constants import AUTH_COOKIE, LOGGED_IN_FLAG_COOKIE
from datajunction_server.errors import (
    DJAuthenticationException,
    DJConfigurationException,
    DJError,
    ErrorCode,
)
from datajunction_server.internal.access.authentication import github
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.utils import Settings, get_settings

_logger = logging.getLogger(__name__)
router = APIRouter(tags=["GitHub OAuth2"])


@router.get("/github/login/", status_code=HTTPStatus.FOUND)
def login() -> RedirectResponse:  # pragma: no cover
    """
    Login
    """
    settings = get_settings()
    if not settings.github_oauth_client_id:
        raise DJConfigurationException(
            http_status_code=HTTPStatus.NOT_IMPLEMENTED,
            errors=[
                DJError(
                    code=ErrorCode.OAUTH_ERROR,
                    message="GITHUB_OAUTH_CLIENT_ID is not set",
                ),
            ],
        )
    return RedirectResponse(
        url=github.get_authorize_url(oauth_client_id=settings.github_oauth_client_id),
        status_code=HTTPStatus.FOUND,
    )


@router.get("/github/token/")
def get_access_token(
    code: str,
    response: Response,
    settings: Settings = Depends(get_settings),
) -> JSONResponse:  # pragma: no cover
    """
    Get an access token using OAuth code
    """
    settings = get_settings()
    params = {
        "client_id": settings.github_oauth_client_id,
        "client_secret": settings.github_oauth_client_secret,
        "code": code,
    }
    headers = {"Accept": "application/json"}
    access_data = requests.post(
        url="https://github.com/login/oauth/access_token",
        params=params,
        headers=headers,
        timeout=10,  # seconds
    ).json()
    if "error" in access_data:
        raise DJAuthenticationException(
            errors=[
                DJError(
                    code=ErrorCode.OAUTH_ERROR,
                    message=(
                        "Received an error from the GitHub authorization "
                        f"server: {access_data['error']}"
                    ),
                ),
            ],
        )
    if "access_token" not in access_data:
        message = "No user access token retrieved from GitHub OAuth API"
        _logger.error(message)
        raise DJAuthenticationException(
            errors=[DJError(message=message, code=ErrorCode.OAUTH_ERROR)],
        )
    user = github.get_github_user(access_data["access_token"])
    response = RedirectResponse(
        url=settings.frontend_host,
    )
    if not user:
        raise DJAuthenticationException(
            http_status_code=HTTPStatus.UNAUTHORIZED,
            errors=DJError(
                code=ErrorCode.OAUTH_ERROR,
                message=(
                    "Could not retrieve user using the GitHub provided access token"
                ),
            ),
        )
    response.set_cookie(
        AUTH_COOKIE,
        create_token(
            {"username": user.username},
            secret=settings.secret,
            iss=settings.url,
            expires_delta=timedelta(days=365),
        ),
        httponly=True,
    )
    response.set_cookie(
        LOGGED_IN_FLAG_COOKIE,
        "true",
    )
    return response
