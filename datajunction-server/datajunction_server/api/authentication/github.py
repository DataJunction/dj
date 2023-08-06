"""
GitHub OAuth Authentication Router
"""

import logging
from http import HTTPStatus

import requests
from fastapi import APIRouter, Request, Response
from starlette.responses import JSONResponse, RedirectResponse

from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.internal.authentication import github
from datajunction_server.models.user import UserOutput
from datajunction_server.utils import get_settings

_logger = logging.getLogger(__name__)
router = APIRouter(tags=["GitHub OAuth2"])


@router.get("/github/login/", status_code=HTTPStatus.FOUND)
async def login():
    """
    Login
    """
    settings = get_settings()
    oauth_client_id = settings.github_oauth_client_id
    return RedirectResponse(
        url=github.get_authorize_url(oauth_client_id=oauth_client_id),
        status_code=HTTPStatus.FOUND,
    )


@router.get("/github/token/")
async def get_access_token(code: str, response: Response):
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
    if "access_token" not in access_data:
        message = "No user access token retrieved from GitHub OAuth API"
        _logger.error(message)
        raise DJException(
            http_status_code=HTTPStatus.UNAUTHORIZED,
            errors=[DJError(message=message, code=ErrorCode.OAUTH_ERROR)],
        )
    token = access_data["access_token"]
    response = JSONResponse(
        content={"message": "Successfully logged in through GitHub OAuth"},
        status_code=HTTPStatus.OK,
    )
    response.set_cookie(key="access_token", value=token, httponly=True)
    return response


@router.get("/github/whoami/", response_model=UserOutput)
async def get_current_user(request: Request) -> UserOutput:
    """
    Returns the current authenticated user
    """
    return request.state.user
