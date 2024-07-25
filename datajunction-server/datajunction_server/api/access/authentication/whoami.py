"""
Router for getting the current active user
"""
from datetime import timedelta
from http import HTTPStatus

from fastapi import Depends, Request
from fastapi.responses import JSONResponse

from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.models.user import UserOutput
from datajunction_server.utils import (
    Settings,
    get_and_update_current_user,
    get_settings,
)

router = SecureAPIRouter(tags=["Who am I?"])


@router.get("/whoami/", response_model=UserOutput)
async def get_user(
    current_user: User = Depends(get_and_update_current_user),
) -> UserOutput:
    """
    Returns the current authenticated user
    """
    return UserOutput.from_orm(current_user)


@router.get("/token/")
async def get_short_lived_token(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> JSONResponse:
    """
    Returns a token that expires in 24 hours
    """
    expires_delta = timedelta(hours=24)
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "token": create_token(
                {"username": request.state.user.username},
                secret=settings.secret,
                iss=settings.url,
                expires_delta=expires_delta,
            ),
        },
    )
