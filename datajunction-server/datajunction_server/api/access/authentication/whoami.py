"""
Router for getting the current active user
"""

from datetime import timedelta
from http import HTTPStatus
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi import Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.models.user import UserOutput
from datajunction_server.utils import (
    Settings,
    get_current_user,
    get_session,
    get_settings,
)

router = SecureAPIRouter(tags=["Who am I?"])


@router.get("/whoami/")
async def whoami(
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    """
    Returns the current authenticated user
    """
    statement = select(
        User.id,
        User.username,
        User.email,
        User.name,
        User.oauth_provider,
        User.is_admin,
        User.last_viewed_notifications_at,
    ).where(User.username == current_user.username)
    result = await session.execute(statement)
    user = result.one_or_none()
    return UserOutput(
        id=user[0],
        username=user[1],
        email=user[2],
        name=user[3],
        oauth_provider=user[4],
        is_admin=user[5],
        last_viewed_notifications_at=user[6],
    )


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
