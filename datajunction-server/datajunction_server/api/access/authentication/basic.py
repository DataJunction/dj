"""
Basic OAuth Authentication Router
"""

from datetime import timedelta
from http import HTTPStatus

from fastapi import APIRouter, Depends, Form
from fastapi.responses import JSONResponse, Response
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.constants import AUTH_COOKIE, LOGGED_IN_FLAG_COOKIE
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.errors import DJAlreadyExistsException, DJError, ErrorCode
from datajunction_server.internal.access.authentication.basic import (
    get_password_hash,
    validate_user_password,
)
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.utils import Settings, get_session, get_settings

router = APIRouter(tags=["Basic OAuth2"])


@router.post("/basic/user/")
async def create_a_user(
    email: str = Form(),
    username: str = Form(),
    password: str = Form(),
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    """
    Create a new user
    """
    user_result = await session.execute(select(User).where(User.username == username))
    if user_result.unique().scalar_one_or_none():
        raise DJAlreadyExistsException(
            errors=[
                DJError(
                    code=ErrorCode.ALREADY_EXISTS,
                    message=f"User {username} already exists.",
                ),
            ],
        )
    new_user = User(
        email=email,
        username=username,
        password=get_password_hash(password),
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(new_user)
    await session.commit()
    await session.refresh(new_user)
    return JSONResponse(
        content={"message": "User successfully created"},
        status_code=HTTPStatus.CREATED,
    )


@router.post("/basic/login/")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
):
    """
    Get a JWT token and set it as an HTTP only cookie
    """
    user = await validate_user_password(
        username=form_data.username,
        password=form_data.password,
        session=session,
    )
    response = JSONResponse(
        content={"message": "Logged in successfully"},
        status_code=HTTPStatus.OK,
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


@router.post("/logout/")
def logout():
    """
    Logout a user by deleting the auth cookie
    """
    response = Response(status_code=HTTPStatus.OK)
    response.delete_cookie(AUTH_COOKIE, httponly=True)
    response.delete_cookie(LOGGED_IN_FLAG_COOKIE)
    return response
