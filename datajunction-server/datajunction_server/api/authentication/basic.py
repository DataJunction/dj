"""
Basic OAuth Authentication Router
"""
from http import HTTPStatus

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlmodel import Session

from datajunction_server.internal.authentication.basic import (
    create_access_token,
    get_password_hash,
    get_user_info,
)
from datajunction_server.models.user import OAuthProvider, User, UserOutput
from datajunction_server.utils import get_session

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/basic/login/")
router = APIRouter(tags=["Basic OAuth2"])


@router.post("/basic/user/")
async def create_a_user(
    username: str = Form(),
    password: str = Form(),
    session: Session = Depends(get_session),
) -> JSONResponse:
    """
    Create a new user
    """
    new_user = User(
        username=username,
        password=get_password_hash(password),
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(new_user)
    session.commit()
    session.refresh(new_user)
    return JSONResponse(
        content={"message": "User successfully created"},
        status_code=HTTPStatus.CREATED,
    )


@router.post("/basic/login/")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: Session = Depends(get_session),
):
    """
    Get a JWT token and set it as an HTTP only cookie
    """
    user = get_user_info(
        username=form_data.username,
        password=form_data.password,
        session=session,
    )
    token = create_access_token(data={"sub": user.username})
    response = JSONResponse(
        content={"message": "Successfully logged in through basic OAuth"},
        status_code=HTTPStatus.OK,
    )
    response.set_cookie(key="access_token", value=token, httponly=True)
    return response


@router.get("/basic/whoami/", response_model=UserOutput)
async def get_current_user(request: Request) -> UserOutput:
    """
    Returns the current authenticated user
    """
    return request.state.user
