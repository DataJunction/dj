"""
Auth dependencies
"""
import logging
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

from dj.models.user import Token, User

_logger = logging.getLogger(__name__)
router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


async def get_current_user(
    token: str = Security(oauth2_scheme),  # pylint: disable=unused-argument
) -> User:  # pylint: disable=unused-argument
    """
    Overrideable placeholder function for getting a user from a token
    """
    return User(username="admin")


class PermissionsChecker:  # pylint: disable=too-few-public-methods
    """
    Overrideable placeholder for checking if a user has required permissions
    """

    def __init__(self, request: Request):
        self.permission = f"{request.method}:{request['path']}"

    def __call__(self, user: User):
        """
        A placeholder method for verifying if a user has the required permissions
        """
        _logger.error(
            "Checking if user %s has permission %s",
            user.username,
            self.permission,
        )


@router.post("/token", response_model=Token)
async def generate_an_auth_token(
    form_data: OAuth2PasswordRequestForm = Depends(),  # pylint: disable=unused-argument
):
    """
    Overrideable placeholder endpoint for generating a bearer token
    """
    if form_data.username != "dj" or form_data.password != "dj":
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Incorrect username and/or password",
        )
    return {"access_token": "unsecure", "token_type": "bearer"}
