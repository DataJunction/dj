"""
Auth dependencies
"""
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

from dj.models.user import Token, User

_logger = logging.getLogger(__name__)
router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)],  # pylint: disable=unused-argument
) -> User:  # pylint: disable=unused-argument
    """
    Overrideable placeholder function for getting a user from a token
    """
    return User(username="unknown")


class PermissionsChecker:  # pylint: disable=too-few-public-methods
    """
    Overrideable placeholder for checking if a user has required permissions
    """

    def __init__(self, request: Request):
        self.request = request

    def __call__(self, user: User):
        """
        A placeholder method for verifying if a user has the required permissions
        """
        permission = f"{self.request.method}:{self.request['path']}"
        _logger.error(
            "Checking if user %s has permission %s",
            user.username,
            permission,
        )


@router.post("/token", response_model=Token)
async def generate_an_auth_token(
    form_data: OAuth2PasswordRequestForm = Depends(),  # pylint: disable=unused-argument
):
    """
    Overrideable placeholder endpoint for generating a bearer token
    """
    return {"access_token": "unsecured", "token_type": "bearer"}
