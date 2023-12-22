"""
Models for users and auth
"""
from typing import Optional

from pydantic import BaseModel

from datajunction_server.database.user import OAuthProvider


class UserOutput(BaseModel):
    """User information to be included in responses"""

    id: int
    username: str
    email: Optional[str]
    name: Optional[str]
    oauth_provider: OAuthProvider
    is_admin: bool = False

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True
