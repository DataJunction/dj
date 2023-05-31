"""
User models
"""
from typing import Any, Dict, Optional

from pydantic import BaseModel


class Token(BaseModel):
    """
    OAuth2 access token response
    """

    access_token: str
    token_type: str
    expires_in: Optional[int]
    refresh_token: Optional[str]
    scope: Optional[str]


class User(BaseModel):
    """
    A user
    """

    username: str
    details: Dict[Any, Any] = {}
