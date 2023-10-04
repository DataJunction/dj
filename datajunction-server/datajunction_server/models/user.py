"""
Models for users and auth
"""
from enum import Enum
from typing import Optional

from pydantic import BaseModel
from sqlmodel import Field, SQLModel


class OAuthProvider(Enum):
    """
    Support oauth providers
    """

    BASIC = "basic"
    GITHUB = "github"
    GOOGLE = "google"


class User(SQLModel, table=True):  # type: ignore
    """Class for a user."""

    __tablename__ = "users"

    id: Optional[int] = Field(default=None, primary_key=True)
    username: str
    password: Optional[str]
    email: Optional[str]
    name: Optional[str]
    oauth_provider: OAuthProvider
    is_admin: bool = False


class UserOutput(BaseModel):
    """User information to be included in responses"""

    id: Optional[int] = Field(default=None, primary_key=True)
    username: str
    email: Optional[str]
    name: Optional[str]
    oauth_provider: OAuthProvider
    is_admin: bool = False
