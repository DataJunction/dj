"""
Models for users and auth
"""
from enum import Enum
from typing import Optional

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.connection import Base


class OAuthProvider(Enum):
    """
    Support oauth providers
    """

    BASIC = "basic"
    GITHUB = "github"
    GOOGLE = "google"


class User(Base):  # pylint: disable=too-few-public-methods
    """Class for a user."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )
    username: Mapped[str]
    password: Mapped[Optional[str]]
    email: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    oauth_provider: Mapped[OAuthProvider] = mapped_column(sa.Enum(OAuthProvider))
    is_admin: Mapped[bool] = mapped_column(default=False)


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
