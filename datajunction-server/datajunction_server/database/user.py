"""User database schema."""
from typing import Optional

from sqlalchemy import BigInteger, Enum, Integer
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.enum import StrEnum


class OAuthProvider(StrEnum):
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
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    username: Mapped[str]
    password: Mapped[Optional[str]]
    email: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    oauth_provider: Mapped[OAuthProvider] = mapped_column(
        Enum(OAuthProvider),
    )
    is_admin: Mapped[bool] = mapped_column(default=False)
