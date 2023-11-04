"""
Models for users and auth
"""
from enum import Enum
from typing import TYPE_CHECKING, List, Optional

from pydantic import BaseModel
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from datajunction_server.models.node import Node, NodeNamespace
    from datajunction_server.models.tag import Tag


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
    nodes: List["Node"] = Relationship(back_populates="created_by")
    namespaces: List["NodeNamespace"] = Relationship(back_populates="created_by")
    tags: List["Tag"] = Relationship(back_populates="created_by")


class UserOutput(BaseModel):
    """User information to be included in responses"""

    id: Optional[int] = Field(default=None, primary_key=True)
    username: str
    email: Optional[str]
    name: Optional[str]
    oauth_provider: OAuthProvider
    is_admin: bool = False
