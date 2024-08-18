"""User database schema."""
from typing import TYPE_CHECKING, Optional

from sqlalchemy import BigInteger, Enum, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.enum import StrEnum

if TYPE_CHECKING:
    from datajunction_server.database.collection import Collection
    from datajunction_server.database.node import Node, NodeRevision
    from datajunction_server.database.tag import Tag


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
    username: Mapped[str] = mapped_column(String, unique=True)
    password: Mapped[Optional[str]]
    email: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    oauth_provider: Mapped[OAuthProvider] = mapped_column(
        Enum(OAuthProvider),
    )
    is_admin: Mapped[bool] = mapped_column(default=False)
    created_collections: Mapped[list["Collection"]] = relationship(
        "Collection",
        back_populates="created_by",
        lazy="joined",
    )
    created_nodes: Mapped[list["Node"]] = relationship(
        "Node",
        back_populates="created_by",
        lazy="joined",
    )
    created_node_revisions: Mapped[list["NodeRevision"]] = relationship(
        "NodeRevision",
        back_populates="created_by",
        lazy="joined",
    )
    created_tags: Mapped[list["Tag"]] = relationship(
        "Tag",
        back_populates="created_by",
        lazy="joined",
    )
