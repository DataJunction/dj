"""Node - owners database schema."""

from sqlalchemy import (
    ForeignKey,
    Index,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datajunction_server.database.base import Base


class NodeOwner(Base):
    """
    Join table for users and nodes that represents ownership
    """

    __tablename__ = "node_owners"
    __table_args__ = (
        Index("idx_node_owners_node_id", "node_id"),
        Index("idx_node_owners_user_id", "user_id"),
    )

    node_id: Mapped[int] = mapped_column(
        ForeignKey(
            "node.id",
            name="fk_node_owners_node_id",
        ),
        primary_key=True,
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey(
            "users.id",
            name="fk_node_owners_user_id",
        ),
        primary_key=True,
    )
    ownership_type: Mapped[str] = mapped_column(
        String(256),
        nullable=True,
    )

    node = relationship(
        "Node",
        back_populates="owner_associations",
        viewonly=True,
    )
    user = relationship(
        "User",
        back_populates="owned_associations",
        viewonly=True,
    )
