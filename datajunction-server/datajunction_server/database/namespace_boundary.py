"""Governed namespace boundary database schema."""

from datetime import UTC, datetime
from functools import partial

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime


class NamespaceBoundary(Base):
    """
    A durable namespace policy linked to its generated owner and deployer roles.

    The namespace name intentionally has no foreign key to ``nodenamespace``.
    Governance survives hard deletion of namespace metadata so recreating the
    namespace cannot orphan or replace its owner grants.
    """

    __tablename__ = "namespace_boundaries"

    namespace: Mapped[str] = mapped_column(String(500), primary_key=True)
    owner_role_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        ForeignKey(
            "roles.id",
            name="fk_namespace_boundaries_owner_role_id_roles",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    deployer_role_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        ForeignKey(
            "roles.id",
            name="fk_namespace_boundaries_deployer_role_id_roles",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_by_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        ForeignKey(
            "users.id",
            name="fk_namespace_boundaries_created_by_id_users",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, UTC),
    )

    __table_args__ = (
        CheckConstraint(
            "owner_role_id <> deployer_role_id",
            name="ck_namespace_boundaries_distinct_roles",
        ),
        Index(
            "idx_namespace_boundaries_owner_role_id",
            "owner_role_id",
            unique=True,
        ),
        Index(
            "idx_namespace_boundaries_deployer_role_id",
            "deployer_role_id",
            unique=True,
        ),
        Index("idx_namespace_boundaries_created_by_id", "created_by_id"),
    )
