"""Registry of JSON Schemas for custom_metadata keys."""

import datetime
from functools import partial

import sqlalchemy as sa
from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime


class CustomMetadataSchema(Base):
    """A JSON Schema registered for a single custom_metadata key."""

    __tablename__ = "custommetadataschema"
    __table_args__ = (
        Index(
            "uq_custommetadataschema_key_scope",
            text("key"),
            text("coalesce(node_type, '')"),
            text("coalesce(namespace, '')"),
            unique=True,
            postgresql_where=text("deactivated_at IS NULL"),
        ),
    )

    # bigint, matching the migration.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    key: Mapped[str] = mapped_column(String, nullable=False)
    node_type: Mapped[str | None] = mapped_column(String, default=None)
    namespace: Mapped[str | None] = mapped_column(String, default=None)
    json_schema: Mapped[dict] = mapped_column(JSONB, nullable=False)
    value_kind: Mapped[str | None] = mapped_column(String, default=None)
    filterable: Mapped[bool] = mapped_column(default=True, server_default=sa.true())
    description: Mapped[str | None] = mapped_column(String, default=None)
    created_by_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("users.id"),
        default=None,
    )
    owner: Mapped[str | None] = mapped_column(String, default=None)
    updated_by_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("users.id"),
        default=None,
    )
    reserved: Mapped[bool] = mapped_column(default=False, server_default=sa.false())
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.datetime.now, datetime.UTC),
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.datetime.now, datetime.UTC),
        onupdate=partial(datetime.datetime.now, datetime.UTC),
    )
    deactivated_at: Mapped[UTCDatetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
    )
