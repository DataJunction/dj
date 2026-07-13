"""Registry of JSON Schemas for custom_metadata keys."""

import datetime
from functools import partial
from typing import Optional

from sqlalchemy import JSON, DateTime, ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime


class CustomMetadataSchema(Base):
    """A JSON Schema registered for a single custom_metadata key."""

    __tablename__ = "custommetadataschema"
    __table_args__ = (
        Index(
            "uq_custommetadataschema_key_type_ns",
            "key",
            "node_type",
            "namespace",
            unique=True,
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String, nullable=False)
    node_type: Mapped[Optional[str]] = mapped_column(String, default=None)
    namespace: Mapped[Optional[str]] = mapped_column(String, default=None)
    json_schema: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        nullable=False,
    )
    value_kind: Mapped[Optional[str]] = mapped_column(String, default=None)
    filterable: Mapped[bool] = mapped_column(default=True)
    description: Mapped[Optional[str]] = mapped_column(String, default=None)
    created_by_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id"),
        default=None,
    )
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.datetime.now, datetime.timezone.utc),
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.datetime.now, datetime.timezone.utc),
        onupdate=partial(datetime.datetime.now, datetime.timezone.utc),
    )
    deactivated_at: Mapped[Optional[UTCDatetime]] = mapped_column(
        DateTime(timezone=True),
        default=None,
    )
