"""History database schema."""

from datetime import datetime, timezone
from functools import partial
from typing import Any, Dict, Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.typing import UTCDatetime


class History(Base):
    """
    An event to store as part of the server's activity history
    """

    __tablename__ = "history"
    __table_args__ = (
        Index("ix_history_entity_name", "entity_name", postgresql_using="btree"),
        Index("ix_history_user", "user", postgresql_using="btree"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    entity_type: Mapped[Optional[EntityType]] = mapped_column(
        Enum(EntityType),
        default=None,
    )
    entity_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    node: Mapped[Optional[str]] = mapped_column(String, default=None)
    version: Mapped[Optional[str]] = mapped_column(String, default=None)
    activity_type: Mapped[Optional[ActivityType]] = mapped_column(
        Enum(ActivityType),
        default=None,
    )
    user: Mapped[Optional[str]] = mapped_column(String, default=None)
    pre: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, default=dict)
    post: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, default=dict)
    details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, default=dict)
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    def __hash__(self) -> int:
        return hash(self.id)
