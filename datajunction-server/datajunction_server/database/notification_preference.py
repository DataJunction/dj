"""History database schema."""

from datetime import datetime, timezone
from functools import partial

from sqlalchemy import (
    ARRAY,
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.database.history import ActivityType, EntityType
from datajunction_server.database.user import User
from datajunction_server.typing import UTCDatetime


class NotificationPreference(Base):  # pylint: disable=too-few-public-methods
    """
    User notification preferences for a specific entity and activity type.
    """

    __tablename__ = "notification_preferences"
    __table_args__ = (
        UniqueConstraint("entity_type", "entity_name", name="uix_entity_type_name"),
        Index("ix_entity_name", "entity_name"),
        Index("ix_entity_type", "entity_type"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    entity_type: Mapped[EntityType] = mapped_column(
        Enum(EntityType, name="notification_entitytype"),
    )
    entity_name: Mapped[str] = mapped_column(String)
    activity_types: Mapped[list[ActivityType]] = mapped_column(
        ARRAY(
            Enum(ActivityType, name="notification_activitytype"),
        ),  # Use ARRAY of enums
        default=list,
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id"),
        nullable=False,
    )
    user: Mapped["User"] = relationship(
        "User",
        back_populates="notification_preferences",
    )
    alert_types: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )
