"""
Model for history.
"""
from datetime import datetime, timezone
from functools import partial
from typing import Any, Dict, Optional

from pydantic.main import BaseModel
from sqlalchemy import JSON, BigInteger, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.connection import Base
from datajunction_server.enum import StrEnum
from datajunction_server.models.base import sqlalchemy_enum_with_value
from datajunction_server.models.node import NodeRevision, NodeStatus
from datajunction_server.models.user import User
from datajunction_server.typing import UTCDatetime


class ActivityType(StrEnum):
    """
    An activity type
    """

    CREATE = "create"
    DELETE = "delete"
    RESTORE = "restore"
    UPDATE = "update"
    REFRESH = "refresh"
    TAG = "tag"
    SET_ATTRIBUTE = "set_attribute"
    STATUS_CHANGE = "status_change"


class EntityType(StrEnum):
    """
    An entity type for which activity can occur
    """

    ATTRIBUTE = "attribute"
    AVAILABILITY = "availability"
    BACKFILL = "backfill"
    CATALOG = "catalog"
    COLUMN_ATTRIBUTE = "column_attribute"
    DEPENDENCY = "dependency"
    ENGINE = "engine"
    LINK = "link"
    MATERIALIZATION = "materialization"
    NAMESPACE = "namespace"
    NODE = "node"
    PARTITION = "partition"
    QUERY = "query"
    TAG = "tag"


class HistoryOutput(BaseModel):
    """
    Output history event
    """

    id: int
    entity_type: Optional[EntityType]
    entity_name: Optional[str]
    node: Optional[str]
    activity_type: Optional[ActivityType]
    user: Optional[str]
    pre: Dict[str, Any]
    post: Dict[str, Any]
    details: Dict[str, Any]
    created_at: UTCDatetime

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class History(Base):  # pylint: disable=too-few-public-methods
    """
    An event to store as part of the server's activity history
    """

    __tablename__ = "history"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    entity_type: Mapped[Optional[EntityType]] = mapped_column(
        sqlalchemy_enum_with_value(EntityType),
        default=None,
    )
    entity_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    node: Mapped[Optional[str]] = mapped_column(String, default=None)
    activity_type: Mapped[Optional[ActivityType]] = mapped_column(
        sqlalchemy_enum_with_value(ActivityType),
        default=None,
    )
    user: Mapped[Optional[str]] = mapped_column(String, default=None)
    pre: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    post: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    details: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    def __hash__(self) -> int:
        return hash(self.id)


def status_change_history(
    node_revision: NodeRevision,
    start_status: NodeStatus,
    end_status: NodeStatus,
    parent_node: str = None,
    current_user: Optional[User] = None,
) -> History:
    """
    Returns a status change history activity entry
    """
    return History(
        entity_type=EntityType.NODE,
        entity_name=node_revision.name,
        node=node_revision.name,
        activity_type=ActivityType.STATUS_CHANGE,
        pre={"status": start_status},
        post={"status": end_status},
        details={"upstream_node": parent_node if parent_node else None},
        user=current_user.username if current_user else None,
    )
