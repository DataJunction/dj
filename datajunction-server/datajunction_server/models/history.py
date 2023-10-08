"""
Model for history.
"""
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from typing import Any, Dict, Optional

from sqlalchemy import DateTime
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlmodel import JSON, Field, SQLModel

from datajunction_server.models.node import NodeRevision, NodeStatus
from datajunction_server.models.user import User
from datajunction_server.typing import UTCDatetime


class ActivityType(str, Enum):
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


class EntityType(str, Enum):
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


class History(SQLModel, table=True):  # type: ignore
    """
    An event to store as part of the server's activity history
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    entity_type: Optional[EntityType] = Field(default=None)
    entity_name: Optional[str] = Field(default=None)
    node: Optional[str] = Field(default=None)
    activity_type: Optional[ActivityType] = Field(default=None)
    user: Optional[str] = Field(default=None)
    pre: Dict[str, Any] = Field(default_factory=dict, sa_column=SqlaColumn(JSON))
    post: Dict[str, Any] = Field(default_factory=dict, sa_column=SqlaColumn(JSON))
    details: Dict[str, Any] = Field(default_factory=dict, sa_column=SqlaColumn(JSON))
    created_at: UTCDatetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
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
