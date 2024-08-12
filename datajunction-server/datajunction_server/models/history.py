"""
Model for history.
"""
from typing import TYPE_CHECKING, Any, Dict, Optional

from pydantic.main import BaseModel

from datajunction_server.database.history import ActivityType, EntityType, History
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.database.node import NodeRevision
    from datajunction_server.database.user import User
    from datajunction_server.models.node import NodeStatus


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


def status_change_history(
    node_revision: "NodeRevision",
    start_status: "NodeStatus",
    end_status: "NodeStatus",
    current_user: "User",
    parent_node: str = None,
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
        user=current_user.username,
    )
