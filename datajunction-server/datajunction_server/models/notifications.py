"""Notification Preference models"""

from typing import List, Optional

from pydantic.main import BaseModel

from datajunction_server.internal.history import ActivityType, EntityType


class NotificationPreferenceModel(BaseModel):
    entity_type: EntityType
    entity_name: Optional[str]
    activity_types: List[ActivityType]
    user_id: int
    username: str
    alert_types: List[str]
