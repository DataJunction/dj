"""Notification Preference models"""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from datajunction_server.internal.history import ActivityType, EntityType


class NotificationPreferenceModel(BaseModel):
    entity_type: EntityType
    entity_name: Optional[str]
    activity_types: List[ActivityType]
    user_id: int
    username: str
    alert_types: List[str]


class NotificationPreferenceOutput(BaseModel):
    entity_type: EntityType
    entity_name: Optional[str]
    activity_types: List[ActivityType]
    alert_types: List[str]

    model_config = ConfigDict(from_attributes=True)
