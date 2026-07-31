"""Notification Preference models"""

from pydantic import BaseModel, ConfigDict

from datajunction_server.internal.history import ActivityType, EntityType


class NotificationPreferenceModel(BaseModel):
    entity_type: EntityType
    entity_name: str | None
    activity_types: list[ActivityType]
    user_id: int
    username: str
    alert_types: list[str]


class NotificationPreferenceOutput(BaseModel):
    entity_type: EntityType
    entity_name: str | None
    activity_types: list[ActivityType]
    alert_types: list[str]

    model_config = ConfigDict(from_attributes=True)
