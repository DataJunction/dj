"""
Module related to all things notifications
"""

from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.notification_preference import NotificationPreference
from datajunction_server.database.history import EntityType


async def get_notification_preferences(
    session: AsyncSession,
    user: str,
    entity_name: Optional[str] = None,
    entity_type: Optional[EntityType] = None,
) -> List[NotificationPreference]:
    """
    Get all notification preferences for a user, optionally filtering to a specific entity name and/or entity type
    """
    statement = select(NotificationPreference).where(
        NotificationPreference.user == user,
    )
    if entity_name:
        statement = statement.where(NotificationPreference.entity_name == entity_name)
    if entity_type:
        statement = statement.where(NotificationPreference.entity_type == entity_type)
    result = await session.execute(statement)
    return result.scalars().all()
