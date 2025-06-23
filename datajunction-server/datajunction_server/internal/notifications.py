"""
Module related to all things notifications
"""

from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datajunction_server.database.notification_preference import NotificationPreference
from datajunction_server.internal.history import EntityType


async def get_user_notification_preferences(
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
    statement = statement.options(selectinload(NotificationPreference.user))
    result = await session.execute(statement)
    return result.scalars().all()


async def get_entity_notification_preferences(
    session: AsyncSession,
    entity_name: str,
    entity_type: EntityType,
) -> List[NotificationPreference]:
    """
    Get all user preferences for a specific notification preference
    """
    result = await session.execute(
        select(NotificationPreference)
        .options(selectinload(NotificationPreference.user))
        .where(NotificationPreference.entity_name == entity_name)
        .where(NotificationPreference.entity_type == entity_type),
    )
    return result.scalars().all()
