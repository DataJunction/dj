"""Dependency for notifications"""

import logging
from http import HTTPStatus
from typing import Annotated, Optional

from fastapi import Body, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from datajunction_server.database.history import ActivityType, EntityType, History
from datajunction_server.database.notification_preference import NotificationPreference
from datajunction_server.database.user import User
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.utils import get_and_update_current_user, get_session

router = SecureAPIRouter(tags=["notifications"])
_logger = logging.getLogger(__name__)


def get_notifier():
    """Returns a method for sending notifications for an event"""

    def notify(event: History):
        """Send a notification for an event"""
        _logger.debug("Sending notification for event %s", event)

    return notify


@router.post("/notifications/subscribe")
async def subscribe(
    entity_type: Annotated[EntityType, Body()],
    entity_name: Annotated[str, Body()],
    activity_types: list[ActivityType],
    alert_types: list[str],
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
) -> JSONResponse:
    """Subscribes to notificaitons by upserting a notification preference"""
    session.add(
        NotificationPreference(
            entity_type=entity_type,
            entity_name=entity_name,
            activity_types=activity_types,
            alert_types=alert_types,
            user=current_user,
        ),
    )
    await session.commit()
    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"Notification preferences successfully saved for {entity_name}"
            ),
        },
    )


@router.delete("/notifications/unsubscribe")
async def unsubscribe(
    entity_type: EntityType,
    entity_name: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
) -> JSONResponse:
    """Unsubscribes from notifications by deleting a notification preference"""
    result = await session.execute(
        select(NotificationPreference).where(
            NotificationPreference.entity_type == entity_type,
            NotificationPreference.entity_name == entity_name,
            NotificationPreference.user_id == current_user.id,
        ),
    )
    notification_preference = result.scalars().first()

    if not notification_preference:
        raise DJDoesNotExistException(
            message=f"No notification preference found for {entity_name}",
            http_status_code=HTTPStatus.NOT_FOUND,
        )
    await session.delete(notification_preference)
    await session.commit()
    return JSONResponse(
        status_code=200,
        content={
            "message": (
                f"Notification preferences successfully removed for {entity_name}"
            ),
        },
    )


@router.get("/notifications/")
async def get_notification_preferences(
    entity_name: Optional[str] = None,
    entity_type: Optional[EntityType] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
) -> JSONResponse:
    """Subscribes to notificaitons by upserting a notification preference"""
    statement = select(NotificationPreference).where(
        NotificationPreference.user == current_user,
    )
    if entity_name:
        statement = statement.where(NotificationPreference.entity_name == entity_name)
    if entity_type:
        statement = statement.where(NotificationPreference.entity_type == entity_type)
    result = await session.execute(statement)
    notification_preferences = [
        {
            "entity_type": pref.entity_type,
            "entity_name": pref.entity_name,
            "activity_types": pref.activity_types,
            "user_id": pref.user.id,
            "username": pref.user.username,
            "alert_types": pref.alert_types,
        }
        for pref in result.scalars().all()
    ]
    return JSONResponse(content=notification_preferences)
