"""Dependency for notifications"""

import logging
from http import HTTPStatus
from typing import Annotated, List, Optional

from datajunction_server.models.notifications import NotificationPreferenceModel
from fastapi import Body, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.dialects.postgresql import insert
from datajunction_server.database.notification_preference import NotificationPreference
from datajunction_server.database.user import User
from datajunction_server.database.history import History
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.notifications import (
    get_entity_notification_preferences,
    get_user_notification_preferences,
)
from datajunction_server.models.notifications import NotificationPreferenceModel
from datajunction_server.utils import (
    get_and_update_current_user,
    get_current_user,
    get_session,
)

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
    """
    Subscribes to notifications by upserting a notification preference.
    If one exists, update it. Otherwise, create a new one.
    """
    stmt = (
        insert(NotificationPreference)
        .values(
            user_id=current_user.id,
            entity_type=entity_type,
            entity_name=entity_name,
            activity_types=activity_types,
            alert_types=alert_types,
        )
        .on_conflict_do_update(
            index_elements=["user_id", "entity_type", "entity_name"],
            set_={
                "activity_types": activity_types,
                "alert_types": alert_types,
            },
        )
    )

    await session.execute(stmt)
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
async def get_preferences(
    entity_name: Optional[str] = None,
    entity_type: Optional[EntityType] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> List[NotificationPreferenceModel]:
    """Gets notification preferences for the current user"""
    notification_preferences = await get_user_notification_preferences(
        session=session,
        user=current_user,
        entity_name=entity_name,
        entity_type=entity_type,
    )
    response = [
        NotificationPreferenceModel(
            entity_type=pref.entity_type,
            entity_name=pref.entity_name,
            activity_types=pref.activity_types,
            user_id=pref.user.id,
            username=pref.user.username,
            alert_types=pref.alert_types,
        )
        for pref in notification_preferences
    ]
    return response


@router.get("/notifications/users")
async def get_users_for_notification(
    entity_name: str,
    entity_type: EntityType,
    session: AsyncSession = Depends(get_session),
) -> list[str]:
    """Get users for the given notification preference"""
    notification_preferences = await get_entity_notification_preferences(
        session=session,
        entity_name=entity_name,
        entity_type=entity_type,
    )
    users = [perf.user.username for perf in notification_preferences]
    return users
