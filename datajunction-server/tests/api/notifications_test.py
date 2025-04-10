"""Tests for notifications router"""

import unittest
from unittest import mock

import pytest
from httpx import AsyncClient

from datajunction_server.api.notifications import get_notifier
from datajunction_server.database.history import History
from datajunction_server.internal.history import ActivityType, EntityType


class TestNotification(unittest.TestCase):
    """Test sending notifications"""

    @mock.patch("datajunction_server.api.notifications._logger")
    def test_notification(self, mock_logger):
        """Test the get_notifier dependency"""
        notify = get_notifier()
        event = History(
            id=1,
            entity_name="bar",
            entity_type=EntityType.NODE,
            activity_type=ActivityType.CREATE,
        )
        notify(event)

        mock_logger.debug.assert_any_call(
            "Sending notification for event %s",
            event,
        )


@pytest.mark.asyncio
async def test_notification_subscription(
    module__client: AsyncClient,
) -> None:
    """
    Test subscribing to notifications.
    """
    response = await module__client.post(
        "/notifications/subscribe",
        json={
            "entity_name": "some_node_name",
            "entity_type": EntityType.NODE,
            "activity_types": [ActivityType.DELETE],
            "alert_types": ["slack", "email"],
        },
    )
    assert response.status_code == 201
    assert response.json() == {
        "message": "Notification preferences successfully saved for some_node_name",
    }


@pytest.mark.asyncio
async def test_notification_preferences(
    module__client: AsyncClient,
) -> None:
    """
    Test retrieving notification preferences.
    """
    response = await module__client.post(
        "/notifications/subscribe",
        json={
            "entity_type": EntityType.NODE,
            "entity_name": "some_node_name2",
            "activity_types": [ActivityType.REFRESH],
            "alert_types": ["slack", "email"],
        },
    )
    assert response.status_code == 201
    response = await module__client.get(
        "/notifications/",
        params={"entity_name": "some_node_name2"},
    )
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0]["entity_name"] == "some_node_name2"
    assert response.json()[0]["username"] == "dj"


@pytest.mark.asyncio
async def test_notification_preferences_with_filters(
    module__client: AsyncClient,
) -> None:
    """
    Test retrieving notification preferences with filters.
    """
    await module__client.post(
        "/notifications/subscribe",
        json={
            "entity_type": EntityType.NODE,
            "entity_name": "name1",
            "activity_types": [ActivityType.STATUS_CHANGE],
            "alert_types": ["slack", "email"],
        },
    )
    await module__client.post(
        "/notifications/subscribe",
        json={
            "entity_name": "name2",
            "entity_type": EntityType.NODE,
            "activity_types": [ActivityType.DELETE],
            "alert_types": ["slack", "email"],
        },
    )
    response = await module__client.get(
        "/notifications/?entity_name=name1",
    )
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["entity_name"] == "name1"

    response = await module__client.get(
        "/notifications/?entity_type=node",
    )
    assert response.status_code == 200
    assert len(response.json()) > 1
    assert response.json()[0]["entity_type"] == "node"


@pytest.mark.asyncio
async def test_notification_unsubscribe(
    module__client: AsyncClient,
) -> None:
    """
    Test unsubscribing from notifications.
    """
    # Subscribe
    response = await module__client.post(
        "/notifications/subscribe",
        json={
            "entity_name": "some_node_name3",
            "entity_type": EntityType.NODE,
            "activity_types": [ActivityType.DELETE],
            "alert_types": ["slack", "email"],
        },
    )
    assert response.status_code == 201

    # Unsubscribe
    response = await module__client.delete(
        "/notifications/unsubscribe",
        params={
            "entity_type": EntityType.NODE,
            "entity_name": "some_node_name3",
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        "message": "Notification preferences successfully removed for some_node_name3",
    }

    # Verify that the notification preference is actually removed
    response = await module__client.get("/notifications/")
    assert response.status_code == 200
    assert all(pref["entity_name"] != "some_node_name3" for pref in response.json())


@pytest.mark.asyncio
async def test_notification_unsubscribe_not_found(
    module__client: AsyncClient,
) -> None:
    """
    Test notification preference not found when unsubscribing
    """
    # Unsubscribe to a notification that doesn't exist
    response = await module__client.delete(
        "/notifications/unsubscribe",
        params={
            "entity_type": EntityType.NODE,
            "entity_name": "does_not_exist",
        },
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_notification_list_users(
    module__client: AsyncClient,
) -> None:
    """
    Test listing all users subscribed to a specific notification
    """
    response = await module__client.post(
        "/notifications/subscribe",
        json={
            "entity_type": EntityType.NODE,
            "entity_name": "some_node_name4",
            "activity_types": [ActivityType.REFRESH],
            "alert_types": ["slack", "email"],
        },
    )
    assert response.status_code == 201
    response = await module__client.get(
        "/notifications/users",
        params={"entity_name": "some_node_name4", "entity_type": EntityType.NODE},
    )
    assert response.status_code == 200
    assert len(response.json()) > 0
    assert response.json()[0] == "dj"
