"""Tests for notification dependency injection"""
import unittest
from unittest.mock import patch

from datajunction_server.api.notification import get_notifier
from datajunction_server.database.history import ActivityType, EntityType, History


class TestNotification(unittest.TestCase):
    """Test sending notifications"""

    @patch("datajunction_server.api.notification._logger")
    def test_notify(self, mock_logger):
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
