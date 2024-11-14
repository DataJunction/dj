"""Tests for notification dependency injection"""
import unittest
from unittest.mock import patch

from datajunction_server.api.notification import Message, get_notifier


class TestNotification(unittest.TestCase):
    """Test sending notifications"""

    @patch("datajunction_server.api.notification._logger")
    def test_notify(self, mock_logger):
        """Test the get_notifier dependency"""
        notify = get_notifier()
        message = Message(
            recipients=["alice@example.com", "bob@example.com"],
            subject="Test Subject",
            body="This is a test body.",
            metadata={"key1": "value1", "key2": "value2"},
        )
        notify(message)

        mock_logger.debug.assert_any_call(
            "Sending notification to %s",
            message.recipients,
        )
        mock_logger.debug.assert_any_call("Subject: %s", message.subject)
        mock_logger.debug.assert_any_call(
            "Sending notification to %s",
            message.recipients,
        )
