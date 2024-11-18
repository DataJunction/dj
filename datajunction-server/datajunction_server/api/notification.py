"""Dependency for notifications"""
import logging

from datajunction_server.database.history import History

_logger = logging.getLogger(__name__)


def get_notifier():
    """Returns a method for sending notifications for an event"""

    def notify(event: History):
        """Send a notification for an event"""
        _logger.debug("Sending notification for event %s", event)

    return notify
