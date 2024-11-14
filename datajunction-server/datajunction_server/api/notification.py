"""Dependency for notifications"""
import logging
from dataclasses import dataclass
from typing import Dict, Optional

_logger = logging.getLogger(__name__)


@dataclass
class Message:
    """A message for use in a notification"""

    recipients: list[str]
    subject: str
    body: str
    metadata: Optional[Dict[str, str]] = None


def get_notifier():
    """Returns a method for processing notifications"""

    def notify(message: Message):
        """Notify the recipients"""
        _logger.debug("Sending notification to %s", message.recipients)
        _logger.debug("Subject: %s", message.subject)
        _logger.debug("Body: %s", message.body)
        _logger.debug("Metadata: %s", message.metadata)

    return notify
