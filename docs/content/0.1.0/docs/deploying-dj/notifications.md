---
weight: 60
title: Notifications
---

In DataJunction, notifications play a vital role in keeping users informed about various activity, such as changes in
the availability state of nodes. This section discusses how notifications are managed within DataJunction and how you
can implement a custom notification solution using FastAPI's dependency injection.

### How Notifications are Used

DataJunction uses notifications to alert users about significant events, such as updates to nodes or changes in their
availability state. By handling notifications within the OSS project, DataJunction provides an opinionated take on where
and when notifications should be sent.

### Default Notification Implementation

Out of the box, DataJunction includes a simple placeholder dependecny that simply logs notifications.

Here's a brief look at this placeholder notification implementation:

```py
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
```

### Custom Notification Implementation

You can implement a custom notification system by using FastAPI's dependency injection and injecting a `get_notifier`
dependency. The custom notifier must handle the `notify` method, which processes the notification messages.

#### Implementing a Custom Notifier

To implement a custom notifier, create a function that processes notification messages and use FastAPI's dependency
injection to inject your custom notifier.

Here's an example of a custom notifier implementation:

```py
from fastapi import Request

def custom_notify(message: Message):
    """Custom logic to send notifications"""
    # Implement the logic to send notifications, e.g., via email or a messaging service
    ...

def get_notifier(request: Request) -> callable:
    """Dependency for retrieving a custom notifier implementation"""
    # You can even add logic to choose between different notifiers based on request headers or other criteria
    return custom_notify
```

### Example Usage

Here's how the `get_notifier` dependency is used within the application to send a notification:

```py
notify = Depends(get_notifier())
notify(
    Message(
        recipients=["dj@datajunction.io"],
        subject="A new availability state has been published",
        body="A new availability state has been published for node foo",
    )
)
```

By customizing the `get_notifier` dependency, you can tailor the notification system to suit your specific needs, such
as integrating with third-party services or implementing advanced notification configuration or logic.
