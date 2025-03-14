---
weight: 60
title: Notifications
---

In DataJunction, notifications are crucial for keeping users informed about various activities, such as changes in the
state of nodes. This document outlines how notifications are managed within DataJunction and how you can implement a
custom notification solution using FastAPI's dependency injection.

## How Notifications are Used

DataJunction uses notifications to alert users about significant events, such as updates to nodes or changes in their
state. By handling notifications within the OSS project, DataJunction provides an opinionated take on where and when
notifications should be sent.

## Default Notification Implementation

Out of the box, DataJunction includes a simple placeholder dependency that logs notifications. This implementation is
designed to be straightforward and easily replaceable with a custom solution.

### Placeholder Notification Implementation

Here's a brief look at the placeholder notification implementation:

```python
import logging
from datajunction_server.database.history import History

_logger = logging.getLogger(__name__)

def get_notifier():
    """Returns a method for sending notifications for an event"""
    def notify(event: History):
        """Send a notification for an event"""
        _logger.debug(f"Sending notification for event %s", event)
    return notify
```

## Custom Notification Implementation

You can implement a custom notification system by using FastAPI's dependency injection and injecting a `get_notifier`
dependency. The custom notifier must handle the `notify` method, which processes the notification events.

### Implementing a Custom Notifier

To implement a custom notifier, create a function that processes notification events and use FastAPI's dependency
injection to inject your custom notifier.

Here's an example of a custom notifier implementation:

```python
from fastapi import Request

def custom_notify(event: History):
    """Custom logic to send notifications"""
    # Implement the logic to send notifications, e.g., via email or a messaging service
    ...

def get_custom_notifier(request: Request) -> callable:
    """Dependency for retrieving a custom notifier implementation"""
    # You can even add logic to choose between different notifiers based on request headers or other criteria
    return custom_notify

# Override the built-in get_notifier with your custom one
app.dependency_overrides[get_notifier] = get_custom_notifier
```

## Example Usage

Here's an illustrative example to help understand how the `get_notifier` dependency is used within the application to send a notification:

```python
event = History(
    id=1,
    entity_name="bar",
    entity_type=EntityType.NODE,
    activity_type=ActivityType.CREATE,
)
notify(event)
```

By customizing the `get_notifier` dependency, you can tailor the notification system to suit your specific needs, such
as integrating with third-party services or implementing advanced notification configuration or logic. Below are the different
types of entities and activities that DataJunction will pass through the notification dependency.

### Entity types

- ATTRIBUTE
- AVAILABILITY
- BACKFILL
- CATALOG
- COLUMN_ATTRIBUTE
- DEPENDENCY
- ENGINE
- LINK
- MATERIALIZATION
- NAMESPACE
- NODE
- PARTITION
- QUERY
- TAG

### Activity types

- CREATE
- DELETE
- RESTORE
- UPDATE
- REFRESH
- TAG
- SET_ATTRIBUTE
- STATUS_CHANGE
