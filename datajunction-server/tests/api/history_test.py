"""
Tests for the history endpoint
"""
from fastapi.testclient import TestClient

from datajunction_server.models.history import ActivityType, EntityType, History


def test_history_hash():
    """
    Test hash comparison of history events
    """
    foo1 = History(
        id=1,
        entity_name="bar",
        entity_type=EntityType.NODE,
        activity_type=ActivityType.CREATE,
    )
    foo2 = History(
        id=1,
        entity_name="bar",
        entity_type=EntityType.NODE,
        activity_type=ActivityType.CREATE,
    )
    assert hash(foo1) == hash(foo2)


def test_get_history(client_with_examples: TestClient):
    """
    Test getting history for a node
    """
    response = client_with_examples.get("/history/node/default.repair_orders/")
    assert response.ok
    history = response.json()
    assert len(history) == 1
    entity = history[0]
    entity.pop("created_at")
    assert history == [
        {
            "id": 1,
            "pre": {},
            "post": {},
            "entity_type": "node",
            "entity_name": "default.repair_orders",
            "activity_type": "create",
            "user": None,
            "details": {},
        },
    ]
