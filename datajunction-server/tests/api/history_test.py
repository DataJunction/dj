"""
Tests for the history endpoint
"""
from unittest import mock

from fastapi.testclient import TestClient

from datajunction_server.database.history import ActivityType, EntityType, History


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


def test_get_history_entity(client_with_roads: TestClient):
    """
    Test getting history for an entity
    """
    response = client_with_roads.get("/history/node/default.repair_orders/")
    assert response.ok
    history = response.json()
    assert len(history) == 1
    entity = history[0]
    entity.pop("created_at")
    assert history == [
        {
            "id": mock.ANY,
            "pre": {},
            "post": {},
            "node": "default.repair_orders",
            "entity_type": "node",
            "entity_name": "default.repair_orders",
            "activity_type": "create",
            "user": "dj",
            "details": {},
        },
    ]


def test_get_history_node(client_with_roads: TestClient):
    """
    Test getting history for a node
    """

    response = client_with_roads.get("/history?node=default.repair_order")
    assert response.ok
    history = response.json()
    assert len(history) == 5
    assert history == [
        {
            "activity_type": "create",
            "node": "default.repair_order",
            "created_at": mock.ANY,
            "details": {},
            "entity_name": "default.repair_order",
            "entity_type": "node",
            "id": mock.ANY,
            "post": {},
            "pre": {},
            "user": "dj",
        },
        {
            "activity_type": "set_attribute",
            "node": "default.repair_order",
            "created_at": mock.ANY,
            "details": {
                "column": "repair_order_id",
                "attributes": [
                    {
                        "name": "primary_key",
                        "namespace": "system",
                    },
                ],
            },
            "entity_name": None,
            "entity_type": "column_attribute",
            "id": mock.ANY,
            "post": {},
            "pre": {},
            "user": "dj",
        },
        {
            "activity_type": "create",
            "node": "default.repair_order",
            "created_at": mock.ANY,
            "details": {
                "dimension": "default.dispatcher",
                "join_cardinality": "many_to_one",
                "join_sql": "default.repair_order.dispatcher_id = "
                "default.dispatcher.dispatcher_id",
                "role": None,
            },
            "entity_name": "default.repair_order",
            "entity_type": "link",
            "id": mock.ANY,
            "post": {},
            "pre": {},
            "user": "dj",
        },
        {
            "activity_type": "create",
            "node": "default.repair_order",
            "created_at": mock.ANY,
            "details": {
                "dimension": "default.hard_hat",
                "join_cardinality": "many_to_one",
                "join_sql": "default.repair_order.hard_hat_id = "
                "default.hard_hat.hard_hat_id",
                "role": None,
            },
            "entity_name": "default.repair_order",
            "entity_type": "link",
            "id": mock.ANY,
            "post": {},
            "pre": {},
            "user": "dj",
        },
        {
            "activity_type": "create",
            "node": "default.repair_order",
            "created_at": mock.ANY,
            "details": {
                "dimension": "default.municipality_dim",
                "join_cardinality": "many_to_one",
                "join_sql": "default.repair_order.municipality_id = "
                "default.municipality_dim.municipality_id",
                "role": None,
            },
            "entity_name": "default.repair_order",
            "entity_type": "link",
            "id": mock.ANY,
            "post": {},
            "pre": {},
            "user": "dj",
        },
    ]


def test_get_history_namespace(client_with_service_setup: TestClient):
    """
    Test getting history for a node context
    """

    response = client_with_service_setup.get("/history/namespace/default")
    assert response.ok
    history = response.json()
    assert len(history) == 1
    assert history == [
        {
            "activity_type": "create",
            "node": None,
            "created_at": mock.ANY,
            "details": {},
            "entity_name": "default",
            "entity_type": "namespace",
            "id": mock.ANY,
            "post": {},
            "pre": {},
            "user": "dj",
        },
    ]
