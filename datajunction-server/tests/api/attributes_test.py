"""
Tests for the attributes API.
"""
from unittest.mock import ANY

from fastapi.testclient import TestClient


def test_adding_new_attribute(
    client: TestClient,
) -> None:
    """
    Test adding an attribute.
    """
    response = client.post(
        "/attributes/",
        json={
            "namespace": "custom",
            "name": "internal",
            "description": "Column for internal use only",
            "allowed_node_types": ["source"],
        },
    )
    data = response.json()
    assert response.status_code == 201
    assert data == {
        "id": ANY,
        "namespace": "custom",
        "name": "internal",
        "description": "Column for internal use only",
        "uniqueness_scope": [],
        "allowed_node_types": ["source"],
    }

    response = client.post(
        "/attributes/",
        json={
            "namespace": "custom",
            "name": "internal",
            "description": "Column for internal use only",
            "allowed_node_types": ["source"],
        },
    )
    assert response.status_code == 500
    data = response.json()
    assert data == {
        "message": "Attribute type `internal` already exists!",
        "errors": [],
        "warnings": [],
    }

    response = client.post(
        "/attributes/",
        json={
            "namespace": "system",
            "name": "logging",
            "description": "Column for logging use only",
            "allowed_node_types": ["source"],
        },
    )
    assert response.status_code == 500
    data = response.json()
    assert data == {
        "message": "Cannot use `system` as the attribute type namespace as it is reserved.",
        "errors": [],
        "warnings": [],
    }


def test_list_attributes(
    client: TestClient,
) -> None:
    """
    Test listing attributes. These should contain the default attributes.
    """
    response = client.get("/attributes/")
    assert response.status_code == 200
    data = response.json()
    data = {
        type_["name"]: {k: type_[k] for k in (type_.keys() - {"id"})} for type_ in data
    }
    assert data == {
        "primary_key": {
            "namespace": "system",
            "uniqueness_scope": [],
            "allowed_node_types": ["source", "transform", "dimension"],
            "name": "primary_key",
            "description": "Points to a column which is part of the primary key of the node",
        },
        "expired_time": {
            "namespace": "system",
            "uniqueness_scope": ["node", "column_type"],
            "allowed_node_types": ["dimension"],
            "name": "expired_time",
            "description": "Points to a column which represents the expired time of a row in "
            "a dimension node. Used to facilitate proper joins with fact nodes "
            "on event time.",
        },
        "effective_time": {
            "namespace": "system",
            "uniqueness_scope": ["node", "column_type"],
            "allowed_node_types": ["dimension"],
            "name": "effective_time",
            "description": "Points to a column which represents the effective time of a row "
            "in a dimension node. Used to facilitate proper joins with fact "
            "nodes on event time.",
        },
        "event_time": {
            "namespace": "system",
            "uniqueness_scope": ["node", "column_type"],
            "allowed_node_types": ["source", "transform"],
            "name": "event_time",
            "description": "Points to a column which represents the time of the event in a "
            "given fact related node. Used to facilitate proper joins with "
            "dimension node to match the desired effect.",
        },
        "dimension": {
            "namespace": "system",
            "uniqueness_scope": [],
            "allowed_node_types": ["source", "transform"],
            "name": "dimension",
            "description": "Points to a dimension attribute column",
        },
    }
