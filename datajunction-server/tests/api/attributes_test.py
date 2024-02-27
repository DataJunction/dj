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
        "dimension": {
            "namespace": "system",
            "uniqueness_scope": [],
            "allowed_node_types": ["source", "transform"],
            "name": "dimension",
            "description": "Points to a dimension attribute column",
        },
    }
