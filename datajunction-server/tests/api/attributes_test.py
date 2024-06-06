"""
Tests for the attributes API.
"""
from unittest.mock import ANY

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_adding_new_attribute(
    module__client: AsyncClient,
) -> None:
    """
    Test adding an attribute.
    """
    response = await module__client.post(
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

    response = await module__client.post(
        "/attributes/",
        json={
            "namespace": "custom",
            "name": "internal",
            "description": "Column for internal use only",
            "allowed_node_types": ["source"],
        },
    )
    assert response.status_code == 409
    data = response.json()
    assert data == {
        "message": "Attribute type `internal` already exists!",
        "errors": [],
        "warnings": [],
    }

    response = await module__client.post(
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


@pytest.mark.asyncio
async def test_list_system_attributes(
    module__client: AsyncClient,
) -> None:
    """
    Test listing attributes. These should contain the default attributes.
    """
    response = await module__client.get("/attributes/")
    assert response.status_code == 200
    data = response.json()
    data = {
        type_["name"]: {k: type_[k] for k in (type_.keys() - {"id"})} for type_ in data
    }
    data_for_system = {k: v for k, v in data.items() if v["namespace"] == "system"}
    assert data_for_system == {
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
