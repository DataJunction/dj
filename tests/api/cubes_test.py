"""
Tests for the cubes API.
"""

from fastapi.testclient import TestClient


def test_read_cube(client_with_examples: TestClient) -> None:
    """
    Test ``GET /cubes/{name}``.
    """
    # Create a cube
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "cube_elements": ["number_of_account_types", "account_type"],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "number_of_accounts_by_account_type",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["version"] == "v1.0"
    assert data["type"] == "cube"
    assert data["name"] == "number_of_accounts_by_account_type"
    assert data["display_name"] == "Number Of Accounts By Account Type"
    assert data["query"] is None

    # Read the cube
    response = client_with_examples.get("/cubes/number_of_accounts_by_account_type")
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "cube"
    assert data["name"] == "number_of_accounts_by_account_type"
    assert data["display_name"] == "Number Of Accounts By Account Type"
    assert data["version"] == "v1.0"
    assert data["description"] == "A cube of number of accounts grouped by account type"
    # Check that creating a cube with a query fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "query": "SELECT 1",
            "cube_elements": ["number_of_account_types", "account_type"],
            "name": "cubes_shouldnt_have_queries",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data["detail"] == [
        {
            "loc": ["body", "query"],
            "msg": "extra fields not permitted",
            "type": "value_error.extra",
        },
    ]

    # Check that creating a cube with no cube elements fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "cube_elements": [],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "cubes_must_have_elements",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one metric is required to create a cube node",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with incompatible nodes fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "cube_elements": ["number_of_account_types", "account_type_table"],
            "description": "",
            "mode": "published",
            "name": "cubes_cant_use_source_nodes",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "Node account_type_table of type source cannot be added to a cube",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with no metric nodes fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "cube_elements": ["account_type"],
            "description": "",
            "mode": "published",
            "name": "cubes_must_have_metrics",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one metric is required to create a cube node",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with no dimension nodes fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "cube_elements": ["number_of_account_types"],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "cubes_must_have_dimensions",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one dimension is required to create a cube node",
        "errors": [],
        "warnings": [],
    }


def test_raise_on_cube_with_multiple_catalogs(
    client_with_examples: TestClient,
) -> None:
    """
    Test raising when creating a cube with multiple catalogs
    """
    # Create a cube
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "cube_elements": ["account_type", "basic.num_comments"],
            "description": "multicatalog cube's raise an error",
            "mode": "published",
            "name": "multicatalog",
        },
    )
    assert not response.ok
    data = response.json()
    assert "Cannot create cube using nodes from multiple catalogs" in data["message"]
