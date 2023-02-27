"""
Tests for the cubes API.
"""

from fastapi.testclient import TestClient


def test_read_cube(client: TestClient) -> None:
    """
    Test ``GET /cubes/{name}``.
    """
    response = client.post(
        "/nodes/",
        json={
            "columns": {
                "id": {"type": "INT"},
                "account_type_name": {"type": "STR"},
                "account_type_classification": {"type": "INT"},
                "preferred_payment_method": {"type": "INT"},
            },
            "description": "A source table for account type data",
            "mode": "published",
            "name": "account_type_table",
            "type": "source",
        },
    )
    assert response.status_code == 201

    response = client.post(
        "/nodes/",
        json={
            "description": "Account type dimension",
            "query": (
                "SELECT id, account_type_name, "
                "account_type_classification FROM "
                "account_type_table"
            ),
            "mode": "published",
            "name": "account_type",
            "type": "dimension",
        },
    )
    assert response.status_code == 201

    response = client.post(
        "/nodes/",
        json={
            "description": "Total number of account types",
            "query": "SELECT count(id) as num_accounts FROM account_type",
            "mode": "published",
            "name": "number_of_account_types",
            "type": "metric",
        },
    )
    assert response.status_code == 201

    # Create a cube
    response = client.post(
        "/nodes/",
        json={
            "cube_elements": ["number_of_account_types", "account_type"],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "number_of_accounts_by_account_type",
            "type": "cube",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["node_id"] == 4
    assert data["version"] == "v1.0"
    assert data["node_revision_id"] == 4
    assert data["type"] == "cube"
    assert data["name"] == "number_of_accounts_by_account_type"
    assert data["display_name"] == "Number Of Accounts By Account Type"
    assert data["query"] is None

    # Read the cube
    response = client.get("/cubes/number_of_accounts_by_account_type")
    assert response.status_code == 200
    data = response.json()
    assert data["node_revision_id"] == 4
    assert data["node_id"] == 4
    assert data["type"] == "cube"
    assert data["name"] == "number_of_accounts_by_account_type"
    assert data["display_name"] == "Number Of Accounts By Account Type"
    assert data["version"] == "v1.0"
    assert data["description"] == "A cube of number of accounts grouped by account type"
    assert {"id": 2, "current_version": "v1.0", "name": "account_type"} in data[
        "cube_elements"
    ]
    assert {
        "id": 3,
        "current_version": "v1.0",
        "name": "number_of_account_types",
    } in data["cube_elements"]

    # Check that creating a cube with a query fails appropriately
    response = client.post(
        "/nodes/",
        json={
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "query": "SELECT 1",
            "name": "cubes_shouldnt_have_queries",
            "type": "cube",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "Query not allowed for node of type cube",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with no cube elements fails appropriately
    response = client.post(
        "/nodes/",
        json={
            "cube_elements": [],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "cubes_must_have_elements",
            "type": "cube",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "Cannot create a cube node with no cube elements",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with incompatible nodes fails appropriately
    response = client.post(
        "/nodes/",
        json={
            "cube_elements": ["number_of_account_types", "account_type_table"],
            "description": "",
            "mode": "published",
            "name": "cubes_cant_use_source_nodes",
            "type": "cube",
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
    response = client.post(
        "/nodes/",
        json={
            "cube_elements": ["account_type"],
            "description": "",
            "mode": "published",
            "name": "cubes_must_have_metrics",
            "type": "cube",
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
    response = client.post(
        "/nodes/",
        json={
            "cube_elements": ["number_of_account_types"],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "cubes_must_have_dimensions",
            "type": "cube",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one dimension is required to create a cube node",
        "errors": [],
        "warnings": [],
    }
