"""Tests for node updates"""
from unittest import mock

import pytest
from httpx import AsyncClient

from datajunction_server.models.node import NodeStatus


@pytest.mark.asyncio
async def test_update_source_node(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test updating a source node that has multiple layers of downstream effects
    non-immediate children should be revalidated:
    - default.regional_repair_efficiency should be affected (invalidated)
    column changes should affect validity:
    - any metric selecting from `quantity` should be invalid (now quantity_v2)
    - any metric selecting from `price` should have updated types (now string)
    """
    await client_with_roads.patch(
        "/nodes/default.repair_order_details/",
        json={
            "columns": [
                {"name": "repair_order_id", "type": "string"},
                {"name": "repair_type_id", "type": "int"},
                {"name": "price", "type": "string"},
                {"name": "quantity_v2", "type": "int"},
                {"name": "discount", "type": "float"},
            ],
        },
    )
    affected_nodes = {
        "default.regional_level_agg": NodeStatus.INVALID,  # NodeStatus.INVALID
        "default.national_level_agg": NodeStatus.INVALID,
        "default.avg_repair_price": NodeStatus.INVALID,
        "default.total_repair_cost": NodeStatus.INVALID,
        "default.discounted_orders_rate": NodeStatus.INVALID,
        "default.total_repair_order_discounts": NodeStatus.INVALID,
        "default.avg_repair_order_discounts": NodeStatus.INVALID,
        "default.regional_repair_efficiency": NodeStatus.INVALID,
    }

    node_history_events = {
        "default.national_level_agg": [
            {
                "activity_type": "update",
                "created_at": mock.ANY,
                "details": {
                    "changes": {"updated_columns": ["total_amount_nationwide"]},
                    "reason": "Caused by update of `default.repair_order_details` to "
                    "v2.0",
                    "upstream": {
                        "node": "default.repair_order_details",
                        "version": "v2.0",
                    },
                },
                "entity_name": "default.national_level_agg",
                "entity_type": "node",
                "id": mock.ANY,
                "node": "default.national_level_agg",
                "post": {"status": "invalid", "version": "v2.0"},
                "pre": {"status": "valid", "version": "v1.0"},
                "user": mock.ANY,
            },
        ],
        "default.regional_level_agg": [
            {
                "activity_type": "update",
                "created_at": mock.ANY,
                "details": {
                    "changes": {
                        "updated_columns": [
                            "avg_repair_amount_in_region",
                            "total_amount_in_region",
                        ],
                    },
                    "reason": "Caused by update of `default.repair_order_details` to v2.0",
                    "upstream": {
                        "node": "default.repair_order_details",
                        "version": "v2.0",
                    },
                },
                "entity_name": "default.regional_level_agg",
                "entity_type": "node",
                "id": mock.ANY,
                "node": "default.regional_level_agg",
                "post": {"status": "invalid", "version": "v2.0"},
                "pre": {"status": "valid", "version": "v1.0"},
                "user": mock.ANY,
            },
        ],
        "default.avg_repair_price": [
            {
                "activity_type": "update",
                "created_at": mock.ANY,
                "details": {
                    "changes": {"updated_columns": ["default_DOT_avg_repair_price"]},
                    "reason": "Caused by update of `default.repair_order_details` to "
                    "v2.0",
                    "upstream": {
                        "node": "default.repair_order_details",
                        "version": "v2.0",
                    },
                },
                "entity_name": "default.avg_repair_price",
                "entity_type": "node",
                "id": mock.ANY,
                "node": "default.avg_repair_price",
                "post": {"status": "invalid", "version": "v2.0"},
                "pre": {"status": "valid", "version": "v1.0"},
                "user": mock.ANY,
            },
        ],
        "default.regional_repair_efficiency": [
            {
                "activity_type": "update",
                "created_at": mock.ANY,
                "details": {
                    "changes": {
                        "updated_columns": ["default_DOT_regional_repair_efficiency"],
                    },
                    "reason": "Caused by update of `default.repair_order_details` to "
                    "v2.0",
                    "upstream": {
                        "node": "default.repair_order_details",
                        "version": "v2.0",
                    },
                },
                "entity_name": "default.regional_repair_efficiency",
                "entity_type": "node",
                "id": mock.ANY,
                "node": "default.regional_repair_efficiency",
                "post": {"status": "invalid", "version": "v2.0"},
                "pre": {"status": "valid", "version": "v1.0"},
                "user": mock.ANY,
            },
        ],
    }

    # check all affected nodes and verify that their statuses have been updated
    for affected, expected_status in affected_nodes.items():
        response = await client_with_roads.get(f"/nodes/{affected}")
        assert response.json()["status"] == expected_status

        # only nodes with a status change will have a history record
        if expected_status == NodeStatus.INVALID:
            response = await client_with_roads.get(f"/history?node={affected}")
            if node_history_events.get(affected):
                assert [
                    event
                    for event in response.json()
                    if event["activity_type"] == "update"
                ] == node_history_events.get(affected)

    await client_with_roads.patch(
        "/nodes/default.national_level_agg/",
        json={
            "query": "SELECT SUM(cast(rd.price AS float) * rd.quantity_v2) AS total_amount "
            "FROM default.repair_order_details rd",
        },
    )
    response = await client_with_roads.get("/nodes/default.national_level_agg")
    data = response.json()
    assert data["status"] == "valid"
    assert data["columns"] == [
        {
            "attributes": [],
            "dimension": None,
            "display_name": "Total Amount",
            "name": "total_amount",
            "partition": None,
            "type": "double",
        },
    ]


@pytest.mark.asyncio
async def test_crud_with_wildcards(
    client_with_roads: AsyncClient,
):
    """
    Test creating and updating nodes with SELECT * in their queries
    """
    response = await client_with_roads.get("/nodes/default.hard_hat")
    old_columns = response.json()["columns"]

    # Using a wildcard should result in a node revision with the same columns
    await client_with_roads.patch(
        "/nodes/default.hard_hat",
        json={"query": "SELECT * FROM default.hard_hats"},
    )
    response = await client_with_roads.get("/nodes/default.hard_hat")
    data = response.json()
    assert data["columns"] == old_columns
    assert data["version"] == "v2.0"

    # Create a transform with wildcards
    response = await client_with_roads.post(
        "/nodes/transform",
        json={
            "name": "default.ny_hard_hats",
            "display_name": "NY Hard Hats",
            "description": "Blah",
            "query": """SELECT
                        hh.*,
                        hhs.*
                        FROM default.hard_hat hh
                        LEFT JOIN default.hard_hat_state hhs
                        ON hh.hard_hat_id = hhs.hard_hat_id
                        WHERE hh.state_id = 'NY'""",
            "mode": "published",
        },
    )
    data = response.json()
    joined_columns = [
        "hard_hat_id",
        "last_name",
        "first_name",
        "title",
        "birth_date",
        "hire_date",
        "address",
        "city",
        "state",
        "postal_code",
        "country",
        "manager",
        "contractor_id",
        "hard_hat_id",
        "state_id",
    ]
    assert [col["name"] for col in data["columns"]] == joined_columns

    # Create a transform based on the earlier transform
    response = await client_with_roads.post(
        "/nodes/transform",
        json={
            "name": "default.ny_hard_hats_2",
            "display_name": "NY Hard Hats 2",
            "description": "Blah",
            "query": "SELECT * FROM default.ny_hard_hats",
            "mode": "published",
        },
    )
    data = response.json()
    assert [col["name"] for col in data["columns"]] == joined_columns

    # Update original hard_hat, which should trigger cascading updates of the children
    await client_with_roads.patch(
        "/nodes/default.hard_hat",
        json={
            "query": "SELECT last_name, first_name, birth_date FROM default.hard_hats",
        },
    )
    response = await client_with_roads.get("/nodes/default.ny_hard_hats")
    data = response.json()
    assert [col["name"] for col in data["columns"]] == [
        "last_name",
        "first_name",
        "birth_date",
        "hard_hat_id",
        "state_id",
    ]
    response = await client_with_roads.get("/nodes/default.ny_hard_hats_2")
    data = response.json()
    assert [col["name"] for col in data["columns"]] == [
        "last_name",
        "first_name",
        "birth_date",
        "hard_hat_id",
        "state_id",
    ]
