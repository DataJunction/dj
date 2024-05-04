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
                "activity_type": "upstream_update",
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
                "pre": {"status": "invalid", "version": "v1.0"},
                "user": mock.ANY,
            },
        ],
        "default.regional_level_agg": [
            {
                "activity_type": "upstream_update",
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
                "pre": {"status": "invalid", "version": "v1.0"},
                "user": mock.ANY,
            },
        ],
        "default.avg_repair_price": [
            {
                "activity_type": "upstream_update",
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
                "pre": {"status": "invalid", "version": "v1.0"},
                "user": mock.ANY,
            },
        ],
        "default.regional_repair_efficiency": [
            {
                "activity_type": "upstream_update",
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
                "pre": {"status": "invalid", "version": "v1.0"},
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
                    if event["activity_type"] == "upstream_update"
                ] == node_history_events.get(affected)
