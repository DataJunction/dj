"""Tests for node updates"""

from unittest import mock

import pytest
from httpx import AsyncClient
import pytest_asyncio

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
                    "changes": {"updated_columns": []},
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
                "post": {"status": "invalid", "version": "v1.0"},
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
                        "updated_columns": [],
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
                "post": {"status": "invalid", "version": "v1.0"},
                "pre": {"status": "valid", "version": "v1.0"},
                "user": mock.ANY,
            },
        ],
        "default.avg_repair_price": [
            {
                "activity_type": "update",
                "created_at": mock.ANY,
                "details": {
                    "changes": {"updated_columns": []},
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
                "post": {"status": "invalid", "version": "v1.0"},
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
                        "updated_columns": [],
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
                "post": {"status": "invalid", "version": "v1.0"},
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
            "description": None,
            "dimension": None,
            "dimension_column": None,
            "display_name": "Total Amount",
            "name": "total_amount",
            "partition": None,
            "type": "double",
        },
    ]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def user_one(module__client_with_roads: AsyncClient):
    await module__client_with_roads.post(
        "/basic/user/",
        data={
            "email": "userone@datajunction.io",
            "username": "userone",
            "password": "userone",
        },
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def cube(module__client_with_roads: AsyncClient):
    await module__client_with_roads.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.num_repair_orders", "default.total_repair_cost"],
            "dimensions": [
                "default.hard_hat.country",
                "default.dispatcher.company_name",
            ],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repair_orders_cube",
        },
    )


@pytest.mark.asyncio
async def test_update_source_node_new_owner(
    module__client_with_roads: AsyncClient,
    user_one,
):
    """
    Test updating a source node with a new owner
    """
    response = await module__client_with_roads.patch(
        "/nodes/default.repair_order_details/",
        json={
            "columns": [
                {"name": "repair_order_id", "type": "string"},
            ],
            "owners": ["dj", "userone"],
        },
    )
    assert {owner["username"] for owner in response.json()["owners"]} == {
        "dj",
        "userone",
    }


@pytest.mark.asyncio
async def test_update_cube_node_new_owner(
    module__client_with_roads: AsyncClient,
    user_one,
    cube,
):
    """
    Test updating a cube with new owners
    """
    response = await module__client_with_roads.get("/nodes/default.repair_orders_cube/")
    assert response.json()["owners"] == [{"username": "dj"}]
    response = await module__client_with_roads.patch(
        "/nodes/default.repair_orders_cube/",
        json={
            "owners": ["userone"],
        },
    )
    assert response.json()["owners"] == [{"username": "userone"}]


@pytest.mark.asyncio
async def test_update_node_non_existent_owners(
    module__client_with_roads: AsyncClient,
) -> None:
    response = await module__client_with_roads.patch(
        "/nodes/default.repair_order_details/",
        json={
            "columns": [
                {"name": "repair_order_id", "type": "string"},
            ],
            "owners": ["nonexistent_user", "dj"],
        },
    )
    assert response.json()["message"] == "Users not found: nonexistent_user"
