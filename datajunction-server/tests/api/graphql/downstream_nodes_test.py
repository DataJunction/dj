"""
Tests for the engine API.
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_downstream_nodes(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes by node type
    """

    # of METRIC type
    query = """
    {
        downstreamNodes(nodeName: "default.repair_orders_fact", nodeType: METRIC) {
            name
            type
            current {
                customMetadata
            }
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["downstreamNodes"] == [
        {
            "name": "default.num_repair_orders",
            "type": "METRIC",
            "current": {"customMetadata": {"foo": "bar"}},
        },
        {
            "name": "default.avg_repair_price",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.total_repair_cost",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.discounted_orders_rate",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.total_repair_order_discounts",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.avg_repair_order_discounts",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.avg_time_to_dispatch",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
    ]

    # of any type
    query = """
    {
        downstreamNodes(nodeName: "default.repair_order_details", nodeType: null) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["downstreamNodes"] == [
        {"name": "default.regional_level_agg", "type": "TRANSFORM"},
        {"name": "default.national_level_agg", "type": "TRANSFORM"},
        {"name": "default.repair_orders_fact", "type": "TRANSFORM"},
        {"name": "default.regional_repair_efficiency", "type": "METRIC"},
        {"name": "default.num_repair_orders", "type": "METRIC"},
        {"name": "default.avg_repair_price", "type": "METRIC"},
        {"name": "default.total_repair_cost", "type": "METRIC"},
        {"name": "default.discounted_orders_rate", "type": "METRIC"},
        {"name": "default.total_repair_order_discounts", "type": "METRIC"},
        {"name": "default.avg_repair_order_discounts", "type": "METRIC"},
        {"name": "default.avg_time_to_dispatch", "type": "METRIC"},
    ]
