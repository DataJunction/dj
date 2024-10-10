"""
Tests for the dimensions API.
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_list_dimension(
    module__client_with_roads_and_acc_revenue: AsyncClient,
) -> None:
    """
    Test ``GET /dimensions/``.
    """
    response = await module__client_with_roads_and_acc_revenue.get("/dimensions/")
    data = response.json()

    assert response.status_code == 200
    assert {(dim["name"], dim["indegree"]) for dim in data} == {
        (dim["name"], dim["indegree"])
        for dim in [
            {"indegree": 3, "name": "default.dispatcher"},
            {"indegree": 2, "name": "default.repair_order"},
            {"indegree": 2, "name": "default.hard_hat"},
            {"indegree": 2, "name": "default.hard_hat_to_delete"},
            {"indegree": 2, "name": "default.municipality_dim"},
            {"indegree": 1, "name": "default.contractor"},
            {"indegree": 2, "name": "default.us_state"},
            {"indegree": 0, "name": "default.local_hard_hats"},
            {"indegree": 0, "name": "default.local_hard_hats_1"},
            {"indegree": 0, "name": "default.local_hard_hats_2"},
            {"indegree": 0, "name": "default.payment_type"},
            {"indegree": 0, "name": "default.account_type"},
            {"indegree": 0, "name": "default.hard_hat_2"},
        ]
    }


@pytest.mark.asyncio
async def test_list_nodes_with_dimension(
    module__client_with_roads_and_acc_revenue: AsyncClient,
) -> None:
    """
    Test ``GET /dimensions/{name}/nodes/``.
    """
    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.hard_hat/nodes/",
    )
    data = response.json()
    roads_nodes = {
        "default.repair_orders",
        "default.repair_order_details",
        "default.regional_level_agg",
        "default.national_level_agg",
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.repair_orders_fact",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }
    assert {node["name"] for node in data} == roads_nodes

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.repair_order/nodes/",
    )
    data = response.json()
    assert {node["name"] for node in data} == roads_nodes

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.us_state/nodes/",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.repair_orders",
        "default.repair_order_details",
        "default.regional_level_agg",
        "default.national_level_agg",
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.repair_orders_fact",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
        "default.contractors",
        "default.repair_type",
    }

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.municipality_dim/nodes/",
    )
    data = response.json()
    assert {node["name"] for node in data} == roads_nodes

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.contractor/nodes/",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.repair_type",
        "default.regional_level_agg",
        "default.regional_repair_efficiency",
    }

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.municipality_dim/nodes/?node_type=metric",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }


@pytest.mark.asyncio
async def test_list_nodes_with_common_dimension(
    module__client_with_roads_and_acc_revenue: AsyncClient,
) -> None:
    """
    Test ``GET /dimensions/common/``.
    """
    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/common/?dimension=default.hard_hat",
    )
    data = response.json()
    roads_nodes = {
        "default.repair_orders",
        "default.repair_order_details",
        "default.regional_level_agg",
        "default.national_level_agg",
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.repair_orders_fact",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }
    assert {node["name"] for node in data} == roads_nodes

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&dimension=default.municipality_dim",
    )
    data = response.json()
    assert {node["name"] for node in data} == roads_nodes

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&dimension=default.payment_type",
    )
    data = response.json()
    assert {node["name"] for node in data} == set()

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&node_type=metric",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }
