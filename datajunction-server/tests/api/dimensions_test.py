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
    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/?prefix=default",
    )
    data = response.json()

    assert response.status_code == 200

    results = {(dim["name"], dim["indegree"]) for dim in data}
    assert ("default.dispatcher", 3) in results
    assert ("default.repair_order", 2) in results
    assert ("default.hard_hat", 2) in results
    assert ("default.hard_hat_to_delete", 2) in results
    assert ("default.municipality_dim", 2) in results
    assert ("default.contractor", 1) in results
    assert ("default.us_state", 2) in results
    assert ("default.local_hard_hats", 0) in results
    assert ("default.local_hard_hats_1", 0) in results
    assert ("default.local_hard_hats_2", 0) in results
    assert ("default.payment_type", 0) in results
    assert ("default.account_type", 0) in results
    assert ("default.hard_hat_2", 0) in results


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
        "default.repair_order",
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
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
    assert {node["name"] for node in data} == {
        "default.repair_order_details",
        "default.repair_orders",
    }

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.us_state/nodes/",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.avg_length_of_employment",
        "default.repair_orders",
        "default.repair_order_details",
        "default.hard_hat",
        "default.repair_order",
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
        "default.avg_repair_price",
        "default.repair_orders_fact",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
        "default.contractors",
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
    }

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.municipality_dim/nodes/?node_type=metric",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
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
        "default.repair_order",
        "default.repair_orders",
        "default.repair_order_details",
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
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
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }
