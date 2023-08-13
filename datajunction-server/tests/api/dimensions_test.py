"""
Tests for the dimensions API.
"""
from fastapi.testclient import TestClient


def test_list_dimension(client_with_examples: TestClient) -> None:
    """
    Test ``GET /dimensions/``.
    """
    response = client_with_examples.get("/dimensions/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) > 10


def test_list_nodes_with_dimension(client_with_examples: TestClient) -> None:
    """
    Test ``GET /dimensions/{name}/nodes/``.
    """
    response = client_with_examples.get("/dimensions/default.hard_hat/nodes/")
    data = response.json()
    roads_nodes = [
        "default.repair_orders",
        "default.repair_order_details",
        "default.regional_level_agg",
        "default.national_level_agg",
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    ]
    assert [node["name"] for node in data] == roads_nodes

    response = client_with_examples.get("/dimensions/default.repair_order/nodes/")
    data = response.json()
    assert [node["name"] for node in data] == roads_nodes

    response = client_with_examples.get("/dimensions/default.us_state/nodes/")
    data = response.json()
    assert [node["name"] for node in data] == roads_nodes

    response = client_with_examples.get("/dimensions/default.municipality_dim/nodes/")
    data = response.json()
    assert [node["name"] for node in data] == roads_nodes

    response = client_with_examples.get("/dimensions/default.contractor/nodes/")
    data = response.json()
    assert [node["name"] for node in data] == [
        "default.repair_type",
        "default.regional_level_agg",
        "default.regional_repair_efficiency",
    ]

    response = client_with_examples.get(
        "/dimensions/default.municipality_dim/nodes/?node_type=metric",
    )
    data = response.json()
    assert [node["name"] for node in data] == [
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    ]


def test_list_nodes_with_common_dimension(client_with_examples: TestClient) -> None:
    """
    Test ``GET /dimensions/common/``.
    """
    response = client_with_examples.get(
        "/dimensions/common/?dimension=default.hard_hat",
    )
    data = response.json()
    roads_nodes = [
        "default.repair_orders",
        "default.repair_order_details",
        "default.regional_level_agg",
        "default.national_level_agg",
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    ]
    assert [node["name"] for node in data] == roads_nodes

    response = client_with_examples.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&dimension=default.municipality_dim",
    )
    data = response.json()
    assert [node["name"] for node in data] == roads_nodes

    response = client_with_examples.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&dimension=default.payment_type",
    )
    data = response.json()
    assert [node["name"] for node in data] == []

    response = client_with_examples.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&node_type=metric",
    )
    data = response.json()
    assert [node["name"] for node in data] == [
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    ]
