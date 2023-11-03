"""
Tests for the dimensions API.
"""
from typing import Callable, List, Optional

from fastapi.testclient import TestClient


def test_list_dimension(client_with_roads: TestClient) -> None:
    """
    Test ``GET /dimensions/``.
    """
    response = client_with_roads.get("/dimensions/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) > 5


def test_list_nodes_with_dimension_access_limited(
    client_with_roads: TestClient,
) -> None:
    """
    Test ``GET /dimensions/{name}/nodes/``.
    """
    # pylint: disable=import-outside-toplevel
    from datajunction_server.internal.access.authorization import validate_access
    from datajunction_server.models import access

    # pylint: enable=import-outside-toplevel

    def validate_access_override():
        def _validate_access(access_control: access.AccessControl):
            for request in access_control.requests:
                if "repair" in request.access_object.name:
                    request.approve()
                else:
                    request.deny()

        return _validate_access

    app = client_with_roads.app
    app.dependency_overrides[validate_access] = validate_access_override

    response = client_with_roads.get("/dimensions/default.hard_hat/nodes/")

    data = response.json()
    roads_repair_nodes = {
        "default.repair_orders",
        "default.repair_order_details",
        "default.regional_repair_efficiency",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.repair_orders_fact",
        "default.total_repair_cost",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
    }
    assert {node["name"] for node in data} == roads_repair_nodes


def test_list_nodes_with_dimension(client_with_roads: TestClient) -> None:
    """
    Test ``GET /dimensions/{name}/nodes/``.
    """
    response = client_with_roads.get("/dimensions/default.hard_hat/nodes/")
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

    response = client_with_roads.get("/dimensions/default.repair_order/nodes/")
    data = response.json()
    assert {node["name"] for node in data} == roads_nodes

    response = client_with_roads.get("/dimensions/default.us_state/nodes/")
    data = response.json()
    assert {node["name"] for node in data} == roads_nodes

    response = client_with_roads.get("/dimensions/default.municipality_dim/nodes/")
    data = response.json()
    assert {node["name"] for node in data} == roads_nodes

    response = client_with_roads.get("/dimensions/default.contractor/nodes/")
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.repair_type",
        "default.regional_level_agg",
        "default.regional_repair_efficiency",
    }

    response = client_with_roads.get(
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


def test_list_nodes_with_common_dimension(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> None:
    """
    Test ``GET /dimensions/common/``.
    """
    custom_client = client_example_loader(["ACCOUNT_REVENUE", "ROADS"])
    response = custom_client.get(
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

    response = custom_client.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&dimension=default.municipality_dim",
    )
    data = response.json()
    assert {node["name"] for node in data} == roads_nodes

    response = custom_client.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&dimension=default.payment_type",
    )
    data = response.json()
    assert {node["name"] for node in data} == set()

    response = custom_client.get(
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
