"""
Tests for client code generator.
"""

from fastapi.testclient import TestClient


def test_generated_python_client_code_new_metric(client_with_examples: TestClient):
    """
    Test generating Python client code for creating a new metric
    """
    response = client_with_examples.get(
        "/client/python/new_node/default.num_repair_orders",
    )
    assert (
        response.json()
        == """from datajunction import DJClient, NodeMode

dj = DJClient(DJ_URL)

num_repair_orders = dj.new_metric(
    description="Number of repair orders",
    query=\"\"\"SELECT  count(repair_order_id) default_DOT_num_repair_orders
 FROM default.repair_orders

\"\"\",
    display_name="Default: Num Repair Orders",
    name="default.num_repair_orders",
    primary_key=[]
)
num_repair_orders.save(NodeMode.PUBLISHED)"""
    )


def test_generated_python_client_code_new_source(client_with_examples: TestClient):
    """
    Test generating Python client code for creating a new source
    """
    response = client_with_examples.get(
        "/client/python/new_node/default.repair_order_details",
    )
    assert (
        response.json()
        == """from datajunction import DJClient, NodeMode

dj = DJClient(DJ_URL)

repair_order_details = dj.new_source(
    description="Details on repair orders",
    table="repair_order_details",
    display_name="Default: Repair Order Details",
    name="default.repair_order_details",
    schema_="roads",
    primary_key=[]
)
repair_order_details.save(NodeMode.PUBLISHED)"""
    )


def test_generated_python_client_code_new_dimension(client_with_examples: TestClient):
    """
    Test generating Python client code for creating a new dimension
    """
    response = client_with_examples.get("/client/python/new_node/default.repair_order")
    assert (
        response.json()
        == """from datajunction import DJClient, NodeMode

dj = DJClient(DJ_URL)

repair_order = dj.new_dimension(
    description="Repair order dimension",
    query=\"\"\"
                        SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id
                        FROM default.repair_orders
                    \"\"\",
    display_name="Default: Repair Order",
    name="default.repair_order",
    primary_key=['repair_order_id']
)
repair_order.save(NodeMode.PUBLISHED)"""
    )


def test_generated_python_client_code_new_cube(client_with_examples: TestClient):
    """
    Test generating Python client code for creating a new dimension
    """
    client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.num_repair_orders", "default.total_repair_cost"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.city",
            ],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repairs_cube",
        },
    )
    response = client_with_examples.get("/client/python/new_node/default.repairs_cube")
    assert (
        response.json()
        == """from datajunction import DJClient, NodeMode

dj = DJClient(DJ_URL)

repairs_cube = dj.new_cube(
    description="Cube of various metrics related to repairs",
    display_name="Default: Repairs Cube",
    name="default.repairs_cube",
    primary_key=[],
    metrics=["default.num_repair_orders", "default.total_repair_cost"],
    dimensions=["default.hard_hat.country", "default.hard_hat.city"]
)
repairs_cube.save(NodeMode.PUBLISHED)"""
    )


def test_generated_python_client_code_adding_materialization(
    client_with_query_service: TestClient,
):
    """
    Test that generating python client code for adding materialization works
    """
    client_with_query_service.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "2.4.4",
            "dialect": "spark",
        },
    )
    client_with_query_service.post(
        "/nodes/basic.transform.country_agg/materialization/",
        json={
            "engine": {
                "name": "spark",
                "version": "2.4.4",
            },
            "config": {
                "partitions": [
                    {
                        "name": "country",
                        "values": ["DE", "MY"],
                        "type_": "categorical",
                    },
                ],
            },
            "schedule": "0 * * * *",
        },
    )
    response = client_with_query_service.get(
        "/client/python/add_materialization/basic.transform.country_agg/country_3491792861",
    )
    assert (
        response.json()
        == """from datajunction import DJClient, MaterializationConfig

dj = DJClient(DJ_URL)

country_agg = dj.transform(
    "basic.transform.country_agg"
)
materialization = MaterializationConfig(
    engine=Engine(
        name="spark",
        version="2.4.4",
    ),
    schedule="0 * * * *",
    config={
        "partitions": [
            {
                "name": "country",
                "values": [
                    "DE",
                    "MY"
                ],
                "range": null,
                "expression": null,
                "type_": "categorical"
            }
        ],
        "spark": {}
    },
)
country_agg.add_materialization(
    materialization
)"""
    )


def test_generated_python_client_code_link_dimension(client_with_examples: TestClient):
    """
    Test generating Python client code for creating a new dimension
    """
    response = client_with_examples.get(
        "/client/python/link_dimension/foo.bar.repair_orders/"
        "municipality_id/foo.bar.municipality_dim/",
    )
    assert (
        response.json()
        == """from datajunction import DJClient, MaterializationConfig

dj = DJClient(DJ_URL)
repair_orders = dj.source(
    "foo.bar.repair_orders"
)
repair_orders.link_dimension(
    "municipality_id",
    "foo.bar.municipality_dim",
)"""
    )
