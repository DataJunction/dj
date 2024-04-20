"""
Tests for client code generator.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_generated_python_client_code_new_metric(client_with_roads: AsyncClient):
    """
    Test generating Python client code for creating a new metric
    """
    response = await client_with_roads.get(
        "/datajunction-clients/python/new_node/default.num_repair_orders",
    )
    assert (
        response.json()
        == """dj = DJBuilder(DJ_URL)

num_repair_orders = dj.create_metric(
    description="Number of repair orders",
    display_name="Default: Num Repair Orders",
    mode="published",
    name="default.num_repair_orders",
    query=\"\"\"SELECT count(repair_order_id) FROM default.repair_orders_fact\"\"\"
)"""
    )


@pytest.mark.asyncio
async def test_generated_python_client_code_new_source(client_with_roads: AsyncClient):
    """
    Test generating Python client code for creating a new source
    """
    response = await client_with_roads.get(
        "/datajunction-clients/python/new_node/default.repair_order_details",
    )
    assert (
        response.json()
        == """dj = DJBuilder(DJ_URL)

repair_order_details = dj.create_source(
    description="Details on repair orders",
    display_name="default.roads.repair_order_details",
    mode="published",
    name="default.repair_order_details",
    schema_="roads",
    table="repair_order_details"
)"""
    )


@pytest.mark.asyncio
async def test_generated_python_client_code_new_dimension(
    client_with_roads: AsyncClient,
):
    """
    Test generating Python client code for creating a new dimension
    """
    response = await client_with_roads.get(
        "/datajunction-clients/python/new_node/default.repair_order",
    )
    assert (
        response.json()
        == """dj = DJBuilder(DJ_URL)

repair_order = dj.create_dimension(
    description="Repair order dimension",
    display_name="Default: Repair Order",
    mode="published",
    name="default.repair_order",
    primary_key=['repair_order_id'],
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
                    \"\"\"
)"""
    )


@pytest.mark.asyncio
async def test_generated_python_client_code_new_cube(client_with_roads: AsyncClient):
    """
    Test generating Python client code for creating a new dimension
    """
    await client_with_roads.post(
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
    response = await client_with_roads.get(
        "/datajunction-clients/python/new_node/default.repairs_cube",
    )
    assert (
        response.json()
        == """dj = DJBuilder(DJ_URL)

repairs_cube = dj.create_cube(
    description="Cube of various metrics related to repairs",
    display_name="Default: Repairs Cube",
    mode="published",
    name="default.repairs_cube",
    metrics=["default.num_repair_orders", "default.total_repair_cost"],
    dimensions=["default.hard_hat.country", "default.hard_hat.city"]
)"""
    )


@pytest.mark.asyncio
async def test_generated_python_client_code_adding_materialization(
    client_with_query_service_example_loader,
):
    """
    Test that generating python client code for adding materialization works
    """
    custom_client = await client_with_query_service_example_loader(["BASIC"])
    await custom_client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "2.4.4",
            "dialect": "spark",
        },
    )
    await custom_client.post(
        "/nodes/basic.transform.country_agg/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {
                "spark": {},
            },
            "schedule": "0 * * * *",
        },
    )
    response = await custom_client.get(
        "/datajunction-clients/python/add_materialization/"
        "basic.transform.country_agg/spark_sql__full",
    )
    assert (
        response.json()
        == """dj = DJBuilder(DJ_URL)

country_agg = dj.transform(
    "basic.transform.country_agg"
)
materialization = MaterializationConfig(
    job="spark_sql",
    strategy="full",
    schedule="0 * * * *",
    config={
        "spark": {}
    },
)
country_agg.add_materialization(
    materialization
)"""
    )


@pytest.mark.asyncio
async def test_generated_python_client_code_link_dimension(
    client_with_namespaced_roads: AsyncClient,
):
    """
    Test generating Python client code for creating a new dimension
    """
    response = await client_with_namespaced_roads.get(
        "/datajunction-clients/python/link_dimension/foo.bar.repair_orders/"
        "municipality_id/foo.bar.municipality_dim/",
    )
    assert (
        response.json()
        == """dj = DJBuilder(DJ_URL)
repair_orders = dj.source(
    "foo.bar.repair_orders"
)
repair_orders.link_dimension(
    "municipality_id",
    "foo.bar.municipality_dim",
)"""
    )
