"""
Tests for client code generator.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_generated_python_client_code_new_metric(
    module__client_with_roads: AsyncClient,
):
    """
    Test generating Python client code for creating a new metric
    """
    response = await module__client_with_roads.get(
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
async def test_generated_python_client_code_new_source(
    module__client_with_roads: AsyncClient,
):
    """
    Test generating Python client code for creating a new source
    """
    response = await module__client_with_roads.get(
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
    module__client_with_roads: AsyncClient,
):
    """
    Test generating Python client code for creating a new dimension
    """
    response = await module__client_with_roads.get(
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
async def test_generated_python_client_code_new_cube(
    module__client_with_roads: AsyncClient,
):
    """
    Test generating Python client code for creating a new dimension
    """
    await module__client_with_roads.post(
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
    response = await module__client_with_roads.get(
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
async def test_generated_python_client_code_link_dimension(
    module__client_with_roads: AsyncClient,
):
    """
    Test generating Python client code for creating a new dimension
    """
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/link_dimension/default.repair_orders/"
        "municipality_id/municipality_dim/",
    )
    assert (
        response.json()
        == """dj = DJBuilder(DJ_URL)
repair_orders = dj.source(
    "default.repair_orders"
)
repair_orders.link_dimension(
    "municipality_id",
    "municipality_dim",
)"""
    )
