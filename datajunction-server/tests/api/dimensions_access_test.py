"""
Tests for the dimensions API.
"""

import pytest
from httpx import AsyncClient

from datajunction_server.api.main import app


@pytest.mark.asyncio
async def test_list_nodes_with_dimension_access_limited(
    module__client_with_roads: AsyncClient,
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

    app.dependency_overrides[validate_access] = validate_access_override

    response = await module__client_with_roads.get(
        "/dimensions/default.hard_hat/nodes/",
    )

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
    app.dependency_overrides.clear()
