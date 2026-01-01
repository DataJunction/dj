"""
Tests for the dimensions API.
"""

import pytest
from httpx import AsyncClient

from datajunction_server.internal.access.authorization import AuthorizationService
from datajunction_server.models import access


class RepairOnlyAuthorizationService(AuthorizationService):
    """
    Authorization service that only approves nodes with 'repair' in the name.
    """

    name = "repair_only"

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(
                request=request,
                approved="repair" in request.access_object.name,
            )
            for request in requests
        ]


@pytest.mark.asyncio
async def test_list_nodes_with_dimension_access_limited(
    module__client_with_roads: AsyncClient,
    mocker,
) -> None:
    """
    Test ``GET /dimensions/{name}/nodes/``.
    """

    def get_repair_only_service():
        return RepairOnlyAuthorizationService()

    mocker.patch(
        "datajunction_server.internal.access.authorization.validator.get_authorization_service",
        get_repair_only_service,
    )

    response = await module__client_with_roads.get(
        "/dimensions/default.hard_hat/nodes/",
    )

    data = response.json()
    roads_repair_nodes = {
        "default.repair_orders",
        "default.repair_order_details",
        "default.repair_order",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.repair_orders_fact",
        "default.total_repair_cost",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
    }
    assert {node["name"] for node in data} == roads_repair_nodes
