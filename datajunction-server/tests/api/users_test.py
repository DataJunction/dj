"""
Tests for users API endpoints
"""

import pytest
from httpx import AsyncClient


class TestUsers:
    """
    Test users API endpoints.
    """

    @pytest.mark.asyncio
    async def test_get_users(self, module__client_with_roads: AsyncClient) -> None:
        """
        Test ``GET /users``
        """

        response = await module__client_with_roads.get("/users?with_activity=true")
        assert response.json() == [{"username": "dj", "count": 69}]

        response = await module__client_with_roads.get("/users")
        assert response.json() == ["dj"]

    @pytest.mark.asyncio
    async def test_list_nodes_by_user(
        self,
        module__client_with_roads: AsyncClient,
    ) -> None:
        """
        Test ``GET /users/{username}``
        """

        response = await module__client_with_roads.get("/users/dj")
        assert {(node["name"], node["type"]) for node in response.json()} == {
            ("default.repair_orders", "source"),
            ("default.repair_orders_view", "source"),
            ("default.repair_order_details", "source"),
            ("default.repair_type", "source"),
            ("default.contractors", "source"),
            ("default.municipality_municipality_type", "source"),
            ("default.municipality_type", "source"),
            ("default.municipality", "source"),
            ("default.dispatchers", "source"),
            ("default.hard_hats", "source"),
            ("default.hard_hat_state", "source"),
            ("default.us_states", "source"),
            ("default.us_region", "source"),
            ("default.repair_order", "dimension"),
            ("default.contractor", "dimension"),
            ("default.hard_hat", "dimension"),
            ("default.hard_hat_2", "dimension"),
            ("default.hard_hat_to_delete", "dimension"),
            ("default.local_hard_hats", "dimension"),
            ("default.local_hard_hats_1", "dimension"),
            ("default.local_hard_hats_2", "dimension"),
            ("default.us_state", "dimension"),
            ("default.dispatcher", "dimension"),
            ("default.municipality_dim", "dimension"),
            ("default.regional_level_agg", "transform"),
            ("default.national_level_agg", "transform"),
            ("default.repair_orders_fact", "transform"),
            ("default.regional_repair_efficiency", "metric"),
            ("default.num_repair_orders", "metric"),
            ("default.avg_repair_price", "metric"),
            ("default.total_repair_cost", "metric"),
            ("default.avg_length_of_employment", "metric"),
            ("default.discounted_orders_rate", "metric"),
            ("default.total_repair_order_discounts", "metric"),
            ("default.avg_repair_order_discounts", "metric"),
            ("default.avg_time_to_dispatch", "metric"),
        }
