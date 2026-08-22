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
        users = response.json()
        # Find the dj user - with all examples loaded, count will be higher
        dj_user = next((u for u in users if u["username"] == "dj"), None)
        assert dj_user is not None
        assert dj_user["count"] >= 54  # At least roads example count

        response = await module__client_with_roads.get("/users")
        assert "dj" in response.json()

    @pytest.mark.asyncio
    async def test_list_nodes_by_user(
        self,
        module__client_with_roads: AsyncClient,
    ) -> None:
        """
        Test ``GET /users/{username}``
        """

        response = await module__client_with_roads.get("/users/dj")
        actual_nodes = {(node["name"], node["type"]) for node in response.json()}
        # Expected nodes from ROADS examples - should be present (may have more from template)
        expected_roads_nodes = {
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
            ("default.num_unique_hard_hats_approx", "metric"),
            ("default.avg_repair_price", "metric"),
            ("default.total_repair_cost", "metric"),
            ("default.avg_length_of_employment", "metric"),
            ("default.discounted_orders_rate", "metric"),
            ("default.total_repair_order_discounts", "metric"),
            ("default.avg_repair_order_discounts", "metric"),
            ("default.avg_time_to_dispatch", "metric"),
        }
        assert expected_roads_nodes.issubset(actual_nodes)

    @pytest.mark.asyncio
    async def test_list_nodes_by_user_no_history(
        self,
        module__client_with_roads: AsyncClient,
    ) -> None:
        """
        Test ``GET /users/{username}`` for a user with no matching history
        returns an empty list without querying nodes/tags/history further.
        """

        response = await module__client_with_roads.get(
            "/users/a-user-with-no-history-whatsoever",
        )
        assert response.status_code == 200
        assert response.json() == []

    @pytest.mark.asyncio
    async def test_list_nodes_by_user_tags_and_edited_by(
        self,
        client_with_roads: AsyncClient,
    ) -> None:
        """
        Test that ``GET /users/{username}`` includes tags on nodes the user
        has touched, and correctly populates `edited_by` from history (rather
        than always returning it empty).
        """

        # Create a tag and attach it to a node created by the `dj` user
        tag_response = await client_with_roads.post(
            "/tags/",
            json={
                "name": "user_activity_tag",
                "description": "A tag for testing user activity",
                "tag_type": "test",
            },
        )
        assert tag_response.status_code == 201

        tag_node_response = await client_with_roads.post(
            "/nodes/default.hard_hat/tags/?tag_names=user_activity_tag",
        )
        assert tag_node_response.status_code == 200

        response = await client_with_roads.get("/users/dj")
        assert response.status_code == 200
        nodes_by_name = {node["name"]: node for node in response.json()}
        hard_hat_node = nodes_by_name["default.hard_hat"]
        assert hard_hat_node["tags"] == [{"name": "user_activity_tag"}]
        assert hard_hat_node["edited_by"] == ["dj"]

    @pytest.mark.asyncio
    async def test_list_nodes_by_user_excludes_deactivated(
        self,
        client_with_roads: AsyncClient,
    ) -> None:
        """
        Test that ``GET /users/{username}`` omits nodes the user touched that
        have since been deactivated.
        """

        response = await client_with_roads.get("/users/dj")
        assert response.status_code == 200
        assert "default.hard_hat" in {node["name"] for node in response.json()}

        delete_response = await client_with_roads.delete("/nodes/default.hard_hat")
        assert delete_response.status_code == 200

        response = await client_with_roads.get("/users/dj")
        assert response.status_code == 200
        assert "default.hard_hat" not in {node["name"] for node in response.json()}
