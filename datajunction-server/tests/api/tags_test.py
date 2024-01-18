"""
Tests for tags.
"""
from unittest import mock

from fastapi.testclient import TestClient


class TestTags:
    """
    Test tags API endpoints.
    """

    def create_tag(self, client: TestClient):
        """
        Creates a tag.
        """
        response = client.post(
            "/tags/",
            json={
                "name": "sales_report",
                "display_name": "Sales Report",
                "description": "All metrics for sales",
                "tag_type": "group",
                "tag_metadata": {},
            },
        )
        return response

    def create_another_tag(self, client: TestClient):
        """
        Creates another tag
        """
        response = client.post(
            "/tags/",
            json={
                "name": "reports",
                "display_name": "Reports",
                "description": "Sales metrics",
                "tag_type": "group",
                "tag_metadata": {},
            },
        )
        return response

    def test_create_and_read_tag(self, client: TestClient) -> None:
        """
        Test ``POST /tags`` and ``GET /tags/{name}``
        """
        response = self.create_tag(client)
        expected_tag_output = {
            "tag_metadata": {},
            "display_name": "Sales Report",
            "description": "All metrics for sales",
            "name": "sales_report",
            "tag_type": "group",
        }
        assert response.status_code == 201
        assert response.json() == expected_tag_output

        response = client.post(
            "/tags/",
            json={
                "name": "sales_report2",
                "display_name": "Sales Report2",
                "tag_type": "group",
            },
        )
        assert response.status_code == 201
        expected_tag_output2 = {
            "tag_metadata": {},
            "display_name": "Sales Report2",
            "description": None,
            "name": "sales_report2",
            "tag_type": "group",
        }
        assert response.json() == expected_tag_output2

        response = client.get("/tags/sales_report/")
        assert response.status_code == 200
        assert response.json() == expected_tag_output

        response = client.get("/tags/sales_report2/")
        assert response.status_code == 200
        assert response.json() == expected_tag_output2

        # Check history
        response = client.get("/history/tag/sales_report/")
        assert response.json() == [
            {
                "activity_type": "create",
                "node": None,
                "created_at": mock.ANY,
                "details": {},
                "entity_name": "sales_report",
                "entity_type": "tag",
                "id": mock.ANY,
                "post": {},
                "pre": {},
                "user": "dj",
            },
        ]

        # Creating it again should raise an exception
        response = self.create_tag(client)
        response_data = response.json()
        assert (
            response_data["message"] == "A tag with name `sales_report` already exists!"
        )

    def test_update_tag(self, client: TestClient) -> None:
        """
        Tests updating a tag.
        """
        response = self.create_tag(client)
        assert response.status_code == 201

        # Trying updating the tag
        response = client.patch(
            "/tags/sales_report/",
            json={
                "description": "Helpful sales metrics",
                "tag_metadata": {"order": 1},
                "display_name": "Sales Metrics",
            },
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == {
            "tag_metadata": {"order": 1},
            "display_name": "Sales Metrics",
            "description": "Helpful sales metrics",
            "name": "sales_report",
            "tag_type": "group",
        }

        # Trying updating the tag
        response = client.patch(
            "/tags/sales_report/",
            json={},
        )
        assert response.json() == {
            "tag_metadata": {"order": 1},
            "display_name": "Sales Metrics",
            "description": "Helpful sales metrics",
            "name": "sales_report",
            "tag_type": "group",
        }

        # Check history
        response = client.get("/history/tag/sales_report/")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("create", "tag"), ("update", "tag"), ("update", "tag")]

    def test_list_tags(self, client: TestClient) -> None:
        """
        Test ``GET /tags``
        """
        response = self.create_tag(client)
        assert response.status_code == 201

        response = client.get("/tags/")
        assert response.status_code == 200
        response_data = response.json()

        assert response_data == [
            {
                "description": "All metrics for sales",
                "display_name": "Sales Report",
                "name": "sales_report",
                "tag_metadata": {},
                "tag_type": "group",
            },
        ]

        client.post(
            "/tags/",
            json={
                "name": "impressions_report",
                "display_name": "Impressions Report",
                "description": "Metrics for various types of impressions",
                "tag_type": "group",
                "tag_metadata": {},
            },
        )

        client.post(
            "/tags/",
            json={
                "name": "rotors",
                "display_name": "Rotors",
                "description": "Department of brakes",
                "tag_type": "business_area",
                "tag_metadata": {},
            },
        )

        response = client.get("/tags/?tag_type=group")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == [
            {
                "description": "All metrics for sales",
                "tag_metadata": {},
                "name": "sales_report",
                "display_name": "Sales Report",
                "tag_type": "group",
            },
            {
                "description": "Metrics for various types of impressions",
                "tag_metadata": {},
                "name": "impressions_report",
                "display_name": "Impressions Report",
                "tag_type": "group",
            },
        ]

        response = client.get("/tags/?tag_type=business_area")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == [
            {
                "name": "rotors",
                "display_name": "Rotors",
                "description": "Department of brakes",
                "tag_type": "business_area",
                "tag_metadata": {},
            },
        ]

    def test_add_tag_to_node(self, client_with_dbt: TestClient) -> None:
        """
        Test ``POST /tags`` and ``GET /tags/{name}``
        """
        response = self.create_tag(client_with_dbt)
        assert response.status_code == 201
        self.create_another_tag(client_with_dbt)

        # Trying tag a node with a nonexistent tag should fail
        response = client_with_dbt.post(
            "/nodes/default.items_sold_count/tags?tag_names=random_tag",
        )
        assert response.status_code == 404
        response_data = response.json()
        assert response_data["message"] == "Tags not found: random_tag"

        # Trying tag a node with an existing tag should succeed
        response = client_with_dbt.post(
            "/nodes/default.items_sold_count/tags/?tag_names=sales_report",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["message"] == (
            "Node `default.items_sold_count` has been successfully "
            "updated with the following tags: sales_report"
        )

        # Test finding all nodes for that tag
        response = client_with_dbt.get(
            "/tags/sales_report/nodes/",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data) == 1
        assert response_data == [
            {
                "display_name": "Default: Items Sold Count",
                "mode": "published",
                "name": "default.items_sold_count",
                "description": "Total units sold",
                "status": "valid",
                "type": "metric",
                "updated_at": mock.ANY,
                "version": "v1.0",
            },
        ]

        # Tag a second node
        response = client_with_dbt.post(
            "/nodes/default.total_profit/tags/?tag_names=sales_report&tag_names=reports",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert (
            response_data["message"]
            == "Node `default.total_profit` has been successfully "
            "updated with the following tags: sales_report, reports"
        )

        # Check history
        response = client_with_dbt.get("/history?node=default.total_profit")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"], activity["details"])
            for activity in history
        ] == [
            ("create", "node", {}),
            ("tag", "node", {"tags": ["sales_report", "reports"]}),
        ]

        # Check finding nodes for tag
        response = client_with_dbt.get(
            "/tags/sales_report/nodes/",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data) == 2
        assert response_data == [
            {
                "display_name": "Default: Items Sold Count",
                "mode": "published",
                "name": "default.items_sold_count",
                "description": "Total units sold",
                "status": "valid",
                "type": "metric",
                "updated_at": mock.ANY,
                "version": "v1.0",
            },
            {
                "display_name": "Default: Total Profit",
                "mode": "published",
                "name": "default.total_profit",
                "description": "Total profit",
                "status": "valid",
                "type": "metric",
                "updated_at": mock.ANY,
                "version": "v1.0",
            },
        ]

        # Check finding nodes for tag
        response = client_with_dbt.get(
            "/tags/random_tag/nodes/",
        )
        assert response.status_code == 404
        response_data = response.json()
        assert (
            response_data["message"] == "A tag with name `random_tag` does not exist."
        )

        # Check finding nodes for tag
        response = client_with_dbt.get(
            "/tags/sales_report/nodes/?node_type=transform",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == []
