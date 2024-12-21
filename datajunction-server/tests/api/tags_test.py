"""
Tests for tags.
"""
from unittest import mock

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.base import Base


class TestTags:
    """
    Test tags API endpoints.
    """

    @pytest_asyncio.fixture(autouse=True)
    async def cleanup_tables(self, module__session: AsyncSession):
        """
        Fixture to clean up tables after each test is run
        """
        yield  # Testing happens
        # Teardown: remove any data from database (even data not created by this session)
        for table in Base.metadata.tables.keys():
            await module__session.execute(text(f'TRUNCATE TABLE "{table}" CASCADE'))
        await module__session.commit()

    async def create_tag(self, module__client: AsyncClient):
        """
        Creates a tag.
        """
        response = await module__client.post(
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

    async def create_another_tag(self, module__client: AsyncClient):
        """
        Creates another tag
        """
        response = await module__client.post(
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

    @pytest.mark.asyncio
    async def test_create_and_read_tag(self, module__client: AsyncClient) -> None:
        """
        Test ``POST /tags`` and ``GET /tags/{name}``
        """
        response = await self.create_tag(module__client)
        expected_tag_output = {
            "tag_metadata": {},
            "display_name": "Sales Report",
            "description": "All metrics for sales",
            "name": "sales_report",
            "tag_type": "group",
        }
        assert response.status_code == 201
        assert response.json() == expected_tag_output

        response = await module__client.post(
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

        response = await module__client.get("/tags/sales_report/")
        assert response.status_code == 200
        assert response.json() == expected_tag_output

        response = await module__client.get("/tags/sales_report2/")
        assert response.status_code == 200
        assert response.json() == expected_tag_output2

        # Check history
        response = await module__client.get("/history/tag/sales_report/")
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
        response = await self.create_tag(module__client)
        response_data = response.json()
        assert (
            response_data["message"] == "A tag with name `sales_report` already exists!"
        )

    @pytest.mark.asyncio
    async def test_update_tag(self, module__client: AsyncClient) -> None:
        """
        Tests updating a tag.
        """
        response = await self.create_tag(module__client)
        assert response.status_code == 201

        # Trying updating the tag
        response = await module__client.patch(
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
        response = await module__client.patch(
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
        response = await module__client.get("/history/tag/sales_report/")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("update", "tag"), ("update", "tag"), ("create", "tag")]

    @pytest.mark.asyncio
    async def test_list_tags(self, module__client: AsyncClient) -> None:
        """
        Test ``GET /tags``
        """
        response = await self.create_tag(module__client)
        assert response.status_code == 201

        response = await module__client.get("/tags/")
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

        await module__client.post(
            "/tags/",
            json={
                "name": "impressions_report",
                "display_name": "Impressions Report",
                "description": "Metrics for various types of impressions",
                "tag_type": "group",
                "tag_metadata": {},
            },
        )

        await module__client.post(
            "/tags/",
            json={
                "name": "rotors",
                "display_name": "Rotors",
                "description": "Department of brakes",
                "tag_type": "business_area",
                "tag_metadata": {},
            },
        )

        response = await module__client.get("/tags/?tag_type=group")
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

        response = await module__client.get("/tags/?tag_type=business_area")
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

    @pytest.mark.asyncio
    async def test_add_tag_to_node(self, client_with_dbt: AsyncClient) -> None:
        """
        Test ``POST /tags`` and ``GET /tags/{name}``
        """
        response = await self.create_tag(client_with_dbt)
        assert response.status_code == 201
        await self.create_another_tag(client_with_dbt)

        # Trying tag a node with a nonexistent tag should fail
        response = await client_with_dbt.post(
            "/nodes/default.items_sold_count/tags?tag_names=random_tag",
        )
        assert response.status_code == 404
        response_data = response.json()
        assert response_data["message"] == "Tags not found: random_tag"

        # Trying tag a node with an existing tag should succeed
        response = await client_with_dbt.post(
            "/nodes/default.items_sold_count/tags/?tag_names=sales_report",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["message"] == (
            "Node `default.items_sold_count` has been successfully "
            "updated with the following tags: sales_report"
        )

        # Test finding all nodes for that tag
        response = await client_with_dbt.get(
            "/tags/sales_report/nodes/",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data) == 1
        assert response_data == [
            {
                "display_name": "Items Sold Count",
                "mode": "published",
                "name": "default.items_sold_count",
                "description": "Total units sold",
                "edited_by": None,
                "tags": None,
                "status": "valid",
                "type": "metric",
                "updated_at": mock.ANY,
                "version": "v1.0",
            },
        ]

        # Tag a second node
        response = await client_with_dbt.post(
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
        response = await client_with_dbt.get("/history?node=default.total_profit")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"], activity["details"])
            for activity in history
        ] == [
            ("tag", "node", {"tags": ["sales_report", "reports"]}),
            ("create", "node", {}),
        ]

        # Check finding nodes for tag
        response = await client_with_dbt.get(
            "/tags/sales_report/nodes/",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data) == 2
        assert response_data == [
            {
                "display_name": "Items Sold Count",
                "mode": "published",
                "name": "default.items_sold_count",
                "description": "Total units sold",
                "status": "valid",
                "edited_by": None,
                "tags": None,
                "type": "metric",
                "updated_at": mock.ANY,
                "version": "v1.0",
            },
            {
                "display_name": "Total Profit",
                "mode": "published",
                "name": "default.total_profit",
                "description": "Total profit",
                "status": "valid",
                "edited_by": None,
                "tags": None,
                "type": "metric",
                "updated_at": mock.ANY,
                "version": "v1.0",
            },
        ]

        # Check getting nodes for tag after deactivating a node
        await client_with_dbt.delete("/nodes/default.total_profit")
        response = await client_with_dbt.get(
            "/tags/sales_report/nodes/",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data) == 1
        assert response_data[0]["name"] == "default.items_sold_count"

        # Check finding nodes for tag
        response = await client_with_dbt.get(
            "/tags/random_tag/nodes/",
        )
        assert response.status_code == 404
        response_data = response.json()
        assert (
            response_data["message"] == "A tag with name `random_tag` does not exist."
        )

        # Check finding nodes for tag
        response = await client_with_dbt.get(
            "/tags/sales_report/nodes/?node_type=transform",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == []
