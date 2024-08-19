# pylint: disable=too-many-lines
"""
Tests for the collections API.
"""
import pytest
from httpx import AsyncClient


class TestCollections:
    """
    Test ``POST /collections/``.
    """

    @pytest.mark.asyncio
    async def test_collections_creating(
        self,
        module__client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test creating a new collection
        """
        response = await module__client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        data = response.json()
        assert response.status_code == 201

        # Ensure the collection shows up in collections list
        response = await module__client_with_account_revenue.get(
            "/collections/",
        )
        assert response.status_code == 200

        data = response.json()
        assert data == [
            {
                "id": 1,
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        ]

        # Ensure that creating the same collection again returns a 409
        response = await module__client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        data = response.json()
        assert response.status_code == 409
        assert "Collection already exists" in str(data)

    @pytest.mark.asyncio
    async def test_collections_adding(
        self,
        module__client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test adding a node to a collection
        """
        # Create a collection
        response = await module__client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "Revenue Project",
                "description": "A collection for nodes related to a revenue project",
            },
        )
        assert response.status_code == 201

        # Add a node to a collection
        response = await module__client_with_account_revenue.post(
            "/collections/Revenue%20Project/nodes/",
            json=["default.payment_type", "default.revenue"],
        )
        assert response.status_code == 204

    @pytest.mark.asyncio
    async def test_collections__nodes_not_found_when_adding(
        self,
        module__client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test adding a node to a collection when the nodes can't be found
        """
        # Create a collection
        response = await module__client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "My Collection",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        assert response.status_code == 201

        # Add a node to a collection
        response = await module__client_with_account_revenue.post(
            "/collections/My%20Collection/nodes/",
            json=["foo.bar", "baz.qux"],
        )
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_collections_removing_nodes(
        self,
        module__client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test removing a node from a collection
        """
        # Create a collection
        response = await module__client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "add_then_remove",
                "description": "A collection to test adding and removing a node",
            },
        )
        data = response.json()
        assert response.status_code == 201

        # Add a node to a collection
        response = await module__client_with_account_revenue.post(
            "/collections/add_then_remove/nodes/",
            json=["default.payment_type"],
        )
        assert response.status_code == 204

        # Remove the node from the collection
        response = await module__client_with_account_revenue.post(
            "/collections/add_then_remove/remove/",
            json=["default.payment_type", "default.revenue"],
        )
        assert response.status_code == 204

        # Confirm node no longer found in collection
        response = await module__client_with_account_revenue.get(
            "/collections/add_then_remove",
        )
        data = response.json()
        for node in data["nodes"]:
            assert node["name"] != "default.payment_type"

    @pytest.mark.asyncio
    async def test_collections_no_errors_when_removing(
        self,
        module__client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test removing a node from a collection when the node can't be found
        """
        # Create a collection
        response = await module__client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "Removing Nodes",
                "description": "This is a collection to test for no errors when removing nodes",
            },
        )
        assert response.status_code == 201

        # Remove the node from the collection
        response = await module__client_with_account_revenue.post(
            "/collections/Removing%20Nodes/remove/",
            json=["foo.bar", "baz.qux"],
        )
        assert response.status_code == 204

    @pytest.mark.asyncio
    async def test_collections_deleting(
        self,
        module__client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test removing a collection
        """
        # Create a collection
        response = await module__client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "DeleteMe",
                "description": "This collection will be deleted",
            },
        )
        assert response.status_code == 201

        # Remove the collection
        response = await module__client_with_account_revenue.delete(
            "/collections/DeleteMe",
        )
        assert response.status_code == 204

        # Ensure the collection does not show up in collections list
        response = await module__client_with_account_revenue.get(
            "/collections/",
        )
        assert response.status_code == 200

        data = response.json()
        for collection in data:
            assert collection["name"] != "DeleteMe"
