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
        assert data == {
            "name": "Accounting",
            "description": "This is a collection that contains accounting related nodes",
        }

        # Ensure the collection shows up in collections list
        response = await module__client_with_account_revenue.get(
            "/collections/",
        )
        assert response.status_code == 200

        data = response.json()
        assert data == [
            {
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
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        assert response.status_code == 201

        # Add a node to a collection
        response = await module__client_with_account_revenue.post(
            "/collections/Accounting/nodes/",
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
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        assert response.status_code == 201

        # Add a node to a collection
        response = await module__client_with_account_revenue.post(
            "/collections/Accounting/nodes/",
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
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        data = response.json()
        assert response.status_code == 201

        # Add a node to a collection
        response = await module__client_with_account_revenue.post(
            "/collections/Accounting/nodes/",
            json=["default.payment_type"],
        )
        assert response.status_code == 204

        # Remove the node from the collection
        response = await module__client_with_account_revenue.post(
            "/collections/Accounting/remove/",
            json=["default.payment_type", "default.revenue"],
        )
        assert response.status_code == 204

        # Confirm node no longer found in collection
        response = await module__client_with_account_revenue.get(
            "/collections/Accounting",
        )
        data = response.json()
        assert data["nodes"] == []

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
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        assert response.status_code == 201

        # Remove the node from the collection
        response = await module__client_with_account_revenue.post(
            "/collections/Accounting/remove/",
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
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        assert response.status_code == 201

        # Remove the collection
        response = await module__client_with_account_revenue.delete(
            "/collections/Accounting",
        )
        assert response.status_code == 204

        # Ensure the collection does not show up in collections list
        response = await module__client_with_account_revenue.get(
            "/collections/",
        )
        assert response.status_code == 200

        data = response.json()
        assert data == []
