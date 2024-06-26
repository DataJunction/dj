# pylint: disable=too-many-lines
"""
Tests for the collections API.
"""
import pytest
from httpx import AsyncClient

class TestDataForNode:
    """
    Test ``POST /collections/``.
    """

    @pytest.mark.asyncio
    async def test_creating_a_collection(
        self,
        client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test creating a new collection
        """
        response = await client_with_account_revenue.post(
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
        response = await client_with_account_revenue.get(
            "/collections/",
        )
        assert response.status_code == 200
        
        data = response.json()
        assert data == [
            {
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            }
        ]

    @pytest.mark.asyncio
    async def test_adding_to_collection(
        self,
        client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test adding a node to a collection
        """
        # Create a collection
        response = await client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        data = response.json()
        assert response.status_code == 201
        
        # Add a node to a collection
        response = await client_with_account_revenue.post(
            "/collections/Accounting/nodes/",
            json=["default.payment_type"],
        )
        assert response.status_code == 204

    @pytest.mark.asyncio
    async def test_removing_from_collection(
        self,
        client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test removing a node to a collection
        """
        # Create a collection
        response = await client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        data = response.json()
        assert response.status_code == 201
        
        # Add a node to a collection
        response = await client_with_account_revenue.post(
            "/collections/Accounting/nodes/",
            json=["default.payment_type"],
        )
        assert response.status_code == 204

        # Remove the node from the collection
        response = await client_with_account_revenue.post(
            "/collections/Accounting/remove/",
            json=["default.payment_type"],
        )
        assert response.status_code == 204
        
        # Confirm node no longer found in collection
        response = await client_with_account_revenue.get(
            "/collections/Accounting",
        )
        data = response.json()
        assert data["nodes"] == []


    @pytest.mark.asyncio
    async def test_deleting_collection(
        self,
        client_with_account_revenue: AsyncClient,
    ) -> None:
        """
        Test removing a collection
        """
        # Create a collection
        response = await client_with_account_revenue.post(
            "/collections/",
            json={
                "name": "Accounting",
                "description": "This is a collection that contains accounting related nodes",
            },
        )
        assert response.status_code == 201
        
        # Remove the collection
        response = await client_with_account_revenue.delete(
            "/collections/Accounting",
        )
        assert response.status_code == 204
        
        # Ensure the collection does not show up in collections list
        response = await client_with_account_revenue.get(
            "/collections/",
        )
        assert response.status_code == 200
        
        data = response.json()
        assert data == []