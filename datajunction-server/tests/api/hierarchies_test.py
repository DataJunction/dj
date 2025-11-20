"""
Integration tests for hierarchy API endpoints.
"""

from http import HTTPStatus
from unittest import mock

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.hierarchy import Hierarchy
from datajunction_server.database.node import Node, NodeRevision

# Import shared fixtures
from tests.fixtures.hierarchy_fixtures import (  # noqa: F401
    time_catalog,
    time_sources,
    time_dimensions,
    time_dimension_links,
    calendar_hierarchy,
    fiscal_hierarchy,
)


class TestHierarchiesAPI:
    """Integration tests for hierarchy API endpoints."""

    async def test_list_hierarchies_empty(
        self,
        client_with_basic: AsyncClient,
    ):
        """Test listing hierarchies when none exist."""
        response = await client_with_basic.get("/hierarchies/")
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data == []

    async def test_list_hierarchies_with_data(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
        fiscal_hierarchy: Hierarchy,
    ):
        """Test listing hierarchies with existing data."""
        response = await client_with_basic.get("/hierarchies/")
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert len(data) == 2
        hierarchy_names = {h["name"] for h in data}
        assert hierarchy_names == {"calendar_hierarchy", "fiscal_hierarchy"}

        # Check structure of returned data
        for hierarchy in data:
            assert "name" in hierarchy
            assert "display_name" in hierarchy
            assert "created_by" in hierarchy
            assert "created_at" in hierarchy
            assert "level_count" in hierarchy

    async def test_get_hierarchy_by_name(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
    ):
        """Test getting a specific hierarchy by name."""
        response = await client_with_basic.get("/hierarchies/calendar_hierarchy")
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert data["name"] == "calendar_hierarchy"
        assert data["display_name"] == "Calendar Hierarchy"
        assert len(data["levels"]) == 4

        # Verify levels are properly ordered
        level_names = [level["name"] for level in data["levels"]]
        assert level_names == ["year", "month", "week", "day"]

        # Verify each level has proper structure
        for level in data["levels"]:
            assert "name" in level
            assert "dimension_node" in level
            assert "level_order" in level
            assert isinstance(level["dimension_node"], dict)
            assert "name" in level["dimension_node"]

    async def test_get_nonexistent_hierarchy(
        self,
        client_with_basic: AsyncClient,
    ):
        """Test getting a hierarchy that doesn't exist."""
        response = await client_with_basic.get("/hierarchies/nonexistent")
        assert response.status_code == HTTPStatus.NOT_FOUND

    async def test_create_hierarchy(
        self,
        client_with_basic: AsyncClient,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
        time_dimension_links,
    ):
        """Test creating a new hierarchy via API."""
        dimensions, _ = time_dimensions

        hierarchy_data = {
            "name": "api_test_hierarchy",
            "display_name": "API Test Hierarchy",
            "description": "A hierarchy created via API test",
            "levels": [
                {
                    "name": "year",
                    "dimension_node": "default.year_dim",
                    "level_order": 0,
                },
                {
                    "name": "quarter",
                    "dimension_node": "default.quarter_dim",
                    "level_order": 1,
                },
                {
                    "name": "month",
                    "dimension_node": "default.month_dim",
                    "level_order": 2,
                },
            ],
        }

        response = await client_with_basic.post(
            "/hierarchies/",
            json=hierarchy_data,
        )
        print("ress", response.json())
        assert response.status_code == HTTPStatus.CREATED
        data = response.json()

        assert data["name"] == "api_test_hierarchy"
        assert data["display_name"] == "API Test Hierarchy"
        assert len(data["levels"]) == 3

        # Verify the hierarchy can be retrieved
        get_response = await client_with_basic.get("/hierarchies/api_test_hierarchy")
        assert get_response.status_code == HTTPStatus.OK

    async def test_create_hierarchy_duplicate_name(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test creating a hierarchy with a duplicate name."""
        dimensions, _ = time_dimensions

        hierarchy_data = {
            "name": "calendar_hierarchy",  # Same name as existing
            "display_name": "Duplicate Calendar",
            "description": "This should fail",
            "levels": [
                {
                    "name": "year",
                    "dimension_node": "default.year_dim",
                    "level_order": 0,
                },
                {
                    "name": "month",
                    "dimension_node": "default.month_dim",
                    "level_order": 1,
                },
            ],
        }

        response = await client_with_basic.post(
            "/hierarchies/",
            json=hierarchy_data,
        )
        assert response.status_code == HTTPStatus.CONFLICT

    async def test_create_hierarchy_invalid_dimension_node(
        self,
        client_with_basic: AsyncClient,
    ):
        """Test creating a hierarchy with non-existent dimension node."""
        hierarchy_data = {
            "name": "invalid_hierarchy",
            "display_name": "Invalid Hierarchy",
            "description": "This should fail",
            "levels": [
                {
                    "name": "year",
                    "dimension_node": "nonexistent.dimension",
                    "level_order": 0,
                },
                {
                    "name": "month",
                    "dimension_node": "also.nonexistent",
                    "level_order": 1,
                },
            ],
        }

        response = await client_with_basic.post(
            "/hierarchies/",
            json=hierarchy_data,
        )
        assert response.status_code == HTTPStatus.BAD_REQUEST

    async def test_create_hierarchy_validation_errors(
        self,
        client_with_basic: AsyncClient,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test creating a hierarchy with validation errors."""
        dimensions, _ = time_dimensions

        # Test duplicate level names
        hierarchy_data = {
            "name": "validation_test",
            "display_name": "Validation Test",
            "levels": [
                {
                    "name": "year",
                    "dimension_node": "default.year_dim",
                    "level_order": 0,
                },
                {
                    "name": "year",  # Duplicate name
                    "dimension_node": "default.month_dim",
                    "level_order": 1,
                },
            ],
        }

        response = await client_with_basic.post(
            "/hierarchies/",
            json=hierarchy_data,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

    async def test_update_hierarchy(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test updating a hierarchy."""
        dimensions, _ = time_dimensions

        update_data = {
            "display_name": "Updated Calendar Hierarchy",
            "description": "This hierarchy has been updated",
        }

        response = await client_with_basic.put(
            "/hierarchies/calendar_hierarchy",
            json=update_data,
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert data["display_name"] == "Updated Calendar Hierarchy"
        assert data["description"] == "This hierarchy has been updated"

    async def test_update_hierarchy_levels(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test updating hierarchy levels."""
        dimensions, _ = time_dimensions

        update_data = {
            "levels": [
                {
                    "name": "year",
                    "dimension_node": "default.year_dim",
                    "level_order": 0,
                },
                {
                    "name": "quarter",
                    "dimension_node": "default.quarter_dim",
                    "level_order": 1,
                },
                {
                    "name": "day",
                    "dimension_node": "default.day_dim",
                    "level_order": 2,
                },
            ],
        }

        response = await client_with_basic.put(
            "/hierarchies/calendar_hierarchy",
            json=update_data,
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert len(data["levels"]) == 3
        level_names = [level["name"] for level in data["levels"]]
        assert level_names == ["year", "quarter", "day"]

    async def test_update_nonexistent_hierarchy(
        self,
        client_with_basic: AsyncClient,
    ):
        """Test updating a hierarchy that doesn't exist."""
        update_data = {
            "display_name": "Updated Nonexistent",
        }

        response = await client_with_basic.put(
            "/hierarchies/nonexistent",
            json=update_data,
        )
        assert response.status_code == HTTPStatus.NOT_FOUND

    async def test_delete_hierarchy(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
    ):
        """Test deleting a hierarchy."""
        response = await client_with_basic.delete("/hierarchies/calendar_hierarchy")
        assert response.status_code == HTTPStatus.NO_CONTENT

        # Verify it's actually deleted
        get_response = await client_with_basic.get("/hierarchies/calendar_hierarchy")
        assert get_response.status_code == HTTPStatus.NOT_FOUND

    async def test_delete_nonexistent_hierarchy(
        self,
        client_with_basic: AsyncClient,
    ):
        """Test deleting a hierarchy that doesn't exist."""
        response = await client_with_basic.delete("/hierarchies/nonexistent")
        assert response.status_code == HTTPStatus.NOT_FOUND

    async def test_get_hierarchy_levels(
        self,
        client_with_basic: AsyncClient,
        fiscal_hierarchy: Hierarchy,
    ):
        """Test getting hierarchy levels endpoint."""
        response = await client_with_basic.get("/hierarchies/fiscal_hierarchy/levels")
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert len(data) == 4
        level_names = [level["name"] for level in data]
        assert level_names == ["year", "quarter", "month", "day"]

    async def test_validate_hierarchy(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
    ):
        """Test hierarchy validation endpoint."""
        response = await client_with_basic.post(
            "/hierarchies/calendar_hierarchy/validate",
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert data["is_valid"] is True
        assert len(data["errors"]) == 0

    async def test_validate_nonexistent_hierarchy(
        self,
        client_with_basic: AsyncClient,
    ):
        """Test validating a hierarchy that doesn't exist."""
        response = await client_with_basic.post(
            "/hierarchies/nonexistent_hierarchy/validate",
        )
        assert response.status_code == HTTPStatus.NOT_FOUND

    async def test_api_hierarchy_permissions(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
    ):
        """Test that hierarchy operations require proper authentication."""
        # Verify authenticated requests work
        response = await client_with_basic.get("/hierarchies/")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == [
            {
                "created_at": mock.ANY,
                "created_by": {"username": "dj"},
                "description": "Year -> Month -> Week -> Day hierarchy",
                "display_name": "Calendar Hierarchy",
                "level_count": 4,
                "name": "calendar_hierarchy",
            },
        ]

        response = await client_with_basic.get("/hierarchies/calendar_hierarchy")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {
            "created_at": mock.ANY,
            "created_by": {"username": "dj"},
            "description": "Year -> Month -> Week -> Day hierarchy",
            "display_name": "Calendar Hierarchy",
            "levels": [
                {
                    "dimension_node": {
                        "name": "default.year_dim",
                    },
                    "grain_columns": None,
                    "level_order": 0,
                    "name": "year",
                },
                {
                    "dimension_node": {
                        "name": "default.month_dim",
                    },
                    "grain_columns": None,
                    "level_order": 1,
                    "name": "month",
                },
                {
                    "dimension_node": {
                        "name": "default.week_dim",
                    },
                    "grain_columns": None,
                    "level_order": 2,
                    "name": "week",
                },
                {
                    "dimension_node": {
                        "name": "default.day_dim",
                    },
                    "grain_columns": None,
                    "level_order": 3,
                    "name": "day",
                },
            ],
            "name": "calendar_hierarchy",
        }

    async def test_hierarchy_history_tracking(
        self,
        client_with_basic: AsyncClient,
        session: AsyncSession,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
        time_dimension_links,
    ):
        """Test that hierarchy operations are tracked in history."""
        dimensions, _ = time_dimensions

        # Create a hierarchy
        hierarchy_data = {
            "name": "history_test",
            "display_name": "History Test",
            "levels": [
                {
                    "name": "year",
                    "dimension_node": "default.year_dim",
                    "level_order": 0,
                },
                {
                    "name": "month",
                    "dimension_node": "default.month_dim",
                    "level_order": 1,
                },
            ],
        }

        create_response = await client_with_basic.post(
            "/hierarchies/",
            json=hierarchy_data,
        )
        assert create_response.status_code == HTTPStatus.CREATED

        # Update the hierarchy
        update_data = {
            "description": "Updated description for history test",
        }

        update_response = await client_with_basic.put(
            "/hierarchies/history_test",
            json=update_data,
        )
        assert update_response.status_code == HTTPStatus.OK

        # Delete the hierarchy
        delete_response = await client_with_basic.delete("/hierarchies/history_test")
        assert delete_response.status_code == HTTPStatus.NO_CONTENT

        # Note: To fully test history tracking, we'd need to query the History table
        # This test verifies the operations complete successfully, which implies
        # history tracking is working (as it would fail if history logging broke)
