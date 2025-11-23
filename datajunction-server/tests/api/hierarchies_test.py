"""
Integration tests for hierarchy API endpoints.
"""

from http import HTTPStatus
from unittest import mock

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.hierarchy import Hierarchy
from datajunction_server.database.history import History
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.internal.history import ActivityType, EntityType

# Import shared fixtures
from tests.fixtures.hierarchy_fixtures import (  # noqa: F401
    time_catalog,
    time_sources,
    time_dimensions,
    time_dimension_links,
    calendar_hierarchy,
    fiscal_hierarchy,
    day_quarter_link,
    month_year_link,
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
                },
                {
                    "name": "quarter",
                    "dimension_node": "default.quarter_dim",
                },
                {
                    "name": "month",
                    "dimension_node": "default.month_dim",
                },
            ],
        }

        response = await client_with_basic.post(
            "/hierarchies/",
            json=hierarchy_data,
        )
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
                },
                {
                    "name": "month",
                    "dimension_node": "default.month_dim",
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
                },
                {
                    "name": "month",
                    "dimension_node": "also.nonexistent",
                },
            ],
        }

        response = await client_with_basic.post(
            "/hierarchies/",
            json=hierarchy_data,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["message"] == (
            "Hierarchy validation failed: Level 'year': Dimension node 'nonexistent.dimension' "
            "does not exist; Level 'month': Dimension node 'also.nonexistent' does not exist"
        )

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
                },
                {
                    "name": "year",  # Duplicate name
                    "dimension_node": "default.month_dim",
                },
            ],
        }

        response = await client_with_basic.post(
            "/hierarchies/",
            json=hierarchy_data,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        # Pydantic catches duplicate names
        data = response.json()
        assert "Level names must be unique" in str(data)

    async def test_update_hierarchy_with_single_level(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
    ):
        """Test Pydantic validation catches update with less than 2 levels."""
        update_data = {
            "levels": [
                {
                    "name": "year",
                    "dimension_node": "default.year_dim",
                },
            ],
        }

        response = await client_with_basic.put(
            "/hierarchies/calendar_hierarchy",
            json=update_data,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        data = response.json()
        assert "must have at least 2 levels" in str(data)

    async def test_update_hierarchy_with_duplicate_names(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
    ):
        """Test Pydantic validation catches duplicate names in update."""
        update_data = {
            "levels": [
                {
                    "name": "level",
                    "dimension_node": "default.year_dim",
                },
                {
                    "name": "level",
                    "dimension_node": "default.month_dim",
                },
            ],
        }

        response = await client_with_basic.put(
            "/hierarchies/calendar_hierarchy",
            json=update_data,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        data = response.json()
        assert "Level names must be unique" in str(data)

    async def test_update_hierarchy_without_levels(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
    ):
        """Test updating hierarchy metadata without changing levels."""
        update_data = {
            "display_name": "Updated Display Name",
            "description": "Updated Description",
        }

        response = await client_with_basic.put(
            "/hierarchies/calendar_hierarchy",
            json=update_data,
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert data["display_name"] == "Updated Display Name"
        assert data["description"] == "Updated Description"
        # Levels should remain unchanged
        assert len(data["levels"]) == 4

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
        session: AsyncSession,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test updating hierarchy levels."""
        dimensions, revisions = time_dimensions

        update_data = {
            "levels": [
                {
                    "name": "year",
                    "dimension_node": "default.year_dim",
                },
                {
                    "name": "quarter",
                    "dimension_node": "default.quarter_dim",
                },
                {
                    "name": "day",
                    "dimension_node": "default.day_dim",
                },
            ],
        }

        # First attempt should fail - no dimension link from day to quarter
        response = await client_with_basic.put(
            "/hierarchies/calendar_hierarchy",
            json=update_data,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        data = response.json()
        assert "No dimension link exists" in data["message"]

        # Now add the missing dimension link from day to quarter
        from datajunction_server.database.dimensionlink import DimensionLink
        from datajunction_server.models.dimensionlink import JoinType

        day_quarter_link = DimensionLink(
            node_revision=revisions["day"],
            dimension=dimensions["quarter"],
            join_sql="default.day_dim.year_id = default.quarter_dim.year_id AND default.day_dim.quarter_id = default.quarter_dim.quarter_id",
            join_type=JoinType.INNER,
        )
        session.add(day_quarter_link)
        await session.commit()

        # Now the update should succeed with valid dimension links
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

    async def test_validate_hierarchy(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
        time_dimension_links: DimensionLink,
        month_year_link,
    ):
        """Test hierarchy validation endpoint."""
        response = await client_with_basic.post(
            "/hierarchies/calendar_hierarchy/validate",
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert data["errors"] == []
        assert data["is_valid"] is True

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
        month_year_link,
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
                },
                {
                    "name": "month",
                    "dimension_node": "default.month_dim",
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

        # Query the History table to verify all operations were tracked
        history_entries = await session.execute(
            select(History)
            .where(
                History.entity_type == EntityType.HIERARCHY,
                History.entity_name == "history_test",
            )
            .order_by(History.created_at),
        )
        history = history_entries.scalars().all()

        # Should have 3 history entries: CREATE, UPDATE, DELETE
        assert len(history) == 3

        # Verify CREATE entry
        create_entry = history[0]
        assert create_entry.activity_type == ActivityType.CREATE
        assert create_entry.entity_name == "history_test"
        assert create_entry.entity_type == EntityType.HIERARCHY
        assert create_entry.user == "dj"
        assert create_entry.post["name"] == "history_test"
        assert create_entry.post["display_name"] == "History Test"
        assert len(create_entry.post["levels"]) == 2
        assert create_entry.pre == {}

        # Verify UPDATE entry
        update_entry = history[1]
        assert update_entry.activity_type == ActivityType.UPDATE
        assert update_entry.entity_name == "history_test"
        assert update_entry.pre["description"] is None
        assert (
            update_entry.post["description"] == "Updated description for history test"
        )

        # Verify DELETE entry
        delete_entry = history[2]
        assert delete_entry.activity_type == ActivityType.DELETE
        assert delete_entry.entity_name == "history_test"
        assert delete_entry.pre["name"] == "history_test"
        assert len(delete_entry.pre["levels"]) == 2
        assert delete_entry.post == {}

    async def test_get_dimension_hierarchies(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
        fiscal_hierarchy: Hierarchy,
    ):
        """Test getting hierarchies that use a specific dimension."""
        # Get hierarchies using month_dim (used in both calendar and fiscal)
        response = await client_with_basic.get("/nodes/default.month_dim/hierarchies/")
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert data["dimension_node"] == "default.month_dim"
        assert len(data["hierarchies"]) == 2

        # Find the calendar hierarchy navigation info
        calendar_nav = next(
            h
            for h in data["hierarchies"]
            if h["hierarchy_name"] == "calendar_hierarchy"
        )
        assert calendar_nav["current_level"] == "month"
        assert calendar_nav["current_level_order"] == 1

        # Check drill-up options (to year)
        assert len(calendar_nav["drill_up"]) == 1
        assert calendar_nav["drill_up"][0]["level_name"] == "year"
        assert calendar_nav["drill_up"][0]["dimension_node"] == "default.year_dim"
        assert calendar_nav["drill_up"][0]["steps"] == 1

        # Check drill-down options (to week, then day)
        assert len(calendar_nav["drill_down"]) == 2
        week_drill = next(
            d for d in calendar_nav["drill_down"] if d["level_name"] == "week"
        )
        assert week_drill["dimension_node"] == "default.week_dim"
        assert week_drill["steps"] == 1

        day_drill = next(
            d for d in calendar_nav["drill_down"] if d["level_name"] == "day"
        )
        assert day_drill["dimension_node"] == "default.day_dim"
        assert day_drill["steps"] == 2

        # Find the fiscal hierarchy navigation info
        fiscal_nav = next(
            h for h in data["hierarchies"] if h["hierarchy_name"] == "fiscal_hierarchy"
        )
        assert fiscal_nav["current_level"] == "month"
        assert fiscal_nav["current_level_order"] == 2

        # Check fiscal drill-up options (to quarter, then year)
        assert len(fiscal_nav["drill_up"]) == 2
        quarter_drill = next(
            d for d in fiscal_nav["drill_up"] if d["level_name"] == "quarter"
        )
        assert quarter_drill["dimension_node"] == "default.quarter_dim"
        assert quarter_drill["steps"] == 1

        year_drill = next(
            d for d in fiscal_nav["drill_up"] if d["level_name"] == "year"
        )
        assert year_drill["steps"] == 2

    async def test_get_dimension_hierarchies_not_used(
        self,
        client_with_basic: AsyncClient,
        calendar_hierarchy: Hierarchy,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test getting hierarchies for a dimension that's not used in any hierarchy."""
        # Quarter is used in fiscal but we only have calendar loaded in this test
        # Actually quarter IS in fiscal_hierarchy, but let's test with a dimension not in calendar
        response = await client_with_basic.get(
            "/nodes/default.quarter_dim/hierarchies/",
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        assert data["dimension_node"] == "default.quarter_dim"
        # Quarter is not in calendar_hierarchy (only year, month, week, day)
        # So it should return empty or only fiscal if that's loaded
        # Since we only have calendar_hierarchy fixture, it should be empty
        assert len(data["hierarchies"]) == 0

    async def test_get_dimension_hierarchies_nonexistent_node(
        self,
        client_with_basic: AsyncClient,
    ):
        """Test getting hierarchies for a non-existent node."""
        response = await client_with_basic.get(
            "/nodes/nonexistent.dimension/hierarchies/",
        )
        assert response.status_code == HTTPStatus.NOT_FOUND
        data = response.json()
        assert "does not exist" in data["message"]

    async def test_get_dimension_hierarchies_non_dimension_node(
        self,
        client_with_basic: AsyncClient,
        time_sources: dict[str, Node],
    ):
        """Test getting hierarchies for a non-dimension node."""
        # Try with a source node
        response = await client_with_basic.get(
            "/nodes/default.year_source/hierarchies/",
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        data = response.json()
        assert "Node 'default.year_source' is not a dimension node" in data["message"]
