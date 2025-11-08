"""
Test hierarchy models and database operations using proper DJ patterns.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.hierarchy import Hierarchy, HierarchyLevel
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User

# Import shared fixtures
from tests.fixtures.hierarchy_fixtures import (  # noqa: F401
    time_catalog,
    time_sources,
    time_dimensions,
    time_dimension_links,
    calendar_hierarchy,
    fiscal_hierarchy,
)


class TestHierarchy:
    """
    Test hierarchy database models using proper DJ source->dimension->link patterns.
    """

    async def test_hierarchy_creation_year_month_week_day(
        self,
        calendar_hierarchy: Hierarchy,
    ):
        """Test creating a hierarchy: Year -> Month -> Week -> Day."""
        # Verify hierarchy was created correctly
        assert calendar_hierarchy.name == "calendar_hierarchy"
        assert calendar_hierarchy.display_name == "Calendar Hierarchy"
        assert len(calendar_hierarchy.levels) == 4

        # Verify levels are ordered correctly
        sorted_levels = sorted(
            calendar_hierarchy.levels,
            key=lambda lvl: lvl.level_order,
        )
        assert sorted_levels[0].name == "year"
        assert sorted_levels[1].name == "month"
        assert sorted_levels[2].name == "week"
        assert sorted_levels[3].name == "day"

    async def test_hierarchy_creation_year_quarter_month_day(
        self,
        fiscal_hierarchy: Hierarchy,
    ):
        """Test creating a hierarchy: Year -> Quarter -> Month -> Day."""
        # Verify hierarchy was created correctly
        assert fiscal_hierarchy.name == "fiscal_hierarchy"
        assert fiscal_hierarchy.display_name == "Fiscal Hierarchy"
        assert len(fiscal_hierarchy.levels) == 4

        # Verify levels are ordered correctly
        sorted_levels = sorted(fiscal_hierarchy.levels, key=lambda lvl: lvl.level_order)
        assert sorted_levels[0].name == "year"
        assert sorted_levels[1].name == "quarter"
        assert sorted_levels[2].name == "month"
        assert sorted_levels[3].name == "day"

    async def test_multiple_hierarchies_same_dimensions(
        self,
        session: AsyncSession,
        calendar_hierarchy: Hierarchy,
        fiscal_hierarchy: Hierarchy,
    ):
        """Test that multiple hierarchies can use the same dimension nodes."""
        # Verify both hierarchies exist
        assert (
            await Hierarchy.get_by_name(session, "calendar_hierarchy")
            == calendar_hierarchy
        )
        assert (
            await Hierarchy.get_by_name(session, "fiscal_hierarchy") == fiscal_hierarchy
        )

        # Verify they use some of the same dimension nodes
        calendar_node_ids = {
            level.dimension_node_id for level in calendar_hierarchy.levels
        }
        fiscal_node_ids = {level.dimension_node_id for level in fiscal_hierarchy.levels}

        # Should share year, month, and day dimensions
        shared_nodes = calendar_node_ids & fiscal_node_ids
        assert len(shared_nodes) == 3  # year, month, and day
        assert [level.name for level in calendar_hierarchy.levels] == [
            "year",
            "month",
            "week",
            "day",
        ]

    async def test_hierarchy_get_using_dimension(
        self,
        session: AsyncSession,
        current_user: User,
        calendar_hierarchy: Hierarchy,
        fiscal_hierarchy: Hierarchy,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test finding hierarchies that use a specific dimension node."""
        dimensions, _ = time_dimensions

        # Create hierarchy that doesn't use year dimension
        weekly_hierarchy = Hierarchy(
            name="weekly_hierarchy",
            display_name="Weekly Only",
            description="Week -> Day hierarchy",
            created_by_id=current_user.id,
        )
        session.add(weekly_hierarchy)
        await session.flush()

        # Add levels for weekly hierarchy
        week_level = HierarchyLevel(
            hierarchy_id=weekly_hierarchy.id,
            name="week",
            dimension_node_id=dimensions["week"].id,
            level_order=0,
        )
        day_level = HierarchyLevel(
            hierarchy_id=weekly_hierarchy.id,
            name="day",
            dimension_node_id=dimensions["day"].id,
            level_order=1,
        )
        session.add_all([week_level, day_level])
        await session.commit()

        # Test finding hierarchies using year dimension
        year_node_id = dimensions["year"].id
        hierarchies_with_year = await Hierarchy.get_using_dimension(
            session,
            year_node_id,
        )
        assert len(hierarchies_with_year) == 2

        hierarchy_names = {h.name for h in hierarchies_with_year}
        assert hierarchy_names == {"calendar_hierarchy", "fiscal_hierarchy"}

        # Test finding hierarchies using week dimension
        week_node_id = dimensions["week"].id
        hierarchies_with_week = await Hierarchy.get_using_dimension(
            session,
            week_node_id,
        )
        assert (
            len(hierarchies_with_week) == 2
        )  # calendar_hierarchy and weekly_hierarchy both use week

        hierarchy_names = {h.name for h in hierarchies_with_week}
        assert hierarchy_names == {"calendar_hierarchy", "weekly_hierarchy"}

    async def test_hierarchy_validation_with_proper_dimensions(
        self,
        session: AsyncSession,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test hierarchy level validation with proper dimension nodes."""
        dimensions, _ = time_dimensions

        # Valid levels using existing dimension nodes
        valid_levels = [
            {
                "name": "year",
                "dimension_node_id": dimensions["year"].id,
                "level_order": 0,
            },
            {
                "name": "month",
                "dimension_node_id": dimensions["month"].id,
                "level_order": 1,
            },
        ]

        errors = await Hierarchy.validate_levels(session, valid_levels)
        assert len(errors) == 0

        # Invalid levels with non-existent dimension node
        invalid_levels = [
            {
                "name": "year",
                "dimension_node_id": 99999,  # Non-existent ID
                "level_order": 0,
            },
            {
                "name": "month",
                "dimension_node_id": dimensions["month"].id,
                "level_order": 1,
            },
        ]

        errors = await Hierarchy.validate_levels(session, invalid_levels)
        assert any("does not exist" in error for error in errors)
