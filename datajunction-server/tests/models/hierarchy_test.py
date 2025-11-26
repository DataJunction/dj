"""
Test hierarchy models and database operations using proper DJ patterns.
"""

from sqlalchemy.ext.asyncio import AsyncSession

import pytest
from pydantic import ValidationError

from datajunction_server.database.hierarchy import Hierarchy, HierarchyLevel
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.models.hierarchy import (
    HierarchyCreateRequest,
    HierarchyLevelInput,
)
from datajunction_server.models.node_type import NodeType

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
            level_order=0,
            dimension_node_id=dimensions["week"].id,
        )
        day_level = HierarchyLevel(
            hierarchy_id=weekly_hierarchy.id,
            name="day",
            level_order=1,
            dimension_node_id=dimensions["day"].id,
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

    async def test_hierarchy_validation_with_invalid_levels(
        self,
        session: AsyncSession,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test hierarchy level validation with proper dimension nodes."""
        dimensions, _ = time_dimensions

        # Invalid levels with existing dimension nodes, where links don't exist
        valid_levels = [
            HierarchyLevelInput(
                name="year",
                dimension_node=dimensions["year"].name,
            ),
            HierarchyLevelInput(
                name="month",
                dimension_node=dimensions["month"].name,
            ),
        ]

        errors, _ = await Hierarchy.validate_levels(session, valid_levels)
        assert len(errors) == 1
        assert "No dimension link exists" in errors[0]

        # Invalid levels with non-existent dimension node
        invalid_levels = [
            HierarchyLevelInput(
                name="year",
                dimension_node="non_existent",  # Non-existent ID
            ),
            HierarchyLevelInput(
                name="month",
                dimension_node=dimensions["month"].name,
            ),
        ]

        errors, _ = await Hierarchy.validate_levels(session, invalid_levels)
        assert any("does not exist" in error for error in errors)

    async def test_hierarchy_validation_less_than_two_levels(
        self,
        session: AsyncSession,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test validation fails with only one level."""
        dimensions, _ = time_dimensions

        single_level = [
            HierarchyLevelInput(
                name="year",
                dimension_node=dimensions["year"].name,
            ),
        ]
        with pytest.raises(ValidationError) as exc_info:
            hierarchy = HierarchyCreateRequest(
                name="single_level_hierarchy",
                levels=single_level,
            )
            HierarchyCreateRequest.model_validate(hierarchy)
        assert "should have at least 2 items" in str(exc_info.value)

    async def test_hierarchy_validation_duplicate_level_names(
        self,
        session: AsyncSession,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test validation fails with duplicate level names."""
        dimensions, _ = time_dimensions

        duplicate_names = [
            HierarchyLevelInput(
                name="time",  # Same name
                dimension_node=dimensions["year"].name,
            ),
            HierarchyLevelInput(
                name="time",  # Same name
                dimension_node=dimensions["month"].name,
            ),
        ]

        errors, _ = await Hierarchy.validate_levels(session, duplicate_names)
        assert any("Level names must be unique" in error for error in errors)

    async def test_hierarchy_validation_non_dimension_node(
        self,
        session: AsyncSession,
        time_sources: dict[str, Node],
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test validation fails when using a non-dimension node."""
        dimensions, _ = time_dimensions

        with_source_node = [
            HierarchyLevelInput(
                name="year",
                dimension_node=dimensions["year"].name,
            ),
            HierarchyLevelInput(
                name="source",
                dimension_node=time_sources["month"].name,  # SOURCE node!
            ),
        ]

        errors, _ = await Hierarchy.validate_levels(session, with_source_node)
        assert any("not a dimension node" in error for error in errors)

    async def test_hierarchy_validation_single_dimension_hierarchy(
        self,
        session: AsyncSession,
        current_user: User,
    ):
        """Test validation of single-dimension hierarchy with grain_columns."""
        from datajunction_server.database.catalog import Catalog
        from datajunction_server.database.column import Column
        from datajunction_server.sql.parsing.types import StringType

        # Create a catalog
        catalog = Catalog(name="test_catalog")
        session.add(catalog)
        await session.flush()

        # Create a single dimension with multiple grain columns
        location_dim = Node(
            name="default.location_dim",
            type=NodeType.DIMENSION,
            current_version="v1",
            created_by_id=current_user.id,
        )
        location_rev = NodeRevision(
            node=location_dim,
            name=location_dim.name,
            type=location_dim.type,
            version="v1",
            catalog_id=catalog.id,
            schema_="default",
            table="locations",
            query="SELECT country, state, city FROM locations",
            columns=[
                Column(name="country", type=StringType(), order=0),
                Column(name="state", type=StringType(), order=1),
                Column(name="city", type=StringType(), order=2),
            ],
            created_by_id=current_user.id,
        )
        session.add(location_rev)
        await session.commit()

        # Create hierarchy with same dimension at different grains
        single_dim_levels = [
            HierarchyLevelInput(
                name="country",
                dimension_node="default.location_dim",
                grain_columns=["country"],
            ),
            HierarchyLevelInput(
                name="state",
                dimension_node="default.location_dim",  # Same dimension!
                grain_columns=["country", "state"],
            ),
        ]

        # Should validate successfully - skips dimension link check
        errors, _ = await Hierarchy.validate_levels(session, single_dim_levels)
        assert len(errors) == 0

    async def test_hierarchy_validation_wrong_cardinality(
        self,
        session: AsyncSession,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test validation fails with wrong cardinality dimension link."""
        from datajunction_server.database.dimensionlink import DimensionLink
        from datajunction_server.models.dimensionlink import JoinType, JoinCardinality

        dimensions, revisions = time_dimensions

        # Add a dimension link with ONE_TO_ONE cardinality (wrong!)
        bad_link = DimensionLink(
            node_revision=revisions["month"],
            dimension=dimensions["year"],
            join_sql="default.month_dim.year_id = default.year_dim.year_id",
            join_type=JoinType.INNER,
            join_cardinality=JoinCardinality.ONE_TO_ONE,
        )
        session.add(bad_link)
        await session.commit()

        levels = [
            HierarchyLevelInput(
                name="year",
                dimension_node=dimensions["year"].name,
            ),
            HierarchyLevelInput(
                name="month",
                dimension_node=dimensions["month"].name,
            ),
        ]

        errors, _ = await Hierarchy.validate_levels(session, levels)
        assert any("cardinality" in error.lower() for error in errors)

    async def test_hierarchy_validation_pk_not_in_join(
        self,
        session: AsyncSession,
        time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    ):
        """Test validation warning when join doesn't reference primary key."""
        from datajunction_server.database.dimensionlink import DimensionLink
        from datajunction_server.models.dimensionlink import JoinType

        dimensions, revisions = time_dimensions

        # Add a dimension link that doesn't use the PK in join SQL
        weird_link = DimensionLink(
            node_revision=revisions["month"],
            dimension=dimensions["year"],
            join_sql="default.month_dim.some_col = default.year_dim.other_col",  # Not PK!
            join_type=JoinType.INNER,
        )
        session.add(weird_link)
        await session.commit()

        levels = [
            HierarchyLevelInput(
                name="year",
                dimension_node=dimensions["year"].name,
            ),
            HierarchyLevelInput(
                name="month",
                dimension_node=dimensions["month"].name,
            ),
        ]

        errors, _ = await Hierarchy.validate_levels(session, levels)
        assert any(
            "WARN" in error or "primary key" in error.lower() for error in errors
        )
