"""
Shared fixtures for hierarchy testing.

This module provides time-based dimension fixtures that can be reused
across both model and API tests for hierarchies.
"""

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import select

from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.hierarchy import Hierarchy, HierarchyLevel
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.models.node_type import NodeType
from datajunction_server.database.column import Column
from datajunction_server.sql.parsing.types import IntegerType, StringType, DateType
from datajunction_server.models.dimensionlink import JoinType


@pytest_asyncio.fixture
async def time_catalog(session: AsyncSession) -> Catalog:
    """Create a catalog for time dimensions."""
    catalog = Catalog(name="time_warehouse")
    session.add(catalog)
    await session.commit()
    return catalog


@pytest_asyncio.fixture
async def time_sources(
    session: AsyncSession,
    current_user: User,
    time_catalog: Catalog,
) -> dict[str, Node]:
    """Create time source tables."""
    sources = {}

    # Year source table
    year_source = Node(
        name="default.year_source",
        type=NodeType.SOURCE,
        current_version="v1",
        created_by_id=current_user.id,
    )
    year_source_rev = NodeRevision(
        node=year_source,
        name=year_source.name,
        type=year_source.type,
        version="v1",
        catalog_id=time_catalog.id,
        schema_="time",
        table="years",
        columns=[
            Column(name="year_id", type=IntegerType(), order=0),
            Column(name="year_name", type=StringType(), order=1),
        ],
        created_by_id=current_user.id,
    )
    session.add(year_source_rev)
    sources["year"] = year_source

    # Quarter source table
    quarter_source = Node(
        name="default.quarter_source",
        type=NodeType.SOURCE,
        current_version="v1",
        created_by_id=current_user.id,
    )
    quarter_source_rev = NodeRevision(
        node=quarter_source,
        name=quarter_source.name,
        type=quarter_source.type,
        version="v1",
        catalog_id=time_catalog.id,
        schema_="time",
        table="quarters",
        columns=[
            Column(name="year_id", type=IntegerType(), order=0),
            Column(name="quarter_id", type=IntegerType(), order=1),
            Column(name="quarter_name", type=StringType(), order=2),
        ],
        created_by_id=current_user.id,
    )
    session.add(quarter_source_rev)
    sources["quarter"] = quarter_source

    # Month source table
    month_source = Node(
        name="default.month_source",
        type=NodeType.SOURCE,
        current_version="v1",
        created_by_id=current_user.id,
    )
    month_source_rev = NodeRevision(
        node=month_source,
        name=month_source.name,
        type=month_source.type,
        version="v1",
        catalog_id=time_catalog.id,
        schema_="time",
        table="months",
        columns=[
            Column(name="year_id", type=IntegerType(), order=0),
            Column(name="quarter_id", type=IntegerType(), order=1),
            Column(name="month_id", type=IntegerType(), order=2),
            Column(name="month_name", type=StringType(), order=3),
        ],
        created_by_id=current_user.id,
    )
    session.add(month_source_rev)
    sources["month"] = month_source

    # Week source table
    week_source = Node(
        name="default.week_source",
        type=NodeType.SOURCE,
        current_version="v1",
        created_by_id=current_user.id,
    )
    week_source_rev = NodeRevision(
        node=week_source,
        name=week_source.name,
        type=week_source.type,
        version="v1",
        catalog_id=time_catalog.id,
        schema_="time",
        table="weeks",
        columns=[
            Column(name="year_id", type=IntegerType(), order=0),
            Column(name="month_id", type=IntegerType(), order=1),
            Column(name="week_id", type=IntegerType(), order=2),
            Column(name="week_name", type=StringType(), order=3),
        ],
        created_by_id=current_user.id,
    )
    session.add(week_source_rev)
    sources["week"] = week_source

    # Day source table
    day_source = Node(
        name="default.day_source",
        type=NodeType.SOURCE,
        current_version="v1",
        created_by_id=current_user.id,
    )
    day_source_rev = NodeRevision(
        node=day_source,
        name=day_source.name,
        type=day_source.type,
        version="v1",
        catalog_id=time_catalog.id,
        schema_="time",
        table="days",
        columns=[
            Column(name="year_id", type=IntegerType(), order=0),
            Column(name="quarter_id", type=IntegerType(), order=1),
            Column(name="month_id", type=IntegerType(), order=2),
            Column(name="week_id", type=IntegerType(), order=3),
            Column(name="day_id", type=IntegerType(), order=4),
            Column(name="day_date", type=DateType(), order=5),
        ],
        created_by_id=current_user.id,
    )
    session.add(day_source_rev)
    sources["day"] = day_source

    await session.commit()
    await session.refresh(year_source)
    await session.refresh(quarter_source)
    await session.refresh(month_source)
    await session.refresh(week_source)
    await session.refresh(day_source)
    return sources


@pytest_asyncio.fixture
async def time_dimensions(
    session: AsyncSession,
    current_user: User,
    time_sources: dict[str, Node],
) -> tuple[dict[str, Node], dict[str, NodeRevision]]:
    """Create time dimension nodes built on source tables."""
    dimensions = {}
    revisions = {}

    # Get or create primary_key attribute type
    result = await session.execute(
        select(AttributeType).where(
            AttributeType.namespace == "system",
            AttributeType.name == "primary_key",
        ),
    )
    primary_key = result.scalar_one_or_none()

    if not primary_key:
        primary_key = AttributeType(
            namespace="system",
            name="primary_key",
            description="Points to a column which is part of the primary key of the node",
            uniqueness_scope=[],
            allowed_node_types=[
                NodeType.SOURCE,
                NodeType.TRANSFORM,
                NodeType.DIMENSION,
            ],
        )
        session.add(primary_key)
        await session.flush()

    # Year dimension
    year_dim = Node(
        name="default.year_dim",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    year_dim_rev = NodeRevision(
        node=year_dim,
        name=year_dim.name,
        type=year_dim.type,
        version="v1",
        query="SELECT year_id, year_name FROM default.year_source",
        columns=[
            Column(
                name="year_id",
                type=IntegerType(),
                order=0,
                attributes=[ColumnAttribute(attribute_type=primary_key)],
            ),
            Column(name="year_name", type=StringType(), order=1),
        ],
        created_by_id=current_user.id,
    )
    session.add(year_dim_rev)
    dimensions["year"] = year_dim
    revisions["year"] = year_dim_rev

    # Quarter dimension
    quarter_dim = Node(
        name="default.quarter_dim",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    quarter_dim_rev = NodeRevision(
        node=quarter_dim,
        name=quarter_dim.name,
        type=quarter_dim.type,
        version="v1",
        query="SELECT year_id, quarter_id, quarter_name FROM default.quarter_source",
        columns=[
            Column(name="year_id", type=IntegerType(), order=0),
            Column(
                name="quarter_id",
                type=IntegerType(),
                order=1,
                attributes=[ColumnAttribute(attribute_type=primary_key)],
            ),
            Column(name="quarter_name", type=StringType(), order=2),
        ],
        created_by_id=current_user.id,
    )
    session.add(quarter_dim_rev)
    dimensions["quarter"] = quarter_dim
    revisions["quarter"] = quarter_dim_rev

    # Month dimension
    month_dim = Node(
        name="default.month_dim",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    month_dim_rev = NodeRevision(
        node=month_dim,
        name=month_dim.name,
        type=month_dim.type,
        version="v1",
        query="SELECT year_id, quarter_id, month_id, month_name FROM default.month_source",
        columns=[
            Column(name="year_id", type=IntegerType(), order=0),
            Column(name="quarter_id", type=IntegerType(), order=1),
            Column(
                name="month_id",
                type=IntegerType(),
                order=2,
                attributes=[ColumnAttribute(attribute_type=primary_key)],
            ),
            Column(name="month_name", type=StringType(), order=3),
        ],
        created_by_id=current_user.id,
    )
    session.add(month_dim_rev)
    dimensions["month"] = month_dim
    revisions["month"] = month_dim_rev

    # Week dimension
    week_dim = Node(
        name="default.week_dim",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    week_dim_rev = NodeRevision(
        node=week_dim,
        name=week_dim.name,
        type=week_dim.type,
        version="v1",
        query="SELECT year_id, month_id, week_id, week_name FROM default.week_source",
        columns=[
            Column(name="year_id", type=IntegerType(), order=0),
            Column(name="month_id", type=IntegerType(), order=1),
            Column(
                name="week_id",
                type=IntegerType(),
                order=2,
                attributes=[ColumnAttribute(attribute_type=primary_key)],
            ),
            Column(name="week_name", type=StringType(), order=3),
        ],
        created_by_id=current_user.id,
    )
    session.add(week_dim_rev)
    dimensions["week"] = week_dim
    revisions["week"] = week_dim_rev

    # Day dimension
    day_dim = Node(
        name="default.day_dim",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    day_dim_rev = NodeRevision(
        node=day_dim,
        name=day_dim.name,
        type=day_dim.type,
        version="v1",
        query="SELECT year_id, quarter_id, month_id, week_id, day_id, day_date FROM default.day_source",
        columns=[
            Column(name="year_id", type=IntegerType(), order=0),
            Column(name="quarter_id", type=IntegerType(), order=1),
            Column(name="month_id", type=IntegerType(), order=2),
            Column(name="week_id", type=IntegerType(), order=3),
            Column(
                name="day_id",
                type=IntegerType(),
                order=4,
                attributes=[ColumnAttribute(attribute_type=primary_key)],
            ),
            Column(name="day_date", type=DateType(), order=5),
        ],
        created_by_id=current_user.id,
    )
    session.add(day_dim_rev)
    dimensions["day"] = day_dim
    revisions["day"] = day_dim_rev

    await session.commit()
    await session.refresh(year_dim)
    await session.refresh(quarter_dim)
    await session.refresh(month_dim)
    await session.refresh(week_dim)
    await session.refresh(day_dim)
    return dimensions, revisions


@pytest_asyncio.fixture
async def time_dimension_links(
    session: AsyncSession,
    time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
) -> None:
    """Create dimension links between time dimensions."""
    dimensions, revisions = time_dimensions

    # Get dimension nodes and revisions
    year_dim = dimensions["year"]
    quarter_dim = dimensions["quarter"]
    month_dim = dimensions["month"]
    week_dim = dimensions["week"]

    quarter_dim_rev = revisions["quarter"]
    month_dim_rev = revisions["month"]
    week_dim_rev = revisions["week"]
    day_dim_rev = revisions["day"]

    # Quarter -> Year link
    quarter_year_link = DimensionLink(
        node_revision=quarter_dim_rev,
        dimension=year_dim,
        join_sql="default.quarter_dim.year_id = default.year_dim.year_id",
        join_type=JoinType.INNER,
    )

    # Month -> Quarter link
    month_quarter_link = DimensionLink(
        node_revision=month_dim_rev,
        dimension=quarter_dim,
        join_sql="default.month_dim.year_id = default.quarter_dim.year_id AND default.month_dim.quarter_id = default.quarter_dim.quarter_id",
        join_type=JoinType.INNER,
    )

    # Week -> Month link
    week_month_link = DimensionLink(
        node_revision=week_dim_rev,
        dimension=month_dim,
        join_sql="default.week_dim.year_id = default.month_dim.year_id AND default.week_dim.month_id = default.month_dim.month_id",
        join_type=JoinType.INNER,
    )

    # Day -> Week link
    day_week_link = DimensionLink(
        node_revision=day_dim_rev,
        dimension=week_dim,
        join_sql="default.day_dim.year_id = default.week_dim.year_id AND default.day_dim.month_id = default.week_dim.month_id AND default.day_dim.week_id = default.week_dim.week_id",
        join_type=JoinType.INNER,
    )

    # Day -> Month link (alternative path)
    day_month_link = DimensionLink(
        node_revision=day_dim_rev,
        dimension=month_dim,
        join_sql="default.day_dim.year_id = default.month_dim.year_id AND default.day_dim.quarter_id = default.month_dim.quarter_id AND default.day_dim.month_id = default.month_dim.month_id",
        join_type=JoinType.INNER,
    )

    session.add_all(
        [
            quarter_year_link,
            month_quarter_link,
            week_month_link,
            day_week_link,
            day_month_link,
        ],
    )
    await session.commit()


@pytest_asyncio.fixture
async def month_year_link(
    session: AsyncSession,
    time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
) -> DimensionLink:
    """Create dimension links between time dimensions."""
    dimensions, revisions = time_dimensions
    year_dim = dimensions["year"]
    month_dim_rev = revisions["month"]
    year_month_link = DimensionLink(
        node_revision=month_dim_rev,
        dimension=year_dim,
        join_sql="default.month_dim.year_id = default.year_dim.year_id",
        join_type=JoinType.INNER,
    )
    session.add(year_month_link)
    await session.commit()
    return year_month_link


@pytest_asyncio.fixture
async def day_quarter_link(
    session: AsyncSession,
    time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
) -> DimensionLink:
    """Create dimension links between time dimensions."""
    dimensions, revisions = time_dimensions
    quarter_dim = dimensions["quarter"]
    day_dim_rev = revisions["day"]
    day_quarter_link = DimensionLink(
        node_revision=day_dim_rev,
        dimension=quarter_dim,
        join_sql="default.day_dim.year_id = default.quarter_dim.year_id AND default.day_dim.quarter_id = default.quarter_dim.quarter_id",
        join_type=JoinType.INNER,
    )
    session.add(day_quarter_link)
    await session.commit()
    return day_quarter_link


@pytest_asyncio.fixture
async def calendar_hierarchy(
    session: AsyncSession,
    current_user: User,
    time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    time_dimension_links: None,
) -> Hierarchy:
    """Create a calendar hierarchy: Year -> Month -> Week -> Day."""
    dimensions, _ = time_dimensions

    hierarchy = Hierarchy(
        name="calendar_hierarchy",
        display_name="Calendar Hierarchy",
        description="Year -> Month -> Week -> Day hierarchy",
        created_by_id=current_user.id,
    )
    session.add(hierarchy)
    await session.flush()

    # Add levels
    levels_data = [
        ("year", dimensions["year"].id, 0),
        ("month", dimensions["month"].id, 1),
        ("week", dimensions["week"].id, 2),
        ("day", dimensions["day"].id, 3),
    ]

    for name, node_id, order in levels_data:
        level = HierarchyLevel(
            hierarchy_id=hierarchy.id,
            name=name,
            dimension_node_id=node_id,
            level_order=order,
        )
        session.add(level)

    await session.commit()
    await session.refresh(hierarchy, ["levels"])
    return hierarchy


@pytest_asyncio.fixture
async def fiscal_hierarchy(
    session: AsyncSession,
    current_user: User,
    time_dimensions: tuple[dict[str, Node], dict[str, NodeRevision]],
    time_dimension_links: None,
) -> Hierarchy:
    """Create a fiscal hierarchy: Year -> Quarter -> Month -> Day."""
    dimensions, _ = time_dimensions

    hierarchy = Hierarchy(
        name="fiscal_hierarchy",
        display_name="Fiscal Hierarchy",
        description="Year -> Quarter -> Month -> Day hierarchy",
        created_by_id=current_user.id,
    )
    session.add(hierarchy)
    await session.flush()

    # Add levels
    levels_data = [
        ("year", dimensions["year"].id, 0),
        ("quarter", dimensions["quarter"].id, 1),
        ("month", dimensions["month"].id, 2),
        ("day", dimensions["day"].id, 3),
    ]

    for name, node_id, order in levels_data:
        level = HierarchyLevel(
            hierarchy_id=hierarchy.id,
            name=name,
            dimension_node_id=node_id,
            level_order=order,
        )
        session.add(level)

    await session.commit()
    await session.refresh(hierarchy, ["levels"])
    return hierarchy
