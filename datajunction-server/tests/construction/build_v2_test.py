"""Tests for building nodes"""

from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import datajunction_server.sql.parsing.types as ct
from datajunction_server.construction.build_v2 import build_node
from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink, JoinType
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJException
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node_type import NodeType
from datajunction_server.naming import amenable_name
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse

from ..sql.utils import compare_query_strings


@pytest_asyncio.fixture
async def primary_key_attribute(session: AsyncSession) -> AttributeType:
    attribute_type = AttributeType(
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
    session.add(attribute_type)
    await session.commit()
    await session.refresh(attribute_type)
    return attribute_type


@pytest_asyncio.fixture
async def events(session: AsyncSession) -> Node:
    events_node = Node(
        name="source.events",
        display_name="Events",
        type=NodeType.SOURCE,
        current_version="1",
    )
    events_node_revision = NodeRevision(
        node=events_node,
        name="source.events",
        display_name="Events",
        type=NodeType.SOURCE,
        version="1",
        schema_="test",
        table="events",
        columns=[
            Column(name="event_id", type=ct.BigIntType(), order=0),
            Column(name="user_id", type=ct.BigIntType(), order=1),
            Column(name="device_id", type=ct.BigIntType(), order=2),
            Column(name="country_code", type=ct.StringType(), order=3),
            Column(name="latency", type=ct.BigIntType(), order=3),
            Column(name="utc_date", type=ct.BigIntType(), order=4),
        ],
    )
    session.add(events_node_revision)
    await session.commit()
    await session.refresh(events_node, ["current"])
    return events_node


@pytest_asyncio.fixture
async def date_dim(session: AsyncSession, primary_key_attribute) -> Node:
    date_node = Node(
        name="shared.date",
        display_name="Date",
        type=NodeType.DIMENSION,
        current_version="1",
    )
    date_node_revision = NodeRevision(
        node=date_node,
        name="shared.date",
        display_name="Date",
        type=NodeType.DIMENSION,
        version="1",
        query="SELECT 1, 2, 3, 4 AS dateint",
        columns=[
            Column(
                name="dateint",
                type=ct.BigIntType(),
                order=0,
                attributes=[ColumnAttribute(attribute_type=primary_key_attribute)],
            ),
        ],
    )
    session.add(date_node_revision)
    await session.commit()
    await session.refresh(date_node, ["current"])
    return date_node


@pytest_asyncio.fixture
async def events_agg(session: AsyncSession) -> Node:
    events_agg_node = Node(
        name="agg.events",
        display_name="Events Aggregated",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    events_agg_node_revision = NodeRevision(
        node=events_agg_node,
        name="agg.events",
        display_name="Events Aggregated",
        type=NodeType.TRANSFORM,
        version="1",
        query="""
        SELECT
          user_id,
          utc_date,
          device_id,
          country_code,
          SUM(latency) AS total_latency
        FROM source.events
        GROUP BY user_id, utc_date. device_id, country_code
        """,
        columns=[
            Column(name="user_id", type=ct.BigIntType(), order=1),
            Column(name="utc_date", type=ct.BigIntType(), order=2),
            Column(name="device_id", type=ct.BigIntType(), order=3),
            Column(name="country_code", type=ct.StringType(), order=4),
            Column(name="total_latency", type=ct.BigIntType(), order=5),
        ],
    )
    session.add(events_agg_node_revision)
    await session.commit()
    await session.refresh(events_agg_node, ["current"])
    return events_agg_node


@pytest_asyncio.fixture
async def devices(
    session: AsyncSession,
    primary_key_attribute: AttributeType,
) -> Node:
    devices_source_node = Node(
        name="source.devices",
        display_name="Devices",
        type=NodeType.SOURCE,
        current_version="1",
    )
    devices_source_node_revision = NodeRevision(
        node=devices_source_node,
        name="source.devices",
        display_name="Devices",
        type=NodeType.SOURCE,
        version="1",
        schema_="test",
        table="devices",
        columns=[
            Column(name="device_id", type=ct.BigIntType(), order=0),
            Column(name="device_name", type=ct.BigIntType(), order=1),
            Column(name="device_manufacturer", type=ct.StringType(), order=2),
        ],
    )

    devices_dim_node = Node(
        name="shared.devices",
        display_name="Devices",
        type=NodeType.DIMENSION,
        current_version="1",
    )
    devices_dim_node_revision = NodeRevision(
        node=devices_dim_node,
        name="shared.devices",
        display_name="Devices",
        type=NodeType.DIMENSION,
        version="1",
        query="""SELECT CAST(device_id AS INT) device_id, CAST(device_name AS STR) device_name, device_manufacturer FROM source.devices""",
        columns=[
            Column(
                name="device_id",
                type=ct.BigIntType(),
                order=0,
                attributes=[ColumnAttribute(attribute_type=primary_key_attribute)],
            ),
            Column(name="device_name", type=ct.StringType(), order=1),
            Column(name="device_manufacturer", type=ct.StringType(), order=2),
        ],
    )
    session.add(devices_source_node_revision)
    session.add(devices_dim_node_revision)
    await session.commit()
    await session.refresh(devices_source_node, ["current"])
    await session.refresh(devices_dim_node, ["current"])
    return devices_dim_node


@pytest_asyncio.fixture
async def manufacturers_dim(
    session: AsyncSession,
    primary_key_attribute: AttributeType,
) -> Node:
    manufacturers_source_node = Node(
        name="source.manufacturers",
        display_name="Manufacturers",
        type=NodeType.SOURCE,
        current_version="1",
    )
    manufacturers_source_node_revision = NodeRevision(
        node=manufacturers_source_node,
        name="source.manufacturers",
        display_name="Manufacturers",
        type=NodeType.SOURCE,
        version="1",
        schema_="test",
        table="manufacturers",
        columns=[
            Column(name="manufacturer_name", type=ct.BigIntType(), order=0),
            Column(name="company_name", type=ct.StringType(), order=1),
            Column(name="created_on", type=ct.TimestampType(), order=2),
        ],
    )

    manufacturers_dim_node = Node(
        name="shared.manufacturers",
        display_name="Manufacturers",
        type=NodeType.DIMENSION,
        current_version="1",
    )
    manufacturers_dim_node_revision = NodeRevision(
        node=manufacturers_dim_node,
        name="shared.manufacturers",
        display_name="Manufacturers",
        type=NodeType.DIMENSION,
        version="1",
        query="""
        SELECT
          CAST(manufacturer_name AS STR) name,
          CAST(company_name AS STR) company_name,
          created_on,
          COUNT(DISTINCT devices.device_id) AS devices_produced
        FROM source.manufacturers manufacturers
        JOIN shared.devices devices ON manufacturers.manufacturer_name = devices.device_manufacturer""",
        columns=[
            Column(
                name="name",
                type=ct.StringType(),
                order=0,
                attributes=[ColumnAttribute(attribute_type=primary_key_attribute)],
            ),
            Column(name="company_name", type=ct.StringType(), order=1),
            Column(name="created_on", type=ct.TimestampType(), order=2),
        ],
    )
    session.add(manufacturers_source_node_revision)
    session.add(manufacturers_dim_node_revision)
    await session.commit()
    await session.refresh(manufacturers_source_node, ["current"])
    await session.refresh(manufacturers_dim_node, ["current"])
    return manufacturers_dim_node


@pytest_asyncio.fixture
async def country_dim(
    session: AsyncSession,
    primary_key_attribute: AttributeType,
) -> Node:
    countries_source_node = Node(
        name="source.countries",
        display_name="Countries",
        type=NodeType.SOURCE,
        current_version="1",
    )
    countries_source_node_revision = NodeRevision(
        node=countries_source_node,
        name="source.countries",
        display_name="Countries",
        type=NodeType.SOURCE,
        version="1",
        schema_="test",
        table="countries",
        columns=[
            Column(name="country_code", type=ct.StringType(), order=0),
            Column(name="country_name", type=ct.StringType(), order=1),
            Column(name="region_code", type=ct.IntegerType(), order=2),
            Column(name="population", type=ct.IntegerType(), order=3),
        ],
    )

    regions_source_node = Node(
        name="source.regions",
        display_name="Regions",
        type=NodeType.SOURCE,
        current_version="1",
    )
    regions_source_node_revision = NodeRevision(
        node=regions_source_node,
        name="source.regions",
        display_name="Regions",
        type=NodeType.SOURCE,
        version="1",
        schema_="test",
        table="regions",
        columns=[
            Column(name="region_code", type=ct.StringType(), order=0),
            Column(name="region_name", type=ct.StringType(), order=1),
        ],
    )

    regions_dim_node = Node(
        name="shared.regions",
        display_name="Regions Dimension",
        type=NodeType.DIMENSION,
        current_version="1",
    )
    regions_dim_node_revision = NodeRevision(
        node=regions_dim_node,
        name="shared.regions",
        display_name="Regions Dimension",
        type=NodeType.DIMENSION,
        version="1",
        query="""
        SELECT
          region_code,
          region_name
        FROM source.regions
        """,
        columns=[
            Column(
                name="region_code",
                type=ct.StringType(),
                order=0,
                attributes=[ColumnAttribute(attribute_type=primary_key_attribute)],
            ),
            Column(name="region_name", type=ct.StringType(), order=1),
        ],
    )
    countries_dim_node = Node(
        name="shared.countries",
        display_name="Countries Dimension",
        type=NodeType.DIMENSION,
        current_version="1",
    )
    countries_dim_node_revision = NodeRevision(
        node=countries_dim_node,
        name="shared.countries",
        display_name="Countries Dimension",
        type=NodeType.DIMENSION,
        version="1",
        query="""
        SELECT
          country_code,
          country_name,
          region_code,
          region_name,
          population
        FROM source.countries countries
        JOIN shared.regions ON countries.region_code = shared.regions.region_code""",
        columns=[
            Column(
                name="country_code",
                type=ct.StringType(),
                order=0,
                attributes=[ColumnAttribute(attribute_type=primary_key_attribute)],
            ),
            Column(name="country_name", type=ct.StringType(), order=1),
            Column(name="region_code", type=ct.StringType(), order=2),
            Column(name="region_name", type=ct.StringType(), order=3),
            Column(name="population", type=ct.IntegerType(), order=4),
        ],
    )
    session.add(countries_source_node_revision)
    session.add(regions_source_node_revision)
    session.add(regions_dim_node_revision)
    session.add(countries_dim_node_revision)
    await session.commit()
    await session.refresh(countries_source_node, ["current"])
    await session.refresh(regions_source_node, ["current"])
    await session.refresh(regions_dim_node, ["current"])
    await session.refresh(countries_dim_node, ["current"])
    return countries_dim_node


@pytest_asyncio.fixture
async def events_agg_countries_link(
    session: AsyncSession,
    events_agg: Node,
    country_dim: Node,
) -> Node:
    link = DimensionLink(
        node_revision=events_agg.current,
        dimension=country_dim,
        join_sql=f"{events_agg.name}.country_code = {country_dim.name}.country_code",
        join_type=JoinType.LEFT,
    )
    session.add(link)
    await session.commit()
    await session.refresh(link)
    return link


@pytest_asyncio.fixture
async def events_devices_link(
    session: AsyncSession,
    events: Node,
    devices: Node,
) -> Node:
    link = DimensionLink(
        node_revision=events.current,
        dimension=devices,
        join_sql=f"{devices.name}.device_id = {events.name}.device_id",
        join_type=JoinType.LEFT,
    )
    session.add(link)
    await session.commit()
    await session.refresh(link)
    return link


@pytest_asyncio.fixture
async def events_agg_devices_link(
    session: AsyncSession,
    events_agg: Node,
    devices: Node,
    manufacturers_dim: Node,
) -> Node:
    link = DimensionLink(
        node_revision=events_agg.current,
        dimension=devices,
        join_sql=f"{devices.name}.device_id = {events_agg.name}.device_id",
        join_type=JoinType.LEFT,
    )
    session.add(link)
    await session.commit()
    await session.refresh(link)

    link2 = DimensionLink(
        node_revision=devices.current,
        dimension=manufacturers_dim,
        join_sql=f"{manufacturers_dim.name}.name = {devices.name}.device_manufacturer",
        join_type=JoinType.LEFT,
    )
    session.add(link2)
    await session.commit()
    await session.refresh(link2)
    await session.refresh(devices, ["current"])
    await session.refresh(events_agg, ["current"])
    return link


@pytest_asyncio.fixture
async def events_agg_date_dim_link(
    session: AsyncSession,
    events_agg: Node,
    date_dim: Node,
) -> Node:
    link = DimensionLink(
        node_revision=events_agg.current,
        dimension=date_dim,
        join_sql=f"{events_agg.name}.utc_date = {date_dim.name}.dateint",
        join_type=JoinType.LEFT,
    )
    session.add(link)
    await session.commit()
    await session.refresh(link)
    return link


@pytest.mark.asyncio
async def test_build_source_node(
    session: AsyncSession,
    events: Node,
):
    """
    Test building a source node
    """
    query_ast = await build_node(
        session,
        events.current,
    )
    assert (
        str(query_ast).strip()
        == str(
            parse(
                """
    SELECT
      event_id,
      user_id,
      device_id,
      country_code,
      latency,
      utc_date
    FROM test.events
    """,
            ),
        ).strip()
    )


@pytest.mark.asyncio
async def test_build_source_node_with_direct_filter(
    session: AsyncSession,
    events: Node,
):
    """
    Test building a source node with a filter on an immediate column on the source node.
    """
    query_ast = await build_node(
        session,
        events.current,
    )
    assert (
        str(query_ast).strip()
        == str(
            parse(
                """
    SELECT
      event_id,
      user_id,
      device_id,
      country_code,
      latency,
      utc_date
    FROM test.events
    """,
            ),
        ).strip()
    )

    query_ast = await build_node(
        session,
        events.current,
        filters=[
            "source.events.utc_date = 20210101",
        ],
    )
    expected = """
    WITH source_DOT_events AS (
      SELECT
        source_DOT_events.event_id,
        source_DOT_events.user_id,
        source_DOT_events.device_id,
        source_DOT_events.country_code,
        source_DOT_events.latency,
        source_DOT_events.utc_date
      FROM test.events AS source_DOT_events
      WHERE  source_DOT_events.utc_date = 20210101
    )
    SELECT
      source_DOT_events.event_id,
      source_DOT_events.user_id,
      source_DOT_events.device_id,
      source_DOT_events.country_code,
      source_DOT_events.latency,
      source_DOT_events.utc_date
    FROM source_DOT_events
    """
    assert str(query_ast).strip() == str(parse(expected)).strip()


@pytest.mark.asyncio
async def test_build_source_with_pushdown_filters(
    session: AsyncSession,
    events: Node,
    devices: Node,
    events_devices_link: DimensionLink,
):
    """
    Test building a source node with a dimension attribute filter that can be
    pushed down to an immediate column on the source node.
    """
    query_ast = await build_node(
        session,
        events.current,
        filters=[
            "shared.devices.device_id = 111",
            "shared.devices.device_id = 222",
        ],
        dimensions=["shared.devices.device_id"],
    )

    expected = """
    WITH source_DOT_events AS (
      SELECT
        source_DOT_events.event_id,
        source_DOT_events.user_id,
        source_DOT_events.device_id,
        source_DOT_events.country_code,
        source_DOT_events.latency,
        source_DOT_events.utc_date
      FROM test.events AS source_DOT_events
      WHERE
        source_DOT_events.device_id = 111 AND source_DOT_events.device_id = 222
    )
    SELECT
      source_DOT_events.event_id,
      source_DOT_events.user_id,
      source_DOT_events.device_id,
      source_DOT_events.country_code,
      source_DOT_events.latency,
      source_DOT_events.utc_date
    FROM source_DOT_events
    """
    assert str(query_ast).strip() == str(parse(expected)).strip()


@pytest.mark.asyncio
async def test_build_source_with_join_filters(
    session: AsyncSession,
    events: Node,
    devices: Node,
    events_devices_link: DimensionLink,
):
    """
    Test building a source node with a dimension attribute filter that
    requires a join to a dimension node.
    """
    query_ast = await build_node(
        session,
        events.current,
        filters=[
            "shared.devices.device_id = 111",
            "shared.devices.device_name = 'iPhone'",
        ],
        dimensions=[
            "shared.devices.device_name",
            "shared.devices.device_manufacturer",
        ],
    )

    expected = """
    WITH
    source_DOT_events AS (
      SELECT  source_DOT_events.event_id,
        source_DOT_events.user_id,
        source_DOT_events.device_id,
        source_DOT_events.country_code,
        source_DOT_events.latency,
        source_DOT_events.utc_date
      FROM test.events AS source_DOT_events
      WHERE  source_DOT_events.device_id = 111
    ),
    shared_DOT_devices AS (
      SELECT
        CAST(source_DOT_devices.device_id AS INT) device_id,
        CAST(source_DOT_devices.device_name AS STRING) device_name,
        source_DOT_devices.device_manufacturer
      FROM test.devices AS source_DOT_devices
      WHERE  CAST(source_DOT_devices.device_id AS INT) = 111 AND CAST(source_DOT_devices.device_name AS STRING) = 'iPhone'
    )
    SELECT  source_DOT_events.event_id,
        source_DOT_events.user_id,
        source_DOT_events.device_id,
        source_DOT_events.country_code,
        source_DOT_events.latency,
        source_DOT_events.utc_date,
        shared_DOT_devices.device_name shared_DOT_devices_DOT_device_name,
        shared_DOT_devices.device_manufacturer shared_DOT_devices_DOT_device_manufacturer,
        shared_DOT_devices.device_id shared_DOT_devices_DOT_device_id
     FROM source_DOT_events LEFT JOIN shared_DOT_devices ON shared_DOT_devices.device_id = source_DOT_events.device_id
    """
    assert str(query_ast).strip() == str(parse(expected)).strip()


@pytest.mark.asyncio
async def test_build_dimension_node(
    session: AsyncSession,
    devices: Node,
):
    """
    Test building a dimension node
    """
    query_ast = await build_node(
        session,
        devices.current,
    )
    expected = """
    WITH shared_DOT_devices AS (
      SELECT
        CAST(source_DOT_devices.device_id AS INT) device_id,
        CAST(source_DOT_devices.device_name AS STRING) device_name,
        source_DOT_devices.device_manufacturer
      FROM test.devices AS source_DOT_devices
    )
    SELECT
      shared_DOT_devices.device_id,
      shared_DOT_devices.device_name,
      shared_DOT_devices.device_manufacturer
    FROM shared_DOT_devices
    """
    assert str(query_ast).strip() == str(parse(expected)).strip()


@pytest.mark.asyncio
async def test_build_dimension_node_with_direct_and_pushdown_filter(
    session: AsyncSession,
    events: Node,
    devices: Node,
    events_agg_devices_link: DimensionLink,
):
    """
    Test building a dimension node with a direct filter and a pushdown filter (the result
    in this case is the same query)
    """
    expected = """
    WITH shared_DOT_devices AS (
      SELECT
        CAST(source_DOT_devices.device_id AS INT) device_id,
        CAST(source_DOT_devices.device_name AS STRING) device_name,
        source_DOT_devices.device_manufacturer
      FROM test.devices AS source_DOT_devices
      WHERE  source_DOT_devices.device_manufacturer = 'Apple'
    )
    SELECT
      shared_DOT_devices.device_id,
      shared_DOT_devices.device_name,
      shared_DOT_devices.device_manufacturer
    FROM shared_DOT_devices
    """
    # Direct filter
    query_ast = await build_node(
        session,
        devices.current,
        filters=["shared.devices.device_manufacturer = 'Apple'"],
    )
    assert str(query_ast).strip() == str(parse(expected)).strip()

    # Pushdown filter
    query_ast = await build_node(
        session, devices.current, filters=["shared.manufacturers.name = 'Apple'"],
    )
    assert str(query_ast).strip() == str(parse(expected)).strip()


@pytest.mark.asyncio
async def test_build_transform_with_pushdown_dimensions_filters(
    session: AsyncSession,
    events: Node,
    events_agg: Node,
    devices: Node,
    events_agg_devices_link: DimensionLink,
    manufacturers_dim: Node,
):
    """
    Test building a transform node with filters and dimensions that can be pushed down
    """
    await session.refresh(events_agg.current, ["dimension_links"])
    query_ast = await build_node(
        session,
        events_agg.current,
        filters=[
            "shared.devices.device_id = 111",
            "shared.devices.device_id = 222",
        ],
        dimensions=["shared.devices.device_id"],
    )
    expected = """
    WITH agg_DOT_events AS (
      SELECT
        source_DOT_events.user_id,
        source_DOT_events.utc_date,
        source_DOT_events.device_id,
        source_DOT_events.country_code,
        SUM(source_DOT_events.latency) AS total_latency
      FROM test.events AS source_DOT_events
      WHERE
        source_DOT_events.device_id = 111 AND source_DOT_events.device_id = 222
      GROUP BY
        source_DOT_events.user_id,
        source_DOT_events.device_id,
        source_DOT_events.country_code
    )
    SELECT
      agg_DOT_events.user_id,
      agg_DOT_events.utc_date,
      agg_DOT_events.device_id,
      agg_DOT_events.country_code,
      agg_DOT_events.total_latency
    FROM agg_DOT_events
    """
    assert str(query_ast).strip() == str(parse(expected)).strip()


@pytest.mark.asyncio
async def test_build_transform_with_join_dimensions_filters(
    session: AsyncSession,
    events: Node,
    events_agg: Node,
    devices: Node,
    events_agg_devices_link: DimensionLink,
    manufacturers_dim: Node,
):
    """
    Test building a transform node with filters and dimensions that require a join
    """
    query_ast = await build_node(
        session,
        events_agg.current,
        filters=[
            "shared.devices.device_name = 'iOS'",
            "shared.devices.device_id = 222",
        ],
        dimensions=["shared.devices.device_manufacturer"],
    )
    expected = """
        WITH agg_DOT_events AS (
          SELECT
            source_DOT_events.user_id,
            source_DOT_events.utc_date,
            source_DOT_events.device_id,
            source_DOT_events.country_code,
            SUM(source_DOT_events.latency) AS total_latency
          FROM test.events AS source_DOT_events
          WHERE source_DOT_events.device_id = 222
          GROUP BY
            source_DOT_events.user_id,
            source_DOT_events.device_id,
            source_DOT_events.country_code
        ),
        shared_DOT_devices AS (
          SELECT CAST(source_DOT_devices.device_id AS INT) device_id,
            CAST(source_DOT_devices.device_name AS STRING) device_name,
            source_DOT_devices.device_manufacturer
          FROM test.devices AS source_DOT_devices
          WHERE  CAST(source_DOT_devices.device_name AS STRING) = 'iOS' AND CAST(source_DOT_devices.device_id AS INT) = 222
        )
        SELECT
          agg_DOT_events.user_id,
          agg_DOT_events.utc_date,
          agg_DOT_events.device_id,
          agg_DOT_events.country_code,
          agg_DOT_events.total_latency,
          shared_DOT_devices.device_manufacturer shared_DOT_devices_DOT_device_manufacturer,
          shared_DOT_devices.device_name shared_DOT_devices_DOT_device_name,
          shared_DOT_devices.device_id shared_DOT_devices_DOT_device_id
        FROM agg_DOT_events LEFT JOIN shared_DOT_devices ON shared_DOT_devices.device_id = agg_DOT_events.device_id
    """
    assert str(query_ast).strip() == str(parse(expected)).strip()


@pytest.mark.asyncio
async def test_build_transform_with_multijoin_dimensions_filters(
    session: AsyncSession,
    events: Node,
    events_agg: Node,
    devices: Node,
    events_agg_devices_link: DimensionLink,
    manufacturers_dim: Node,
    country_dim: Node,
):
    """
    Test building a transform node with filters and dimensions that require
    multiple joins (multiple hops in the dimensions graph). This tests the join type
    where dimension nodes themselves have a query that references an existing CTE
    in the query.
    """
    query_ast = await build_node(
        session,
        events_agg.current,
        filters=[
            "shared.manufacturers.company_name = 'Apple'",
            "shared.devices.device_id = 123",
            "shared.devices.device_manufacturer = 'Something'",
        ],
        dimensions=["shared.devices.device_manufacturer"],
    )
    expected = """
        WITH
        agg_DOT_events AS (
          SELECT
            source_DOT_events.user_id,
            source_DOT_events.utc_date,
            source_DOT_events.device_id,
            source_DOT_events.country_code,
            SUM(source_DOT_events.latency) AS total_latency
          FROM test.events AS source_DOT_events
          WHERE  source_DOT_events.device_id = 123
          GROUP BY
            source_DOT_events.user_id,
            source_DOT_events.device_id,
            source_DOT_events.country_code
        ),
        shared_DOT_devices AS (
          SELECT
            CAST(source_DOT_devices.device_id AS INT) device_id,
            CAST(source_DOT_devices.device_name AS STRING) device_name,
            source_DOT_devices.device_manufacturer
          FROM test.devices AS source_DOT_devices
          WHERE  CAST(source_DOT_devices.device_id AS INT) = 123 AND source_DOT_devices.device_manufacturer = 'Something'
        ),
        shared_DOT_manufacturers AS (
          SELECT
            CAST(source_DOT_manufacturers.manufacturer_name AS STRING) name,
            CAST(source_DOT_manufacturers.company_name AS STRING) company_name,
            source_DOT_manufacturers.created_on,
            COUNT( DISTINCT shared_DOT_devices.device_id) AS devices_produced
          FROM test.manufacturers AS source_DOT_manufacturers
          JOIN shared_DOT_devices ON source_DOT_manufacturers.manufacturer_name = shared_DOT_devices.device_manufacturer
          WHERE  CAST(source_DOT_manufacturers.company_name AS STRING) = 'Apple'
        )
        SELECT
          agg_DOT_events.user_id,
          agg_DOT_events.utc_date,
          agg_DOT_events.device_id,
          agg_DOT_events.country_code,
          agg_DOT_events.total_latency,
          shared_DOT_devices.device_manufacturer shared_DOT_devices_DOT_device_manufacturer,
          shared_DOT_devices.device_id shared_DOT_devices_DOT_device_id,
          shared_DOT_manufacturers.company_name shared_DOT_manufacturers_DOT_company_name
        FROM agg_DOT_events LEFT JOIN shared_DOT_devices ON shared_DOT_devices.device_id = agg_DOT_events.device_id
        LEFT JOIN shared_DOT_manufacturers ON shared_DOT_manufacturers.name = shared_DOT_devices.device_manufacturer
    """
    assert str(query_ast).strip() == str(parse(expected)).strip()


@pytest.mark.asyncio
async def test_build_fail_no_join_path_found(
    session: AsyncSession,
    events: Node,
    events_agg: Node,
    country_dim: Node,
):
    """
    Test failed node building due to not being able to find a join path to the dimension
    """
    with pytest.raises(DJException) as exc_info:
        await build_node(
            session,
            events_agg.current,
            filters=["shared.countries.region_name = 'APAC'"],
            dimensions=[
                "shared.countries.region_name",
            ],
        )
    assert (
        "This dimension attribute cannot be joined in: shared.countries.region_name. "
        "Please make sure that shared.countries is linked to agg.events"
    ) in str(exc_info.value)


@pytest.mark.asyncio
async def test_build_transform_with_multijoin_dimensions_with_extra_ctes(
    session: AsyncSession,
    events: Node,
    events_agg: Node,
    devices: Node,
    events_agg_devices_link: DimensionLink,
    manufacturers_dim: Node,
    country_dim: Node,
    events_agg_countries_link: DimensionLink,
):
    """
    Test building a transform node with filters and dimensions that require
    multiple joins (multiple hops in the dimensions graph). This tests the join type
    where dimension nodes themselves have a query that brings in an additional node that
    is not already a CTE on the query.
    """
    query_ast = await build_node(
        session,
        events_agg.current,
        filters=[
            "shared.manufacturers.company_name = 'Apple'",
            "shared.countries.region_name = 'APAC'",
        ],
        dimensions=[
            "shared.devices.device_manufacturer",
            "shared.countries.region_name",
        ],
    )
    expected = """
    WITH
    agg_DOT_events AS (
        SELECT
        source_DOT_events.user_id,
        source_DOT_events.utc_date,
        source_DOT_events.device_id,
        source_DOT_events.country_code,
        SUM(source_DOT_events.latency) AS total_latency
        FROM test.events AS source_DOT_events
        GROUP BY  source_DOT_events.user_id, source_DOT_events.device_id, source_DOT_events.country_code
    ),
    shared_DOT_devices AS (
    SELECT  CAST(source_DOT_devices.device_id AS INT) device_id,
        CAST(source_DOT_devices.device_name AS STRING) device_name,
        source_DOT_devices.device_manufacturer
        FROM test.devices AS source_DOT_devices
    ),
    shared_DOT_regions AS (
        SELECT
        source_DOT_regions.region_code,
        source_DOT_regions.region_name
        FROM test.regions AS source_DOT_regions
    ),
    shared_DOT_countries AS (
    SELECT  source_DOT_countries.country_code,
        source_DOT_countries.country_name,
        shared_DOT_regions.region_code,
        shared_DOT_regions.region_name,
        source_DOT_countries.population
        FROM test.countries AS source_DOT_countries
        JOIN shared_DOT_regions ON source_DOT_countries.region_code = shared_DOT_regions.region_code
        WHERE  shared_DOT_regions.region_name = 'APAC'
    ),
    shared_DOT_manufacturers AS (
    SELECT  CAST(source_DOT_manufacturers.manufacturer_name AS STRING) name,
        CAST(source_DOT_manufacturers.company_name AS STRING) company_name,
        source_DOT_manufacturers.created_on,
        COUNT( DISTINCT shared_DOT_devices.device_id) AS devices_produced
        FROM test.manufacturers AS source_DOT_manufacturers JOIN shared_DOT_devices ON source_DOT_manufacturers.manufacturer_name = shared_DOT_devices.device_manufacturer
        WHERE  CAST(source_DOT_manufacturers.company_name AS STRING) = 'Apple'
    )

    SELECT  agg_DOT_events.user_id,
        agg_DOT_events.utc_date,
        agg_DOT_events.device_id,
        agg_DOT_events.country_code,
        agg_DOT_events.total_latency,
        shared_DOT_devices.device_manufacturer shared_DOT_devices_DOT_device_manufacturer,
        shared_DOT_countries.region_name shared_DOT_countries_DOT_region_name,
        shared_DOT_manufacturers.company_name shared_DOT_manufacturers_DOT_company_name
        FROM agg_DOT_events LEFT JOIN shared_DOT_devices ON shared_DOT_devices.device_id = agg_DOT_events.device_id
    LEFT JOIN shared_DOT_countries ON agg_DOT_events.country_code = shared_DOT_countries.country_code
    LEFT JOIN shared_DOT_manufacturers ON shared_DOT_manufacturers.name = shared_DOT_devices.device_manufacturer
    """
    assert str(query_ast).strip() == str(parse(expected)).strip()


@pytest.mark.asyncio
async def test_build_with_source_filters(
    session: AsyncSession,
    events: Node,
    events_agg: Node,
    date_dim: Node,
    events_agg_date_dim_link: DimensionLink,
):
    """
    Test build node with filters on source
    """
    query_ast = await build_node(
        session,
        events_agg.current,
        filters=["shared.date.dateint = 20250101"],
    )
    expected = """
    WITH
    agg_DOT_events AS (
    SELECT  source_DOT_events.user_id,
        source_DOT_events.utc_date,
        source_DOT_events.device_id,
        source_DOT_events.country_code,
        SUM(source_DOT_events.latency) AS total_latency
     FROM test.events AS source_DOT_events
     WHERE  source_DOT_events.utc_date = 20250101
     GROUP BY  source_DOT_events.user_id, source_DOT_events.device_id, source_DOT_events.country_code
    )

    SELECT  agg_DOT_events.user_id,
        agg_DOT_events.utc_date,
        agg_DOT_events.device_id,
        agg_DOT_events.country_code,
        agg_DOT_events.total_latency
    FROM agg_DOT_events
    """
    assert str(query_ast).strip() == str(parse(expected)).strip()
