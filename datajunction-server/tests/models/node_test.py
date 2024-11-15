"""
Tests for ``datajunction_server.models.node``.
"""

# pylint: disable=use-implicit-booleaness-not-comparison


import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.node import (
    AvailabilityStateBase,
    NodeCursor,
    PartitionAvailability,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.typing import UTCDatetime


def test_node_relationship(session: AsyncSession) -> None:
    """
    Test the n:n self-referential relationships.
    """
    node_a = Node(name="A", current_version="1")
    node_a_rev = NodeRevision(name="A", version="1", node=node_a)

    node_b = Node(name="B", current_version="1")
    node_a_rev = NodeRevision(name="B", version="1", node=node_b)

    node_c = Node(name="C", current_version="1")
    node_c_rev = NodeRevision(
        name="C",
        version="1",
        node=node_c,
        parents=[node_a, node_b],
    )

    session.add(node_c_rev)

    assert node_a.children == [node_c_rev]
    assert node_b.children == [node_c_rev]
    assert node_c.children == []

    assert node_a_rev.parents == []
    assert node_a_rev.parents == []
    assert node_c_rev.parents == [node_a, node_b]


def test_extra_validation() -> None:
    """
    Test ``extra_validation``.
    """
    node = Node(name="A", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
    )
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()
    assert str(excinfo.value) == "Node A of type metric needs a query"

    node = Node(name="A", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT count(repair_order_id) "
        "AS Anum_repair_orders "
        "FROM repair_orders",
    )
    node_revision.extra_validation()

    node = Node(name="A", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT repair_order_id + "
        "repair_order_id AS Anum_repair_orders "
        "FROM repair_orders",
    )
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()
    assert str(excinfo.value) == (
        "Metric A has an invalid query, should have an aggregate expression"
    )

    node = Node(name="AA", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT ln(count(distinct repair_order_id)) FROM repair_orders",
    )
    node_revision.extra_validation()

    node = Node(name="A", type=NodeType.TRANSFORM, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT * FROM B",
    )
    node_revision.extra_validation()

    node = Node(name="A", type=NodeType.TRANSFORM, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
    )
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()
    assert str(excinfo.value) == "Node A of type transform needs a query"

    node = Node(name="A", type=NodeType.CUBE, current_version="1")
    node_revision = NodeRevision(name=node.name, type=node.type, node=node, version="1")
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()
    assert str(excinfo.value) == "Node A of type cube node needs cube elements"

    node = Node(name="A", type=NodeType.TRANSFORM, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT * FROM B",
        required_dimensions=["B.x"],
    )
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()

    assert str(excinfo.value) == (
        "Node A of type transform cannot have "
        "bound dimensions which are only for metrics."
    )


def test_merging_availability_simple_no_partitions() -> None:
    """
    Test merging simple availability for no partitions.
    """
    avail_1 = AvailabilityStateBase(
        catalog="catalog",
        schema_="schema",
        table="foo",
        valid_through_ts=111,
    )
    avail_2 = AvailabilityStateBase(
        catalog="catalog",
        schema_="schema",
        table="foo",
        valid_through_ts=222,
    )
    assert avail_1.merge(avail_2).dict() == {
        "min_temporal_partition": None,
        "max_temporal_partition": None,
        "catalog": "catalog",
        "schema_": "schema",
        "table": "foo",
        "valid_through_ts": 222,
        "categorical_partitions": [],
        "temporal_partitions": [],
        "partitions": [],
        "url": None,
        "links": {},
    }


def test_merging_availability_complex_no_partitions() -> None:
    """
    Test merging complex availability for no partitions.
    """
    avail_1 = AvailabilityStateBase(
        catalog="druid",
        schema_="",
        table="dj_product__launchpad__launchpad_cube",
        min_temporal_partition=["20230924"],
        max_temporal_partition=["20230924"],
        categorical_partitions=[],
        temporal_partitions=[],
        partitions=[],
        valid_through_ts=20230924,
    )
    avail_2 = AvailabilityStateBase(
        catalog="druid",
        schema_="",
        table="dj_product__launchpad__launchpad_cube",
        min_temporal_partition=["20230926"],
        max_temporal_partition=["20230927"],
        categorical_partitions=[],
        temporal_partitions=[],
        partitions=[],
        valid_through_ts=20230927,
    )
    assert avail_1.merge(avail_2).dict() == {
        "min_temporal_partition": ["20230924"],
        "max_temporal_partition": ["20230927"],
        "catalog": "druid",
        "schema_": "",
        "table": "dj_product__launchpad__launchpad_cube",
        "valid_through_ts": 20230927,
        "categorical_partitions": [],
        "temporal_partitions": [],
        "partitions": [],
        "url": None,
        "links": {},
    }


def test_merging_availability_complex_with_partitions() -> None:
    """
    Test merging complex availability with partitions.
    """
    avail_1 = AvailabilityStateBase(
        catalog="iceberg",
        schema_="salad",
        table="dressing",
        min_temporal_partition=["20230101"],
        max_temporal_partition=["20230925"],
        categorical_partitions=["country"],
        temporal_partitions=["region_date"],
        partitions=[
            PartitionAvailability(
                value=[None],
                valid_through_ts=20230404,
                min_temporal_partition=["20230101"],
                max_temporal_partition=["20230404"],
            ),
            PartitionAvailability(
                value=["US"],
                valid_through_ts=20230925,
                min_temporal_partition=["20230924"],
                max_temporal_partition=["20230925"],
            ),
        ],
        valid_through_ts=20230925,
    )
    avail_2 = AvailabilityState(
        catalog="iceberg",
        schema_="salad",
        table="dressing",
        min_temporal_partition=["20230101"],
        max_temporal_partition=["20231010"],
        categorical_partitions=["country"],
        temporal_partitions=["region_date"],
        partitions=[
            PartitionAvailability(
                value=["US"],
                valid_through_ts=20230926,
                min_temporal_partition=["20230924"],
                max_temporal_partition=["20230926"],
            ),
            PartitionAvailability(
                value=["CA"],
                valid_through_ts=20231010,
                min_temporal_partition=["20220101"],
                max_temporal_partition=["20231010"],
            ),
        ],
        valid_through_ts=20231015,
    )
    avail_1 = avail_1.merge(avail_2)
    assert avail_1.dict() == {
        "catalog": "iceberg",
        "schema_": "salad",
        "table": "dressing",
        "min_temporal_partition": ["20230101"],
        "max_temporal_partition": ["20231010"],
        "valid_through_ts": 20231015,
        "categorical_partitions": ["country"],
        "temporal_partitions": ["region_date"],
        "partitions": [
            {
                "value": ["CA"],
                "valid_through_ts": 20231010,
                "min_temporal_partition": ["20220101"],
                "max_temporal_partition": ["20231010"],
            },
            {
                "value": ["US"],
                "valid_through_ts": 20230926,
                "min_temporal_partition": ["20230101"],
                "max_temporal_partition": ["20230926"],
            },
        ],
        "url": None,
        "links": {},
    }


def test_node_cursors() -> None:
    """
    Test encoding and decoding node cursors
    """
    created_at = UTCDatetime(
        year=2024,
        month=1,
        day=1,
        hour=12,
        minute=30,
        second=33,
    )

    cursor = NodeCursor(created_at=created_at, id=1010)

    encoded_cursor = (
        "eyJjcmVhdGVkX2F0IjogIjIwMjQtMDEtMDFUMTI6MzA6MzMiLCAiaWQiOiAxMDEwfQ=="
    )
    assert cursor.encode() == encoded_cursor

    decoded_cursor = NodeCursor.decode(encoded_cursor)
    assert decoded_cursor.created_at == cursor.created_at  # pylint: disable=no-member
    assert decoded_cursor.id == cursor.id  # pylint: disable=no-member
