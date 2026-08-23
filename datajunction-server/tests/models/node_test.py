"""
Tests for ``datajunction_server.models.node``.
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.models.node import (
    AvailabilityStateBase,
    NodeCursor,
    PartitionAvailability,
    TemporalPartitionRange,
    union_ranges,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import (
    Granularity,
    PartitionGrain,
    PartitionType,
)
from datajunction_server.sql.parsing.types import StringType
from datajunction_server.typing import UTCDatetime


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT SUM(x) FROM a", True),
        ("SELECT COUNT(x) FROM a", True),
        ("SELECT COUNT(DISTINCT x) FROM a", True),
        ("SELECT MAX(x) FROM a", True),
        ("SELECT SUM(x * price) FROM a", True),
        ("SELECT SUM(x) AS total FROM a", True),  # aliased projection
        # AVG decomposes into SUM + COUNT (two components), so it is NOT a
        # 1:1-mappable measure — model it as a derived metric over its own
        # SUM and COUNT measures instead.
        ("SELECT AVG(x) FROM a", False),
        ("SELECT AVG(x) AS avg_x FROM a", False),  # aliased AVG
        ("SELECT SUM(x) / COUNT(y) AS ratio FROM a", False),  # aliased ratio
        ("SELECT SUM(x) / COUNT(y) FROM a", False),
        ("SELECT 1.5 * SUM(x) FROM a", False),
        ("SELECT SUM(x) + SUM(y) FROM a", False),
        ("SELECT SUM(x), COUNT(y) FROM a", False),  # more than one projection
        ("SELECT MAX_BY(x, y) FROM a", False),  # single agg, but non-decomposable
    ],
)
def test_is_measure(query: str, expected: bool) -> None:
    """
    A metric is a measure iff its query is a single top-level aggregation call
    (no cross-measure arithmetic).
    """
    revision = NodeRevision(
        name="m",
        type=NodeType.METRIC,
        version="1",
        query=query,
        parents=[],
    )
    assert revision.is_measure is expected


def test_is_measure_false_for_non_metric() -> None:
    """Only metric nodes can be measures."""
    revision = NodeRevision(
        name="t",
        type=NodeType.TRANSFORM,
        version="1",
        query="SELECT SUM(x) FROM a",
        parents=[],
    )
    assert revision.is_measure is False


def test_is_measure_false_for_derived_metric() -> None:
    """
    A metric that references another metric is derived, not a measure, even if
    its top-level expression looks like a single aggregation.
    """
    base = Node(name="base", type=NodeType.METRIC, current_version="1")
    revision = NodeRevision(
        name="m",
        type=NodeType.METRIC,
        version="1",
        query="SELECT SUM(base) FROM base",
        parents=[base],
    )
    assert revision.is_measure is False


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
        query="SELECT count(repair_order_id) AS Anum_repair_orders FROM repair_orders",
    )
    node_revision.extra_validation()

    # ARRAY_AGG is tagged is_aggregation so the fan-out guard can see it, which also
    # makes it a valid sole aggregate here. It was rejected before that tagging.
    node = Node(name="A", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT array_agg(repair_order_id) AS Aorder_ids FROM repair_orders",
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

    node = Node(name="ABC", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT CASE WHEN COUNT(repair_order_id) = 1 THEN 1 ELSE 0 END FROM repair_orders",
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
    assert avail_1.merge(avail_2).model_dump() == {
        "min_temporal_partition": None,
        "max_temporal_partition": None,
        "temporal_ranges": [],
        "catalog": "catalog",
        "schema_": "schema",
        "table": "foo",
        "valid_through_ts": 222,
        "categorical_partitions": [],
        "temporal_partitions": [],
        "partitions": [],
        "url": None,
        "links": {},
        "total_partitions": None,
        "total_row_count": None,
        "total_size_bytes": None,
        "ttl_days": None,
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
    assert avail_1.merge(avail_2).model_dump() == {
        "min_temporal_partition": ["20230924"],
        "max_temporal_partition": ["20230927"],
        "temporal_ranges": [
            {
                "min_temporal_partition": ["20230924"],
                "max_temporal_partition": ["20230924"],
            },
            {
                "min_temporal_partition": ["20230926"],
                "max_temporal_partition": ["20230927"],
            },
        ],
        "catalog": "druid",
        "schema_": "",
        "table": "dj_product__launchpad__launchpad_cube",
        "valid_through_ts": 20230927,
        "categorical_partitions": [],
        "temporal_partitions": [],
        "partitions": [],
        "url": None,
        "links": {},
        "total_size_bytes": None,
        "total_row_count": None,
        "total_partitions": None,
        "ttl_days": None,
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
    assert avail_1.model_dump() == {
        "catalog": "iceberg",
        "schema_": "salad",
        "table": "dressing",
        "min_temporal_partition": ["20230101"],
        "max_temporal_partition": ["20231010"],
        "temporal_ranges": [
            {
                "min_temporal_partition": ["20230101"],
                "max_temporal_partition": ["20231010"],
            },
        ],
        "valid_through_ts": 20231015,
        "categorical_partitions": ["country"],
        "temporal_partitions": ["region_date"],
        "partitions": [
            {
                "value": ["CA"],
                "valid_through_ts": 20231010,
                "min_temporal_partition": ["20220101"],
                "max_temporal_partition": ["20231010"],
                "temporal_ranges": [
                    {
                        "min_temporal_partition": ["20220101"],
                        "max_temporal_partition": ["20231010"],
                    },
                ],
            },
            {
                "value": ["US"],
                "valid_through_ts": 20230926,
                "min_temporal_partition": ["20230101"],
                "max_temporal_partition": ["20230926"],
                "temporal_ranges": [
                    {
                        "min_temporal_partition": ["20230101"],
                        "max_temporal_partition": ["20230404"],
                    },
                    {
                        "min_temporal_partition": ["20230924"],
                        "max_temporal_partition": ["20230926"],
                    },
                ],
            },
        ],
        "url": None,
        "links": {},
        "total_size_bytes": None,
        "total_row_count": None,
        "total_partitions": None,
        "ttl_days": None,
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
    assert decoded_cursor.created_at == cursor.created_at
    assert decoded_cursor.id == cursor.id


DAILY = PartitionGrain(formats=["yyyyMMdd"], granularity=Granularity.DAY)
HOURLY = PartitionGrain(formats=["yyyyMMdd", "HH"], granularity=Granularity.HOUR)


def _ranges(pairs):
    """
    Build temporal ranges from (min, max) pairs.
    """
    return [
        TemporalPartitionRange(
            min_temporal_partition=low,
            max_temporal_partition=high,
        )
        for low, high in pairs
    ]


@pytest.mark.parametrize(
    "pairs, grain, expected",
    [
        # Nothing to union
        ([], None, []),
        # A single range comes back untouched
        ([(["20240101"], ["20240131"])], None, [(["20240101"], ["20240131"])]),
        # Disjoint ranges stay apart
        (
            [(["20240101"], ["20240131"]), (["20240301"], ["20240331"])],
            DAILY,
            [(["20240101"], ["20240131"]), (["20240301"], ["20240331"])],
        ),
        # Overlapping ranges merge without a grain
        (
            [(["20240101"], ["20240131"]), (["20240120"], ["20240210"])],
            None,
            [(["20240101"], ["20240210"])],
        ),
        # A nested range disappears into the one that contains it
        (
            [(["20240101"], ["20240331"]), (["20240201"], ["20240210"])],
            None,
            [(["20240101"], ["20240331"])],
        ),
        # Out-of-order input comes back sorted
        (
            [(["20240301"], ["20240331"]), (["20240101"], ["20240131"])],
            None,
            [(["20240101"], ["20240131"]), (["20240301"], ["20240331"])],
        ),
        # The grain says the second range starts the day after the first ends
        (
            [(["20240101"], ["20240131"]), (["20240201"], ["20240210"])],
            DAILY,
            [(["20240101"], ["20240210"])],
        ),
        # Without a grain the same two ranges stay apart
        (
            [(["20240101"], ["20240131"]), (["20240201"], ["20240210"])],
            None,
            [(["20240101"], ["20240131"]), (["20240201"], ["20240210"])],
        ),
        # Adjacency across a multi-column value rolls the hour over
        (
            [
                (["20240101", "00"], ["20240101", "23"]),
                (["20240102", "00"], ["20240102", "05"]),
            ],
            HOURLY,
            [(["20240101", "00"], ["20240102", "05"])],
        ),
        # A daily grain cannot step a date-plus-hour value, so the gap stays
        (
            [
                (["20240101", "00"], ["20240101", "23"]),
                (["20240102", "00"], ["20240102", "05"]),
            ],
            DAILY,
            [
                (["20240101", "00"], ["20240101", "23"]),
                (["20240102", "00"], ["20240102", "05"]),
            ],
        ),
        # A range with no bounds at all is dropped
        (
            [(None, None), (["20240101"], ["20240131"])],
            None,
            [(["20240101"], ["20240131"])],
        ),
        # An open lower bound sorts first and swallows what it covers
        (
            [(["20240101"], ["20240131"]), (None, ["20240210"])],
            None,
            [(None, ["20240210"])],
        ),
        # An open upper bound swallows everything after it
        (
            [(["20240101"], None), (["20240301"], ["20240331"])],
            None,
            [(["20240101"], None)],
        ),
    ],
)
def test_union_ranges(pairs, grain, expected) -> None:
    """
    Test coalescing temporal ranges.
    """
    merged = union_ranges(_ranges(pairs), grain)
    assert [
        (item.min_temporal_partition, item.max_temporal_partition) for item in merged
    ] == expected


def test_union_ranges_cap() -> None:
    """
    Test that too many ranges collapse into one bounding range.
    """
    pairs = [([f"2024{day:04}"], [f"2024{day:04}"]) for day in range(1, 12, 2)]
    assert union_ranges(_ranges(pairs), cap=6) == _ranges(pairs)
    assert union_ranges(_ranges(pairs), cap=5) == _ranges(
        [(["20240001"], ["20240011"])],
    )


def test_union_ranges_from_dicts() -> None:
    """
    Test unioning ranges that are still stored as dicts.
    """
    stored = [
        {
            "min_temporal_partition": ["20240101"],
            "max_temporal_partition": ["20240131"],
        },
        {
            "min_temporal_partition": ["20240201"],
            "max_temporal_partition": ["20240210"],
        },
    ]
    assert union_ranges(stored, DAILY) == _ranges([(["20240101"], ["20240210"])])


def test_availability_bounds_track_ranges() -> None:
    """
    Test that the stored bounds follow the first and last range.
    """
    availability = AvailabilityState(
        catalog="default",
        table="pmts",
        valid_through_ts=20240210,
        temporal_ranges=[
            {
                "min_temporal_partition": ["20240101"],
                "max_temporal_partition": ["20240131"],
            },
            {
                "min_temporal_partition": ["20240201"],
                "max_temporal_partition": ["20240210"],
            },
        ],
    )
    assert availability.min_temporal_partition == ["20240101"]
    assert availability.max_temporal_partition == ["20240210"]

    availability.min_temporal_partition = ["20231201"]
    availability.max_temporal_partition = ["20240215"]
    assert availability.temporal_ranges == [
        {
            "min_temporal_partition": ["20231201"],
            "max_temporal_partition": ["20240131"],
        },
        {
            "min_temporal_partition": ["20240201"],
            "max_temporal_partition": ["20240215"],
        },
    ]


def test_availability_bounds_without_ranges() -> None:
    """
    Test setting bounds on an availability state that has no ranges.
    """
    availability = AvailabilityState(
        catalog="default",
        table="pmts",
        valid_through_ts=20240210,
    )
    assert availability.min_temporal_partition == []
    assert availability.max_temporal_partition == []

    availability.max_temporal_partition = []
    assert availability.temporal_ranges is None

    availability.max_temporal_partition = ["20240210"]
    assert availability.temporal_ranges == [
        {"min_temporal_partition": None, "max_temporal_partition": ["20240210"]},
    ]
    assert availability.min_temporal_partition == []
    assert availability.max_temporal_partition == ["20240210"]


@pytest.mark.parametrize(
    "columns, names, expected",
    [
        ([], None, None),
        ([("birth_date", "yyyyMMdd", Granularity.DAY)], None, (["yyyyMMdd"], "day")),
        ([("birth_date", None, Granularity.DAY)], None, None),
        ([("birth_date", "yyyyMMdd", None)], None, None),
        (
            [
                ("hour", "HH", Granularity.HOUR),
                ("birth_date", "yyyyMMdd", Granularity.DAY),
            ],
            ["birth_date", "hour"],
            (["yyyyMMdd", "HH"], "hour"),
        ),
        (
            [("birth_date", "yyyyMMdd", Granularity.DAY)],
            ["unknown"],
            None,
        ),
    ],
)
def test_temporal_grain(columns, names, expected) -> None:
    """
    Test reading the grain and formats off a node's temporal partitions.
    """
    revision = NodeRevision(
        name="default.hard_hats",
        type=NodeType.DIMENSION,
        columns=[
            Column(
                name=name,
                type=StringType(),
                order=index,
                partition=Partition(
                    type_=PartitionType.TEMPORAL,
                    format=format_,
                    granularity=granularity,
                ),
            )
            for index, (name, format_, granularity) in enumerate(columns)
        ],
    )
    grain = revision.temporal_grain(names)
    assert (
        None if grain is None else (grain.formats, grain.granularity.value)
    ) == expected
