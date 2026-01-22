"""Availability state scalars"""

from typing import List, Optional

import strawberry

from datajunction_server.api.graphql.scalars import BigInt


@strawberry.type
class PartitionAvailability:
    """
    Partition-level availability
    """

    min_temporal_partition: Optional[List[str]]
    max_temporal_partition: Optional[List[str]]

    # This list maps to the ordered list of categorical partitions at the node level.
    # For example, if the node's `categorical_partitions` are configured as ["country", "group_id"],
    # a valid entry for `value` may be ["DE", null].
    value: List[Optional[str]]

    # Valid through timestamp (BigInt to handle millisecond timestamps)
    valid_through_ts: Optional[BigInt]


@strawberry.type
class AvailabilityState:
    """
    A materialized table that is available for the node
    """

    catalog: str
    schema_: Optional[str]
    table: str
    valid_through_ts: BigInt  # BigInt to handle millisecond timestamps
    url: Optional[str]

    # An ordered list of categorical partitions like ["country", "group_id"]
    # or ["region_id", "age_group"]
    categorical_partitions: Optional[List[str]]

    # An ordered list of temporal partitions like ["date", "hour"] or ["date"]
    temporal_partitions: Optional[List[str]]

    # Node-level temporal ranges
    min_temporal_partition: Optional[List[str]]
    max_temporal_partition: Optional[List[str]]

    # Partition-level availabilities
    partitions: Optional[List[PartitionAvailability]]
