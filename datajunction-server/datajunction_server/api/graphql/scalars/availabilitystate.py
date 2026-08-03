"""Availability state scalars"""

import strawberry

from datajunction_server.api.graphql.scalars import BigInt


@strawberry.type
class PartitionAvailability:
    """
    Partition-level availability
    """

    min_temporal_partition: list[str] | None
    max_temporal_partition: list[str] | None

    # This list maps to the ordered list of categorical partitions at the node level.
    # For example, if the node's `categorical_partitions` are configured as ["country", "group_id"],
    # a valid entry for `value` may be ["DE", null].
    value: list[str | None]

    # Valid through timestamp (BigInt to handle millisecond timestamps)
    valid_through_ts: BigInt | None


@strawberry.type
class AvailabilityState:
    """
    A materialized table that is available for the node
    """

    catalog: str
    schema_: str | None
    table: str
    valid_through_ts: BigInt  # BigInt to handle millisecond timestamps
    url: str | None

    # An ordered list of categorical partitions like ["country", "group_id"]
    # or ["region_id", "age_group"]
    categorical_partitions: list[str] | None

    # An ordered list of temporal partitions like ["date", "hour"] or ["date"]
    temporal_partitions: list[str] | None

    # Node-level temporal ranges
    min_temporal_partition: list[str] | None
    max_temporal_partition: list[str] | None

    # Partition-level availabilities
    partitions: list[PartitionAvailability] | None
