"""
Pydantic models for source table metadata.
"""

from typing import List, Optional

from pydantic import BaseModel, Field

from datajunction_server.typing import UTCDatetime


class PartitionMetadataItem(BaseModel):
    """
    A single partition's statistics.
    """

    partition_value: str = Field(
        ...,
        description="Partition value (e.g., '20260131' for date-based partitions)",
    )
    size_bytes: int = Field(
        ...,
        description="Size of partition in bytes",
        ge=0,
    )
    row_count: int = Field(
        ...,
        description="Number of rows in partition",
        ge=0,
    )


class PartitionMetadataUpsertInput(BaseModel):
    """
    Input model for upserting partition metadata.
    Used by metadata collection systems when posting partition statistics to DJ API.
    """

    partitions: List[PartitionMetadataItem] = Field(
        ...,
        description="List of partition statistics to upsert",
    )


class PartitionMetadataOutput(BaseModel):
    """
    Output model for per-partition statistics.
    """

    partition_value: str = Field(
        ...,
        description="Partition value (e.g., '20260131' for date-based partitions)",
    )
    size_bytes: int = Field(
        ...,
        description="Size of partition in bytes",
    )
    row_count: int = Field(
        ...,
        description="Number of rows in partition",
    )
    updated_at: UTCDatetime = Field(
        ...,
        description="When this partition metadata was last updated",
    )

    model_config = {"from_attributes": True}


class SourceTableMetadataInput(BaseModel):
    """
    Input model for source table metadata.
    Used by metadata collection systems when posting table-level statistics to DJ API.
    """

    total_size_bytes: int = Field(
        ...,
        description="Total size of table in bytes",
        ge=0,
    )
    total_row_count: int = Field(
        ...,
        description="Total number of rows in table",
        ge=0,
    )
    total_partitions: int = Field(
        ...,
        description="Total number of partitions",
        ge=0,
    )
    earliest_partition_value: Optional[str] = Field(
        None,
        description="Earliest partition value (e.g., '20200101')",
    )
    latest_partition_value: Optional[str] = Field(
        None,
        description="Latest partition value (e.g., '20260131')",
    )
    freshness_timestamp: Optional[int] = Field(
        None,
        description="Unix timestamp indicating data freshness from external metadata sources",
    )
    ttl_days: Optional[int] = Field(
        None,
        description="Time-to-live in days (data retention period)",
        ge=0,
    )


class SourceTableMetadataOutput(BaseModel):
    """
    Output model for source table metadata.
    """

    total_size_bytes: int = Field(
        ...,
        description="Total size of table in bytes",
    )
    total_row_count: int = Field(
        ...,
        description="Total number of rows in table",
    )
    total_partitions: int = Field(
        ...,
        description="Total number of partitions",
    )
    earliest_partition_value: Optional[str] = Field(
        None,
        description="Earliest partition value (e.g., '20200101')",
    )
    latest_partition_value: Optional[str] = Field(
        None,
        description="Latest partition value (e.g., '20260131')",
    )
    freshness_timestamp: Optional[int] = Field(
        None,
        description="Unix timestamp indicating data freshness from external metadata sources",
    )
    ttl_days: Optional[int] = Field(
        None,
        description="Time-to-live in days (data retention period)",
    )
    updated_at: UTCDatetime = Field(
        ...,
        description="When this metadata was last updated",
    )

    model_config = {"from_attributes": True}
