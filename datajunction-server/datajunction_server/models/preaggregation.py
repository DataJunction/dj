"""
Models for pre-aggregation API requests and responses.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.node import PartitionAvailability


class PlanPreAggregationsRequest(BaseModel):
    """
    Request model for planning pre-aggregations from metrics + dimensions.

    This is the primary way to create pre-aggregations. DJ computes grain groups
    from the metrics/dimensions and creates PreAggregation records with generated SQL.
    """

    metrics: List[str] = Field(
        description="List of metric node names (e.g., ['default.revenue', 'default.orders'])",
    )
    dimensions: List[str] = Field(
        description="List of dimension references (e.g., ['default.date_dim.date_id'])",
    )
    filters: Optional[List[str]] = Field(
        default=None,
        description="Optional SQL filters to apply",
    )

    # Materialization config (applied to all created pre-aggs)
    strategy: Optional[MaterializationStrategy] = Field(
        default=None,
        description="Materialization strategy (FULL or INCREMENTAL_TIME)",
    )
    schedule: Optional[str] = Field(
        default=None,
        description="Cron expression for scheduled materialization",
    )
    lookback_window: Optional[str] = Field(
        default=None,
        description="Lookback window for incremental materialization (e.g., '3 days')",
    )


class PlanPreAggregationsResponse(BaseModel):
    """Response model for /preaggs/plan endpoint."""

    preaggs: List["PreAggregationInfo"]


class UpdatePreAggregationAvailabilityRequest(BaseModel):
    """Request model for updating pre-aggregation availability."""

    catalog: str = Field(description="Catalog where materialized table exists")
    schema_: Optional[str] = Field(
        default=None,
        alias="schema",
        description="Schema where materialized table exists",
    )
    table: str = Field(description="Table name of materialized data")
    valid_through_ts: int = Field(
        description="Timestamp (epoch) through which data is valid",
    )
    url: Optional[str] = Field(
        default=None,
        description="URL to materialization job or dashboard",
    )
    links: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional links related to the materialization",
    )

    # Partition configuration
    categorical_partitions: Optional[List[str]] = Field(
        default_factory=list,
        description="Ordered list of categorical partition columns",
    )
    temporal_partitions: Optional[List[str]] = Field(
        default_factory=list,
        description="Ordered list of temporal partition columns",
    )

    # Temporal ranges
    min_temporal_partition: Optional[List[str]] = Field(
        default_factory=list,
        description="Minimum temporal partition value",
    )
    max_temporal_partition: Optional[List[str]] = Field(
        default_factory=list,
        description="Maximum temporal partition value (high-water mark)",
    )

    # Partition-level details
    partitions: Optional[List[PartitionAvailability]] = Field(
        default_factory=list,
        description="Detailed partition-level availability",
    )

    class Config:
        populate_by_name = True


class PreAggregationInfo(BaseModel):
    """Response model for a pre-aggregation."""

    id: int
    node_revision_id: int
    node_name: str  # Derived from node_revision relationship
    node_version: str  # Derived from node_revision relationship
    grain_columns: List[str]
    measures: List[Dict[str, Any]]  # Full measure info (MetricComponent format)
    sql: str  # The generated SQL for materializing this pre-agg
    grain_group_hash: str

    # Materialization config
    strategy: Optional[MaterializationStrategy] = None
    schedule: Optional[str] = None
    lookback_window: Optional[str] = None

    # Availability (derived from AvailabilityState)
    status: str = "pending"  # "pending" | "active"
    materialized_table_ref: Optional[str] = None
    max_partition: Optional[List[str]] = None

    # Metadata
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class PreAggregationListResponse(BaseModel):
    """Paginated list of pre-aggregations."""

    items: List[PreAggregationInfo]
    total: int
    limit: int
    offset: int


class PreAggregationFilters(BaseModel):
    """Query filters for listing pre-aggregations."""

    node_name: Optional[str] = Field(
        default=None,
        description="Filter by node name (latest version if node_version not specified)",
    )
    node_version: Optional[str] = Field(
        default=None,
        description="Filter by node version (requires node_name)",
    )
    grain: Optional[List[str]] = Field(
        default=None,
        description="Filter by grain columns (exact match)",
    )
    grain_group_hash: Optional[str] = Field(
        default=None,
        description="Filter by grain group hash",
    )
    measures: Optional[List[str]] = Field(
        default=None,
        description="Filter by measure names (pre-agg must contain ALL specified)",
    )
    status: Optional[str] = Field(
        default=None,
        description="Filter by status: 'pending' or 'active'",
    )
    limit: int = Field(default=50, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


# Forward reference update
PlanPreAggregationsResponse.model_rebuild()
