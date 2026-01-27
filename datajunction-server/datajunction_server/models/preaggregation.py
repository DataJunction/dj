"""
Models for pre-aggregation API requests and responses.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from datajunction_server.enum import StrEnum
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.node import PartitionAvailability
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.models.decompose import PreAggMeasure
from datajunction_server.models.query import V3ColumnMetadata


class WorkflowStatus(StrEnum):
    """Status of a pre-aggregation workflow."""

    ACTIVE = "active"
    PAUSED = "paused"


class WorkflowUrl(BaseModel):
    """A labeled workflow URL for scheduler-agnostic display."""

    label: str = Field(
        description="Label for the workflow (e.g., 'scheduled', 'backfill')",
    )
    url: str = Field(description="URL to the workflow")


class GrainMode(StrEnum):
    """
    Grain matching mode for pre-aggregation lookup.

    - EXACT: Pre-agg grain must match requested grain exactly
    - SUPERSET: Pre-agg grain must contain all requested columns (and possibly more)
    """

    EXACT = "exact"
    SUPERSET = "superset"


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
    measures: List[PreAggMeasure]  # Full measure info (MetricComponent format)
    columns: Optional[List[V3ColumnMetadata]] = None  # Output columns with types
    sql: str  # The generated SQL for materializing this pre-agg
    grain_group_hash: str
    preagg_hash: str  # Unique hash including measures (used for table/workflow naming)

    # Materialization config
    strategy: Optional[MaterializationStrategy] = None
    schedule: Optional[str] = None
    lookback_window: Optional[str] = None

    # Workflow state (persisted)
    workflow_urls: Optional[List[WorkflowUrl]] = None  # Labeled workflow URLs
    workflow_status: Optional[str] = (
        None  # WorkflowStatus.ACTIVE | WorkflowStatus.PAUSED | None
    )

    # Availability (derived from AvailabilityState)
    status: str = "pending"  # "pending" | "running" | "active"
    materialized_table_ref: Optional[str] = None
    max_partition: Optional[List[str]] = None

    # Related metrics (computed from FrozenMeasure relationships)
    related_metrics: Optional[List[str]] = None  # Metric names that use these measures

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


class TemporalPartitionColumn(BaseModel):
    """
    A single temporal partition column with its format and granularity.

    Used for incremental materialization to generate partition filters.
    Supports both single-column (e.g., date_id) and multi-column (e.g., dateint + hour)
    partition schemes.
    """

    column_name: str = Field(description="Column name in the output")
    column_type: Optional[str] = Field(
        default="int",
        description="Column data type (e.g., 'int', 'string')",
    )
    format: Optional[str] = Field(
        default=None,
        description="Format string (e.g., 'yyyyMMdd' for date, None for integer hour)",
    )
    granularity: Optional[Granularity] = Field(
        default=None,
        description="Time granularity this column represents (DAY, HOUR, etc.)",
    )
    expression: Optional[str] = Field(
        default=None,
        description="Optional SQL expression for filter generation",
    )


# Default daily schedule (midnight UTC)
DEFAULT_SCHEDULE = "0 0 * * *"


class PreAggMaterializationInput(BaseModel):
    """
    Input for materializing a pre-aggregation.

    Sent to the query service's POST /preaggs/materialize endpoint.
    Creates a scheduled workflow that runs on the configured schedule.
    The query service uses `preagg_id` to callback to DJ's
    POST /preaggs/{preagg_id}/availability/ when materialization completes.
    """

    # Pre-agg identity (for callback routing)
    preagg_id: int = Field(description="Pre-aggregation ID for callback routing")

    # Output table (derived at call time, not stored)
    output_table: str = Field(
        description="Target table name (e.g., 'orders_fact__preagg_abc12345')",
    )

    # Source node info
    node: NodeNameVersion = Field(description="Source node name and version")

    # Grain and measures
    grain: List[str] = Field(description="Grain columns (fully qualified)")
    measures: List[PreAggMeasure] = Field(
        description="Measures with MetricComponent info",
    )

    # The SQL query to materialize
    query: str = Field(description="SQL query for materialization")

    # Output columns metadata
    columns: List[ColumnMetadata] = Field(description="Output column metadata")

    # Upstream tables used in the query (for dependency tracking)
    upstream_tables: List[str] = Field(
        default_factory=list,
        description="List of upstream tables used in the query (e.g., ['catalog.schema.table'])",
    )

    # Partition info (for incremental materialization)
    # Supports multi-column partitions (e.g., dateint + hour for hourly)
    temporal_partitions: List[TemporalPartitionColumn] = Field(
        default_factory=list,
        description="Temporal partition columns for incremental materialization",
    )

    # Materialization config
    strategy: MaterializationStrategy = Field(
        description="Materialization strategy (FULL or INCREMENTAL_TIME)",
    )
    schedule: str = Field(
        default=DEFAULT_SCHEDULE,
        description="Cron schedule for recurring materialization (default: daily at midnight)",
    )
    lookback_window: Optional[str] = Field(
        default=None,
        description="Lookback window for incremental (e.g., '3 days')",
    )
    timezone: Optional[str] = Field(
        default="US/Pacific",
        description="Timezone for scheduling",
    )
    druid_spec: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Druid ingestion spec",
    )
    activate: bool = Field(
        default=True,
        description="Whether to activate the workflow immediately",
    )


# =============================================================================
# Workflow Management Models
# =============================================================================


class WorkflowResponse(BaseModel):
    """Response model for workflow operations."""

    workflow_url: Optional[str] = Field(
        default=None,
        description="URL to the scheduled workflow definition",
    )
    status: str = Field(
        description="Workflow status: 'active', 'paused', or 'none'",
    )
    message: Optional[str] = Field(
        default=None,
        description="Additional information about the operation",
    )


class DeactivatedWorkflowInfo(BaseModel):
    """Info about a single deactivated workflow."""

    id: int = Field(description="Pre-aggregation ID")
    workflow_name: Optional[str] = Field(
        default=None,
        description="Name of the deactivated workflow",
    )


class BulkDeactivateWorkflowsResponse(BaseModel):
    """Response model for bulk workflow deactivation."""

    deactivated_count: int = Field(
        description="Number of workflows successfully deactivated",
    )
    deactivated: List[DeactivatedWorkflowInfo] = Field(
        default_factory=list,
        description="Details of each deactivated workflow",
    )
    skipped_count: int = Field(
        default=0,
        description="Number of pre-aggs skipped (no active workflow)",
    )
    message: Optional[str] = Field(
        default=None,
        description="Additional information about the operation",
    )


# =============================================================================
# Backfill Models
# =============================================================================


class BackfillRequest(BaseModel):
    """Request model for running a backfill."""

    start_date: date = Field(
        description="Start date for backfill (inclusive)",
    )
    end_date: Optional[date] = Field(
        default=None,
        description="End date for backfill (inclusive). Defaults to today.",
    )


class BackfillResponse(BaseModel):
    """Response model for backfill operation."""

    job_url: str = Field(
        description="URL to the backfill job",
    )
    start_date: date = Field(
        description="Start date of the backfill",
    )
    end_date: date = Field(
        description="End date of the backfill",
    )
    status: str = Field(
        default="running",
        description="Job status",
    )


# =============================================================================
# Query Service Input Models (sent to dj-query)
# =============================================================================


# WorkflowInput is now consolidated into PreAggMaterializationInput
# Use PreAggMaterializationInput for both one-time and scheduled materializations


class BackfillInput(BaseModel):
    """
    Simplified input for running a backfill in query service.

    The workflow must already exist (created via POST /preaggs/{id}/materialize).
    Query Service uses node_name for readable workflow names and output_table
    checksum for uniqueness.

    Note: For single-date runs, use same start_date and end_date.
    """

    preagg_id: int = Field(description="Pre-aggregation ID")
    output_table: str = Field(
        description="Output table name (used to derive workflow checksum)",
    )
    node_name: str = Field(
        description="Node name (used for readable workflow name)",
    )
    start_date: date = Field(description="Backfill start date")
    end_date: date = Field(description="Backfill end date")


class CubeBackfillInput(BaseModel):
    """
    Input for running a cube backfill in query service.

    The cube workflow must already exist (created via POST /cubes/{name}/materialize).
    Query Service uses cube_name and cube_version to derive workflow names via checksum.
    """

    cube_name: str = Field(description="Cube name (e.g., 'ads.my_cube')")
    cube_version: str = Field(
        description="Cube version (e.g., 'v1.0'). Required for workflow name checksum.",
    )
    start_date: date = Field(description="Backfill start date")
    end_date: date = Field(description="Backfill end date")


# Forward reference update
PlanPreAggregationsResponse.model_rebuild()
