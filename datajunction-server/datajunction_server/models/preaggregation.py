"""
Models for pre-aggregation API requests and responses.
"""

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJWarning
from datajunction_server.models.decompose import PreAggMeasure
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.node import PartitionAvailability
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.models.preagg_binding import (
    REQUEST_SURFACE,
    validate_column_bindings,
)
from datajunction_server.models.query import ColumnMetadata, V3ColumnMetadata


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

    metrics: list[str] = Field(
        description="List of metric node names (e.g., ['default.revenue', 'default.orders'])",
    )
    dimensions: list[str] = Field(
        description="List of dimension references (e.g., ['default.date_dim.date_id'])",
    )
    filters: list[str] | None = Field(
        default=None,
        description="Optional SQL filters to apply",
    )

    # Materialization config (applied to all created pre-aggs)
    strategy: MaterializationStrategy | None = Field(
        default=None,
        description="Materialization strategy (FULL or INCREMENTAL_TIME)",
    )
    schedule: str | None = Field(
        default=None,
        description="Cron expression for scheduled materialization",
    )
    lookback_window: str | None = Field(
        default=None,
        description="Lookback window for incremental materialization (e.g., '3 days')",
    )


class PlanPreAggregationsResponse(BaseModel):
    """Response model for /preaggs/plan endpoint."""

    preaggs: list["PreAggregationInfo"]
    warnings: list[DJWarning] = []


class ExternalPreAggTable(BaseModel):
    """An externally-built table backing a registered pre-aggregation."""

    catalog: str = Field(description="Catalog where the external table exists")
    schema_: str = Field(
        alias="schema",
        description="Schema where the external table exists",
    )
    table: str = Field(description="Name of the external table")
    valid_through_ts: int | None = Field(
        default=None,
        description=(
            "Timestamp (epoch) through which the external data is valid. When "
            "provided, availability is set immediately; otherwise the pre-agg is "
            "registered as pending until reported via the availability endpoint."
        ),
    )

    class Config:
        populate_by_name = True


class RegisterPreAggregationsRequest(BaseModel):
    """
    Request model for registering an externally-built pre-aggregation table.

    Unlike /preaggs/plan (where DJ generates and owns the materialization), this
    adopts a table already built by an external pipeline. DJ decomposes the
    metrics to determine the required measures, validates each declared column
    against the table, and records the pre-aggregation so grain resolution can
    route queries to it.

    Every metric and every dimension is declared together with the physical
    column of the external table that holds it, as a map::

        {
          "metrics": {"default.view_secs": "view_secs_sum"},
          "dimensions": {"default.page_d.page_id": "page"}
        }

    This is the same declaration a ``kind: preagg`` YAML spec makes, so the two
    surfaces share their validation -- see
    :mod:`datajunction_server.models.preagg_binding`. Both maps require a value
    for every key, including a dimension whose physical column matches its DJ
    column name.

    What goes under ``metrics`` are the measures the table stores. A derived
    metric has no column of its own, so it is not listed and cannot be; it is
    served anyway, since registration and query-time matching both work on
    decomposed measure identities rather than metric names.
    """

    name: str | None = Field(
        default=None,
        description=(
            "Optional stable handle for the pre-aggregation, used by YAML deploy "
            "reconciliation and availability-by-name callbacks."
        ),
    )
    metrics: dict[str, str] = Field(
        description=(
            "Maps each is_measure metric name the external table serves to the "
            "physical column holding its pre-aggregated value. A derived metric "
            "has no column and is not listed; it is covered once the measures it "
            "decomposes into are."
        ),
    )
    dimensions: dict[str, str] = Field(
        description=(
            "Maps each dimension reference defining the table's grain to the "
            "physical column in the external table backing it. Required for every "
            "dimension, including one stored under the DJ column name."
        ),
    )
    table: ExternalPreAggTable = Field(description="The externally-built table")

    @model_validator(mode="before")
    @classmethod
    def require_column_bindings(cls, data: Any) -> Any:
        """
        Hold the caller to the map form. Shared with the ``kind: preagg`` YAML
        spec, which enforces the identical rules against a spec file.
        """
        return validate_column_bindings(data, REQUEST_SURFACE)


class UpdatePreAggregationAvailabilityRequest(BaseModel):
    """Request model for updating pre-aggregation availability."""

    catalog: str = Field(description="Catalog where materialized table exists")
    schema_: str | None = Field(
        default=None,
        alias="schema",
        description="Schema where materialized table exists",
    )
    table: str = Field(description="Table name of materialized data")
    valid_through_ts: int = Field(
        description="Timestamp (epoch) through which data is valid",
    )
    url: str | None = Field(
        default=None,
        description="URL to materialization job or dashboard",
    )
    links: dict[str, Any] | None = Field(
        default_factory=dict,
        description="Additional links related to the materialization",
    )

    # Partition configuration
    categorical_partitions: list[str] | None = Field(
        default_factory=list,
        description="Ordered list of categorical partition columns",
    )
    temporal_partitions: list[str] | None = Field(
        default_factory=list,
        description="Ordered list of temporal partition columns",
    )

    # Temporal ranges
    min_temporal_partition: list[str] | None = Field(
        default_factory=list,
        description="Minimum temporal partition value",
    )
    max_temporal_partition: list[str] | None = Field(
        default_factory=list,
        description="Maximum temporal partition value (high-water mark)",
    )

    # Partition-level details
    partitions: list[PartitionAvailability] | None = Field(
        default_factory=list,
        description="Detailed partition-level availability",
    )

    @field_validator("min_temporal_partition", "max_temporal_partition", mode="before")
    @classmethod
    def coerce_temporal_partition_to_str(cls, value):
        """Coerce int partition values (e.g., 20260428) to strings."""
        if value is None:
            return value
        return [str(part) for part in value]

    class Config:
        populate_by_name = True


class PreAggregationInfo(BaseModel):
    """Response model for a pre-aggregation."""

    id: int
    node_revision_id: int
    node_name: str  # Derived from node_revision relationship
    node_version: str  # Derived from node_revision relationship
    grain_columns: list[str]
    measures: list[PreAggMeasure]  # Full measure info (MetricComponent format)
    columns: list[V3ColumnMetadata] | None = None  # Output columns with types
    sql: str  # The generated SQL for materializing this pre-agg
    grain_group_hash: str
    preagg_hash: str  # Unique hash including measures (used for table/workflow naming)
    name: str | None = None  # Stable handle for externally-registered pre-aggs

    # Materialization config
    strategy: MaterializationStrategy | None = None
    schedule: str | None = None
    lookback_window: str | None = None

    # Workflow state (persisted)
    workflow_urls: list[WorkflowUrl] | None = None  # Labeled workflow URLs
    workflow_status: str | None = (
        None  # WorkflowStatus.ACTIVE | WorkflowStatus.PAUSED | None
    )
    workflow_names: list[str] | None = None  # Workflow names for deactivation

    # Availability (derived from AvailabilityState)
    status: str = "pending"  # "pending" | "running" | "active"
    materialized_table_ref: str | None = None
    max_partition: list[str] | None = None

    # Related metrics (computed from FrozenMeasure relationships)
    related_metrics: list[str] | None = None  # Metric names that use these measures

    # Metadata
    created_at: datetime
    updated_at: datetime | None = None

    class Config:
        from_attributes = True


class PreAggregationListResponse(BaseModel):
    """Paginated list of pre-aggregations."""

    items: list[PreAggregationInfo]
    total: int
    limit: int
    offset: int


class PreAggregationFilters(BaseModel):
    """Query filters for listing pre-aggregations."""

    node_name: str | None = Field(
        default=None,
        description="Filter by node name (latest version if node_version not specified)",
    )
    node_version: str | None = Field(
        default=None,
        description="Filter by node version (requires node_name)",
    )
    grain: list[str] | None = Field(
        default=None,
        description="Filter by grain columns (exact match)",
    )
    grain_group_hash: str | None = Field(
        default=None,
        description="Filter by grain group hash",
    )
    measures: list[str] | None = Field(
        default=None,
        description="Filter by measure names (pre-agg must contain ALL specified)",
    )
    status: str | None = Field(
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
    column_type: str | None = Field(
        default="int",
        description="Column data type (e.g., 'int', 'string')",
    )
    format: str | None = Field(
        default=None,
        description="Format string (e.g., 'yyyyMMdd' for date, None for integer hour)",
    )
    granularity: Granularity | None = Field(
        default=None,
        description="Time granularity this column represents (DAY, HOUR, etc.)",
    )
    expression: str | None = Field(
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
    grain: list[str] = Field(description="Grain columns (fully qualified)")
    measures: list[PreAggMeasure] = Field(
        description="Measures with MetricComponent info",
    )

    # The SQL query to materialize
    query: str = Field(description="SQL query for materialization")

    # Output columns metadata
    columns: list[ColumnMetadata] = Field(description="Output column metadata")

    # Upstream tables used in the query (for dependency tracking)
    upstream_tables: list[str] = Field(
        default_factory=list,
        description="List of upstream tables used in the query (e.g., ['catalog.schema.table'])",
    )

    # Partition info (for incremental materialization)
    # Supports multi-column partitions (e.g., dateint + hour for hourly)
    temporal_partitions: list[TemporalPartitionColumn] = Field(
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
    lookback_window: str | None = Field(
        default=None,
        description="Lookback window for incremental (e.g., '3 days')",
    )
    timezone: str | None = Field(
        default="US/Pacific",
        description="Timezone for scheduling",
    )
    druid_spec: dict[str, Any] | None = Field(
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

    workflow_url: str | None = Field(
        default=None,
        description="URL to the scheduled workflow definition",
    )
    status: str = Field(
        description="Workflow status: 'active', 'paused', or 'none'",
    )
    message: str | None = Field(
        default=None,
        description="Additional information about the operation",
    )


class DeactivatedWorkflowInfo(BaseModel):
    """Info about a single deactivated workflow."""

    id: int = Field(description="Pre-aggregation ID")
    workflow_name: str | None = Field(
        default=None,
        description="Name of the deactivated workflow",
    )


class BulkDeactivateWorkflowsResponse(BaseModel):
    """Response model for bulk workflow deactivation."""

    deactivated_count: int = Field(
        description="Number of workflows successfully deactivated",
    )
    deactivated: list[DeactivatedWorkflowInfo] = Field(
        default_factory=list,
        description="Details of each deactivated workflow",
    )
    skipped_count: int = Field(
        default=0,
        description="Number of pre-aggs skipped (no active workflow)",
    )
    message: str | None = Field(
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
    end_date: date | None = Field(
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
