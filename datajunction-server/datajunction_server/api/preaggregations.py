"""
Pre-aggregation related APIs.

These endpoints support both DJ-managed and user-managed materialization flows:

Flow A (DJ-managed):
1. User calls POST /preaggs/plan with metrics + dimensions
2. User calls POST /preaggs/{id}/materialize to trigger DJ's query service
3. Query service materializes and posts back availability

Flow B (User-managed):
1. User calls POST /preaggs/plan with metrics + dimensions
2. User gets pre-agg IDs and SQL from response
3. User materializes in their own query service
4. User calls POST /preaggs/{id}/availability/ to report completion
"""

import logging
from http import HTTPStatus
from typing import List, Optional
from datetime import date as date_type

from fastapi import Depends, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.construction.build_v3.builder import build_measures_sql
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.preaggregation import (
    PreAggregation,
    VALID_PREAGG_STRATEGIES,
    compute_grain_group_hash,
    compute_expression_hash,
    compute_preagg_hash,
    compute_preagg_slug,
)
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJInvalidInputException,
    DJQueryServiceClientException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.preaggregation import (
    BackfillRequest,
    BackfillInput,
    BackfillResponse,
    CreateWorkflowRequest,
    PlanPreAggregationsRequest,
    PlanPreAggregationsResponse,
    PreAggregationInfo,
    PreAggregationListResponse,
    PreAggMaterializationInput,
    TemporalPartitionColumn,
    UpdatePreAggregationAvailabilityRequest,
    WorkflowInput,
    WorkflowResponse,
)
from datajunction_server.models.decompose import PreAggMeasure
from datajunction_server.models.query import ColumnMetadata, V3ColumnMetadata
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import get_query_service_client, get_session

_logger = logging.getLogger(__name__)
router = SecureAPIRouter(tags=["preaggregations"])


def _compute_output_table(node_name: str, grain_group_hash: str) -> str:
    """
    Compute the output table name for a pre-aggregation.

    Format: {node_short}_preagg_{hash[:8]}
    """
    node_short = node_name.replace(".", "_")
    return f"{node_short}_preagg_{grain_group_hash[:8]}"


def _preagg_to_info(preagg: PreAggregation) -> PreAggregationInfo:
    """Convert a PreAggregation ORM object to a PreAggregationInfo response model."""
    return PreAggregationInfo(
        id=preagg.id,
        slug=preagg.slug,
        node_revision_id=preagg.node_revision_id,
        node_name=preagg.node_revision.name,
        node_version=preagg.node_revision.version,
        grain_columns=preagg.grain_columns,
        measures=preagg.measures,
        columns=preagg.columns,  # Include output columns with types
        sql=preagg.sql,
        grain_group_hash=preagg.grain_group_hash,
        strategy=preagg.strategy,
        schedule=preagg.schedule,
        lookback_window=preagg.lookback_window,
        scheduled_workflow_url=preagg.scheduled_workflow_url,
        workflow_status=preagg.workflow_status,
        status=preagg.status,
        materialized_table_ref=preagg.materialized_table_ref,
        max_partition=preagg.max_partition,
        created_at=preagg.created_at,
        updated_at=preagg.updated_at,
    )


# =============================================================================
# GET Endpoints - List and Query
# =============================================================================


@router.get(
    "/preaggs/",
    response_model=PreAggregationListResponse,
    name="List Pre-aggregations",
)
async def list_preaggregations(
    node_name: Optional[str] = Query(
        default=None,
        description="Filter by node name",
    ),
    node_version: Optional[str] = Query(
        default=None,
        description="Filter by node version (requires node_name)",
    ),
    grain: Optional[str] = Query(
        default=None,
        description="Comma-separated grain columns (exact match)",
    ),
    grain_group_hash: Optional[str] = Query(
        default=None,
        description="Filter by grain group hash",
    ),
    measures: Optional[str] = Query(
        default=None,
        description="Comma-separated measures (pre-agg must contain ALL)",
    ),
    status: Optional[str] = Query(
        default=None,
        description="Filter by status: 'pending' or 'active'",
    ),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    *,
    session: AsyncSession = Depends(get_session),
) -> PreAggregationListResponse:
    """
    List pre-aggregations with optional filters.

    Filter options:
    - node_name: Filter by the source node name
    - node_version: Filter by node version (if omitted, uses latest version)
    - grain: Comma-separated grain columns for exact match
    - grain_group_hash: Direct lookup by grain group hash
    - measures: Comma-separated measures - pre-agg must contain ALL specified
    - status: Filter by 'pending' (no availability) or 'active' (has availability)
    """
    # Build base query with eager loading
    stmt = select(PreAggregation).options(
        joinedload(PreAggregation.node_revision),
        joinedload(PreAggregation.availability),
    )

    # Filter by node_name (and optionally version)
    if node_name:
        node = await Node.get_by_name(session, node_name)
        if not node:
            raise DJDoesNotExistException(f"Node '{node_name}' not found")

        if node_version:
            # Find specific revision
            target_revision = None
            for rev in node.revisions:
                if rev.version == node_version:
                    target_revision = rev
                    break
            if not target_revision:
                raise DJDoesNotExistException(
                    f"Version '{node_version}' not found for node '{node_name}'",
                )
            stmt = stmt.where(PreAggregation.node_revision_id == target_revision.id)
        else:
            # Use latest version
            stmt = stmt.where(PreAggregation.node_revision_id == node.current.id)

    # Filter by grain_group_hash (direct lookup)
    if grain_group_hash:
        stmt = stmt.where(PreAggregation.grain_group_hash == grain_group_hash)

    # Filter by grain columns (exact match)
    grain_cols: Optional[List[str]] = None
    if grain:
        grain_cols = [g.strip() for g in grain.split(",")]

    # Execute query for total count (before pagination)
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_result = await session.execute(count_stmt)
    total = total_result.scalar() or 0

    # Apply pagination
    stmt = stmt.offset(offset).limit(limit)

    # Execute main query
    result = await session.execute(stmt)
    preaggs = list(result.scalars().unique().all())

    # Post-filter by grain columns (exact match)
    if grain_cols:
        preaggs = [p for p in preaggs if sorted(p.grain_columns) == sorted(grain_cols)]

    # Post-filter by measures (superset match - pre-agg must contain ALL by name)
    if measures:
        measure_list = [m.strip() for m in measures.split(",")]
        needed = set(measure_list)
        preaggs = [
            p for p in preaggs if needed <= {m.get("name", "") for m in p.measures}
        ]

    # Post-filter by status
    if status:
        if status not in ("pending", "active"):
            raise DJDoesNotExistException(
                f"Invalid status '{status}'. Must be 'pending' or 'active'",
            )
        preaggs = [p for p in preaggs if p.status == status]

    # Convert to response models
    items = [_preagg_to_info(p) for p in preaggs]

    return PreAggregationListResponse(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/preaggs/{preagg_id}",
    response_model=PreAggregationInfo,
    name="Get Pre-aggregation by ID",
)
async def get_preaggregation(
    preagg_id: int,
    *,
    session: AsyncSession = Depends(get_session),
) -> PreAggregationInfo:
    """
    Get a single pre-aggregation by its ID.

    The response includes the SQL needed for materialization.
    """
    stmt = (
        select(PreAggregation)
        .options(
            joinedload(PreAggregation.node_revision),
            joinedload(PreAggregation.availability),
        )
        .where(PreAggregation.id == preagg_id)
    )
    result = await session.execute(stmt)
    preagg = result.scalar_one_or_none()

    if not preagg:
        raise DJDoesNotExistException(f"Pre-aggregation with ID {preagg_id} not found")

    return _preagg_to_info(preagg)


@router.get(
    "/preaggs/by-slug/{slug:path}",
    response_model=PreAggregationInfo,
    name="Get Pre-aggregation by Slug",
)
async def get_preaggregation_by_slug(
    slug: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> PreAggregationInfo:
    """
    Get a single pre-aggregation by its slug.

    The slug format is: {node_name}-{hash}
    Example: common.orders.orders_fact-abc12345

    The response includes the SQL needed for materialization.
    """
    stmt = (
        select(PreAggregation)
        .options(
            joinedload(PreAggregation.node_revision),
            joinedload(PreAggregation.availability),
        )
        .where(PreAggregation.slug == slug)
    )
    result = await session.execute(stmt)
    preagg = result.scalar_one_or_none()

    if not preagg:
        raise DJDoesNotExistException(f"Pre-aggregation with slug '{slug}' not found")

    return _preagg_to_info(preagg)


# =============================================================================
# POST Endpoints - Plan, Materialize, Availability
# =============================================================================


@router.post(
    "/preaggs/plan",
    response_model=PlanPreAggregationsResponse,
    status_code=HTTPStatus.CREATED,
    name="Plan Pre-aggregations",
)
async def plan_preaggregations(
    data: PlanPreAggregationsRequest,
    *,
    session: AsyncSession = Depends(get_session),
) -> PlanPreAggregationsResponse:
    """
    Create pre-aggregations from metrics + dimensions.

    This is the primary way to create pre-aggregations. DJ:
    1. Computes grain groups from the metrics/dimensions (same as /sql/measures/v3)
    2. Generates SQL for each grain group
    3. Creates PreAggregation records (or returns existing ones if they match)
    4. Returns the pre-aggs with their IDs and SQL

    After calling this endpoint:
    - Flow A: Call POST /preaggs/{id}/materialize to have DJ materialize
    - Flow B: Use the returned SQL to materialize yourself, then call
              POST /preaggs/{id}/availability/ to report completion
    """
    _logger.info(
        "Planning pre-aggregations for metrics=%s dimensions=%s",
        data.metrics,
        data.dimensions,
    )

    # Validate strategy if provided
    if data.strategy and data.strategy not in VALID_PREAGG_STRATEGIES:
        raise DJInvalidInputException(
            message=f"Invalid strategy '{data.strategy}'. "
            f"Valid strategies: {[s.value for s in VALID_PREAGG_STRATEGIES]}",
        )

    # Build measures SQL - this computes grain groups from metrics + dimensions
    # We set use_materialized=False since we're generating SQL for materialization
    # For INCREMENTAL_TIME strategy, include temporal filters with DJ_LOGICAL_TIMESTAMP()
    include_temporal_filters = data.strategy == MaterializationStrategy.INCREMENTAL_TIME
    measures_result = await build_measures_sql(
        session=session,
        metrics=data.metrics,
        dimensions=data.dimensions,
        filters=data.filters,
        dialect=Dialect.SPARK,
        use_materialized=False,
        include_temporal_filters=include_temporal_filters,
        lookback_window=data.lookback_window if include_temporal_filters else None,
    )

    created_preaggs: list[PreAggregation] = []

    # Process each grain group and create PreAggregation records
    for grain_group in measures_result.grain_groups:
        # Get the parent node from context
        parent_node = measures_result.ctx.nodes.get(grain_group.parent_name)
        if not parent_node or not parent_node.current:
            _logger.warning(
                "Parent node %s not found in context, skipping grain group",
                grain_group.parent_name,
            )
            continue

        node_revision_id = parent_node.current.id

        # Validate: INCREMENTAL_TIME requires temporal partition columns
        if data.strategy == MaterializationStrategy.INCREMENTAL_TIME:
            source_temporal_cols = parent_node.current.temporal_partition_columns()
            if not source_temporal_cols:
                raise DJInvalidInputException(
                    message=(
                        f"INCREMENTAL_TIME strategy requires the source node "
                        f"'{grain_group.parent_name}' to have temporal partition columns. "
                        f"Either add temporal partition columns to the source node or use "
                        f"FULL strategy instead."
                    ),
                )

        # Convert grain to fully qualified dimension references
        # The grain_group.grain contains column aliases, we need the full refs
        # Use the requested dimensions that correspond to these grain columns
        grain_columns = list(measures_result.requested_dimensions)

        # Convert MetricComponents to PreAggMeasure with expr_hash
        measures = [
            PreAggMeasure(
                **component.model_dump(),
                expr_hash=compute_expression_hash(component.expression),
            )
            for component in grain_group.components
        ]

        # Get the SQL for this grain group
        sql = grain_group.sql

        # Convert column metadata to V3ColumnMetadata for storage
        columns = [
            V3ColumnMetadata(
                name=col.name,
                type=col.type,
                semantic_type=col.semantic_type,
                semantic_entity=col.semantic_name,
            )
            for col in grain_group.columns
        ]

        # Compute grain_group_hash for lookup
        grain_group_hash = compute_grain_group_hash(node_revision_id, grain_columns)

        # Compute unique slug: {node_name}-{hash}
        preagg_hash = compute_preagg_hash(node_revision_id, grain_columns, measures)
        slug = compute_preagg_slug(grain_group.parent_name, preagg_hash)

        # Check if a matching pre-agg already exists
        existing = await PreAggregation.find_matching(
            session=session,
            node_revision_id=node_revision_id,
            grain_columns=grain_columns,
            measure_expr_hashes={m.expr_hash for m in measures if m.expr_hash},
        )

        if existing:
            _logger.info(
                "Found existing pre-agg id=%s for grain_group_hash=%s",
                existing.id,
                grain_group_hash,
            )
            # Update config if provided (allows re-running plan with new settings)
            if data.strategy:
                existing.strategy = data.strategy
            if data.schedule:
                existing.schedule = data.schedule
            if data.lookback_window:
                existing.lookback_window = data.lookback_window
            # Update SQL and columns in case they changed
            existing.sql = sql
            existing.columns = columns
            # Backfill slug if not set (for pre-existing pre-aggs)
            if not existing.slug:
                existing.slug = slug
            created_preaggs.append(existing)
        else:
            # Create new pre-aggregation
            preagg = PreAggregation(
                node_revision_id=node_revision_id,
                grain_columns=grain_columns,
                measures=measures,
                columns=columns,
                sql=sql,
                grain_group_hash=grain_group_hash,
                slug=slug,
                strategy=data.strategy,
                schedule=data.schedule,
                lookback_window=data.lookback_window,
            )
            session.add(preagg)
            created_preaggs.append(preagg)
            _logger.info(
                "Created new pre-agg for parent=%s grain=%s measures=%d",
                grain_group.parent_name,
                grain_columns,
                len(measures),
            )

    await session.commit()

    # Re-fetch pre-aggs with eager-loaded relationships
    preagg_ids = [p.id for p in created_preaggs]
    stmt = (
        select(PreAggregation)
        .options(
            joinedload(PreAggregation.node_revision),
            joinedload(PreAggregation.availability),
        )
        .where(PreAggregation.id.in_(preagg_ids))
    )
    result = await session.execute(stmt)
    loaded_preaggs = list(result.scalars().unique().all())

    return PlanPreAggregationsResponse(
        preaggs=[_preagg_to_info(p) for p in loaded_preaggs],
    )


@router.post(
    "/preaggs/{preagg_id}/materialize",
    response_model=PreAggregationInfo,
    name="Materialize Pre-aggregation",
)
async def materialize_preaggregation(
    preagg_id: int,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> PreAggregationInfo:
    """
    Trigger materialization for a pre-aggregation (Flow A).

    DJ will call the configured query service to materialize this pre-agg
    using the stored SQL. The query service will post back availability
    when materialization completes.

    For user-managed materialization (Flow B), use the SQL from
    GET /preaggs/{id} and call POST /preaggs/{id}/availability/ when done.
    """
    # Get the pre-agg with node_revision and its columns/partitions
    stmt = (
        select(PreAggregation)
        .options(
            joinedload(PreAggregation.node_revision).options(
                *NodeRevision.default_load_options(),
            ),
            joinedload(PreAggregation.availability),
        )
        .where(PreAggregation.id == preagg_id)
    )
    result = await session.execute(stmt)
    preagg = result.scalar_one_or_none()

    if not preagg:
        raise DJDoesNotExistException(f"Pre-aggregation with ID {preagg_id} not found")

    # Validate strategy is set
    if not preagg.strategy:
        raise DJInvalidInputException(
            message="Strategy must be set before materialization. "
            "Update the pre-aggregation with a strategy first.",
        )

    # Validate schedule for incremental
    if (
        preagg.strategy == MaterializationStrategy.INCREMENTAL_TIME
        and not preagg.schedule
    ):
        raise DJInvalidInputException(
            message="Schedule is required for INCREMENTAL_TIME strategy.",
        )

    # Check if source node has temporal partition columns (needed for INCREMENTAL_TIME)
    source_temporal_cols = preagg.node_revision.temporal_partition_columns()
    if (
        preagg.strategy == MaterializationStrategy.INCREMENTAL_TIME
        and not source_temporal_cols
    ):
        raise DJInvalidInputException(
            message=(
                f"INCREMENTAL_TIME strategy requires the source node "
                f"'{preagg.node_revision.name}' to have temporal partition columns. "
                f"Either add temporal partition columns to the source node or use "
                f"FULL strategy instead."
            ),
        )

    # Derive output table name: {node_short}_preagg_{hash[:8]}
    # Note: Use single underscore to avoid '__' which is disallowed in workflow names
    node_short = preagg.node_revision.name.split(".")[-1]
    output_table = f"{node_short}_preagg_{preagg.grain_group_hash[:8]}"

    # Build temporal partition columns from source node (for incremental)
    # Supports multi-column partitions (e.g., dateint + hour for hourly)
    temporal_partitions: list[TemporalPartitionColumn] = []
    for temporal_col in source_temporal_cols:
        temporal_partitions.append(
            TemporalPartitionColumn(
                column_name=temporal_col.name,
                format=temporal_col.partition.format
                if temporal_col.partition
                else None,
                granularity=(
                    str(temporal_col.partition.granularity.value)
                    if temporal_col.partition and temporal_col.partition.granularity
                    else None
                ),
            ),
        )

    # Build columns metadata from stored V3ColumnMetadata
    columns: list[ColumnMetadata] = []

    if preagg.columns:
        # Convert V3ColumnMetadata to ColumnMetadata for the materialization API
        for col in preagg.columns:
            columns.append(
                ColumnMetadata(
                    name=col.name,
                    type=col.type,
                    semantic_entity=col.semantic_entity,
                    semantic_type=col.semantic_type,
                ),
            )

    # Build materialization input
    mat_input = PreAggMaterializationInput(
        preagg_id=preagg_id,
        output_table=output_table,
        node=NodeNameVersion(
            name=preagg.node_revision.name,
            version=preagg.node_revision.version,
        ),
        grain=preagg.grain_columns,
        measures=preagg.measures,
        query=preagg.sql,
        columns=columns,
        temporal_partitions=temporal_partitions,
        strategy=preagg.strategy,
        schedule=preagg.schedule,
        lookback_window=preagg.lookback_window,
    )

    # Call query service
    _logger.info(
        "Triggering materialization for preagg_id=%s output_table=%s strategy=%s",
        preagg_id,
        output_table,
        preagg.strategy.value,
    )
    request_headers = dict(request.headers)

    try:
        mat_result = query_service_client.materialize_preagg(
            mat_input,
            request_headers=request_headers,
        )
    except Exception as e:
        _logger.exception(
            "Failed to trigger materialization for preagg_id=%s: %s",
            preagg_id,
            str(e),
        )
        raise DJQueryServiceClientException(
            message=f"Failed to trigger materialization: {e}",
        )

    # Check if materialization was actually scheduled (no URLs means failure)
    if not mat_result.urls:
        raise DJQueryServiceClientException(
            message=(
                "Query service did not return any workflow URLs. "
                "Check query service logs for details."
            ),
        )

    _logger.info(
        "Materialization scheduled for preagg_id=%s, urls=%s",
        preagg_id,
        mat_result.urls,
    )

    # Return pre-agg info with workflow URLs and "running" status
    preagg_info = _preagg_to_info(preagg)
    preagg_info.status = "running"  # Override to reflect that job is running
    preagg_info.workflow_urls = mat_result.urls
    return preagg_info


class UpdatePreAggregationConfigRequest(BaseModel):
    """Request model for updating a pre-aggregation's materialization config."""

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


@router.patch(
    "/preaggs/{preagg_id}/config",
    response_model=PreAggregationInfo,
    name="Update Pre-aggregation Config",
)
async def update_preaggregation_config(
    preagg_id: int,
    data: UpdatePreAggregationConfigRequest,
    *,
    session: AsyncSession = Depends(get_session),
) -> PreAggregationInfo:
    """
    Update the materialization configuration of a single pre-aggregation.

    Use this endpoint to configure individual pre-aggs with different
    strategies, schedules, or lookback windows.
    """
    # Get the pre-aggregation
    stmt = (
        select(PreAggregation)
        .options(
            joinedload(PreAggregation.node_revision),
            joinedload(PreAggregation.availability),
        )
        .where(PreAggregation.id == preagg_id)
    )
    result = await session.execute(stmt)
    preagg = result.scalar_one_or_none()

    if not preagg:
        raise DJDoesNotExistException(f"Pre-aggregation with ID {preagg_id} not found")

    # Update only the fields that are provided
    if data.strategy is not None:
        preagg.strategy = data.strategy
    if data.schedule is not None:
        preagg.schedule = data.schedule
    if data.lookback_window is not None:
        preagg.lookback_window = data.lookback_window

    await session.commit()
    await session.refresh(preagg, ["node_revision", "availability"])

    _logger.info(
        "Updated config for pre-aggregation id=%s strategy=%s schedule=%s lookback=%s",
        preagg_id,
        preagg.strategy,
        preagg.schedule,
        preagg.lookback_window,
    )

    return _preagg_to_info(preagg)


# =============================================================================
# Workflow Management Endpoints
# =============================================================================


@router.post(
    "/preaggs/{preagg_id}/workflow",
    response_model=WorkflowResponse,
    name="Create Scheduled Workflow",
)
async def create_preagg_workflow(
    preagg_id: int,
    data: CreateWorkflowRequest = CreateWorkflowRequest(),
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> WorkflowResponse:
    """
    Create and optionally activate a scheduled workflow for this pre-aggregation.

    This creates the recurring workflow that will materialize the pre-agg
    on the configured schedule. Use this after configuring the pre-agg with
    PATCH /preaggs/{id}/config.

    The workflow URL is stored on the pre-aggregation for future reference.
    """
    # Get the pre-agg with node_revision and its columns/partitions
    stmt = (
        select(PreAggregation)
        .options(
            joinedload(PreAggregation.node_revision).options(
                *NodeRevision.default_load_options(),
            ),
            joinedload(PreAggregation.availability),
        )
        .where(PreAggregation.id == preagg_id)
    )
    result = await session.execute(stmt)
    preagg = result.scalar_one_or_none()

    if not preagg:
        raise DJDoesNotExistException(f"Pre-aggregation with ID {preagg_id} not found")

    # Validate: need strategy and schedule configured
    if not preagg.strategy:
        raise DJInvalidInputException(
            "Pre-aggregation must have a strategy configured. "
            "Use PATCH /preaggs/{id}/config first.",
        )

    if not preagg.schedule:
        raise DJInvalidInputException(
            "Pre-aggregation must have a schedule configured. "
            "Use PATCH /preaggs/{id}/config first.",
        )

    # Build output table name
    node_short = preagg.node_revision.name.replace(".", "_")
    output_table = f"{node_short}_preagg_{preagg.grain_group_hash[:8]}"

    # Common date column patterns (used for output name fallback and type inference)
    DATE_COLUMNS = {
        "dateint",
        "date",
        "date_int",
        "utc_date",
        "ds",
        "dt",
        "day",
        "hour",
    }

    # Build column type map from node revision columns
    col_type_map: dict[str, str] = {}
    if preagg.node_revision and preagg.node_revision.columns:
        for col in preagg.node_revision.columns:
            col_type_map[col.name] = str(col.type) if col.type else "string"

    # Helper to determine column type
    def get_column_type(col_name: str) -> str:
        if col_name in col_type_map:
            return col_type_map[col_name]
        # Date-like columns default to int (yyyyMMdd format)
        return "int" if col_name.lower() in DATE_COLUMNS else "string"

    # Get temporal partition info and build type mappings in one pass
    temporal_partitions = []
    grain_col_short_names = {gc.split(".")[-1] for gc in preagg.grain_columns}

    if preagg.node_revision:
        for temporal_col in preagg.node_revision.temporal_partition_columns():
            source_name = temporal_col.name
            source_type = str(temporal_col.type) if temporal_col.type else "int"
            output_name = source_name  # default

            # Strategy 1: Source name directly in grain
            if source_name in grain_col_short_names:
                output_name = source_name

            # Strategy 2: Linked dimension - find matching grain column
            elif temporal_col.dimension:
                dim_name = temporal_col.dimension.name
                for gc in preagg.grain_columns:
                    if gc.startswith(dim_name + "."):
                        output_name = gc.split(".")[-1]
                        break

            # Strategy 3: Fallback - look for common date columns in grain
            if output_name == source_name and output_name not in grain_col_short_names:
                for date_col in DATE_COLUMNS:
                    if date_col in grain_col_short_names:
                        output_name = date_col
                        break

            # Map output column to source type (for DDL generation)
            if output_name != source_name:
                col_type_map[output_name] = source_type

            _logger.info(
                "Temporal partition: source=%s -> output=%s",
                source_name,
                output_name,
            )

            temporal_partitions.append(
                TemporalPartitionColumn(
                    column_name=output_name,
                    format=temporal_col.partition.format
                    if temporal_col.partition
                    else None,
                    granularity=(
                        str(temporal_col.partition.granularity.value)
                        if temporal_col.partition and temporal_col.partition.granularity
                        else None
                    ),
                ),
            )

    # Build columns metadata from stored V3ColumnMetadata
    columns: list[ColumnMetadata] = []

    if preagg.columns:
        # Convert V3ColumnMetadata to ColumnMetadata for the workflow API
        for col in preagg.columns:
            columns.append(
                ColumnMetadata(
                    name=col.name,
                    type=col.type,
                    semantic_entity=col.semantic_entity,
                    semantic_type=col.semantic_type,
                ),
            )
    else:
        # Fallback for pre-aggs created before columns were stored
        _logger.warning(
            "Pre-agg %s has no stored columns, using fallback types",
            preagg_id,
        )
        added_col_names: set[str] = set()

        for grain_col in preagg.grain_columns:
            col_name = grain_col.split(".")[-1]
            if col_name not in added_col_names:
                columns.append(
                    ColumnMetadata(
                        name=col_name,
                        type=get_column_type(col_name),
                        semantic_entity=grain_col,
                        semantic_type="dimension",
                    ),
                )
                added_col_names.add(col_name)

        # Add temporal partition columns (if not already in grain)
        for tp in temporal_partitions:
            if tp.column_name not in added_col_names:
                columns.append(
                    ColumnMetadata(
                        name=tp.column_name,
                        type=get_column_type(tp.column_name),
                        semantic_type="dimension",
                    ),
                )
                added_col_names.add(tp.column_name)

        for measure in preagg.measures:
            # PreAggMeasure has .name attribute
            columns.append(
                ColumnMetadata(
                    name=measure.name,
                    type="double",  # Fallback type for legacy pre-aggs
                    semantic_type="measure",
                ),
            )

    _logger.info(
        "Building workflow input: columns=%s, temporal_partitions=%s",
        [c.name for c in columns],
        [tp.column_name for tp in temporal_partitions],
    )

    # Build workflow input
    workflow_input = WorkflowInput(
        preagg_id=preagg_id,
        output_table=output_table,
        node=NodeNameVersion(
            name=preagg.node_revision.name,
            version=preagg.node_revision.version,
        ),
        grain=preagg.grain_columns,
        measures=preagg.measures,
        query=preagg.sql,
        columns=columns,
        temporal_partitions=temporal_partitions,
        strategy=preagg.strategy,
        schedule=preagg.schedule,
        lookback_window=preagg.lookback_window,
        activate=data.activate,
    )

    # Call query service
    _logger.info(
        "Creating workflow for preagg_id=%s output_table=%s activate=%s",
        preagg_id,
        output_table,
        data.activate,
    )
    request_headers = dict(request.headers)

    try:
        workflow_result = query_service_client.create_preagg_workflow(
            workflow_input,
            request_headers=request_headers,
        )
    except Exception as e:
        _logger.exception(
            "Failed to create workflow for preagg_id=%s: %s",
            preagg_id,
            str(e),
        )
        raise DJQueryServiceClientException(
            message=f"Failed to create workflow: {e}",
        )

    # Update pre-agg with workflow URL
    preagg.scheduled_workflow_url = workflow_result.get("workflow_url")
    preagg.workflow_status = "active" if data.activate else "paused"
    await session.commit()

    _logger.info(
        "Created workflow for preagg_id=%s workflow_url=%s status=%s",
        preagg_id,
        preagg.scheduled_workflow_url,
        preagg.workflow_status,
    )

    return WorkflowResponse(
        workflow_url=preagg.scheduled_workflow_url,
        status=preagg.workflow_status or "none",
        message="Workflow created successfully",
    )


@router.delete(
    "/preaggs/{preagg_id}/workflow",
    response_model=WorkflowResponse,
    name="Deactivate Scheduled Workflow",
)
async def delete_preagg_workflow(
    preagg_id: int,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> WorkflowResponse:
    """
    Deactivate (pause) the scheduled workflow for this pre-aggregation.

    The workflow definition is kept but will not run on schedule.
    Call POST /preaggs/{id}/workflow to re-activate.
    """
    # Get the pre-agg
    stmt = (
        select(PreAggregation)
        .options(
            joinedload(PreAggregation.node_revision),
            joinedload(PreAggregation.availability),
        )
        .where(PreAggregation.id == preagg_id)
    )
    result = await session.execute(stmt)
    preagg = result.scalar_one_or_none()

    if not preagg:
        raise DJDoesNotExistException(f"Pre-aggregation with ID {preagg_id} not found")

    if not preagg.scheduled_workflow_url:
        return WorkflowResponse(
            workflow_url=None,
            status="none",
            message="No workflow exists for this pre-aggregation",
        )

    # Call query service to deactivate
    request_headers = dict(request.headers)
    try:
        query_service_client.deactivate_preagg_workflow(
            preagg_id,
            request_headers=request_headers,
        )
    except Exception as e:
        _logger.exception(
            "Failed to deactivate workflow for preagg_id=%s: %s",
            preagg_id,
            str(e),
        )
        raise DJQueryServiceClientException(
            message=f"Failed to deactivate workflow: {e}",
        )

    # Update status
    preagg.workflow_status = "paused"
    await session.commit()

    _logger.info(
        "Deactivated workflow for preagg_id=%s",
        preagg_id,
    )

    return WorkflowResponse(
        workflow_url=preagg.scheduled_workflow_url,
        status="paused",
        message="Workflow deactivated successfully",
    )


# =============================================================================
# Backfill & Run Endpoints
# =============================================================================


@router.post(
    "/preaggs/{preagg_id}/backfill",
    response_model=BackfillResponse,
    name="Run Backfill",
)
async def run_preagg_backfill(
    preagg_id: int,
    data: BackfillRequest,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> BackfillResponse:
    """
    Run a backfill for the specified date range.

    This triggers a one-time job to process historical data from start_date
    to end_date. The workflow must already exist (created via POST /workflow).

    Use this to:
    - Initially populate a new pre-aggregation
    - Re-process data after a bug fix
    - Catch up on missed partitions
    """
    # Get the pre-agg (minimal load - just need ID, node_revision name, and workflow URL)
    stmt = (
        select(PreAggregation)
        .options(joinedload(PreAggregation.node_revision))
        .where(PreAggregation.id == preagg_id)
    )
    result = await session.execute(stmt)
    preagg = result.scalar_one_or_none()

    if not preagg:
        raise DJDoesNotExistException(f"Pre-aggregation with ID {preagg_id} not found")

    # Validate: workflow must exist
    if not preagg.scheduled_workflow_url:
        raise DJInvalidInputException(
            "Pre-aggregation must have a workflow created first. "
            "Use POST /preaggs/{id}/workflow first.",
        )

    # Default end_date to today
    end_date = data.end_date or date_type.today()

    # Compute output table (Query Service derives workflow name from this)
    output_table = _compute_output_table(
        preagg.node_revision.name,
        preagg.grain_group_hash,
    )

    # Build simplified backfill input
    backfill_input = BackfillInput(
        preagg_id=preagg_id,
        output_table=output_table,
        start_date=data.start_date,
        end_date=end_date,
    )

    # Call query service
    _logger.info(
        "Running backfill for preagg_id=%s from %s to %s output_table=%s",
        preagg_id,
        data.start_date,
        end_date,
        output_table,
    )
    request_headers = dict(request.headers)

    try:
        backfill_result = query_service_client.run_preagg_backfill(
            backfill_input,
            request_headers=request_headers,
        )
    except Exception as e:
        _logger.exception(
            "Failed to run backfill for preagg_id=%s: %s",
            preagg_id,
            str(e),
        )
        raise DJQueryServiceClientException(
            message=f"Failed to run backfill: {e}",
        )

    job_url = backfill_result.get("job_url", "")
    _logger.info(
        "Started backfill for preagg_id=%s job_url=%s",
        preagg_id,
        job_url,
    )

    return BackfillResponse(
        job_url=job_url,
        start_date=data.start_date,
        end_date=end_date,
        status="running",
    )


@router.post(
    "/preaggs/{preagg_id}/availability/",
    response_model=PreAggregationInfo,
    name="Update Pre-aggregation Availability",
)
async def update_preaggregation_availability(
    preagg_id: int,
    data: UpdatePreAggregationAvailabilityRequest,
    *,
    session: AsyncSession = Depends(get_session),
) -> PreAggregationInfo:
    """
    Update the availability state of a pre-aggregation (Flow B).

    Call this endpoint after your query service has materialized the data.
    The availability state includes:
    - catalog/schema/table: Where the materialized data lives
    - valid_through_ts: Timestamp through which data is valid
    - min/max_temporal_partition: Temporal partition range (high-water mark)
    - partitions: Detailed partition-level availability

    This is the callback endpoint for external query services to report
    materialization status back to DJ.
    """
    _logger.info(
        "Updating availability for pre-aggregation id=%s table=%s.%s.%s",
        preagg_id,
        data.catalog,
        data.schema_,
        data.table,
    )

    # Get the pre-aggregation
    stmt = (
        select(PreAggregation)
        .options(
            joinedload(PreAggregation.node_revision),
            joinedload(PreAggregation.availability),
        )
        .where(PreAggregation.id == preagg_id)
    )
    result = await session.execute(stmt)
    preagg = result.scalar_one_or_none()

    if not preagg:
        raise DJDoesNotExistException(f"Pre-aggregation with ID {preagg_id} not found")

    # Create or update availability state
    old_availability = preagg.availability

    if (
        old_availability
        and old_availability.catalog == data.catalog
        and old_availability.schema_ == data.schema_
        and old_availability.table == data.table
    ):
        # Update existing availability - merge temporal ranges
        if data.min_temporal_partition:
            if (
                not old_availability.min_temporal_partition
                or data.min_temporal_partition < old_availability.min_temporal_partition
            ):
                old_availability.min_temporal_partition = [
                    str(p) for p in data.min_temporal_partition
                ]

        if data.max_temporal_partition:
            if (
                not old_availability.max_temporal_partition
                or data.max_temporal_partition > old_availability.max_temporal_partition
            ):
                old_availability.max_temporal_partition = [
                    str(p) for p in data.max_temporal_partition
                ]

        old_availability.valid_through_ts = data.valid_through_ts
        old_availability.url = data.url
        old_availability.links = data.links or {}

        if data.partitions:
            old_availability.partitions = [
                p.model_dump() if hasattr(p, "model_dump") else p
                for p in data.partitions
            ]

        _logger.info(
            "Updated existing availability for pre-aggregation id=%s",
            preagg_id,
        )
    else:
        # Create new availability state
        new_availability = AvailabilityState(
            catalog=data.catalog,
            schema_=data.schema_,
            table=data.table,
            valid_through_ts=data.valid_through_ts,
            url=data.url,
            links=data.links or {},
            categorical_partitions=data.categorical_partitions or [],
            temporal_partitions=data.temporal_partitions or [],
            min_temporal_partition=[str(p) for p in data.min_temporal_partition or []],
            max_temporal_partition=[str(p) for p in data.max_temporal_partition or []],
            partitions=[
                p.model_dump() if hasattr(p, "model_dump") else p
                for p in (data.partitions or [])
            ],
        )
        session.add(new_availability)
        await session.flush()  # Get the ID

        preagg.availability_id = new_availability.id
        _logger.info(
            "Created new availability (id=%s) for pre-aggregation id=%s",
            new_availability.id,
            preagg_id,
        )

    await session.commit()
    await session.refresh(preagg, ["node_revision", "availability"])

    return _preagg_to_info(preagg)
