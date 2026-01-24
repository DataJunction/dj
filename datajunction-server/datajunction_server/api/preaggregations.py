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
from sqlalchemy.orm import joinedload, load_only, selectinload

from datajunction_server.construction.build_v3.builder import build_measures_sql
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.column import Column
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.measure import FrozenMeasure
from datajunction_server.database.preaggregation import (
    PreAggregation,
    VALID_PREAGG_STRATEGIES,
    compute_grain_group_hash,
    compute_expression_hash,
)
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJInvalidInputException,
    DJQueryServiceClientException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.node_type import NodeNameVersion, NodeType
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.preaggregation import (
    BackfillRequest,
    BackfillInput,
    BackfillResponse,
    BulkDeactivateWorkflowsResponse,
    DeactivatedWorkflowInfo,
    GrainMode,
    DEFAULT_SCHEDULE,
    PlanPreAggregationsRequest,
    PlanPreAggregationsResponse,
    PreAggregationInfo,
    PreAggregationListResponse,
    PreAggMaterializationInput,
    UpdatePreAggregationAvailabilityRequest,
    WorkflowResponse,
    WorkflowStatus,
    WorkflowUrl,
)
from datajunction_server.construction.build_v3.preagg_matcher import (
    get_temporal_partitions,
)
from datajunction_server.models.decompose import MetricRef, PreAggMeasure
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.query import ColumnMetadata, V3ColumnMetadata
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.dag import get_upstream_nodes
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


async def _get_upstream_source_tables(
    session: AsyncSession,
    node_name: str,
) -> List[str]:
    """
    Get upstream source table names for a node using DJ's lineage graph.

    Traverses the node's upstream dependencies to find all source nodes,
    then returns their fully qualified table names (catalog.schema.table).

    Args:
        session: Database session
        node_name: The node name to find upstream sources for

    Returns:
        List of fully qualified table names (e.g., ['catalog.schema.table'])
    """
    try:
        # Get all upstream source nodes with catalog info eagerly loaded
        upstream_sources = await get_upstream_nodes(
            session,
            node_name,
            node_type=NodeType.SOURCE,
            options=[
                joinedload(Node.current).joinedload(NodeRevision.catalog),
            ],
        )

        upstream_tables = []
        for node in upstream_sources:
            rev = node.current
            if rev and rev.catalog:  # pragma: no branch
                # Build fully qualified table name
                parts = [rev.catalog.name]
                if rev.schema_:  # pragma: no branch
                    parts.append(rev.schema_)
                if rev.table:  # pragma: no branch
                    parts.append(rev.table)
                upstream_tables.append(".".join(parts))

        return list(set(upstream_tables))  # Remove duplicates
    except Exception as e:  # pragma: no cover
        _logger.warning("Failed to get upstream source tables for %s: %s", node_name, e)
        return []


async def _preagg_to_info(
    preagg: PreAggregation,
    session: AsyncSession,
) -> PreAggregationInfo:
    """Convert a PreAggregation ORM object to a PreAggregationInfo response model."""
    # Look up related metrics from FrozenMeasure relationships for each measure
    measures_with_metrics: list[PreAggMeasure] = []
    all_related_metrics: set[str] = set()

    # Fetch all frozen measures in a single query to avoid N+1
    measure_names = [measure.name for measure in preagg.measures or []]
    frozen_measures = await FrozenMeasure.get_by_names(session, measure_names)
    frozen_measures_map = {fm.name: fm for fm in frozen_measures}

    for measure in preagg.measures or []:
        # Find which metrics use this measure
        measure_metrics: list[MetricRef] = []
        frozen = frozen_measures_map.get(measure.name)
        if frozen:
            for nr in frozen.used_by_node_revisions:
                if nr.type == NodeType.METRIC:  # pragma: no branch
                    measure_metrics.append(
                        MetricRef(name=nr.name, display_name=nr.display_name),
                    )
                    all_related_metrics.add(nr.name)

        # Create new PreAggMeasure with used_by_metrics populated
        measures_with_metrics.append(
            PreAggMeasure(
                name=measure.name,
                expression=measure.expression,
                aggregation=measure.aggregation,
                merge=measure.merge,
                rule=measure.rule,
                expr_hash=measure.expr_hash,
                used_by_metrics=sorted(measure_metrics, key=lambda m: m.name)
                if measure_metrics
                else None,
            ),
        )

    return PreAggregationInfo(
        id=preagg.id,
        node_revision_id=preagg.node_revision_id,
        node_name=preagg.node_revision.name,
        node_version=preagg.node_revision.version,
        grain_columns=preagg.grain_columns,
        measures=measures_with_metrics,
        columns=preagg.columns,
        sql=preagg.sql,
        grain_group_hash=preagg.grain_group_hash,
        strategy=preagg.strategy,
        schedule=preagg.schedule,
        lookback_window=preagg.lookback_window,
        workflow_urls=preagg.workflow_urls,  # PydanticListType handles conversion
        workflow_status=preagg.workflow_status,
        status=preagg.status,
        materialized_table_ref=preagg.materialized_table_ref,
        max_partition=preagg.max_partition,
        related_metrics=sorted(all_related_metrics) if all_related_metrics else None,
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
        description="Comma-separated grain columns to match",
    ),
    grain_mode: GrainMode = Query(
        default=GrainMode.EXACT,
        description="Grain matching mode: 'exact' (default) or 'superset' (pre-agg contains all requested + maybe more)",
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
    include_stale: bool = Query(
        default=False,
        description="Include pre-aggs from older node versions (stale)",
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
    - grain: Comma-separated grain columns to match
    - grain_mode: 'exact' (default) requires exact match, 'superset' finds pre-aggs
      that contain all requested columns (and possibly more - finer grain)
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
        # Minimal load: only need node.id and node.current.id for filtering
        node = await Node.get_by_name(
            session,
            node_name,
            options=[
                load_only(Node.id),
                joinedload(Node.current).load_only(NodeRevision.id),
            ],
        )
        if not node:
            raise DJDoesNotExistException(f"Node '{node_name}' not found")

        if node_version:
            # Find specific revision using async-safe query instead of iterating node.revisions
            revision_stmt = select(NodeRevision).where(
                NodeRevision.node_id == node.id,
                NodeRevision.version == node_version,
            )
            revision_result = await session.execute(revision_stmt)
            target_revision = revision_result.scalar_one_or_none()
            if not target_revision:
                raise DJDoesNotExistException(
                    f"Version '{node_version}' not found for node '{node_name}'",
                )
            stmt = stmt.where(PreAggregation.node_revision_id == target_revision.id)
        elif include_stale:
            # Include all revisions for this node
            all_revisions_stmt = select(NodeRevision.id).where(
                NodeRevision.node_id == node.id,
            )
            stmt = stmt.where(
                PreAggregation.node_revision_id.in_(all_revisions_stmt),
            )
        else:
            # Use latest version only (default)
            stmt = stmt.where(PreAggregation.node_revision_id == node.current.id)

    # Filter by grain_group_hash (direct lookup)
    if grain_group_hash:
        stmt = stmt.where(PreAggregation.grain_group_hash == grain_group_hash)

    # Parse grain columns for filtering
    grain_cols: Optional[List[str]] = None
    if grain:
        grain_cols = [g.strip().lower() for g in grain.split(",")]

    # Execute query for total count (before pagination)
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_result = await session.execute(count_stmt)
    total = total_result.scalar() or 0

    # Apply pagination
    stmt = stmt.offset(offset).limit(limit)

    # Execute main query
    result = await session.execute(stmt)
    preaggs = list(result.scalars().unique().all())

    _logger.info(
        "list_preaggs: found %d pre-aggs before filtering (node_name=%s)",
        len(preaggs),
        node_name,
    )

    # Post-filter by grain columns (compare full dimension names, case-insensitive)
    if grain_cols:
        requested_grain = set(g.lower() for g in grain_cols)
        _logger.info(
            "list_preaggs: filtering by grain=%s, mode=%s",
            requested_grain,
            grain_mode,
        )

        before_count = len(preaggs)
        if grain_mode == GrainMode.EXACT:
            # Exact match: pre-agg grain must match exactly
            filtered = []
            for p in preaggs:
                preagg_grain = set(col.lower() for col in p.grain_columns)
                matches = preagg_grain == requested_grain
                _logger.info(
                    "  preagg %s: grain=%s, matches=%s",
                    p.id,
                    preagg_grain,
                    matches,
                )
                if matches:
                    filtered.append(p)
            preaggs = filtered
        else:  # superset
            # Superset match: pre-agg grain must contain ALL requested columns
            # (pre-agg can have more columns = finer grain)
            filtered = []
            for p in preaggs:
                preagg_grain = set(col.lower() for col in p.grain_columns)
                # Check if requested_grain is subset of preagg_grain
                matches = requested_grain <= preagg_grain
                missing = requested_grain - preagg_grain
                _logger.info(
                    "  preagg %s: grain=%s, requested=%s, missing=%s, matches=%s",
                    p.id,
                    preagg_grain,
                    requested_grain,
                    missing,
                    matches,
                )
                if matches:
                    filtered.append(p)
            preaggs = filtered

        _logger.info(
            "list_preaggs: grain filter reduced %d -> %d",
            before_count,
            len(preaggs),
        )

    # Post-filter by measures (superset match - pre-agg must contain ALL by name)
    if measures:
        measure_list = [m.strip().lower() for m in measures.split(",")]
        needed = set(measure_list)
        _logger.info("list_preaggs: filtering by measures=%s", needed)

        before_count = len(preaggs)
        filtered = []
        for p in preaggs:
            preagg_measures = {
                m.name.lower() if hasattr(m, "name") else m.get("name", "").lower()
                for m in p.measures
            }
            matches = needed <= preagg_measures
            missing = needed - preagg_measures
            _logger.info(
                "  preagg %s: measures=%s, missing=%s, matches=%s",
                p.id,
                preagg_measures,
                missing,
                matches,
            )
            if matches:
                filtered.append(p)  # pragma: no cover
        preaggs = filtered

        _logger.info(
            "list_preaggs: measures filter reduced %d -> %d",
            before_count,
            len(preaggs),
        )

    # Post-filter by status
    if status:
        if status not in ("pending", "active"):
            raise DJDoesNotExistException(
                f"Invalid status '{status}'. Must be 'pending' or 'active'",
            )
        preaggs = [p for p in preaggs if p.status == status]

    # Convert to response models (with related metrics lookup)
    items = [await _preagg_to_info(p, session) for p in preaggs]

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

    return await _preagg_to_info(preagg, session)


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
        if not parent_node or not parent_node.current:  # pragma: no cover
            _logger.warning(
                "Parent node %s not found in context, skipping grain group",
                grain_group.parent_name,
            )
            continue

        node_revision_id = parent_node.current.id

        # Validate: INCREMENTAL_TIME requires temporal partition columns
        if data.strategy == MaterializationStrategy.INCREMENTAL_TIME:
            source_temporal_cols = parent_node.current.temporal_partition_columns()
            if not source_temporal_cols:  # pragma: no branch
                raise DJInvalidInputException(
                    message=(
                        f"INCREMENTAL_TIME strategy requires the upstream node "
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
        # Use component_aliases to get readable names when available
        # (metrics with 1 component use readable names like "total_revenue",
        # metrics with multiple components use hashed names for uniqueness)
        measures = [
            PreAggMeasure(
                **{
                    **component.model_dump(),
                    "name": grain_group.component_aliases.get(
                        component.name,
                        component.name,
                    ),
                },
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
                semantic_name=col.semantic_name,
            )
            for col in grain_group.columns
        ]

        # Compute grain_group_hash for lookup
        grain_group_hash = compute_grain_group_hash(node_revision_id, grain_columns)

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
            # if data.strategy:
            existing.strategy = data.strategy or existing.strategy
            # if data.schedule:
            existing.schedule = data.schedule or existing.schedule
            # if data.lookback_window:
            existing.lookback_window = data.lookback_window or existing.lookback_window
            # Update SQL and columns in case they changed
            existing.sql = sql
            existing.columns = columns
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
        preaggs=[await _preagg_to_info(p, session) for p in loaded_preaggs],
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
    Create/update a scheduled workflow for this pre-aggregation.

    This creates a recurring workflow that materializes the pre-agg on schedule.
    Call this endpoint to:
    - Initially set up materialization for a pre-agg
    - Refresh/recreate the workflow after config changes

    The workflow runs on the configured schedule (default: daily at midnight).
    The query service will callback to POST /preaggs/{id}/availability/ when
    each run completes.

    For user-managed materialization, use the SQL from GET /preaggs/{id}
    and call POST /preaggs/{id}/availability/ when done.
    """
    # Get the pre-agg with node_revision and its columns/partitions/dimension_links
    stmt = (
        select(PreAggregation)
        .options(
            joinedload(PreAggregation.node_revision).options(
                load_only(NodeRevision.name, NodeRevision.version),
                selectinload(NodeRevision.columns).options(
                    load_only(Column.name, Column.type),
                    joinedload(Column.partition),
                    joinedload(Column.dimension).load_only(Node.name),
                ),
                selectinload(NodeRevision.dimension_links).options(
                    joinedload(DimensionLink.dimension).load_only(Node.name),
                ),
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
            "Use PATCH /preaggs/{id}/config or set strategy in POST /preaggs/plan.",
        )

    # Check if source node has temporal partition columns (needed for INCREMENTAL_TIME)
    if preagg.strategy == MaterializationStrategy.INCREMENTAL_TIME:
        source_temporal_cols = preagg.node_revision.temporal_partition_columns()
        if not source_temporal_cols:  # pragma: no branch
            raise DJInvalidInputException(
                message=(
                    f"INCREMENTAL_TIME strategy requires the source node "
                    f"'{preagg.node_revision.name}' to have temporal partition columns. "
                    f"Either add temporal partition columns to the source node or use "
                    f"FULL strategy instead."
                ),
            )

    # Build output table name
    node_short = preagg.node_revision.name.replace(".", "_")
    output_table = f"{node_short}_preagg_{preagg.grain_group_hash[:8]}"

    # Get temporal partition info
    temporal_partitions = get_temporal_partitions(preagg)

    # Build columns metadata from stored V3ColumnMetadata
    columns: list[ColumnMetadata] = []
    column_names: set[str] = set()

    if preagg.columns:
        # Convert V3ColumnMetadata to ColumnMetadata for the materialization API
        for col in preagg.columns:  # pragma: no cover
            columns.append(
                ColumnMetadata(
                    name=col.name,
                    type=col.type,
                    semantic_entity=col.semantic_name,
                    semantic_type=col.semantic_type,
                ),
            )
            column_names.add(col.name)

    # Ensure temporal partition columns are included in columns list
    # (they may not be if partition column wasn't selected as a dimension)
    for tp in temporal_partitions:
        if tp.column_name not in column_names:  # pragma: no branch
            columns.append(
                ColumnMetadata(
                    name=tp.column_name,
                    type=tp.column_type or "int",
                ),
            )
            column_names.add(tp.column_name)
            _logger.info(
                "Added partition column to columns list: %s (type=%s)",
                tp.column_name,
                tp.column_type,
            )

    # Get upstream source tables using DJ's node lineage
    upstream_tables = await _get_upstream_source_tables(
        session,
        preagg.node_revision.name,
    )

    # Use schedule from pre-agg or default to daily
    schedule = preagg.schedule or DEFAULT_SCHEDULE

    _logger.info(
        "Building materialization input: columns=%s, temporal_partitions=%s, "
        "upstream_tables=%s, schedule=%s",
        [c.name for c in columns],
        [tp.column_name for tp in temporal_partitions],
        upstream_tables,
        schedule,
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
        upstream_tables=upstream_tables,
        temporal_partitions=temporal_partitions,
        strategy=preagg.strategy,
        schedule=schedule,
        lookback_window=preagg.lookback_window,
        activate=True,
    )

    # Call query service
    _logger.info(
        "Creating workflow for preagg_id=%s output_table=%s strategy=%s schedule=%s",
        preagg_id,
        output_table,
        preagg.strategy.value,
        schedule,
    )
    request_headers = dict(request.headers)

    try:
        mat_result = query_service_client.materialize_preagg(
            mat_input,
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

    # Store labeled workflow URLs from query service response
    # Query service returns 'workflow_urls' list of {label, url} objects
    # PydanticListType handles serialization to/from WorkflowUrl objects
    workflow_urls_data = mat_result.get("workflow_urls", [])
    if workflow_urls_data:
        preagg.workflow_urls = [
            WorkflowUrl(label=wf["label"], url=wf["url"]) for wf in workflow_urls_data
        ]
    else:
        # Fallback: convert legacy 'urls' list to labeled format
        urls = mat_result.get("urls", [])
        if urls:  # pragma: no branch
            labeled_urls: list[WorkflowUrl] = []
            for url in urls:
                if ".main" in url or "scheduled" in url.lower():
                    labeled_urls.append(WorkflowUrl(label="scheduled", url=url))
                elif ".backfill" in url or "adhoc" in url.lower():
                    labeled_urls.append(WorkflowUrl(label="backfill", url=url))
                else:
                    labeled_urls.append(WorkflowUrl(label="workflow", url=url))
            preagg.workflow_urls = labeled_urls

    preagg.workflow_status = WorkflowStatus.ACTIVE
    # Also update schedule if it wasn't set (using default)
    if not preagg.schedule:
        preagg.schedule = schedule
    await session.commit()

    _logger.info(
        "Created workflow for preagg_id=%s, workflow_urls=%s, status=active",
        preagg_id,
        preagg.workflow_urls,
    )

    # Return pre-agg info with workflow URLs
    await session.refresh(preagg, ["node_revision", "availability"])
    return await _preagg_to_info(preagg, session)


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

    return await _preagg_to_info(preagg, session)


# =============================================================================
# Workflow Management Endpoints
# =============================================================================


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
    Call POST /preaggs/{id}/materialize to re-activate.
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

    if not preagg.workflow_urls:
        return WorkflowResponse(
            workflow_url=None,
            status="none",
            message="No workflow exists for this pre-aggregation",
        )

    # Compute output_table - the resource identifier that Query Service uses
    output_table = _compute_output_table(
        preagg.node_revision.name,
        preagg.grain_group_hash,
    )

    # Call query service to deactivate using the resource identifier (output_table)
    # Query Service owns the workflow naming patterns and reconstructs them from output_table
    request_headers = dict(request.headers)
    try:
        query_service_client.deactivate_preagg_workflow(
            output_table,
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

    # Clear all materialization config and workflow state - clean slate for reconfiguration
    preagg.strategy = None
    preagg.schedule = None
    preagg.lookback_window = None
    preagg.workflow_urls = None
    preagg.workflow_status = None
    await session.commit()

    _logger.info(
        "Deactivated workflow and cleared config for preagg_id=%s",
        preagg_id,
    )

    return WorkflowResponse(
        workflow_url=None,
        status="none",
        message="Workflow deactivated and configuration cleared. You can reconfigure materialization.",
    )


@router.delete(
    "/preaggs/workflows",
    response_model=BulkDeactivateWorkflowsResponse,
    name="Bulk Deactivate Workflows",
)
async def bulk_deactivate_preagg_workflows(
    node_name: str = Query(
        description="Node name to deactivate workflows for (required)",
    ),
    stale_only: bool = Query(
        default=False,
        description="If true, only deactivate workflows for stale pre-aggs "
        "(pre-aggs built for non-current node versions)",
    ),
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> BulkDeactivateWorkflowsResponse:
    """
    Bulk deactivate workflows for pre-aggregations of a node.

    This is useful for cleaning up stale pre-aggregations after a node
    has been updated. When stale_only=true, only deactivates workflows
    for pre-aggs that were built for older node versions.

    Staleness is determined by comparing the pre-agg's node_revision_id
    to the node's current revision.
    """
    # Get the node and its current revision
    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            load_only(Node.id),
            joinedload(Node.current).load_only(NodeRevision.id),
        ],
    )
    if not node:
        raise DJDoesNotExistException(f"Node '{node_name}' not found")

    current_revision_id = node.current.id if node.current else None

    # Build query for pre-aggs with active workflows
    stmt = (
        select(PreAggregation)
        .options(joinedload(PreAggregation.node_revision))
        .join(PreAggregation.node_revision)
        .where(
            NodeRevision.node_id == node.id,
            PreAggregation.workflow_status == WorkflowStatus.ACTIVE,
        )
    )

    # If stale_only, filter to non-current revisions
    if stale_only and current_revision_id:
        stmt = stmt.where(PreAggregation.node_revision_id != current_revision_id)

    result = await session.execute(stmt)
    preaggs = result.scalars().all()

    if not preaggs:
        return BulkDeactivateWorkflowsResponse(
            deactivated_count=0,
            deactivated=[],
            skipped_count=0,
            message="No active workflows found matching criteria",
        )

    deactivated = []
    skipped_count = 0
    request_headers = dict(request.headers)

    for preagg in preaggs:
        if not preagg.workflow_urls:  # pragma: no cover
            skipped_count += 1
            continue

        # Compute output_table for workflow identification
        output_table = _compute_output_table(
            preagg.node_revision.name,
            preagg.grain_group_hash,
        )

        # Extract workflow name from URLs if available
        workflow_name = None
        if preagg.workflow_urls:  # pragma: no branch
            for wf_url in preagg.workflow_urls:  # pragma: no branch
                if (
                    hasattr(wf_url, "label") and wf_url.label == "scheduled"
                ):  # pragma: no branch
                    # Extract workflow name from URL path
                    workflow_name = wf_url.url.split("/")[-1] if wf_url.url else None
                    break

        try:
            query_service_client.deactivate_preagg_workflow(
                output_table,
                request_headers=request_headers,
            )

            # Clear workflow state
            preagg.strategy = None
            preagg.schedule = None
            preagg.lookback_window = None
            preagg.workflow_urls = None
            preagg.workflow_status = None

            deactivated.append(
                DeactivatedWorkflowInfo(
                    id=preagg.id,
                    workflow_name=workflow_name,
                ),
            )

            _logger.info(
                "Bulk deactivate: deactivated workflow for preagg_id=%s",
                preagg.id,
            )
        except Exception as e:  # pragma: no cover
            _logger.warning(
                "Bulk deactivate: failed to deactivate workflow for preagg_id=%s: %s",
                preagg.id,
                str(e),
            )
            # Continue with other pre-aggs even if one fails

    await session.commit()

    return BulkDeactivateWorkflowsResponse(
        deactivated_count=len(deactivated),
        deactivated=deactivated,
        skipped_count=skipped_count,
        message=f"Deactivated {len(deactivated)} workflow(s) for node '{node_name}'"
        + (" (stale only)" if stale_only else ""),
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
    if not preagg.workflow_urls:
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
        node_name=preagg.node_revision.name,
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
            if (  # pragma: no branch
                not old_availability.min_temporal_partition
                or data.min_temporal_partition < old_availability.min_temporal_partition
            ):
                old_availability.min_temporal_partition = [
                    str(p) for p in data.min_temporal_partition
                ]

        if data.max_temporal_partition:
            if (  # pragma: no branch
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
            old_availability.partitions = [  # pragma: no cover
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

    return await _preagg_to_info(preagg, session)
