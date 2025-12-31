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

from fastapi import Depends, Query, Request
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.construction.build_v3.builder import build_measures_sql
from datajunction_server.database import Node
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.preaggregation import (
    PreAggregation,
    VALID_PREAGG_STRATEGIES,
    compute_grain_group_hash,
    compute_expression_hash,
)
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJInvalidInputException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.preaggregation import (
    PlanPreAggregationsRequest,
    PlanPreAggregationsResponse,
    PreAggregationInfo,
    PreAggregationListResponse,
    UpdatePreAggregationAvailabilityRequest,
)
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import get_query_service_client, get_session

_logger = logging.getLogger(__name__)
router = SecureAPIRouter(tags=["preaggregations"])


def _preagg_to_info(preagg: PreAggregation) -> PreAggregationInfo:
    """Convert a PreAggregation ORM object to a PreAggregationInfo response model."""
    return PreAggregationInfo(
        id=preagg.id,
        node_revision_id=preagg.node_revision_id,
        node_name=preagg.node_revision.name,
        node_version=preagg.node_revision.version,
        grain_columns=preagg.grain_columns,
        measures=preagg.measures,
        sql=preagg.sql,
        grain_group_hash=preagg.grain_group_hash,
        strategy=preagg.strategy,
        schedule=preagg.schedule,
        lookback_window=preagg.lookback_window,
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
    measures_result = await build_measures_sql(
        session=session,
        metrics=data.metrics,
        dimensions=data.dimensions,
        filters=data.filters,
        dialect=Dialect.SPARK,
        use_materialized=False,
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

        # Convert grain to fully qualified dimension references
        # The grain_group.grain contains column aliases, we need the full refs
        # Use the requested dimensions that correspond to these grain columns
        grain_columns = list(measures_result.requested_dimensions)

        # Convert MetricComponents to dict format with expr_hash
        measures_dicts = []
        for component in grain_group.components:
            measure_dict = component.model_dump()
            measure_dict["expr_hash"] = compute_expression_hash(component.expression)
            measures_dicts.append(measure_dict)

        # Get the SQL for this grain group
        sql = grain_group.sql

        # Compute grain_group_hash for lookup
        grain_group_hash = compute_grain_group_hash(node_revision_id, grain_columns)

        # Check if a matching pre-agg already exists
        existing = await PreAggregation.find_matching(
            session=session,
            node_revision_id=node_revision_id,
            grain_columns=grain_columns,
            measure_expr_hashes={m["expr_hash"] for m in measures_dicts},
        )

        if existing:
            _logger.info(
                "Found existing pre-agg id=%s for grain_group_hash=%s",
                existing.id,
                grain_group_hash,
            )
            created_preaggs.append(existing)
        else:
            # Create new pre-aggregation
            preagg = PreAggregation(
                node_revision_id=node_revision_id,
                grain_columns=grain_columns,
                measures=measures_dicts,
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
                len(measures_dicts),
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
    from datajunction_server.database.node import NodeRevision
    from datajunction_server.models.node_type import NodeNameVersion
    from datajunction_server.models.preaggregation import PreAggMaterializationInput
    from datajunction_server.models.query import ColumnMetadata

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

    # Derive output table name: {node_short}__preagg_{hash[:8]}
    node_short = preagg.node_revision.name.split(".")[-1]
    output_table = f"{node_short}__preagg_{preagg.grain_group_hash[:8]}"

    # Build temporal partition columns from source node (for incremental)
    # Supports multi-column partitions (e.g., dateint + hour for hourly)
    from datajunction_server.models.preaggregation import TemporalPartitionColumn

    temporal_partitions: list[TemporalPartitionColumn] = []
    for temporal_col in preagg.node_revision.temporal_partition_columns():
        temporal_partitions.append(
            TemporalPartitionColumn(
                column_name=temporal_col.name,
                format=temporal_col.partition.format_ if temporal_col.partition else None,
                granularity=(
                    temporal_col.partition.granularity if temporal_col.partition else None
                ),
                expression=(
                    temporal_col.partition.expression if temporal_col.partition else None
                ),
            )
        )

    # Build columns metadata from grain + measures
    columns: list[ColumnMetadata] = []

    # Add grain columns
    for grain_col in preagg.grain_columns:
        # grain_col is fully qualified like "default.date_dim.date_id"
        col_name = grain_col.split(".")[-1]
        columns.append(
            ColumnMetadata(
                name=col_name,
                type="string",  # Simplified; could derive from node columns
                semantic_entity=grain_col,
                semantic_type="dimension",
            )
        )

    # Add measure columns
    for measure in preagg.measures:
        columns.append(
            ColumnMetadata(
                name=measure.get("name", ""),
                type="double",  # Simplified; could derive from expression
                semantic_type="measure",
            )
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
    mat_result = query_service_client.materialize_preagg(
        mat_input,
        request_headers=request_headers,
    )

    _logger.info(
        "Materialization scheduled for preagg_id=%s, urls=%s",
        preagg_id,
        mat_result.urls,
    )

    # Return pre-agg info (status still "pending" until query service callbacks)
    return _preagg_to_info(preagg)


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
