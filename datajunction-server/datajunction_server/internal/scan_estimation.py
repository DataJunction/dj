"""
Scan estimation for query cost analysis.

Estimates the amount of data that will be scanned when executing a query
by looking up table size metadata from AvailabilityState records.
"""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.types import BuildContext, GrainGroupSQL
from datajunction_server.database.availabilitystate import (
    AvailabilityState,
    NodeAvailabilityState,
)
from datajunction_server.models.sql import ScanEstimate, SourceScanInfo


async def calculate_scan_estimate(
    session: AsyncSession,
    grain_group: GrainGroupSQL,
    ctx: BuildContext,
) -> Optional[ScanEstimate]:
    """
    Calculate scan estimate for a single grain group.

    Uses the scanned_sources tracked during SQL generation to look up
    availability states and compute total scan size.

    Args:
        session: Database session
        grain_group: A single grain group with scanned_sources
        ctx: Build context with node information

    Returns:
        ScanEstimate with total bytes and per-source breakdowns, or None if
        no size data is available.
    """
    # Collect source names from this grain group
    all_source_names = set()
    for scanned in grain_group.scanned_sources:
        all_source_names.add(scanned.source_name)

    print(f"[SCAN DEBUG] Scanned source names: {all_source_names}")

    if not all_source_names:
        print("[SCAN DEBUG] No scanned sources found")
        return None

    # Query availability states for these sources
    sources: list[SourceScanInfo] = []
    total_bytes = 0

    for source_name in all_source_names:
        print(f"[SCAN DEBUG] Processing source: {source_name}")

        # Look up the source node
        node = ctx.nodes.get(source_name)
        if not node or not node.current:
            print("[SCAN DEBUG]   Node not found in context")
            # Still add the source, but with no metadata
            sources.append(
                SourceScanInfo(
                    source_name=source_name,
                    total_bytes=None,
                    partition_columns=[],
                    total_partition_count=None,
                ),
            )
            continue

        print(f"[SCAN DEBUG]   Node.current.id: {node.current.id}")
        print(f"[SCAN DEBUG]   Node.type: {node.type}")

        # Query for availability state via NodeAvailabilityState
        stmt = (
            select(AvailabilityState)
            .join(
                NodeAvailabilityState,
                NodeAvailabilityState.availability_id == AvailabilityState.id,
            )
            .where(NodeAvailabilityState.node_id == node.current.id)
        )
        result = await session.execute(stmt)
        availability = result.scalar_one_or_none()

        print(f"[SCAN DEBUG]   Availability found: {availability is not None}")
        if availability:
            print(f"[SCAN DEBUG]   Availability.id: {availability.id}")
            print(f"[SCAN DEBUG]   total_size_bytes: {availability.total_size_bytes}")
            print(f"[SCAN DEBUG]   total_row_count: {availability.total_row_count}")

        # Always add the source to the list
        if availability and availability.total_size_bytes:
            # We have size data - include it and add to total
            size_bytes = availability.total_size_bytes
            total_bytes += size_bytes

            sources.append(
                SourceScanInfo(
                    source_name=source_name,
                    catalog=availability.catalog,
                    schema_=availability.schema_,
                    table=availability.table,
                    total_bytes=size_bytes,
                    partition_columns=availability.temporal_partitions or [],
                    total_partition_count=availability.total_partitions,
                    # Filter-based estimates omitted - we can't calculate them accurately yet
                    # scan_bytes, scan_percentage, scanned_partition_count remain None
                ),
            )
        else:
            # No availability data or no size - still add source with None values
            sources.append(
                SourceScanInfo(
                    source_name=source_name,
                    catalog=availability.catalog if availability else None,
                    schema_=availability.schema_ if availability else None,
                    table=availability.table if availability else None,
                    total_bytes=None,
                    partition_columns=availability.temporal_partitions or []
                    if availability
                    else [],
                    total_partition_count=availability.total_partitions
                    if availability
                    else None,
                ),
            )

    # If no sources were scanned, return None
    if not sources:
        print("[SCAN DEBUG] No sources were scanned")
        return None

    # Check if using materialization
    # TODO: Check if pre-aggs are actually being used in this specific grain group
    has_materialization = ctx.use_materialized

    # Set total_bytes to None if no sources have size data
    total_bytes_result = total_bytes if total_bytes > 0 else None

    print(
        f"[SCAN DEBUG] Returning scan estimate: total_bytes={total_bytes_result}, sources={len(sources)}",
    )
    return ScanEstimate(
        total_bytes=total_bytes_result,
        sources=sources,
        has_materialization=has_materialization,
    )
