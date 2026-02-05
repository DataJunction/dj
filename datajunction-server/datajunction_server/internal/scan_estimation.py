"""
Scan estimation for query cost analysis.

Estimates the amount of data that will be scanned when executing a query
by reading table size metadata from already-loaded AvailabilityState records.
"""

from typing import Optional

from datajunction_server.construction.build_v3.types import BuildContext
from datajunction_server.models.sql import ScanEstimate, SourceScanInfo


def calculate_scan_estimate(
    physical_tables: list[str],
    ctx: BuildContext,
) -> Optional[ScanEstimate]:
    """
    Calculate scan estimate for a single grain group.

    Uses the physical tables tracked during SQL generation to look up
    availability states and compute total scan size.

    Args:
        session: Database session
        physical_tables: A list of table names involved in the query
        ctx: Build context with node information

    Returns:
        ScanEstimate with total bytes and per-source breakdowns, or None if
        no size data is available.
    """
    if not physical_tables:
        return None

    # Query availability states for these sources
    sources: list[SourceScanInfo] = []
    total_bytes = 0

    for source_name in physical_tables:
        # Look up the node by source name
        node = ctx.nodes.get(source_name)

        # Still add the source, but with no metadata
        if not node or not node.current:
            sources.append(
                SourceScanInfo(
                    source_name=source_name,
                    total_bytes=None,
                    partition_columns=[],
                    total_partition_count=None,
                ),
            )
            continue

        # Availability is already eagerly loaded in load_nodes()
        availability = node.current.availability
        size_bytes = (
            availability.total_size_bytes
            if availability and availability.total_size_bytes
            else 0
        )
        total_bytes += size_bytes

        # Always add the source to the list
        sources.append(
            SourceScanInfo(
                source_name=source_name,
                catalog=availability.catalog if availability else None,
                schema_=availability.schema_ if availability else None,
                table=availability.table if availability else None,
                total_bytes=size_bytes,
                partition_columns=(
                    availability.temporal_partitions or [] if availability else []
                ),
                total_partition_count=(
                    availability.total_partitions if availability else None
                ),
                # Filter-based estimates omitted - we can't calculate them accurately yet
                # scan_bytes, scan_percentage, scanned_partition_count remain None
            ),
        )

    # TODO: Check if pre-aggs are actually being used in this specific grain group

    # Set total_bytes to None if no sources have size data
    total_bytes_result = total_bytes if total_bytes > 0 else None
    return ScanEstimate(
        total_bytes=total_bytes_result,
        sources=sources,
        has_materialization=ctx.use_materialized,
    )
