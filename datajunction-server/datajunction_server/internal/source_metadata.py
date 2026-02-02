"""
Internal functions for source table metadata management.
"""

import logging
from typing import List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.source_metadata import (
    SourcePartitionMetadata,
    SourceTableMetadata,
)
from datajunction_server.models.source_metadata import (
    PartitionMetadataInput,
    SourceTableMetadataInput,
)

_logger = logging.getLogger(__name__)


async def upsert_source_table_metadata(
    session: AsyncSession,
    node_id: int,
    metadata: SourceTableMetadataInput,
) -> SourceTableMetadata:
    """
    Insert or update source table metadata for a node.

    Args:
        session: Database session
        node_id: ID of the source node
        metadata: Table metadata to upsert

    Returns:
        The created or updated SourceTableMetadata instance
    """
    # Check if metadata already exists for this node
    result = await session.execute(
        select(SourceTableMetadata).where(SourceTableMetadata.node_id == node_id),
    )
    existing = result.scalar_one_or_none()

    if existing:
        # Update existing metadata
        existing.total_size_bytes = metadata.total_size_bytes
        existing.total_row_count = metadata.total_row_count
        existing.total_partitions = metadata.total_partitions
        existing.earliest_partition_value = metadata.earliest_partition_value
        existing.latest_partition_value = metadata.latest_partition_value
        existing.freshness_timestamp = metadata.freshness_timestamp
        existing.ttl_days = metadata.ttl_days
        _logger.info(f"Updated source table metadata for node_id={node_id}")
        table_metadata = existing
    else:
        # Create new metadata
        table_metadata = SourceTableMetadata(
            node_id=node_id,
            total_size_bytes=metadata.total_size_bytes,
            total_row_count=metadata.total_row_count,
            total_partitions=metadata.total_partitions,
            earliest_partition_value=metadata.earliest_partition_value,
            latest_partition_value=metadata.latest_partition_value,
            freshness_timestamp=metadata.freshness_timestamp,
            ttl_days=metadata.ttl_days,
        )
        session.add(table_metadata)
        _logger.info(f"Created source table metadata for node_id={node_id}")

    await session.flush()
    await session.refresh(table_metadata)
    return table_metadata


async def upsert_partition_metadata(
    session: AsyncSession,
    source_table_metadata_id: int,
    partition_stats: List[PartitionMetadataInput],
) -> None:
    """
    Insert or update partition-level metadata.

    Replaces all existing partition metadata for the source table with the new stats.
    This ensures we only keep the last 90 days of partition data.

    Args:
        session: Database session
        source_table_metadata_id: ID of the SourceTableMetadata instance
        partition_stats: List of partition statistics to insert
    """
    # Delete existing partition metadata for this source table
    await session.execute(
        delete(SourcePartitionMetadata).where(
            SourcePartitionMetadata.source_table_metadata_id
            == source_table_metadata_id,
        ),
    )

    # Insert new partition metadata
    for stat in partition_stats:
        partition_metadata = SourcePartitionMetadata(
            source_table_metadata_id=source_table_metadata_id,
            partition_value=stat.partition_value,
            size_bytes=stat.size_bytes,
            row_count=stat.row_count,
        )
        session.add(partition_metadata)

    _logger.info(
        f"Upserted {len(partition_stats)} partition metadata records "
        f"for source_table_metadata_id={source_table_metadata_id}",
    )


async def get_source_table_metadata(
    session: AsyncSession,
    node_id: int,
) -> Optional[SourceTableMetadata]:
    """
    Retrieve source table metadata for a node.

    Args:
        session: Database session
        node_id: ID of the source node

    Returns:
        SourceTableMetadata instance or None if not found
    """
    result = await session.execute(
        select(SourceTableMetadata).where(SourceTableMetadata.node_id == node_id),
    )
    return result.scalar_one_or_none()


async def get_partition_metadata_range(
    session: AsyncSession,
    source_table_metadata_id: int,
    from_partition: Optional[str] = None,
    to_partition: Optional[str] = None,
) -> List[SourcePartitionMetadata]:
    """
    Query partition metadata in a date range.

    Args:
        session: Database session
        source_table_metadata_id: ID of the SourceTableMetadata instance
        from_partition: Start of partition range (inclusive)
        to_partition: End of partition range (inclusive)

    Returns:
        List of SourcePartitionMetadata instances ordered by partition_value descending
    """
    query = select(SourcePartitionMetadata).where(
        SourcePartitionMetadata.source_table_metadata_id == source_table_metadata_id,
    )

    if from_partition:
        query = query.where(
            SourcePartitionMetadata.partition_value >= from_partition,
        )

    if to_partition:
        query = query.where(SourcePartitionMetadata.partition_value <= to_partition)

    query = query.order_by(SourcePartitionMetadata.partition_value.desc())

    result = await session.execute(query)
    return list(result.scalars().all())
