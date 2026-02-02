"""Source table metadata database schema."""

from datetime import datetime, timezone
from functools import partial
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import DateTime, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime


class SourceTableMetadata(Base):
    """
    Metadata about upstream source tables.

    Stores table-level aggregates including size, row count, partition ranges,
    and freshness information. Updated by metadata collection systems that
    gather statistics from external data catalogs and metadata stores.
    """

    __tablename__ = "sourcetablemetadata"

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )

    # Link to node
    node_id: Mapped[int] = mapped_column(
        ForeignKey(
            "node.id",
            name="fk_sourcetablemetadata_node_id_node",
            ondelete="CASCADE",
        ),
        nullable=False,
    )

    # Table-level aggregates
    total_size_bytes: Mapped[int] = mapped_column(
        sa.BigInteger(),
        nullable=False,
    )
    total_row_count: Mapped[int] = mapped_column(
        sa.BigInteger(),
        nullable=False,
    )
    total_partitions: Mapped[int] = mapped_column(
        nullable=False,
    )

    # Partition range (string representation of partition values)
    earliest_partition_value: Mapped[Optional[str]] = mapped_column(nullable=True)
    latest_partition_value: Mapped[Optional[str]] = mapped_column(nullable=True)

    # Freshness and retention
    # Unix timestamp indicating data freshness from external metadata sources
    freshness_timestamp: Mapped[Optional[int]] = mapped_column(
        sa.BigInteger(),
        nullable=True,
    )
    # TTL in days (data retention period from external metadata sources)
    ttl_days: Mapped[Optional[int]] = mapped_column(nullable=True)

    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
        onupdate=partial(datetime.now, timezone.utc),
    )

    __table_args__ = (Index("idx_sourcetablemetadata_node_id", "node_id"),)


class SourcePartitionMetadata(Base):
    """
    Per-partition size and row count data for source tables.

    Stores statistics for individual partitions to enable cost estimation
    and partition-level analysis. Populated by metadata collection systems
    that gather statistics from external data catalogs and metadata stores.
    """

    __tablename__ = "sourcepartitionmetadata"

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )

    # Link to source table metadata
    source_table_metadata_id: Mapped[int] = mapped_column(
        ForeignKey(
            "sourcetablemetadata.id",
            name="fk_sourcepartitionmetadata_source_table_metadata_id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )

    # Partition value (e.g., "20260131" for date-based partitions)
    partition_value: Mapped[str] = mapped_column(nullable=False)

    # Partition-level statistics
    size_bytes: Mapped[int] = mapped_column(
        sa.BigInteger(),
        nullable=False,
    )
    row_count: Mapped[int] = mapped_column(
        sa.BigInteger(),
        nullable=False,
    )

    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
        onupdate=partial(datetime.now, timezone.utc),
    )

    __table_args__ = (
        Index(
            "idx_sourcepartitionmetadata_source_table_metadata_id_partition_value",
            "source_table_metadata_id",
            "partition_value",
        ),
    )
