"""Availability state database schema."""

from datetime import UTC, datetime
from functools import partial
from typing import Any

import sqlalchemy as sa
from sqlalchemy import JSON, DateTime, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.models.node import BuildCriteria, PartitionAvailability
from datajunction_server.typing import UTCDatetime


class AvailabilityState(Base):
    """
    The availability of materialized data for a node
    """

    __tablename__ = "availabilitystate"

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )

    catalog: Mapped[str]
    schema_: Mapped[str | None] = mapped_column(nullable=True)
    table: Mapped[str]
    valid_through_ts: Mapped[int] = mapped_column(sa.BigInteger())
    url: Mapped[str | None]
    links: Mapped[dict[str, Any] | None] = mapped_column(JSON, default=dict)

    # An ordered list of categorical partitions like ["country", "group_id"]
    # or ["region_id", "age_group"]
    categorical_partitions: Mapped[list[str] | None] = mapped_column(
        JSON,
        default=[],
    )

    # An ordered list of temporal partitions like ["date", "hour"] or ["date"]
    temporal_partitions: Mapped[list[str] | None] = mapped_column(
        JSON,
        default=[],
    )

    # Node-level temporal ranges
    min_temporal_partition: Mapped[list[str] | None] = mapped_column(
        JSON,
        default=[],
    )
    max_temporal_partition: Mapped[list[str] | None] = mapped_column(
        JSON,
        default=[],
    )

    # Partition-level availabilities
    partitions: Mapped[list[PartitionAvailability] | None] = mapped_column(
        JSON,
        default=[],
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, UTC),
    )

    # Table-level size metadata
    total_size_bytes: Mapped[int | None] = mapped_column(
        sa.BigInteger(),
        nullable=True,
    )
    total_row_count: Mapped[int | None] = mapped_column(
        sa.BigInteger(),
        nullable=True,
    )
    total_partitions: Mapped[int | None] = mapped_column(nullable=True)
    ttl_days: Mapped[int | None] = mapped_column(nullable=True)

    def is_available(
        self,
        criteria: BuildCriteria | None = None,
    ) -> bool:  # pragma: no cover
        """
        Determine whether an availability state is useable given criteria
        """
        # TODO: we should evaluate this availability state against the criteria.
        #       Remember that VTTS can be also evaluated at runtime dependency.
        return True


class NodeAvailabilityState(Base):
    """
    Join table for availability state
    """

    __tablename__ = "nodeavailabilitystate"
    __table_args__ = (Index("ix_nodeavailabilitystate_node_id", "node_id"),)

    availability_id: Mapped[int] = mapped_column(
        ForeignKey(
            "availabilitystate.id",
            name="fk_nodeavailabilitystate_availability_id_availabilitystate",
        ),
        primary_key=True,
    )
    node_id: Mapped[int] = mapped_column(
        ForeignKey(
            "noderevision.id",
            name="fk_nodeavailabilitystate_node_id_noderevision",
            ondelete="CASCADE",
        ),
        primary_key=True,
    )
