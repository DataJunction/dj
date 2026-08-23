"""Availability state database schema."""

from datetime import UTC, datetime
from functools import partial
from typing import Any

import sqlalchemy as sa
from sqlalchemy import JSON, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
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

    # Disjoint, inclusive temporal ranges the table covers
    temporal_ranges: Mapped[list[dict] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        default=list,
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

    @property
    def min_temporal_partition(self) -> list[str]:
        """
        The lowest temporal partition covered.
        """
        ranges = self.temporal_ranges or []
        return (ranges[0].get("min_temporal_partition") if ranges else None) or []

    @min_temporal_partition.setter
    def min_temporal_partition(self, value) -> None:
        self._set_bound("min_temporal_partition", value)

    @property
    def max_temporal_partition(self) -> list[str]:
        """
        The highest temporal partition covered.
        """
        ranges = self.temporal_ranges or []
        return (ranges[-1].get("max_temporal_partition") if ranges else None) or []

    @max_temporal_partition.setter
    def max_temporal_partition(self, value) -> None:
        self._set_bound("max_temporal_partition", value)

    def _set_bound(self, key: str, value) -> None:
        """
        Move one end of the outermost range.
        """
        ranges = [dict(item) for item in self.temporal_ranges or []]
        if not ranges:
            if not value:
                return
            ranges = [{"min_temporal_partition": None, "max_temporal_partition": None}]
        index = 0 if key == "min_temporal_partition" else -1
        ranges[index][key] = list(value) if value else None
        self.temporal_ranges = ranges

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
