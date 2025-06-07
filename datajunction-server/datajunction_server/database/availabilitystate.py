"""Availability state database schema."""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import JSON, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.models.node import BuildCriteria, PartitionAvailability
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.database.materialization import Materialization


class AvailabilityState(Base):
    """
    The availability of materialized data for a node
    """

    __tablename__ = "availabilitystate"

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )

    # Identifying where the dataset lives
    catalog: Mapped[str]
    schema_: Mapped[Optional[str]] = mapped_column(nullable=True)
    table: Mapped[str]

    # Indicates data freshness
    valid_through_ts: Mapped[int] = mapped_column(sa.BigInteger())

    # Arbitrary JSON metadata. This can encompass any URLs associated with the materialized dataset
    custom_metadata: Mapped[Optional[Dict]] = mapped_column(
        JSONB,
        default=dict,
    )

    # The materialization that this availability is associated with, if any
    materialization_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "materialization.id",
            name="fk_availability_materialization_id_materialization",
        ),
    )
    materialization: Mapped[Optional["Materialization"]] = relationship(
        back_populates="availability",
        primaryjoin="Materialization.id==AvailabilityState.materialization_id",
    )

    # An ordered list of categorical partitions like ["country", "group_id"]
    # or ["region_id", "age_group"]
    categorical_partitions: Mapped[Optional[List[str]]] = mapped_column(
        JSON,
        default=[],
    )

    # An ordered list of temporal partitions like ["date", "hour"] or ["date"]
    temporal_partitions: Mapped[Optional[List[str]]] = mapped_column(
        JSON,
        default=[],
    )

    # Node-level temporal ranges
    min_temporal_partition: Mapped[Optional[List[str]]] = mapped_column(
        JSON,
        default=[],
    )
    max_temporal_partition: Mapped[Optional[List[str]]] = mapped_column(
        JSON,
        default=[],
    )

    # Partition-level availabilities
    partitions: Mapped[Optional[List[PartitionAvailability]]] = mapped_column(
        JSON,
        default=[],
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
    )

    def is_available(
        self,
        criteria: Optional[BuildCriteria] = None,
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
        ),
        primary_key=True,
    )
