"""Availability state database schema."""
from datetime import datetime, timezone
from functools import partial
from typing import List, Optional

import sqlalchemy as sa
from sqlalchemy import JSON, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.models.node import BuildCriteria, PartitionAvailability
from datajunction_server.typing import UTCDatetime


class AvailabilityState(Base):  # pylint: disable=too-few-public-methods
    """
    The availability of materialized data for a node
    """

    __tablename__ = "availabilitystate"

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )

    catalog: Mapped[str]
    schema_: Mapped[Optional[str]] = mapped_column(nullable=True)
    table: Mapped[str]
    valid_through_ts: Mapped[int]
    url: Mapped[Optional[str]]

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
        criteria: Optional[BuildCriteria] = None,  # pylint: disable=unused-argument
    ) -> bool:  # pragma: no cover
        """
        Determine whether an availability state is useable given criteria
        """
        # Criteria to determine if an availability state should be used needs to be added
        return True


class NodeAvailabilityState(Base):  # pylint: disable=too-few-public-methods
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
