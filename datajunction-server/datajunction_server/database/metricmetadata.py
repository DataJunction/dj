"""Metric metadata database schema."""
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import Enum
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.models.node import (
    MetricDirection,
    MetricMetadataInput,
    MetricUnit,
)


class MetricMetadata(
    Base,
):  # pylint: disable=too-few-public-methods,unsubscriptable-object
    """
    Additional metric metadata
    """

    __tablename__ = "metricmetadata"

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )

    direction: Mapped[Optional[MetricDirection]] = mapped_column(
        Enum(MetricDirection),
        default=MetricDirection.NEUTRAL,
        nullable=True,
    )
    unit: Mapped[Optional[MetricUnit]] = mapped_column(
        Enum(MetricUnit),
        default=MetricUnit.UNKNOWN,
        nullable=True,
    )

    @classmethod
    def from_input(cls, input_data: "MetricMetadataInput") -> "MetricMetadata":
        """
        Parses a MetricMetadataInput object to a MetricMetadata object
        """
        return MetricMetadata(
            direction=input_data.direction,
            unit=MetricUnit[input_data.unit.upper()] if input_data.unit else None,
        )
