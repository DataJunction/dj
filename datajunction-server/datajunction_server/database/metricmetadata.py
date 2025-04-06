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


class MetricMetadata(Base):
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

    # Formatting fields
    significant_digits: Mapped[int | None] = mapped_column(
        sa.Integer,
        nullable=True,
        comment="Number of significant digits to display (if set).",
    )
    min_decimal_exponent: Mapped[int | None] = mapped_column(
        sa.Integer,
        nullable=True,
        comment="Minimum exponent to still use decimal formatting; below this, use scientific notation.",
    )
    max_decimal_exponent: Mapped[int | None] = mapped_column(
        sa.Integer,
        nullable=True,
        comment="Maximum exponent to still use decimal formatting; above this, use scientific notation.",
    )

    @classmethod
    def from_input(cls, input_data: "MetricMetadataInput") -> "MetricMetadata":
        """
        Parses a MetricMetadataInput object to a MetricMetadata object
        """
        return MetricMetadata(
            direction=input_data.direction,
            unit=MetricUnit[input_data.unit.upper()] if input_data.unit else None,
            significant_digits=input_data.significant_digits,
            min_decimal_exponent=input_data.min_decimal_exponent,
            max_decimal_exponent=input_data.max_decimal_exponent,
        )
