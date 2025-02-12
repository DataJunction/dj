"""Measure database schema."""

from typing import List, Optional

from sqlalchemy import BigInteger, Enum, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.database.column import Column
from datajunction_server.models.base import labelize
from datajunction_server.models.measure import AggregationRule


class Measure(Base):  # type: ignore
    """
    Measure class.

    Measure is a basic data modelling concept that helps with making Metric nodes portable,
    that is, so they can be computed on various DJ nodes using the same Metric definitions.

    By default, if a node column is not a Dimension or Dimension attribute then it should
    be a Measure.
    """

    __tablename__ = "measures"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(unique=True)
    display_name: Mapped[Optional[str]] = mapped_column(
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    description: Mapped[Optional[str]]
    columns: Mapped[List["Column"]] = relationship(
        back_populates="measure",
        lazy="joined",
    )
    additive: Mapped[AggregationRule] = mapped_column(
        Enum(AggregationRule),
        default=AggregationRule.NON_ADDITIVE,
    )
