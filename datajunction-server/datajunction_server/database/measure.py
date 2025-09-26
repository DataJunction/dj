"""Measure database schema."""

from typing import List, Optional

from sqlalchemy import (
    BigInteger,
    Enum,
    ForeignKey,
    Integer,
    String,
    JSON,
    TypeDecorator,
    select,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.database.base import Base
from datajunction_server.database.column import Column
from datajunction_server.models.base import labelize
from datajunction_server.models.measure import AggregationRule
from datajunction_server.database.node import NodeRevision
from datajunction_server.models.cube_materialization import (
    AggregationRule as MeasureAggregationRule,
)


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


class MeasureAggregationRuleType(TypeDecorator):
    impl = JSON

    def process_bind_param(self, value, dialect):
        if value is None:
            return None  # pragma: no cover
        if isinstance(value, MeasureAggregationRule):
            return value.model_dump()
        raise ValueError(  # pragma: no cover
            f"Expected AggregationRule, got {type(value)}",
        )

    def process_result_value(self, value, dialect):
        if value is None:
            return None  # pragma: no cover
        if isinstance(value, str):
            return MeasureAggregationRule.model_validate_json(value)  # pragma: no cover
        return MeasureAggregationRule.model_validate(value)


class FrozenMeasure(Base):
    """
    A frozen measure represents a binding of a measure expression and aggregation rule
    to a specific node revision in the data graph.
    """

    __tablename__ = "frozen_measures"

    id: Mapped[int] = mapped_column(BigInteger(), primary_key=True)

    # Stable, versioned name used in compiled SQL
    name: Mapped[str] = mapped_column(unique=True)

    # TODO: Link to the abstract measure definition, which could reference multiple
    # frozen measures. This lets us track semantic meaning vs. physical binding.
    # measure_id: Mapped[int] = mapped_column(ForeignKey("measures.id"))

    # The specific versioned node this measure binds to, and guarantees that the expression
    # resolves safely against this schema.
    upstream_revision_id: Mapped[int] = mapped_column(
        ForeignKey(
            "noderevision.id",
            name="fk_frozen_measure_upstream_revision_id_noderevision",
            ondelete="CASCADE",
        ),
    )
    upstream_revision: Mapped["NodeRevision"] = relationship(
        "NodeRevision",
        lazy="selectin",
    )

    # A logical SQL expression
    expression: Mapped[str]

    # How to aggregate the resolved expression (e.g., SUM, COUNT, AVG)
    aggregation: Mapped[str]

    # Additivity or rollup rule - tells the planner if this measure can be summed,
    # needs special handling, or is non-additive.
    rule: Mapped[MeasureAggregationRule] = mapped_column(MeasureAggregationRuleType)

    # Associated node revisions that use this measure
    used_by_node_revisions: Mapped[list["NodeRevision"]] = relationship(
        secondary="node_revision_frozen_measures",
        back_populates="frozen_measures",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<FrozenMeasure(id={self.id!r}, name='{self.name}', "
            f"aggregation='{self.aggregation}', "
            f"upstream_revision_id={self.upstream_revision_id})>"
        )

    @classmethod
    async def get_by_name(
        cls,
        session: AsyncSession,
        name: str,
    ) -> Optional["FrozenMeasure"]:
        """
        Get a measure by name
        """
        statement = (
            select(FrozenMeasure)
            .where(FrozenMeasure.name == name)
            .options(
                selectinload(FrozenMeasure.used_by_node_revisions),
            )
        )
        result = await session.execute(statement)
        return result.unique().scalar_one_or_none()

    @classmethod
    async def find_by(
        cls,
        session: AsyncSession,
        prefix: Optional[str] = None,
        aggregation: Optional[str] = None,
        upstream_name: Optional[str] = None,
        upstream_version: Optional[str] = None,
    ) -> list["FrozenMeasure"]:
        """
        Find frozen measure by search params
        """
        stmt = select(FrozenMeasure)

        filters = []

        if prefix:
            filters.append(FrozenMeasure.name.like(f"{prefix}%"))

        if aggregation:
            filters.append(FrozenMeasure.aggregation == aggregation.upper())

        if upstream_name:
            stmt = stmt.join(
                NodeRevision,
                FrozenMeasure.upstream_revision_id == NodeRevision.id,
            )
            filters.append(NodeRevision.name == upstream_name)
            if upstream_version:
                filters.append(NodeRevision.version == upstream_version)

        if filters:
            stmt = stmt.where(*filters)

        result = await session.execute(stmt)
        return result.scalars().all()


class NodeRevisionFrozenMeasure(Base):
    """
    Join table tying NodeRevisions to FrozenMeasures.
    """

    __tablename__ = "node_revision_frozen_measures"

    id: Mapped[int] = mapped_column(BigInteger(), primary_key=True)
    node_revision_id: Mapped[int] = mapped_column(
        ForeignKey("noderevision.id", ondelete="CASCADE"),
    )
    frozen_measure_id: Mapped[int] = mapped_column(
        ForeignKey("frozen_measures.id", ondelete="CASCADE"),
    )
