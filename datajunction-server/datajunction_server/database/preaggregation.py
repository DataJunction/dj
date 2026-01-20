"""Pre-aggregation database schema."""

import hashlib
import json
from datetime import datetime, timezone
from functools import partial
from typing import List, Optional, Set

import sqlalchemy as sa
from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    String,
    Text,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.base import Base, PydanticListType
from datajunction_server.database.node import NodeRevision
from datajunction_server.models.decompose import PreAggMeasure
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.preaggregation import WorkflowUrl
from datajunction_server.models.query import V3ColumnMetadata
from datajunction_server.typing import UTCDatetime


# Valid materialization strategies for pre-aggregations
# (subset of MaterializationStrategy)
VALID_PREAGG_STRATEGIES = {
    MaterializationStrategy.FULL,
    MaterializationStrategy.INCREMENTAL_TIME,
}


def compute_expression_hash(expression: str) -> str:
    """
    Compute a hash of a measure expression for identity matching.

    This ensures that measures with different expressions are not incorrectly
    matched even if they have the same name.

    Args:
        expression: The SQL expression (e.g., "price * quantity")

    Returns:
        MD5 hash string of the expression (truncated to 12 chars)
    """
    # Normalize whitespace for consistent hashing
    normalized = " ".join(expression.split())
    return hashlib.md5(normalized.encode()).hexdigest()[:12]


def compute_grain_group_hash(
    node_revision_id: int,
    grain_columns: List[str],
) -> str:
    """
    Compute the grain group hash for a pre-aggregation.

    This enables fast lookups to find pre-aggs with the same node revision + grain.
    Multiple pre-aggs can share the same grain_group_hash (with different measures).
    The hash is computed from: node_revision_id + sorted(grain_columns)

    Note: grain_columns should be fully qualified dimension references
    (e.g., "default.date_dim.date_id", not just "date_id").

    Args:
        node_revision_id: The ID of the node revision
        grain_columns: Fully qualified dimension/column references

    Returns:
        MD5 hash string of the grain group
    """
    content = f"{node_revision_id}:{json.dumps(sorted(grain_columns))}"
    return hashlib.md5(content.encode()).hexdigest()


def get_measure_expr_hashes(measures: List[PreAggMeasure]) -> Set[str]:
    """
    Extract expression hashes from a list of measures.

    Args:
        measures: List of PreAggMeasure objects

    Returns:
        Set of expression hashes
    """
    return {m.expr_hash for m in measures if m.expr_hash}


def compute_preagg_hash(
    node_revision_id: int,
    grain_columns: List[str],
    measures: List[PreAggMeasure],
) -> str:
    """
    Compute a unique hash for a pre-aggregation.

    This hash uniquely identifies a pre-aggregation by combining:
    - node_revision_id: Which node version
    - grain_columns: What dimensions we're grouping by
    - measure expr_hashes: What aggregations we're computing

    Args:
        node_revision_id: The ID of the node revision
        grain_columns: Fully qualified dimension/column references
        measures: List of PreAggMeasure objects

    Returns:
        MD5 hash string (8 chars) uniquely identifying this pre-agg
    """
    measure_hashes = sorted([m.expr_hash for m in measures if m.expr_hash])
    content = (
        f"{node_revision_id}:"
        f"{json.dumps(sorted(grain_columns))}:"
        f"{json.dumps(measure_hashes)}"
    )
    return hashlib.md5(content.encode()).hexdigest()[:8]


class PreAggregation(Base):
    """
    First-class pre-aggregation entity that can be shared across cubes.

    A pre-aggregation represents a materialized grouping of measures at a specific grain,
    enabling efficient metric calculations by pre-computing aggregations.

    Pre-aggregations are ALWAYS created by DJ (via /preaggs/plan endpoint) from
    metrics + dimensions. Users never manually construct them - this ensures
    consistency between DJ-managed (Flow A) and user-managed (Flow B) materialization.

    Key concepts:
    - `node_revision`: The specific node revision this pre-agg is based on
    - `grain_columns`: Fully qualified dimension references that define the aggregation level
    - `measures`: Full MetricComponent info for matching and re-aggregation
    - `sql`: The generated SQL for materializing this pre-agg
    - `grain_group_hash`: Hash of (node_revision_id + sorted(grain_columns)) for grouping

    Measure format (MetricComponent):
    - name: Column name in materialized table
    - expression: The raw SQL expression
    - expr_hash: Hash of expression for identity matching
    - aggregation: Phase 1 function (e.g., "SUM")
    - merge: Phase 2 re-aggregation function
    - rule: Aggregation rules (type, level)

    Availability tracking:
    - Materialization status is tracked via AvailabilityState
    - Flow A: DJ's query service posts availability after materialization
    - Flow B: User's query service posts to /preaggs/{id}/availability/
    """

    __tablename__ = "pre_aggregation"

    id: Mapped[int] = mapped_column(
        sa.BigInteger(),
        primary_key=True,
        autoincrement=True,
    )

    # This is for a specific node revision
    node_revision_id: Mapped[int] = mapped_column(
        ForeignKey(
            "noderevision.id",
            name="fk_pre_aggregation_node_revision_id_noderevision",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )

    # Grain columns are fully qualified dimension/column references:
    # - Linked dimensions: "namespace.dim_node.column" (e.g., "default.date_dim.date_id")
    # - Direct columns on node: "namespace.node.column" (e.g., "default.orders.order_status")
    grain_columns: Mapped[List[str]] = mapped_column(JSON, nullable=False)

    # Measures with full MetricComponent info for matching and re-aggregation
    # Stored as PreAggMeasure which extends MetricComponent with expr_hash
    measures: Mapped[List[PreAggMeasure]] = mapped_column(
        PydanticListType(PreAggMeasure),
        nullable=False,
    )

    # Output columns with types (grain columns + measure columns)
    # This stores the schema of the materialized table for:
    # - Table creation with correct types
    # - Validation of materialized data
    columns: Mapped[Optional[List[V3ColumnMetadata]]] = mapped_column(
        PydanticListType(V3ColumnMetadata),
        nullable=True,
    )

    # The SQL for materializing this pre-agg (always DJ-generated)
    sql: Mapped[str] = mapped_column(Text, nullable=False)

    # Grain group key: hash(node_revision_id + sorted(grain_columns))
    # Groups pre-aggs by revision+grain. Multiple pre-aggs can share this hash.
    grain_group_hash: Mapped[str] = mapped_column(
        String,
        nullable=False,
        index=True,
    )

    # === Materialization Config ===
    strategy: Mapped[Optional[MaterializationStrategy]] = mapped_column(
        Enum(MaterializationStrategy),
        nullable=True,
    )

    # Cron expression for scheduled materialization
    schedule: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Lookback window for incremental materialization (e.g., "3 days")
    lookback_window: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # === Workflow State ===
    # Labeled workflow URLs: [WorkflowUrl(label="scheduled", url="..."), ...]
    # Scheduler-agnostic: DJ Server stores what Query Service returns
    workflow_urls: Mapped[Optional[List[WorkflowUrl]]] = mapped_column(
        PydanticListType(WorkflowUrl),
        nullable=True,
    )

    # Workflow status: "active" | "paused" | None (no workflow)
    workflow_status: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # === Availability ===
    availability_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "availabilitystate.id",
            name="fk_pre_aggregation_availability_id_availabilitystate",
        ),
        nullable=True,
    )

    # === Metadata ===
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[Optional[UTCDatetime]] = mapped_column(
        DateTime(timezone=True),
        onupdate=partial(datetime.now, timezone.utc),
        nullable=True,
    )

    # === Relationships ===
    node_revision: Mapped["NodeRevision"] = relationship(
        "NodeRevision",
        passive_deletes=True,
    )
    availability: Mapped[Optional["AvailabilityState"]] = relationship(
        "AvailabilityState",
    )

    @property
    def materialized_table_ref(self) -> Optional[str]:
        """Full table reference for SQL substitution. Derived from availability."""
        if not self.availability:
            return None
        parts = [
            p
            for p in [
                self.availability.catalog,
                self.availability.schema_,
                self.availability.table,
            ]
            if p
        ]
        return ".".join(parts) if parts else None

    @property
    def status(self) -> str:
        """Derived from availability state."""
        if not self.availability:
            return "pending"
        return "active"

    @property
    def max_partition(self) -> Optional[List[str]]:
        """High-water mark - data is available up to this partition."""
        if not self.availability:
            return None
        return self.availability.max_temporal_partition

    @classmethod
    async def get_by_grain_group_hash(
        cls,
        session: AsyncSession,
        grain_group_hash: str,
    ) -> List["PreAggregation"]:
        """
        Get all pre-aggregations with the given grain group hash.
        """
        from sqlalchemy.orm import joinedload, selectinload

        from datajunction_server.database.dimensionlink import DimensionLink

        statement = (
            select(cls)
            .options(
                joinedload(cls.node_revision).options(
                    selectinload(NodeRevision.columns),
                    selectinload(NodeRevision.dimension_links).options(
                        joinedload(DimensionLink.dimension),
                    ),
                ),
            )
            .where(cls.grain_group_hash == grain_group_hash)
        )
        result = await session.execute(statement)
        return list(result.scalars().unique().all())

    @classmethod
    async def get_by_id(
        cls,
        session: AsyncSession,
        pre_agg_id: int,
    ) -> Optional["PreAggregation"]:
        """Get a pre-aggregation by ID."""
        statement = select(cls).where(cls.id == pre_agg_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    @classmethod
    async def find_matching(
        cls,
        session: AsyncSession,
        node_revision_id: int,
        grain_columns: List[str],
        measure_expr_hashes: Set[str],
    ) -> Optional["PreAggregation"]:
        """
        Find an existing pre-agg that covers the requested measures.

        Looks up by grain_group_hash, then checks if any candidate
        has a superset of the required measures (by expr_hash).

        Returns:
            Matching PreAggregation if found, None otherwise
        """
        grain_group_hash = compute_grain_group_hash(node_revision_id, grain_columns)
        candidates = await cls.get_by_grain_group_hash(session, grain_group_hash)

        for candidate in candidates:
            existing_hashes = get_measure_expr_hashes(candidate.measures)
            if measure_expr_hashes <= existing_hashes:
                return candidate

        return None

    # TODO: Remove this once we have a way to test pre-aggregations
    def get_column_type(  # pragma: no cover
        self,
        col_name: str,
        default: str = "string",
    ) -> str:
        """Look up column type from pre-aggregation metadata."""
        if self.columns:
            for col in self.columns:
                if col.name == col_name:
                    return col.type
        return default
