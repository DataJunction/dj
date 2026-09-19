"""Pre-aggregation database schema."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from functools import partial
from typing import Any

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
    grain_columns: list[str],
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


def canonical_params(params: dict[str, Any] | None) -> str:
    """
    Stable string form of a component's tuning parameters, for identity.

    Keys are sorted and numeric values normalized, so ``{"compression": 200}``
    and ``{"compression": 200.0}`` cannot yield two identities for one sketch.
    """
    if not params:
        return ""

    def norm(value: Any) -> Any:
        # 200 and 200.0 are the same compression; don't let the literal's
        # spelling fork the identity. (bools are not floats, so they pass
        # through untouched.)
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    # `default=str` so an exotic value can't raise from inside pre-agg matching.
    # Params arrive as parsed YAML/JSON, so this should be unreachable; an opaque
    # TypeError deep in the matcher is a bad way to find out otherwise.
    return json.dumps(
        {key: norm(params[key]) for key in sorted(params)},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def measure_identity_token(
    expr_hash: str,
    aggregation: str | None,
    params: dict[str, Any] | None = None,
) -> str:
    """
    Canonical token identifying one measure: expression hash plus Phase-1
    aggregation, plus tuning parameters when the aggregation takes them.

    The hash alone is not an identity -- ``SUM(x)`` and ``MAX(x)`` hash alike --
    and a partial is reusable only by a metric that accumulates the same way.
    Everything comparing or hashing measures goes through this.

    Parameters extend that reasoning to sketches: a t-digest accumulated at
    ``compression=100`` is not interchangeable with one at ``compression=1000``,
    though both are ``acme_tdigest_agg`` over the same expression. The segment is
    appended **only** when params are present, so tokens for the overwhelming
    majority of components -- plain ``SUM``, ``COUNT``, ``MAX`` -- are byte-identical
    to what this returned before params existed. Stored ``preagg_hash`` values are
    built from these tokens, so that stability is a compatibility requirement, not
    a nicety.

    Note the quantile *fractions* a metric asks for are deliberately not here. One
    sketch column serves p50, p95 and p99; folding fractions in would fragment it
    into three identical columns.
    """
    base = f"{expr_hash}:{(aggregation or '').strip().upper()}"
    suffix = canonical_params(params)
    return f"{base}:{suffix}" if suffix else base


def get_measure_identities(measures: list[PreAggMeasure]) -> set[str]:
    """
    Identity tokens for a list of measures (see ``measure_identity_token``).

    Args:
        measures: List of PreAggMeasure objects

    Returns:
        Set of identity tokens
    """
    return {
        measure_identity_token(m.expr_hash, m.aggregation, m.params)
        for m in measures
        if m.expr_hash
    }


def compute_preagg_hash_from_hashes(
    node_revision_id: int,
    grain_columns: list[str],
    measure_identities: list[str],
) -> str:
    """
    Compute a unique hash for a pre-aggregation from measure identity tokens.

    This hash uniquely identifies a pre-aggregation by combining:
    - node_revision_id: Which node version
    - grain_columns: What dimensions we're grouping by
    - measure_identities: Identity tokens for the measures (expression hash plus
      Phase-1 aggregation -- see ``measure_identity_token``)

    The aggregation matters here because this hash is UNIQUE-constrained: without
    it, SUM- and MAX-backed pre-aggs over the same expression and grain collide.
    Computed only at insert -- existing rows keep their stored value, and so keep
    their materialization table names.

    Args:
        node_revision_id: The ID of the node revision
        grain_columns: Fully qualified dimension/column references
        measure_identities: List of measure identity tokens

    Returns:
        MD5 hash string (8 chars) uniquely identifying this pre-agg
    """
    content = (
        f"{node_revision_id}:"
        f"{json.dumps(sorted(grain_columns))}:"
        f"{json.dumps(sorted(measure_identities))}"
    )
    return hashlib.md5(content.encode()).hexdigest()[:8]


def compute_preagg_hash(
    node_revision_id: int,
    grain_columns: list[str],
    measures: list[PreAggMeasure],
) -> str:
    """
    Compute a unique hash for a pre-aggregation.

    This hash uniquely identifies a pre-aggregation by combining:
    - node_revision_id: Which node version
    - grain_columns: What dimensions we're grouping by
    - measure identities: What we're computing, and how we aggregate it

    Args:
        node_revision_id: The ID of the node revision
        grain_columns: Fully qualified dimension/column references
        measures: List of PreAggMeasure objects

    Returns:
        MD5 hash string (8 chars) uniquely identifying this pre-agg
    """
    return compute_preagg_hash_from_hashes(
        node_revision_id,
        grain_columns,
        sorted(get_measure_identities(measures)),
    )


class PreAggregation(Base):
    """First-class pre-aggregation entity that can be shared across cubes."""

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
    grain_columns: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    # Measures with full MetricComponent info for matching and re-aggregation
    # Stored as PreAggMeasure which extends MetricComponent with expr_hash
    measures: Mapped[list[PreAggMeasure]] = mapped_column(
        PydanticListType(PreAggMeasure),
        nullable=False,
    )

    # Output columns with types (grain columns + measure columns)
    # This stores the schema of the materialized table for:
    # - Table creation with correct types
    # - Validation of materialized data
    columns: Mapped[list[V3ColumnMetadata] | None] = mapped_column(
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

    # Unique pre-agg hash: hash(node_revision_id + grain_columns + measure_expr_hashes)
    # Uniquely identifies this pre-agg. Used for table/workflow naming.
    # Note: unique=True creates a unique index, so no separate index needed
    preagg_hash: Mapped[str] = mapped_column(
        String(8),
        nullable=False,
        unique=True,
    )

    # Optional stable, human-supplied handle for externally-registered pre-aggs.
    # Used by YAML deploy reconciliation and availability-by-name callbacks.
    name: Mapped[str | None] = mapped_column(String, nullable=True)

    # === Materialization Config ===
    strategy: Mapped[MaterializationStrategy | None] = mapped_column(
        Enum(MaterializationStrategy),
        nullable=True,
    )

    # Cron expression for scheduled materialization
    schedule: Mapped[str | None] = mapped_column(String, nullable=True)

    # Lookback window for incremental materialization (e.g., "3 days")
    lookback_window: Mapped[str | None] = mapped_column(String, nullable=True)

    # === Workflow State ===
    # Labeled workflow URLs: [WorkflowUrl(label="scheduled", url="..."), ...]
    # Scheduler-agnostic: DJ Server stores what Query Service returns
    workflow_urls: Mapped[list[WorkflowUrl] | None] = mapped_column(
        PydanticListType(WorkflowUrl),
        nullable=True,
    )

    # Workflow status: "active" | "paused" | None (no workflow)
    workflow_status: Mapped[str | None] = mapped_column(String, nullable=True)

    # Workflow names returned by the query service (used for deactivation)
    workflow_names: Mapped[list[str] | None] = mapped_column(
        JSON,
        nullable=True,
        default=None,
    )

    # === Availability ===
    availability_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "availabilitystate.id",
            name="fk_pre_aggregation_availability_id_availabilitystate",
        ),
        nullable=True,
    )

    # === Metadata ===
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, UTC),
        nullable=False,
    )
    updated_at: Mapped[UTCDatetime | None] = mapped_column(
        DateTime(timezone=True),
        onupdate=partial(datetime.now, UTC),
        nullable=True,
    )

    # === Relationships ===
    node_revision: Mapped[NodeRevision] = relationship(
        "NodeRevision",
        passive_deletes=True,
    )
    availability: Mapped[AvailabilityState | None] = relationship(
        "AvailabilityState",
    )

    @property
    def materialized_table_ref(self) -> str | None:
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
    def max_partition(self) -> list[str] | None:
        """High-water mark - data is available up to this partition."""
        if not self.availability:
            return None
        return self.availability.max_temporal_partition

    @classmethod
    async def get_by_grain_group_hash(
        cls,
        session: AsyncSession,
        grain_group_hash: str,
    ) -> list[PreAggregation]:
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
                joinedload(cls.availability),
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
    ) -> PreAggregation | None:
        """Get a pre-aggregation by ID."""
        statement = select(cls).where(cls.id == pre_agg_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    @classmethod
    async def get_external_by_namespace(
        cls,
        session: AsyncSession,
        namespace: str,
    ) -> list[PreAggregation]:
        """
        Get all EXTERNAL-strategy pre-aggregations whose node lives in the given
        namespace. Used by deploy-time reconciliation to diff against the spec.
        """
        from sqlalchemy.orm import joinedload

        from datajunction_server.database.node import Node

        statement = (
            select(cls)
            .join(NodeRevision, cls.node_revision_id == NodeRevision.id)
            .join(Node, NodeRevision.node_id == Node.id)
            .options(
                joinedload(cls.node_revision),
                joinedload(cls.availability),
            )
            .where(
                # The deployment namespace is a prefix: nodes live in it or in a
                # sub-namespace (e.g. "ns" and "ns.default").
                sa.or_(
                    Node.namespace == namespace,
                    Node.namespace.like(f"{namespace}.%"),
                ),
                cls.strategy == MaterializationStrategy.EXTERNAL,
            )
        )
        result = await session.execute(statement)
        return list(result.scalars().unique().all())

    @classmethod
    async def find_matching(
        cls,
        session: AsyncSession,
        node_revision_id: int,
        grain_columns: list[str],
        measure_identities: set[str],
    ) -> PreAggregation | None:
        """Find the row this exact declaration already occupies, if any."""
        grain_group_hash = compute_grain_group_hash(node_revision_id, grain_columns)
        candidates = await cls.get_by_grain_group_hash(session, grain_group_hash)

        for candidate in candidates:
            if measure_identities == get_measure_identities(candidate.measures):
                return candidate

        return None

    @classmethod
    async def find_latest_for_node(
        cls,
        session: AsyncSession,
        node_name: str,
        grain_columns: list[str],
        measure_identities: set[str],
    ) -> PreAggregation | None:
        """
        Find the most recent pre-agg for the same declaration on ANY revision of
        the node, i.e. the predecessor of the one about to be inserted.

        There are two lookups because there are two different questions, and
        they deliberately match differently.

        ``find_matching`` is revision-scoped and asks "is this the same
        declaration I am upserting?". It decides whether to overwrite a row, so
        it has to be exact: same ``grain_group_hash`` (which embeds
        ``node_revision_id``) and the same set of measure identities.

        This one is node-scoped and asks "what did this declaration look like
        before?". The answer has to survive a new revision, so it cannot use
        that hash at all and instead joins through ``NodeRevision`` to ``Node``
        to match on node name. It also wants COVERING rather than exact
        measures: the point is to recognise the earlier incarnation of a
        declaration whose measures may have shifted, and it only reads the
        predecessor -- the caller re-checks the physical table coordinates
        before inheriting anything, which is what keeps a merely-similar
        pre-agg from being mistaken for the same one.

        Returns:
            The newest matching PreAggregation, or None
        """
        from sqlalchemy.orm import joinedload

        from datajunction_server.database.node import Node

        statement = (
            select(cls)
            .join(NodeRevision, cls.node_revision_id == NodeRevision.id)
            .join(Node, NodeRevision.node_id == Node.id)
            .options(joinedload(cls.availability))
            .where(Node.name == node_name)
            .order_by(cls.id.desc())
        )
        result = await session.execute(statement)
        wanted_grain = sorted(grain_columns)
        for candidate in result.scalars().unique().all():
            if sorted(candidate.grain_columns) != wanted_grain:
                continue
            if measure_identities <= get_measure_identities(candidate.measures):
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
