"""Node-related scalars."""

import datetime
from enum import Enum
from typing import List, Optional

import strawberry
from strawberry.scalars import JSON
from strawberry.types import Info
from sqlalchemy.orm.attributes import InstrumentedAttribute, set_committed_value

from datajunction_server.api.graphql.scalars import BigInt
from datajunction_server.api.graphql.utils import extract_fields
from datajunction_server.api.graphql.scalars.availabilitystate import (
    AvailabilityState,
    PartitionAvailability,
)
from datajunction_server.api.graphql.scalars.catalog_engine import Catalog
from datajunction_server.api.graphql.scalars.column import (
    Column,
    NodeName,
    NodeNameVersion,
    Partition,
)
from datajunction_server.api.graphql.scalars.git_info import GitRepositoryInfo
from datajunction_server.api.graphql.scalars.materialization import (
    Backfill,
    MaterializationConfig,
    PartitionBackfill,
)
from datajunction_server.api.graphql.scalars.metricmetadata import (
    DecomposedMetric,
    MetricMetadata,
)
from datajunction_server.api.graphql.scalars.user import User
from datajunction_server.database.dimensionlink import (
    JoinCardinality as JoinCardinality_,
)
from datajunction_server.database.dimensionlink import JoinType as JoinType_
from datajunction_server.database.node import Node as DBNode
from datajunction_server.database.node import NodeRevision as DBNodeRevision
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node import NodeMode as NodeMode_
from datajunction_server.models.node import NodeStatus as NodeStatus_
from datajunction_server.models.node import NodeType as NodeType_
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse

NodeType = strawberry.enum(NodeType_)
NodeStatus = strawberry.enum(NodeStatus_)
NodeMode = strawberry.enum(NodeMode_)
JoinType = strawberry.enum(JoinType_)
JoinCardinality = strawberry.enum(JoinCardinality_)


_DOT = "_DOT_"


class _NameOnlyRevision:
    """
    Minimal stand-in returned by the cube_metrics fast path when only ``name``
    is requested.  A plain object is ~100x cheaper to construct than a full
    ``DBNodeRevision`` ORM instance.
    """

    __slots__ = ("name",)

    def __init__(self, name: str):
        self.name = name


@strawberry.enum
class NodeSortField(Enum):
    """
    Available node sort fields
    """

    NAME = ("name", DBNode.name)
    DISPLAY_NAME = ("display_name", DBNodeRevision.display_name)
    TYPE = ("type", DBNode.type)
    STATUS = ("status", DBNodeRevision.status)
    MODE = ("mode", DBNodeRevision.mode)
    CREATED_AT = ("created_at", DBNode.created_at)
    UPDATED_AT = ("updated_at", DBNodeRevision.updated_at)

    # The database column that this sort field maps to
    column: InstrumentedAttribute

    def __new__(cls, value, column):
        obj = object.__new__(cls)
        obj._value_ = value  # GraphQL will serialize this
        obj.column = column
        return obj


@strawberry.type
class CubeElement:
    """
    An element in a cube, either a metric or dimension
    """

    name: str
    display_name: str
    type: str
    partition: Optional[Partition]


@strawberry.type
class DimensionLink:
    """
    A dimension link between a dimension and a node
    """

    dimension: NodeName
    join_type: JoinType  # type: ignore
    join_sql: str
    join_cardinality: Optional[JoinCardinality]  # type: ignore
    role: Optional[str]
    foreign_keys: JSON
    default_value: Optional[str]


@strawberry.type
class DimensionAttribute:
    """
    A dimensional column attribute
    """

    name: str
    attribute: str | None
    role: str | None = None
    properties: list[str]
    type: str

    _dimension_node: Optional["Node"] = None

    @strawberry.field(description="The dimension node this attribute belongs to")
    async def dimension_node(self, info: Info) -> Optional["Node"]:
        """
        Lazy load the dimension node when queried.
        """
        if self._dimension_node:
            return self._dimension_node

        # Extract dimension node name from the full attribute name
        # e.g., "default.us_state.state_name" -> "default.us_state"
        dimension_node_name = self.name.rsplit(".", 1)[0]

        # Use the DataLoader from context to batch this lookup with other concurrent lookups
        node_loader = info.context["node_loader"]
        return await node_loader.load(dimension_node_name)  # type: ignore


@strawberry.type
class NodeRevision:
    """
    The base fields of a node revision, which does not include joined in entities.
    """

    id: BigInt
    type: NodeType  # type: ignore
    name: str
    display_name: Optional[str]
    version: str
    status: NodeStatus  # type: ignore
    mode: Optional[NodeMode]  # type: ignore
    description: str = ""
    updated_at: datetime.datetime
    custom_metadata: Optional[JSON] = None

    @strawberry.field
    def catalog(self, root: "DBNodeRevision") -> Optional[Catalog]:
        """
        Catalog for the node
        """
        return Catalog.from_pydantic(root.catalog)  # type: ignore

    query: Optional[str] = None

    @strawberry.field
    def columns(
        self,
        root: "DBNodeRevision",
        attributes: list[str] | None = None,
    ) -> list[Column]:
        """
        The columns of the node
        """
        # Pre-seed Partition.column back-ref so temporal_expression()'s
        # self.column.type access doesn't trip DetachedInstanceError during
        # post-resolver serialization. The joined-eager load on
        # Column.partition doesn't reliably fill the reverse side.
        for col in root.columns:
            if col.partition is not None:
                set_committed_value(col.partition, "column", col)
        return [
            Column(  # type: ignore
                name=col.name,
                display_name=col.display_name,
                type=col.type,
                attributes=col.attributes,
                dimension=(
                    NodeName(name=col.dimension.name)  # type: ignore
                    if col.dimension
                    else None
                ),
                partition=Partition(
                    type_=col.partition.type_,  # type: ignore
                    format=col.partition.format,
                    granularity=col.partition.granularity,
                    expression=col.partition.temporal_expression(),
                )
                if col.partition
                else None,
            )
            for col in root.columns
            if (
                any(col.has_attribute(attr) for attr in attributes)
                if attributes
                else True
            )
        ]

    # Dimensions and data graph-related outputs
    @strawberry.field
    def dimension_links(self) -> list[DimensionLink]:
        """
        Returns the dimension links for this node revision.
        """
        # Pre-seed each link's node rev to short-circuit the lazy load.
        for link in self.dimension_links:
            set_committed_value(link, "node_revision", self)
        return [
            link
            for link in self.dimension_links
            if link.dimension is not None  # handles hard-deleted dimension nodes
            and link.dimension.deactivated_at
            is None  # handles deactivated dimension nodes
        ]

    parents: List[NodeNameVersion]

    # Materialization-related outputs
    @strawberry.field
    def availability(self, root: "DBNodeRevision") -> Optional[AvailabilityState]:
        """
        The availability state of materialized data for this node
        """
        if not root.availability:
            return None
        return AvailabilityState(  # type: ignore
            catalog=root.availability.catalog,
            schema_=root.availability.schema_,
            table=root.availability.table,
            valid_through_ts=root.availability.valid_through_ts,
            url=root.availability.url,
            categorical_partitions=root.availability.categorical_partitions,
            temporal_partitions=root.availability.temporal_partitions,
            min_temporal_partition=root.availability.min_temporal_partition,
            max_temporal_partition=root.availability.max_temporal_partition,
            partitions=[
                PartitionAvailability(  # type: ignore
                    min_temporal_partition=p.min_temporal_partition,
                    max_temporal_partition=p.max_temporal_partition,
                    value=p.value,
                    valid_through_ts=p.valid_through_ts,
                )
                for p in (root.availability.partitions or [])
            ]
            if root.availability.partitions
            else None,
        )

    @strawberry.field
    def materializations(
        self,
        root: "DBNodeRevision",
    ) -> Optional[List[MaterializationConfig]]:
        """
        The materialization configurations for this node
        """
        if not root.materializations:
            return []
        return [
            MaterializationConfig(  # type: ignore
                name=m.name,
                config=m.config,
                schedule=m.schedule,
                job=m.job,
                strategy=str(m.strategy.value) if m.strategy else None,
                backfills=[
                    Backfill(  # type: ignore
                        spec=[
                            PartitionBackfill(  # type: ignore
                                column_name=p["column_name"],
                                values=p.get("values"),
                                range=p.get("range"),
                            )
                            for p in (b.spec or [])
                        ]
                        if b.spec
                        else None,
                        urls=b.urls,
                    )
                    for b in (m.backfills or [])
                ],
            )
            for m in root.materializations
        ]

    # Only source nodes will have these fields
    schema_: Optional[str]
    table: Optional[str]

    # Only metrics will have these fields
    required_dimensions: List[Column] | None = None

    @strawberry.field
    def primary_key(self, root: "DBNodeRevision") -> list[str]:
        """
        The primary key of the node
        """
        return [col.name for col in root.primary_key()]

    @strawberry.field
    def metric_metadata(
        self,
        root: "DBNodeRevision",
        info: Info,
    ) -> MetricMetadata | None:
        """
        Metric metadata
        """
        if root.type != NodeType.METRIC:
            return None

        # Parsing the metric SQL + walking its AST for `expression` and
        # `incompatible_druid_functions` is ANTLR-heavy. Skip it entirely
        # when the client didn't request either sub-field.
        requested = extract_fields(info)
        needs_ast = (
            "expression" in requested or "incompatible_druid_functions" in requested
        )
        expression: str | None = None
        incompatible: set[str] = set()
        if needs_ast:
            query_ast = parse(root.query)
            functions = [func.function() for func in query_ast.find_all(ast.Function)]
            expression = str(query_ast.select.projection[0])
            incompatible = {
                func.__name__.upper()
                for func in functions
                if Dialect.DRUID not in func.dialects
            }
        return MetricMetadata(  # type: ignore
            direction=root.metric_metadata.direction if root.metric_metadata else None,
            unit=root.metric_metadata.unit.value
            if root.metric_metadata and root.metric_metadata.unit
            else None,
            significant_digits=root.metric_metadata.significant_digits
            if root.metric_metadata
            else None,
            min_decimal_exponent=root.metric_metadata.min_decimal_exponent
            if root.metric_metadata
            else None,
            max_decimal_exponent=root.metric_metadata.max_decimal_exponent
            if root.metric_metadata
            else None,
            expression=expression or "",
            incompatible_druid_functions=incompatible,
        )

    @strawberry.field
    def is_derived_metric(self, root: "DBNodeRevision") -> bool:
        """
        Returns True if this metric references other metrics (making it a derived metric).
        A derived metric is a metric whose parent(s) include other metric nodes.
        """
        if root.type != NodeType_.METRIC:
            return False
        return root.is_derived_metric

    @strawberry.field
    async def extracted_measures(
        self,
        root: "DBNodeRevision",
        info: Info,
    ) -> DecomposedMetric | None:
        """
        A list of metric components for a metric node.

        Uses the request-scoped extracted_measures_loader so that a GraphQL
        query returning N metrics batches all extractions into a single
        session + shared nodes_cache/parent_map (instead of opening N
        independent sessions via resolver_session).
        """
        if root.type != NodeType.METRIC:
            return None

        # Fast path: derive_frozen_measures (internal/nodes.py:291, background
        # task on node create/update) persists `str(derived_sql)` to the
        # NodeRevision.derived_expression column. When the fragment only reads
        # `derivedQuery`, we return the cached scalar and skip extract(). The
        # GQL `derivedExpression` field is an alias for `combiner`, not for
        # the column of the same name — so we have to fall through to the
        # full path whenever it's requested.
        requested = extract_fields(info)
        needs_full_extract = (
            "components" in requested
            or "combiner" in requested
            or "derived_expression" in requested
        )

        if not needs_full_extract and root.derived_expression:
            return DecomposedMetric(  # type: ignore
                components=[],
                combiner="",
                derived_query=root.derived_expression,
            )

        # Full path: DataLoader batch + extract().
        loader = info.context["extracted_measures_loader"]
        result = await loader.load(root.id)
        if result is None:
            return None
        components, derived_ast = result
        # The derived_expression is the combiner (how to combine merged components)
        combiner_expr = str(derived_ast.select.projection[0])
        return DecomposedMetric(  # type: ignore
            components=components,
            combiner=combiner_expr,
            derived_query=str(derived_ast),
        )

    # Only cubes will have these fields
    @strawberry.field
    def cube_metrics(self, root: "DBNodeRevision", info: Info) -> List["NodeRevision"]:  # type: ignore[return-value]
        """
        Metrics for a cube node
        """
        if root.type != NodeType.CUBE:
            return []
        ordering = root.ordering()

        # Name-only path: metric names pre-fetched by _attach_raw_columns.
        if info.context.get("cube_name_only"):  # type: ignore
            metric_names: set[str] = getattr(root, "_cube_metric_names", set())
            stubs: list = [
                _NameOnlyRevision(name=col.name)
                for col in root.columns
                if col.name in metric_names
            ]
            return sorted(stubs, key=lambda x: ordering[x.name])

        # Full path: node_revision loaded on each cube element
        return sorted(
            [
                node_revision
                for _, node_revision in root.cube_elements_with_nodes()
                if node_revision and node_revision.type == NodeType_.METRIC
            ],
            key=lambda x: ordering[x.name],
        )

    @strawberry.field
    def cube_filters(self, root: "DBNodeRevision") -> List[str]:
        """
        Filters for a cube node
        """
        if root.type != NodeType.CUBE:
            return []
        return root.cube_filters or []

    @strawberry.field
    def cube_dimensions(
        self,
        root: "DBNodeRevision",
        info: Info,
    ) -> List[DimensionAttribute]:
        """
        Dimensions for a cube node
        """
        if root.type != NodeType.CUBE:
            return []

        # Name-only path: metric names pre-fetched by _attach_raw_columns.
        if info.context.get("cube_name_only"):  # type: ignore
            metric_names: set[str] = getattr(root, "_cube_metric_names", set())
            ordering = root.ordering()
            return sorted(
                [
                    DimensionAttribute(  # type: ignore
                        name=col.name + (col.dimension_column or ""),
                        attribute=None,
                        role=col.dimension_column,
                        _dimension_node=None,
                        type=str(col.type) if col.type else "",
                        properties=[],
                    )
                    for col in root.columns
                    if col.name not in metric_names
                ],
                key=lambda x: ordering.get(
                    x.name,
                    ordering.get(x.name.split("[")[0], 0),
                ),
            )

        # Full path: node_revision loaded on each cube element. Cube columns
        # store the full dotted name (e.g. "ns.dim.col") and the role suffix
        # (e.g. "[event_date]") separately, so build the role lookup keyed by
        # the same full name we reconstruct from each cube element below.
        dimension_to_roles = {
            col.name: col.dimension_column or "" for col in root.columns
        }
        ordering = root.ordering()

        def make_attr(element, node_revision):
            full_name = f"{node_revision.name}.{element.name}"
            role = dimension_to_roles.get(full_name, "")
            return DimensionAttribute(  # type: ignore
                name=full_name + role,
                attribute=element.name,
                role=role,
                _dimension_node=node_revision,
                type=element.type,
                properties=element.attribute_names(),
            )

        return sorted(
            [
                make_attr(element, node_revision)
                for element, node_revision in root.cube_elements_with_nodes()
                if node_revision and node_revision.type != NodeType.METRIC
            ],
            # Strip the role suffix when ordering — `ordering` is keyed by the
            # role-less column name.
            key=lambda x: ordering.get(
                x.name,
                ordering.get(x.name.split("[", 1)[0], 0),
            ),
        )


@strawberry.type
class TagBase:
    """
    A DJ node tag without any referential fields
    """

    name: str
    tag_type: str
    description: str | None
    display_name: str | None
    tag_metadata: JSON | None = strawberry.field(default_factory=dict)


@strawberry.type
class Node:
    """
    A DJ node
    """

    id: BigInt
    name: str
    type: NodeType  # type: ignore
    current_version: str
    created_at: datetime.datetime
    deactivated_at: Optional[datetime.datetime]

    current: NodeRevision
    revisions: list[NodeRevision]

    tags: list[TagBase]
    created_by: User
    owners: list[User]

    @strawberry.field
    def edited_by(self, root: "DBNode") -> list[str]:
        """
        The users who edited this node
        """
        return root.edited_by

    @strawberry.field
    def git_info(self, root: "DBNode") -> Optional[GitRepositoryInfo]:
        """
        Git repository information for this node's namespace.

        Pre-resolved at the parent (findNodes) level and attached to the node
        as ``_resolved_git_info``. A sync resolver avoids the ~0.7ms-per-item
        asyncio scheduling overhead that dominates for large lists.
        """
        return getattr(root, "_resolved_git_info", None)
