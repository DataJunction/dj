from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.alias_registry import AliasRegistry
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import MetricComponent, Aggregability
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import to_sql
from datajunction_server.sql.parsing.backends.antlr4 import parse

if TYPE_CHECKING:
    from datajunction_server.database.preaggregation import PreAggregation

logger = logging.getLogger(__name__)


@dataclass
class BuildContext:
    """
    Immutable context passed through the SQL generation pipeline.

    Contains all the information needed to build SQL for a set of metrics
    and dimensions.
    """

    session: AsyncSession
    metrics: list[str]
    dimensions: list[str]
    filters: list[str] = field(default_factory=list)
    dialect: Dialect = Dialect.SPARK
    alias_registry: AliasRegistry = field(default_factory=AliasRegistry)

    # Whether to use materialized tables when available (default: True)
    # Set to False when building SQL for materialization to avoid circular references
    use_materialized: bool = True

    # Temporal filter settings for incremental materialization
    # When True, adds DJ_LOGICAL_TIMESTAMP() filters on temporal partition columns
    include_temporal_filters: bool = False
    lookback_window: str | None = None

    # Loaded data (populated by load_nodes)
    nodes: dict[str, Node] = field(default_factory=dict)

    # Preloaded join paths: (source_revision_id, dim_name, role) -> list[DimensionLink]
    # Populated by load_nodes() using a single recursive CTE query
    join_paths: dict[tuple[int, str, str], list[DimensionLink]] = field(
        default_factory=dict,
    )

    # Parent map: child_node_name -> list of parent_node_names
    # Populated by find_upstream_node_names(), used to find metric parents without
    # needing to eager-load the parents relationship
    parent_map: dict[str, list[str]] = field(default_factory=dict)

    # Table alias counter for generating unique aliases
    _table_alias_counter: int = field(default=0)

    # Parent revision IDs from load_nodes, used by load_available_preaggs
    _parent_revision_ids: set[int] = field(default_factory=set)

    # AST cache: node_name -> parsed query AST (avoids re-parsing same query)
    _parsed_query_cache: dict[str, ast.Query] = field(default_factory=dict)

    # Pre-aggregation cache: maps node_revision_id to list of available PreAggregation records
    # Populated by load_available_preaggs() when use_materialized=True
    available_preaggs: dict[int, list["PreAggregation"]] = field(default_factory=dict)

    # Populated by setup_build_context() after decomposition
    metric_groups: list["MetricGroup"] = field(default_factory=list)
    decomposed_metrics: dict[str, "DecomposedMetricInfo"] = field(default_factory=dict)

    def next_table_alias(self, base_name: str) -> str:
        """Generate a unique table alias."""
        self._table_alias_counter += 1
        # Use short alias like t1, t2, etc.
        return f"t{self._table_alias_counter}"

    def get_parsed_query(self, node: Node) -> ast.Query:
        """
        Get the parsed query AST for a node, using cache if available.

        Important: Returns a reference to the cached AST. If you need to modify
        it, make a copy first to avoid corrupting the cache.
        """
        if node.name in self._parsed_query_cache:
            return self._parsed_query_cache[node.name]

        if not node.current or not node.current.query:  # pragma: no cover
            raise DJInvalidInputException(f"Node {node.name} has no query")

        query_ast = parse(node.current.query)
        self._parsed_query_cache[node.name] = query_ast
        return query_ast

    def get_parent_node(self, metric_node: Node) -> Node:
        """Get the parent node of a metric (the node it's defined on)."""
        # Use cached parent_map
        parent_names = self.parent_map.get(metric_node.name, [])
        if not parent_names:  # pragma: no cover
            raise DJInvalidInputException(
                f"Metric {metric_node.name} has no parent node",
            )

        # Metrics typically have one parent (the node they SELECT FROM)
        parent_name = parent_names[0]
        parent = self.nodes.get(parent_name)
        if not parent:  # pragma: no cover
            raise DJInvalidInputException(f"Parent node not found: {parent_name}")
        return parent

    def get_metric_node(self, metric_name: str) -> Node:
        """Get a metric node by name, raising if not found or not a metric."""
        node = self.nodes.get(metric_name)
        if not node:  # pragma: no cover
            raise DJInvalidInputException(f"Metric not found: {metric_name}")
        if node.type != NodeType.METRIC:  # pragma: no cover
            raise DJInvalidInputException(f"Not a metric node: {metric_name}")
        return node


@dataclass
class ColumnMetadata:
    """
    Metadata about a column in the generated SQL.

    This is V3's simplified column metadata focused on what's actually useful:
    - Identifying the output column name
    - Linking back to the semantic entity (node.column for dims, node for metrics)
    - Distinguishing column types via semantic_type
    """

    name: str  # SQL alias in output (clean name)
    semantic_name: (
        str  # Full semantic path (e.g., 'v3.customer.name' or 'v3.total_revenue')
    )
    type: str  # SQL type (string, number, etc.)
    semantic_type: str  # "dimension", "metric", "metric_component", or "metric_input"


@dataclass
class GrainGroupSQL:
    """
    SQL for a single grain group within measures SQL.

    Each grain group represents metrics that can be computed at the same aggregation level.
    Different aggregability levels produce different grain groups:
    - FULL: aggregates to requested dimensions
    - LIMITED: aggregates to requested dimensions + level columns
    - NONE: stays at native grain (primary key)

    Merged grain groups (is_merged=True) contain components from multiple aggregability
    levels and output raw values. Aggregations are applied in the final SELECT.

    The query is stored as an AST object. Use the `sql` property to render to string.
    """

    query: ast.Query  # AST object - only convert to string at API boundary
    columns: list[ColumnMetadata]
    grain: list[
        str
    ]  # Column names in GROUP BY (beyond requested dims for LIMITED/NONE)
    aggregability: Aggregability
    metrics: list[str]  # Metric names covered by this grain group
    parent_name: str  # Name of the parent node (fact/transform) for this grain group
    # Mapping from component name (hashed) to actual SQL alias in the output
    # Used by metrics SQL to correctly reference component columns
    component_aliases: dict[str, str] = field(default_factory=dict)

    # Merge tracking: when True, aggregations happen in final SELECT, not in CTE
    is_merged: bool = False

    # For merged groups: original aggregability per component (component.name -> Aggregability)
    # Used by generate_metrics_sql() to apply correct aggregation in final SELECT
    component_aggregabilities: dict[str, Aggregability] = field(default_factory=dict)

    # Metric components included in this grain group (for materialization planning)
    # Each component has: name, expression, aggregation (phase 1), merge (phase 2), rule
    components: list[MetricComponent] = field(default_factory=list)

    # Dialect for rendering SQL (used for dialect-specific function names)
    dialect: Dialect = Dialect.SPARK

    @property
    def sql(self) -> str:
        """Render the query AST to SQL string for the target dialect."""
        return to_sql(self.query, self.dialect)


@dataclass
class GeneratedMeasuresSQL:
    """
    Output of measures SQL generation.

    Contains multiple grain groups, each at a different aggregation level.
    These can be:
    - Materialized separately for efficient queries
    - Combined by metrics SQL into a single executable query

    Also includes the build context and decomposed metrics to avoid
    redundant database queries when generating metrics SQL.
    """

    grain_groups: list[GrainGroupSQL]
    dialect: Dialect
    requested_dimensions: list[str]  # Original dimension refs for context

    # Internal: passed to build_metrics_sql to avoid redundant work
    # These are not serialized in API responses
    ctx: "BuildContext"
    decomposed_metrics: dict[str, "DecomposedMetricInfo"] = field(default_factory=dict)


@dataclass
class GeneratedSQL:
    """
    Output of metrics SQL generation (single combined SQL).

    This is the final, executable SQL that combines all grain groups
    and applies final metric expressions.

    The query is stored as an AST object. Use the `sql` property to render to string.
    """

    query: ast.Query  # AST object - only convert to string at API boundary
    columns: list[ColumnMetadata]
    dialect: Dialect

    @property
    def sql(self) -> str:
        """Render the query AST to SQL string for the target dialect."""
        return to_sql(self.query, self.dialect)


@dataclass
class JoinPath:
    """
    Represents a path from a fact/transform to a dimension via dimension links.
    """

    links: list[DimensionLink]  # Ordered list of links to traverse
    target_dimension: Node  # The final dimension node
    role: Optional[str] = (
        None  # Role qualifier if specified (e.g., "from", "to", "customer->home")
    )

    @property
    def target_node_name(self) -> str:
        return self.target_dimension.name


@dataclass
class ResolvedDimension:
    """
    A dimension that has been resolved to its join path.
    """

    original_ref: str  # Original reference (e.g., "v3.customer.name[order]")
    node_name: str  # Dimension node name (e.g., "v3.customer")
    column_name: str  # Column name (e.g., "name")
    role: Optional[str]  # Role if specified (e.g., "order")
    join_path: Optional[
        JoinPath
    ]  # Join path from fact to this dimension (None if local)
    is_local: bool  # True if dimension is on the fact table itself


@dataclass
class DimensionRef:
    """Parsed dimension reference."""

    node_name: str
    column_name: str
    role: Optional[str] = None


class ColumnType:
    """Types of columns that can be resolved."""

    METRIC = "metric"
    COMPONENT = "component"
    DIMENSION = "dimension"


@dataclass
class ColumnRef:
    """Reference to a column in a CTE."""

    cte_alias: str
    column_name: str
    column_type: str = ColumnType.COMPONENT  # Default for backward compatibility


@dataclass
class ColumnResolver:
    """
    Resolves semantic references (metric names, component names, dimension refs)
    to their CTE column locations.

    Provides a unified interface for looking up where any reference should
    resolve to in the generated SQL.
    """

    _entries: dict[str, ColumnRef] = field(default_factory=dict)

    @classmethod
    def from_base_metrics(
        cls,
        base_metrics_result: "BaseMetricsResult",
        dimension_aliases: dict[str, str],
        dim_cte_alias: str,
    ) -> "ColumnResolver":
        """
        Create a ColumnResolver from base metrics processing results.

        Args:
            base_metrics_result: Result from process_base_metrics
            dimension_aliases: Mapping from dimension refs to column aliases
            dim_cte_alias: CTE alias for dimension columns

        Returns:
            Populated ColumnResolver
        """
        resolver = cls()

        # Register metrics
        for name, info in base_metrics_result.metric_exprs.items():
            resolver.register(name, info.cte_alias, info.short_name, ColumnType.METRIC)

        # Register components
        for name, ref in base_metrics_result.component_refs.items():
            resolver.register(
                name,
                ref.cte_alias,
                ref.column_name,
                ColumnType.COMPONENT,
            )

        # Register dimensions
        for dim_ref, col_alias in dimension_aliases.items():
            resolver.register(dim_ref, dim_cte_alias, col_alias, ColumnType.DIMENSION)

        return resolver

    def register(
        self,
        name: str,
        cte_alias: str,
        column_name: str,
        column_type: str,
    ) -> None:
        """Register a name -> column mapping."""
        self._entries[name] = ColumnRef(cte_alias, column_name, column_type)

    def resolve(self, name: str) -> ColumnRef | None:
        """Resolve a name to its column reference."""
        return self._entries.get(name)  # pragma: no cover

    def get_by_type(self, col_type: str) -> dict[str, ColumnRef]:
        """Get all entries of a specific type."""
        return {k: v for k, v in self._entries.items() if v.column_type == col_type}

    # Compatibility methods for existing replace_*_refs_in_ast functions
    def metric_refs(self) -> dict[str, tuple[str, str]]:
        """Get metric refs as tuples for replace_metric_refs_in_ast."""
        return {
            name: (ref.cte_alias, ref.column_name)
            for name, ref in self._entries.items()
            if ref.column_type == ColumnType.METRIC
        }

    def component_refs(self) -> dict[str, tuple[str, str]]:
        """Get component refs as tuples for replace_component_refs_in_ast."""
        return {
            name: (ref.cte_alias, ref.column_name)
            for name, ref in self._entries.items()
            if ref.column_type == ColumnType.COMPONENT
        }

    def dimension_refs(self) -> dict[str, tuple[str, str]]:
        """Get dimension refs as tuples for replace_dimension_refs_in_ast."""
        return {
            name: (ref.cte_alias, ref.column_name)
            for name, ref in self._entries.items()
            if ref.column_type == ColumnType.DIMENSION
        }


@dataclass
class MetricExprInfo:
    """
    Information about a metric expression for the final SELECT.

    Contains the AST expression, short name for aliasing, and which CTE
    the metric comes from.
    """

    expr_ast: "ast.Expression"
    short_name: str
    cte_alias: str


@dataclass
class BaseMetricsResult:
    """
    Result of processing base metrics from grain groups.

    Contains all the mappings needed for derived metric resolution
    and final query building.
    """

    all_metrics: set[str]  # All metric names in grain groups
    metric_exprs: dict[str, MetricExprInfo]  # metric_name -> expression info
    component_refs: dict[str, ColumnRef]  # component_name -> column reference


@dataclass
class DecomposedMetricInfo:
    """
    Information about a decomposed metric.

    Contains the metric's components (for measures SQL), combiner expression
    (for metrics SQL), and aggregability info.
    """

    metric_node: Node
    components: list[MetricComponent]  # The decomposed components
    aggregability: Aggregability  # Overall aggregability (FULL, LIMITED, NONE)
    combiner: (
        str  # Expression combining merged components into final value (deprecated)
    )
    derived_ast: ast.Query  # The full derived query AST

    @property
    def combiner_ast(self) -> ast.Expression:
        """
        Get the combiner expression as an AST node.

        This is the first projection element from the derived query AST,
        which contains the metric expression with component references.
        """
        from copy import deepcopy

        # Return a copy to avoid mutating the original AST
        return deepcopy(self.derived_ast.select.projection[0])  # type: ignore

    @property
    def is_fully_decomposable(self) -> bool:
        """True if all components have FULL aggregability."""
        return all(c.rule.type == Aggregability.FULL for c in self.components)

    def is_derived_for_parents(
        self,
        parent_names: list[str],
        nodes: dict[str, Node],
    ) -> bool:
        """
        Check if this metric references other metrics (not just a simple aggregation).

        Args:
            parent_names: List of parent node names from the parent_map
            nodes: Dict of loaded nodes

        Returns:
            True if any parent is a metric
        """
        for parent_name in parent_names:
            parent_node = nodes.get(parent_name)
            if parent_node and parent_node.type == NodeType.METRIC:
                return True
        return False


@dataclass
class MetricGroup:
    """
    A group of metrics that share the same parent node.

    All metrics in a group can be computed in the same SELECT statement.
    Contains decomposed metric info with components and aggregability.
    """

    parent_node: Node
    decomposed_metrics: list[DecomposedMetricInfo]  # Decomposed metrics with components

    @property
    def overall_aggregability(self) -> Aggregability:
        """
        Get the worst-case aggregability across all metrics in this group.
        """
        if not self.decomposed_metrics:  # pragma: no cover
            return Aggregability.NONE

        if any(m.aggregability == Aggregability.NONE for m in self.decomposed_metrics):
            return Aggregability.NONE
        if any(
            m.aggregability == Aggregability.LIMITED for m in self.decomposed_metrics
        ):
            return Aggregability.LIMITED
        return Aggregability.FULL


@dataclass
class GrainGroup:
    """
    A group of metric components that share the same effective grain.

    Components in the same grain group can be computed in a single SELECT
    with the same GROUP BY clause.

    Grain groups are determined by aggregability:
    - FULL: requested dimensions only
    - LIMITED: requested dimensions + level columns (e.g., customer_id for COUNT DISTINCT)
    - NONE: native grain (primary key of parent node)

    Grain groups from the same parent can be merged into a single CTE at the
    finest grain. When merged, is_merged=True and original component aggregabilities
    are preserved in component_aggregabilities for proper aggregation in final SELECT.

    Non-decomposable metrics (like MAX_BY) have empty components but need a grain
    group at native grain to pass through raw rows for aggregation in metrics SQL.
    """

    parent_node: Node
    aggregability: Aggregability
    grain_columns: list[str]  # Columns to GROUP BY (beyond requested dimensions)
    components: list[tuple[Node, MetricComponent]]  # (metric_node, component) pairs

    # Merge tracking: when True, aggregations happen in final SELECT, not in CTE
    is_merged: bool = False

    # For merged groups: tracks original aggregability per component
    # Maps component.name -> original Aggregability
    component_aggregabilities: dict[str, Aggregability] = field(default_factory=dict)

    # Non-decomposable metrics that couldn't be broken into components
    # These need their raw metric expression applied in the final SELECT
    non_decomposable_metrics: list["DecomposedMetricInfo"] = field(default_factory=list)

    @property
    def grain_key(self) -> tuple[str, Aggregability, tuple[str, ...]]:
        """
        Key for grouping: (parent_name, aggregability, sorted grain columns).
        """
        return (
            self.parent_node.name,
            self.aggregability,
            tuple(sorted(self.grain_columns)),
        )
