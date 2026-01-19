"""Used for extracting components from metric definitions."""

import hashlib
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from datajunction_server.database.node import Node, NodeRevision, NodeRelationship
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.naming import amenable_name
from datajunction_server.sql import functions as dj_functions
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse


# =============================================================================
# AST Builder Helpers
# =============================================================================


def make_func(name: str, *args: ast.Expression | str) -> ast.Function:
    """Build a Function AST node."""
    return ast.Function(
        ast.Name(name),
        args=[ast.Column(ast.Name(a)) if isinstance(a, str) else a for a in args],
    )


# =============================================================================
# Decomposition Framework
# =============================================================================


@dataclass
class ComponentDef:
    """
    Defines one component of an aggregation decomposition.

    Attributes:
        suffix: Name suffix for the component (e.g., "_sum", "_count", "_hll")
        accumulate: Phase 1 - how to build from raw data. Can be:
            - Simple function name: "SUM" -> SUM(expr)
            - Template with {}: "SUM(POWER({}, 2))" -> SUM(POWER(expr, 2))
            - Template with {0}, {1}: "SUM({0} * {1})" for multi-arg functions
        merge: Phase 2 function - combine pre-aggregated values (e.g., "SUM", "hll_union")
        arg_index: Which function argument to use (default 0). Use None for multi-arg templates.
    """

    suffix: str
    accumulate: str
    merge: str
    arg_index: int | None = 0  # Which arg to use, or None for multi-arg templates


class AggDecomposition(ABC):
    """
    Abstract base class for aggregation decompositions.

    Defines the three phases of metric decomposition:
    1. Accumulate: How to build components from raw data (via `components`)
    2. Merge: How to combine pre-aggregated components (via `components`)
    3. Combiner: How to produce the final metric value from merged components

    Subclasses must define `components` and implement `combine()`.
    """

    @property
    @abstractmethod
    def components(self) -> list[ComponentDef]:
        """Define the components needed for this aggregation."""

    @abstractmethod
    def combine(self, components: list[MetricComponent]) -> ast.Expression:
        """Build the combiner expression from merged metric components."""


# =============================================================================
# Decomposition Registry
# =============================================================================

DECOMPOSITION_REGISTRY: dict[type, type[AggDecomposition] | None] = {}


def decomposes(func_class: type):
    """Decorator to register a decomposition class for a function."""

    def decorator(decomp_class: type[AggDecomposition]):
        DECOMPOSITION_REGISTRY[func_class] = decomp_class
        return decomp_class

    return decorator


def not_decomposable(func_class: type):
    """Mark a function as not decomposable (requires full dataset)."""
    DECOMPOSITION_REGISTRY[func_class] = None


# =============================================================================
# Simple Decompositions (accumulate == merge)
# =============================================================================


@decomposes(dj_functions.Sum)
class SumDecomposition(AggDecomposition):
    """SUM: simple additive aggregation."""

    @property
    def components(self) -> list[ComponentDef]:
        return [ComponentDef("_sum", "SUM", "SUM")]

    def combine(self, components: list[MetricComponent]):
        return make_func("SUM", components[0].name)


@decomposes(dj_functions.Max)
class MaxDecomposition(AggDecomposition):
    """MAX: maximum value is associative."""

    @property
    def components(self) -> list[ComponentDef]:
        return [ComponentDef("_max", "MAX", "MAX")]

    def combine(self, components: list[MetricComponent]):
        return make_func("MAX", components[0].name)


@decomposes(dj_functions.Min)
class MinDecomposition(AggDecomposition):
    """MIN: minimum value is associative."""

    @property
    def components(self) -> list[ComponentDef]:
        return [ComponentDef("_min", "MIN", "MIN")]

    def combine(self, components: list[MetricComponent]):
        return make_func("MIN", components[0].name)


@decomposes(dj_functions.AnyValue)
class AnyValueDecomposition(AggDecomposition):
    """ANY_VALUE: any value from the group."""

    @property
    def components(self) -> list[ComponentDef]:
        return [ComponentDef("_any_value", "ANY_VALUE", "ANY_VALUE")]

    def combine(self, components: list[MetricComponent]):
        return make_func("ANY_VALUE", components[0].name)


# =============================================================================
# Count Decompositions (accumulate=COUNT, merge=SUM)
# =============================================================================


@decomposes(dj_functions.Count)
class CountDecomposition(AggDecomposition):
    """COUNT: count rows, merge by summing counts."""

    @property
    def components(self) -> list[ComponentDef]:
        return [ComponentDef("_count", "COUNT", "SUM")]

    def combine(self, components: list[MetricComponent]):
        return make_func("SUM", components[0].name)


@decomposes(dj_functions.CountIf)
class CountIfDecomposition(AggDecomposition):
    """COUNT_IF: conditional count, merge by summing counts."""

    @property
    def components(self) -> list[ComponentDef]:
        return [ComponentDef("_count_if", "COUNT_IF", "SUM")]

    def combine(self, components: list[MetricComponent]):
        return make_func("SUM", components[0].name)


# =============================================================================
# AVG Decomposition (needs sum and count)
# =============================================================================


@decomposes(dj_functions.Avg)
class AvgDecomposition(AggDecomposition):
    """AVG = SUM / COUNT, requires two components."""

    @property
    def components(self) -> list[ComponentDef]:
        return [
            ComponentDef("_sum", "SUM", "SUM"),
            ComponentDef("_count", "COUNT", "SUM"),
        ]

    def combine(self, components: list[MetricComponent]):
        return ast.BinaryOp(
            op=ast.BinaryOpKind.Divide,
            left=make_func("SUM", components[0].name),
            right=make_func("SUM", components[1].name),
        )


# =============================================================================
# HLL Sketch Decomposition (approximate distinct count)
# =============================================================================


@decomposes(dj_functions.ApproxCountDistinct)
class ApproxCountDistinctDecomposition(AggDecomposition):
    """
    APPROX_COUNT_DISTINCT using HyperLogLog sketches.

    Uses Spark function names (hll_sketch_agg, hll_union, hll_sketch_estimate).
    Translation to other dialects happens in the transpilation layer.
    """

    @property
    def components(self) -> list[ComponentDef]:
        return [
            ComponentDef(
                suffix="_hll",
                accumulate="hll_sketch_agg",
                merge="hll_union_agg",
            ),
        ]

    def combine(self, components: list[MetricComponent]):
        return make_func(
            "hll_sketch_estimate",
            make_func("hll_union_agg", components[0].name),
        )


# =============================================================================
# Variance Decompositions
# =============================================================================


class VarianceDecompositionBase(AggDecomposition):
    """
    Base class for variance decompositions.

    Variance can be computed from three components:
    - sum(x): sum of values
    - sum(x²): sum of squared values
    - count: number of values

    VAR_POP = E[X²] - E[X]² = sum(x²)/n - (sum(x)/n)²
    VAR_SAMP uses Bessel's correction: n/(n-1) * VAR_POP
    """

    @property
    def components(self) -> list[ComponentDef]:
        return [
            ComponentDef("_sum", "SUM", "SUM"),
            ComponentDef("_sum_sq", "SUM(POWER({}, 2))", "SUM"),  # sum of x²
            ComponentDef("_count", "COUNT", "SUM"),
        ]

    def _make_var_pop(self, components: list[MetricComponent]) -> ast.Expression:
        """Build VAR_POP: E[X²] - E[X]²"""
        sum_col = components[0].name
        sum_sq_col = components[1].name
        count_col = components[2].name

        # E[X²] = SUM(sum_sq) / SUM(count)
        mean_of_squares = ast.BinaryOp(
            op=ast.BinaryOpKind.Divide,
            left=make_func("SUM", sum_sq_col),
            right=make_func("SUM", count_col),
        )

        # E[X]² = (SUM(sum) / SUM(count))²
        square_of_mean = make_func(
            "POWER",
            ast.BinaryOp(
                op=ast.BinaryOpKind.Divide,
                left=make_func("SUM", sum_col),
                right=make_func("SUM", count_col),
            ),
            ast.Number(2),
        )

        return ast.BinaryOp(
            op=ast.BinaryOpKind.Minus,
            left=mean_of_squares,
            right=square_of_mean,
        )

    def _make_var_samp(self, components: list[MetricComponent]) -> ast.Expression:
        """
        Build VAR_SAMP with Bessel's correction.

        = (n * SUM(x²) - SUM(x)²) / (n * (n-1))
        where n = SUM(count)
        """
        sum_col = components[0].name
        sum_sq_col = components[1].name
        count_col = components[2].name

        n = make_func("SUM", count_col)
        sum_x = make_func("SUM", sum_col)
        sum_x_sq = make_func("SUM", sum_sq_col)

        # Numerator: n * sum_x_sq - sum_x²
        numerator = ast.BinaryOp(
            op=ast.BinaryOpKind.Minus,
            left=ast.BinaryOp(op=ast.BinaryOpKind.Multiply, left=n, right=sum_x_sq),
            right=make_func("POWER", sum_x, ast.Number(2)),
        )

        # Denominator: n * (n - 1)
        denominator = ast.BinaryOp(
            op=ast.BinaryOpKind.Multiply,
            left=n,
            right=ast.BinaryOp(op=ast.BinaryOpKind.Minus, left=n, right=ast.Number(1)),
        )

        return ast.BinaryOp(
            op=ast.BinaryOpKind.Divide,
            left=numerator,
            right=denominator,
        )


@decomposes(dj_functions.VarPop)
class VarPopDecomposition(VarianceDecompositionBase):
    """Population variance: E[X²] - E[X]²"""

    def combine(self, components: list[MetricComponent]):
        return self._make_var_pop(components)


@decomposes(dj_functions.VarSamp)
class VarSampDecomposition(VarianceDecompositionBase):
    """Sample variance with Bessel's correction."""

    def combine(self, components: list[MetricComponent]):
        return self._make_var_samp(components)


@decomposes(dj_functions.Variance)
class VarianceDecomposition(VarianceDecompositionBase):
    """VARIANCE (alias for VAR_SAMP in Spark)."""

    def combine(self, components: list[MetricComponent]):
        return self._make_var_samp(components)  # pragma: no cover


# =============================================================================
# Standard Deviation Decompositions (square root of variance)
# =============================================================================


@decomposes(dj_functions.StddevPop)
class StddevPopDecomposition(VarianceDecompositionBase):
    """Population standard deviation: sqrt(VAR_POP)"""

    def combine(self, components: list[MetricComponent]):
        return make_func("SQRT", self._make_var_pop(components))


@decomposes(dj_functions.StddevSamp)
class StddevSampDecomposition(VarianceDecompositionBase):
    """Sample standard deviation: sqrt(VAR_SAMP)"""

    def combine(self, components: list[MetricComponent]):
        return make_func("SQRT", self._make_var_samp(components))


@decomposes(dj_functions.Stddev)
class StddevDecomposition(VarianceDecompositionBase):
    """STDDEV (alias for STDDEV_SAMP in Spark)."""

    def combine(self, components: list[MetricComponent]):
        return make_func("SQRT", self._make_var_samp(components))  # pragma: no cover


# =============================================================================
# Covariance Decompositions (two-argument functions)
# =============================================================================


class CovarianceDecompositionBase(AggDecomposition):
    """
    Base class for covariance decompositions.

    Covariance between X and Y can be computed from four components:
    - sum(x): sum of first variable
    - sum(y): sum of second variable
    - sum(x*y): sum of products
    - count: number of pairs

    COVAR_POP = E[XY] - E[X]*E[Y] = sum(xy)/n - (sum(x)/n)*(sum(y)/n)
    COVAR_SAMP uses Bessel's correction
    """

    @property
    def components(self) -> list[ComponentDef]:
        return [
            ComponentDef("_sum_x", "SUM({0})", "SUM", arg_index=0),
            ComponentDef("_sum_y", "SUM({1})", "SUM", arg_index=1),
            ComponentDef("_sum_xy", "SUM({0} * {1})", "SUM", arg_index=None),
            ComponentDef("_count", "COUNT({0})", "SUM", arg_index=0),
        ]

    def _make_covar_pop(self, components: list[MetricComponent]) -> ast.Expression:
        """Build COVAR_POP: E[XY] - E[X]*E[Y]"""
        sum_x_col = components[0].name
        sum_y_col = components[1].name
        sum_xy_col = components[2].name
        count_col = components[3].name

        n = make_func("SUM", count_col)
        sum_x = make_func("SUM", sum_x_col)
        sum_y = make_func("SUM", sum_y_col)
        sum_xy = make_func("SUM", sum_xy_col)

        # E[XY] = sum(xy) / n
        mean_of_products = ast.BinaryOp(
            op=ast.BinaryOpKind.Divide,
            left=sum_xy,
            right=n,
        )

        # E[X] * E[Y] = (sum(x)/n) * (sum(y)/n)
        product_of_means = ast.BinaryOp(
            op=ast.BinaryOpKind.Multiply,
            left=ast.BinaryOp(op=ast.BinaryOpKind.Divide, left=sum_x, right=n),
            right=ast.BinaryOp(op=ast.BinaryOpKind.Divide, left=sum_y, right=n),
        )

        return ast.BinaryOp(
            op=ast.BinaryOpKind.Minus,
            left=mean_of_products,
            right=product_of_means,
        )

    def _make_covar_samp(self, components: list[MetricComponent]) -> ast.Expression:
        """
        Build COVAR_SAMP with Bessel's correction.

        = (n * sum(xy) - sum(x) * sum(y)) / (n * (n-1))
        """
        sum_x_col = components[0].name
        sum_y_col = components[1].name
        sum_xy_col = components[2].name
        count_col = components[3].name

        n = make_func("SUM", count_col)
        sum_x = make_func("SUM", sum_x_col)
        sum_y = make_func("SUM", sum_y_col)
        sum_xy = make_func("SUM", sum_xy_col)

        # Numerator: n * sum(xy) - sum(x) * sum(y)
        numerator = ast.BinaryOp(
            op=ast.BinaryOpKind.Minus,
            left=ast.BinaryOp(op=ast.BinaryOpKind.Multiply, left=n, right=sum_xy),
            right=ast.BinaryOp(op=ast.BinaryOpKind.Multiply, left=sum_x, right=sum_y),
        )

        # Denominator: n * (n - 1)
        denominator = ast.BinaryOp(
            op=ast.BinaryOpKind.Multiply,
            left=n,
            right=ast.BinaryOp(op=ast.BinaryOpKind.Minus, left=n, right=ast.Number(1)),
        )

        return ast.BinaryOp(
            op=ast.BinaryOpKind.Divide,
            left=numerator,
            right=denominator,
        )


@decomposes(dj_functions.CovarPop)
class CovarPopDecomposition(CovarianceDecompositionBase):
    """Population covariance: E[XY] - E[X]*E[Y]"""

    def combine(self, components: list[MetricComponent]):
        return self._make_covar_pop(components)


@decomposes(dj_functions.CovarSamp)
class CovarSampDecomposition(CovarianceDecompositionBase):
    """Sample covariance with Bessel's correction."""

    def combine(self, components: list[MetricComponent]):
        return self._make_covar_samp(components)


# =============================================================================
# Correlation Decomposition (CORR = COVAR / (STDDEV_X * STDDEV_Y))
# =============================================================================


@decomposes(dj_functions.Corr)
class CorrDecomposition(AggDecomposition):
    """
    Pearson correlation coefficient.

    CORR(X,Y) = COVAR(X,Y) / (STDDEV(X) * STDDEV(Y))

    Requires 6 components:
    - sum(x), sum(y): for means
    - sum(x²), sum(y²): for variances
    - sum(xy): for covariance
    - count: for all calculations
    """

    @property
    def components(self) -> list[ComponentDef]:
        return [
            ComponentDef("_sum_x", "SUM({0})", "SUM", arg_index=0),
            ComponentDef("_sum_y", "SUM({1})", "SUM", arg_index=1),
            ComponentDef("_sum_x_sq", "SUM(POWER({0}, 2))", "SUM", arg_index=0),
            ComponentDef("_sum_y_sq", "SUM(POWER({1}, 2))", "SUM", arg_index=1),
            ComponentDef("_sum_xy", "SUM({0} * {1})", "SUM", arg_index=None),
            ComponentDef("_count", "COUNT({0})", "SUM", arg_index=0),
        ]

    def combine(self, components: list[MetricComponent]) -> ast.Expression:
        """
        Build CORR: COVAR(X,Y) / (STDDEV(X) * STDDEV(Y))

        Using population formulas:
        COVAR_POP = E[XY] - E[X]*E[Y]
        VAR_POP = E[X²] - E[X]²
        """
        sum_x_col = components[0].name
        sum_y_col = components[1].name
        sum_x_sq_col = components[2].name
        sum_y_sq_col = components[3].name
        sum_xy_col = components[4].name
        count_col = components[5].name

        n = make_func("SUM", count_col)
        sum_x = make_func("SUM", sum_x_col)
        sum_y = make_func("SUM", sum_y_col)
        sum_x_sq = make_func("SUM", sum_x_sq_col)
        sum_y_sq = make_func("SUM", sum_y_sq_col)
        sum_xy = make_func("SUM", sum_xy_col)

        # Covariance numerator: n * sum(xy) - sum(x) * sum(y)
        covar_num = ast.BinaryOp(
            op=ast.BinaryOpKind.Minus,
            left=ast.BinaryOp(op=ast.BinaryOpKind.Multiply, left=n, right=sum_xy),
            right=ast.BinaryOp(op=ast.BinaryOpKind.Multiply, left=sum_x, right=sum_y),
        )

        # Variance X: n * sum(x²) - sum(x)²
        var_x = ast.BinaryOp(
            op=ast.BinaryOpKind.Minus,
            left=ast.BinaryOp(op=ast.BinaryOpKind.Multiply, left=n, right=sum_x_sq),
            right=make_func("POWER", sum_x, ast.Number(2)),
        )

        # Variance Y: n * sum(y²) - sum(y)²
        var_y = ast.BinaryOp(
            op=ast.BinaryOpKind.Minus,
            left=ast.BinaryOp(op=ast.BinaryOpKind.Multiply, left=n, right=sum_y_sq),
            right=make_func("POWER", sum_y, ast.Number(2)),
        )

        # Denominator: sqrt(var_x * var_y)
        denominator = make_func(
            "SQRT",
            ast.BinaryOp(op=ast.BinaryOpKind.Multiply, left=var_x, right=var_y),
        )

        return ast.BinaryOp(
            op=ast.BinaryOpKind.Divide,
            left=covar_num,
            right=denominator,
        )


# =============================================================================
# Non-Decomposable Aggregations
# =============================================================================

# These require access to the full dataset and cannot be pre-aggregated
not_decomposable(dj_functions.MaxBy)
not_decomposable(dj_functions.MinBy)


# =============================================================================
# Decomposition Lookup
# =============================================================================


def get_decomposition(func_class: type) -> AggDecomposition | None:
    """Get decomposition instance for a function class, or None if not decomposable."""
    decomp_class = DECOMPOSITION_REGISTRY.get(func_class)
    if decomp_class is None:
        return None
    return decomp_class()


# =============================================================================
# Decomposition Result
# =============================================================================


@dataclass
class DecompositionResult:
    """Result of decomposing an aggregation function."""

    components: list[MetricComponent]
    combiner: ast.Node


@dataclass
class BaseMetricData:
    """Data for a single base metric."""

    name: str
    query: str


@dataclass
class MetricData:
    """All data needed to extract components from a metric."""

    query: str
    is_derived: bool
    base_metrics: list[BaseMetricData]


# =============================================================================
# Metric Component Extractor
# =============================================================================


class MetricComponentExtractor:
    """
    Extracts metric components from a metric definition and generates SQL derived
    from those components.

    For base metrics: decomposes aggregation functions (SUM, AVG, etc.) into components.
    For derived metrics: collects components from base metrics and substitutes references.
    """

    def __init__(self, node_revision_id: int):
        """
        Extract metric components from a specific metric revision.

        Args:
            node_revision_id: ID of the metric node revision
        """
        self._node_revision_id = node_revision_id

    @classmethod
    async def from_node_name(  # pragma: no cover
        cls,
        node_name: str,
        session: AsyncSession,
    ) -> "MetricComponentExtractor":
        """
        Create extractor for the latest version of a metric.

        Args:
            node_name: Name of the metric node
            session: Database session

        Returns:
            MetricComponentExtractor for the current revision
        """
        stmt = (
            select(NodeRevision.id)
            .join(Node, Node.id == NodeRevision.node_id)
            .where(
                Node.name == node_name,
                Node.current_version == NodeRevision.version,
            )
        )
        result = await session.execute(stmt)
        revision_id = result.scalar_one()
        return cls(revision_id)

    async def extract(
        self,
        session: AsyncSession,
        *,
        nodes_cache: dict[str, "Node"] | None = None,
        parent_map: dict[str, list[str]] | None = None,
        metric_node: "Node | None" = None,
        parsed_query_cache: dict[str, ast.Query] | None = None,
        _visited: set[str] | None = None,
    ) -> tuple[list[MetricComponent], ast.Query]:
        """
        Extract metric components from the query.

        For base metrics: decomposes aggregation functions into components.
        For derived metrics: collects components from base metrics and substitutes references.
        Supports nested derived metrics via recursive inline expansion.

        Args:
            session: Database session for loading metric data
            nodes_cache: Optional dict of node_name -> Node with current revision loaded.
                If provided along with parent_map and metric_node, avoids DB queries.
            parent_map: Optional dict of child_name -> list of parent_names.
                Required if nodes_cache is provided.
            metric_node: Optional metric Node object.
                Required if nodes_cache is provided.
            parsed_query_cache: Optional dict of query_string -> parsed AST.
                Used to avoid re-parsing the same query multiple times.
        """
        # Use cache if available, otherwise query DB
        if (
            nodes_cache is not None
            and parent_map is not None
            and metric_node is not None
        ):
            metric_data = self._build_metric_data_from_cache(
                metric_node,
                nodes_cache,
                parent_map,
            )
        else:
            metric_data = await self._load_metric_data(session)

        # Helper to parse with cache
        def cached_parse(query: str) -> ast.Query:
            if parsed_query_cache is not None:
                if query not in parsed_query_cache:  # pragma: no cover
                    parsed_query_cache[query] = parse(query)

                # Return a deep copy to avoid AST mutation issues
                return deepcopy(parsed_query_cache[query])  # pragma: no cover
            return parse(query)

        # Parse queries (pure computation, no DB)
        query_ast = cached_parse(metric_data.query)

        # Initialize visited set for cycle detection
        if _visited is None:
            _visited = set()

        # Add current metric to visited (use metric_node name if available)
        current_metric_name = metric_node.name if metric_node else None
        if current_metric_name:
            if current_metric_name in _visited:
                raise ValueError(
                    f"Circular metric reference detected: {current_metric_name}",
                )
            _visited.add(current_metric_name)

        # Extract components from each parent metric
        all_components = []
        components_tracker = set()
        base_metrics_data = {}

        for base_metric in metric_data.base_metrics:
            # Check if this parent metric is itself a derived metric
            is_parent_derived = False
            parent_node = None
            parent_revision_id = None

            if nodes_cache and parent_map:
                # Cache path: check parent_map for metric parents
                parent_metric_parents = [
                    name
                    for name in parent_map.get(base_metric.name, [])
                    if name in nodes_cache and nodes_cache[name].type == NodeType.METRIC
                ]
                is_parent_derived = len(parent_metric_parents) > 0
                if is_parent_derived:
                    parent_node = nodes_cache[base_metric.name]
                    parent_revision_id = parent_node.current.id
            else:
                # Non-cache path: query DB to check if parent metric has metric parents
                # Create an alias for the parent node table
                ParentNode = aliased(Node, name="parent_node")
                parent_check_stmt = (
                    select(
                        NodeRevision.id.label("revision_id"),
                    )
                    .select_from(Node)
                    .join(
                        NodeRevision,
                        (NodeRevision.node_id == Node.id)
                        & (Node.current_version == NodeRevision.version),
                    )
                    .join(
                        NodeRelationship,
                        NodeRelationship.child_id == NodeRevision.id,
                    )
                    .join(
                        ParentNode,
                        NodeRelationship.parent_id == ParentNode.id,
                    )
                    .where(
                        Node.name == base_metric.name,
                        ParentNode.type == NodeType.METRIC,
                    )
                )
                parent_check_result = await session.execute(parent_check_stmt)
                parent_row = parent_check_result.first()
                if parent_row:
                    is_parent_derived = True
                    parent_revision_id = parent_row.revision_id

            if is_parent_derived and parent_revision_id:
                # Recursively extract the derived metric (inline expansion)
                parent_extractor = MetricComponentExtractor(parent_revision_id)
                base_components, derived_ast = await parent_extractor.extract(
                    session,
                    nodes_cache=nodes_cache,
                    parent_map=parent_map,
                    metric_node=parent_node,
                    parsed_query_cache=parsed_query_cache,
                    _visited=_visited,
                )
            else:
                # True base metric - decompose aggregations
                base_ast = cached_parse(base_metric.query)
                base_components, derived_ast = self._extract_base(base_ast)

            for comp in base_components:
                if comp.name not in components_tracker:
                    components_tracker.add(comp.name)
                    all_components.append(comp)

            # Store for derived metric substitution
            base_metrics_data[base_metric.name] = (
                base_components,
                str(derived_ast.select.projection[0]),
            )

        # For derived metrics: substitute metric references in query
        # For base metrics: use the decomposed query directly
        if metric_data.is_derived:
            query_ast = self._substitute_metric_references(query_ast, base_metrics_data)
        else:
            # Base metric - use the decomposed AST directly
            query_ast = derived_ast

        return all_components, query_ast

    def _build_metric_data_from_cache(
        self,
        metric_node: "Node",
        nodes_cache: dict[str, "Node"],
        parent_map: dict[str, list[str]],
    ) -> MetricData:
        """
        Build MetricData from pre-loaded nodes cache instead of querying DB.

        Args:
            metric_node: The metric node to extract from
            nodes_cache: Dict of node_name -> Node with current revision loaded
            parent_map: Dict of child_name -> list of parent_names

        Returns:
            MetricData with query, is_derived flag, and list of base metrics
        """
        if not metric_node.current or not metric_node.current.query:
            raise ValueError(
                f"Metric {metric_node.name} has no query",
            )  # pragma: no cover

        # Find parent metrics from cache
        parent_names = parent_map.get(metric_node.name, [])
        metric_parents = [
            name
            for name in parent_names
            if name in nodes_cache and nodes_cache[name].type == NodeType.METRIC
        ]

        if metric_parents:
            # Derived metric - base metrics are the parent metrics
            return MetricData(
                query=metric_node.current.query,
                is_derived=True,
                base_metrics=[
                    BaseMetricData(
                        name=parent_name,
                        query=nodes_cache[parent_name].current.query,
                    )
                    for parent_name in metric_parents
                    if nodes_cache[parent_name].current
                    and nodes_cache[parent_name].current.query
                ],
            )
        else:
            # Base metric - base metric is itself
            return MetricData(
                query=metric_node.current.query,
                is_derived=False,
                base_metrics=[
                    BaseMetricData(
                        name=metric_node.name,
                        query=metric_node.current.query,
                    ),
                ],
            )

    async def _load_metric_data(self, session: AsyncSession) -> MetricData:
        """
        Load all metric data in a single query.

        Returns:
            MetricData with query, is_derived flag, and list of base metrics
        """
        # Query to get metric parents (if any)
        parent_stmt = (
            select(
                Node.name.label("parent_name"),
                NodeRevision.query.label("parent_query"),
            )
            .select_from(NodeRelationship)
            .join(Node, NodeRelationship.parent_id == Node.id)
            .join(
                NodeRevision,
                (NodeRevision.node_id == Node.id)
                & (NodeRevision.version == Node.current_version),
            )
            .where(
                NodeRelationship.child_id == self._node_revision_id,
                Node.type == NodeType.METRIC,
            )
        )
        parent_result = await session.execute(parent_stmt)
        parent_rows = parent_result.all()

        # Query to get this metric's own data
        this_metric_stmt = (
            select(
                NodeRevision.query,
                Node.name,
            )
            .join(Node, NodeRevision.node_id == Node.id)
            .where(
                NodeRevision.id == self._node_revision_id,
                Node.current_version == NodeRevision.version,
            )
        )
        this_result = await session.execute(this_metric_stmt)
        this_row = this_result.one()

        if parent_rows:
            # Derived metric - base metrics are the parents
            return MetricData(
                query=this_row.query,
                is_derived=True,
                base_metrics=[
                    BaseMetricData(name=row.parent_name, query=row.parent_query)
                    for row in parent_rows
                ],
            )
        else:
            # Base metric - base metric is itself
            return MetricData(
                query=this_row.query,
                is_derived=False,
                base_metrics=[BaseMetricData(name=this_row.name, query=this_row.query)],
            )

    def _extract_base(
        self,
        query_ast: ast.Query,
    ) -> tuple[list[MetricComponent], ast.Query]:
        """
        Extract components from a base metric by decomposing aggregations.

        Returns:
            Tuple of (components, modified_query_ast)
        """
        components: list[MetricComponent] = []
        components_tracker: set[str] = set()

        if query_ast.select.from_:  # pragma: no branch
            query_ast = self._normalize_aliases(query_ast)

            for func in query_ast.find_all(ast.Function):
                dj_function = func.function()
                if dj_function and dj_function.is_aggregation:
                    result = self._decompose(func, dj_function, query_ast)
                    if result:
                        # Apply combiner to AST
                        func.parent.replace(from_=func, to=result.combiner)  # type: ignore
                        # Collect unique components
                        for comp in sorted(result.components, key=lambda m: m.name):
                            if comp.name not in components_tracker:
                                components_tracker.add(comp.name)
                                components.append(comp)

        return components, query_ast

    def _substitute_metric_references(
        self,
        query_ast: ast.Query,
        base_metrics_data: dict[str, tuple[list[MetricComponent], str]],
    ) -> ast.Query:
        """
        Substitute metric references in derived metric query with their combiner expressions.

        Args:
            query_ast: The derived metric's query AST
            base_metrics_data: Dict of metric_name -> (components, combiner_expr)

        Returns:
            Modified query_ast with substitutions
        """
        # Find and replace metric references with their combiner expressions
        for col in list(query_ast.find_all(ast.Column)):
            col_name = col.identifier()
            if col_name in base_metrics_data:
                _, combiner_expr = base_metrics_data[col_name]
                if combiner_expr and col.parent:  # pragma: no branch
                    # Parse the combiner expression (wrap in SELECT to make it valid SQL)
                    combiner_ast = parse(f"SELECT {combiner_expr}").select.projection[0]
                    col.parent.replace(from_=col, to=combiner_ast)

        return query_ast

    def _normalize_aliases(self, query_ast: ast.Query) -> ast.Query:
        """
        Remove table aliases from the query to normalize column references.

        Returns:
            Modified query_ast
        """
        parent_node_alias = query_ast.select.from_.relations[  # type: ignore
            0
        ].primary.alias  # type: ignore
        if parent_node_alias:
            for col in query_ast.find_all(ast.Column):
                if col.namespace and col.namespace[0].name == parent_node_alias.name:
                    col.name = ast.Name(col.name.name)
            query_ast.select.from_.relations[0].primary.set_alias(None)  # type: ignore
        return query_ast

    def _decompose(
        self,
        func: ast.Function,
        dj_function: type,
        query_ast: ast.Query,
    ) -> DecompositionResult | None:
        """Decompose an aggregation function using the registry."""
        decomposition = get_decomposition(dj_function)

        if decomposition is None:
            # Not decomposable (e.g., MAX_BY, MIN_BY)
            return None

        # Build components from decomposition
        components = [
            self._make_component(func, comp_def, query_ast)
            for comp_def in decomposition.components
        ]

        # Build combiner AST
        is_distinct = func.quantifier == ast.SetQuantifier.Distinct

        if is_distinct:
            # DISTINCT aggregations can't be pre-aggregated, so keep original function
            # Just replace column references with component names
            combiner_ast: ast.Expression = ast.Function(
                func.name,
                args=[ast.Column(ast.Name(components[0].name))],
                quantifier=ast.SetQuantifier.Distinct,
            )
        else:
            combiner_ast = decomposition.combine(components)

        return DecompositionResult(components, combiner_ast)

    def _make_component(
        self,
        func: ast.Function,
        comp_def: ComponentDef,
        query_ast: ast.Query,
    ) -> MetricComponent:
        """Create a MetricComponent from a function and component definition."""
        is_distinct = func.quantifier == ast.SetQuantifier.Distinct

        # Determine which args to use based on arg_index
        if comp_def.arg_index is not None:
            # Single-arg component
            arg = func.args[comp_def.arg_index]
            expression = str(arg)
            columns = list(arg.find_all(ast.Column))
        else:
            # Multi-arg component (e.g., for CORR, COVAR)
            # Expression combines all args
            expression = " * ".join(str(a) for a in func.args)
            columns = []
            for a in func.args:
                columns.extend(a.find_all(ast.Column))

        # Build component name from columns in the expression
        if is_distinct:
            # DISTINCT uses column names + "_distinct"
            base_name = (
                "_".join(amenable_name(str(col)) for col in columns) + "_distinct"
            )
        elif columns:
            # Normal case: column names + suffix
            base_name = (
                "_".join(amenable_name(str(col)) for col in columns) + comp_def.suffix
            )
        else:
            # No columns (e.g., COUNT(*)) - use suffix without leading underscore
            base_name = comp_def.suffix.lstrip("_") or "count"

        short_hash = self._short_hash(expression, query_ast)

        # Build accumulate expression with template expansion
        accumulate_expr = self._expand_template(comp_def.accumulate, func.args)

        return MetricComponent(
            name=f"{base_name}_{short_hash}",
            expression=expression,
            aggregation=None if is_distinct else accumulate_expr,
            merge=None if is_distinct else comp_def.merge,
            rule=AggregationRule(
                type=Aggregability.LIMITED if is_distinct else Aggregability.FULL,
                level=[str(a) for a in func.args] if is_distinct else None,
            ),
        )

    def _expand_template(self, template: str, args: list) -> str:
        """
        Expand accumulate template with function arguments.

        Supports:
        - Simple function name: "SUM" -> "SUM"
        - Single placeholder {}: "SUM(POWER({}, 2))" -> "SUM(POWER(x, 2))"
        - Indexed placeholders: "SUM({0} * {1})" -> "SUM(x * y)"
        """
        if "{" not in template:
            # Simple function name, no expansion needed
            return template

        # Check for indexed placeholders {0}, {1}, etc.
        if "{0}" in template or "{1}" in template:
            result = template
            for i, arg in enumerate(args):
                result = result.replace(f"{{{i}}}", str(arg))
            return result

        # Single {} placeholder - use first arg
        return template.replace("{}", str(args[0]))

    def _short_hash(self, expression: str, query_ast: ast.Query) -> str:
        """Generate a short hash for the given expression."""
        signature = expression + str(query_ast.select.from_)
        return hashlib.md5(signature.encode("utf-8")).hexdigest()[:8]
