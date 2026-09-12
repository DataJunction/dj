"""Used for extracting components from metric definitions."""

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from datajunction_server.database.node import Node, NodeRelationship, NodeRevision
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.reaggregate import (
    ReaggregateSpec,
    dimension_reaggregate_rules,
    is_supported_dimension_reaggregate_function,
    parse_reaggregate_spec,
)
from datajunction_server.naming import amenable_col_names
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


def safe_denominator(expr: ast.Expression) -> ast.Expression:
    """Wrap expr in NULLIF(expr, 0) to make it safe as a divisor.

    Idempotent: if expr is already NULLIF(_, 0) or a numeric literal,
    returns it unchanged.  Caller passes the RHS of a Divide.
    """
    # Numeric literals: x / 100 doesn't need wrapping.
    if isinstance(expr, ast.Number):
        return expr
    # Already wrapped.
    if (
        isinstance(expr, ast.Function)
        and expr.name.name.upper() == "NULLIF"
        and len(expr.args) == 2
        and isinstance(expr.args[1], ast.Number)
        and expr.args[1].value == 0
    ):
        return expr
    return ast.Function(
        ast.Name("NULLIF"),
        args=[expr, ast.Number(value=0)],
    )


def wrap_divisions_in_nullif(expr: ast.Expression) -> ast.Expression:
    """Walk expr and wrap the RHS of every Divide BinaryOp in NULLIF(_, 0)
    so division-by-zero produces NULL instead of NaN/Infinity/error.

    Mutates and returns expr.  Idempotent via :func:`safe_denominator`.
    """
    for node in expr.find_all(ast.BinaryOp):
        if node.op != ast.BinaryOpKind.Divide:
            continue
        wrapped = safe_denominator(node.right)
        if wrapped is not node.right:
            node.right = wrapped
    return expr


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


def merge_inflates(merge: str | None) -> bool:
    """
    Whether re-applying this merge over duplicated rows inflates the result.

    Only a SUM merge re-adds the duplicates; MIN/MAX/hll_union_agg are idempotent,
    and a DISTINCT component has no merge at all. Callers holding an extracted
    MetricComponent use this; callers holding the original call use
    is_duplication_invariant.
    """
    return merge == "SUM"


# Aggregations that duplicate rows cannot inflate, but that have no decomposition to
# read a merge from. The bar is "extra copies of a row cannot change the answer", not
# "the answer is deterministic": a pick-one-row aggregation qualifies even though which
# row it picks is arbitrary, matching the verdict ANY_VALUE already gets from its
# ANY_VALUE merge. Anything sensitive to how many times a row appears -- ARRAY_AGG,
# COLLECT_LIST, MEDIAN, PERCENTILE, COUNT_MIN_SKETCH, the moment-based stats -- stays
# off the list and is treated as sensitive by default.
DUPLICATION_INVARIANT_AGGREGATIONS = frozenset(
    {
        # Picks a single row. dj_functions.Last is deliberately absent: LAST is a
        # grammar keyword (ORDER BY ... NULLS LAST), so LAST(x) doesn't parse and
        # can never reach a metric query.
        dj_functions.MaxBy,
        dj_functions.MinBy,
        dj_functions.First,
        dj_functions.FirstValue,
        dj_functions.LastValue,
        # Reads a set of distinct values
        dj_functions.CollectSet,
        dj_functions.ApproxCountDistinctDsHll,
        dj_functions.ApproxCountDistinctDsTheta,
        dj_functions.HllSketchAgg,
        dj_functions.HllUnionAgg,
        # Sketch merges: row-wise in Spark/Druid rather than true aggregates, but
        # idempotent either way, so duplication can't move them.
        dj_functions.HllUnion,
        dj_functions.HllSketchUnion,
        # Idempotent boolean AND
        dj_functions.Every,
    },
)


def is_duplication_invariant(func: ast.Function) -> bool:
    """
    Whether row duplication (e.g. from a fan-out join) leaves this aggregation call's
    result unchanged.

    DISTINCT reads a set, so it's invariant whatever the function. Otherwise the merge
    decides, for the aggregations that decompose. The rest have no merge to read, so
    the ones known to survive duplication are listed in
    DUPLICATION_INVARIANT_AGGREGATIONS and the remainder are assumed sensitive
    (fail closed).
    """
    if func.quantifier == ast.SetQuantifier.Distinct:
        return True
    func_class = func.function()
    if func_class in DUPLICATION_INVARIANT_AGGREGATIONS:
        return True
    decomposition = get_decomposition(func_class)
    if decomposition is None:
        return False
    return not any(
        merge_inflates(component.merge) for component in decomposition.components
    )


def is_metric_duplication_sensitive(query_ast: ast.Query) -> bool:
    """
    Whether row duplication (e.g. from a fan-out join) changes this metric's result:
    true if any aggregation it applies inflates under duplication.
    """
    for func in query_ast.find_all(ast.Function):
        if not func.function().is_aggregation:
            continue
        if not is_duplication_invariant(func):
            return True
    return False


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
    reaggregate: ReaggregateSpec | dict | None = None
    fixed_grain: list[str] | None = None


@dataclass
class MetricData:
    """All data needed to extract components from a metric."""

    query: str
    is_derived: bool
    base_metrics: list[BaseMetricData]


# =============================================================================
# Metric Component Extractor
# =============================================================================


# AVG of one repeated value returns that value, unlike SUM. But it only
# sees one value per frame if the window's own partition covers the
# declared grain -- see `_window_covers_the_grain`.
_SAFE_ONLY_WHEN_THE_WINDOW_COVERS_THE_GRAIN = frozenset({"AVG"})


def _is_windowed_aggregate(func: ast.Function) -> bool:
    """Whether ``func`` is an aggregate carrying its own OVER clause."""
    if func.over is None:
        return False
    try:
        dj_function = func.function()
    except KeyError:  # pragma: no cover - unknown functions are not aggregates
        return False
    return bool(dj_function and dj_function.is_aggregation)


def _window_covers_the_grain(
    fixed_grain: list[str],
    partition_cols: set[str],
) -> bool:
    """
    Whether a window's own partition preserves a name's declared grain.

    The global grain (`[]`) is constant on every row and therefore always
    covered. A non-empty grain is covered only when the window names every
    one of its dimensions in its own `PARTITION BY`.
    """
    return not fixed_grain or set(fixed_grain) <= partition_cols


def _names_accumulated_by_a_window(
    query_ast: ast.Query,
) -> tuple[set[str], dict[str, list[set[str]]]]:
    """
    Names a window frame would accumulate, split into two groups.

    The first is accumulated no matter what -- a plain `SUM`/`COUNT`-style
    window on it is never safe, regardless of any grain. The second is only
    unsafe if the name's *own* declared grain isn't covered by that window's
    partition, so it can't be decided here: a name with no `fixed_grain` of
    its own can still inherit one from a metric it's derived from, and that
    isn't known until decomposition resolves it. The second value maps each
    such name to every partition-by column set it appeared under, for the
    caller to check once it knows the name's real grain.

    Safe either way: a plain divisor, `ORDER BY`/`PARTITION BY`, and any
    `is_duplication_invariant` function (same test the fan-out guard uses).
    """
    accumulated: set[str] = set()
    grain_sensitive: dict[str, list[set[str]]] = {}
    for func in query_ast.find_all(ast.Function):
        if func.over is None:
            continue
        if is_duplication_invariant(func):
            continue
        fn_name = func.name.name.upper()
        names = {
            col.identifier() for arg in func.args for col in arg.find_all(ast.Column)
        }
        if fn_name in _SAFE_ONLY_WHEN_THE_WINDOW_COVERS_THE_GRAIN:
            # A bare column only -- `DATE_TRUNC('month', date_id)` groups
            # several `date_id` values together, so a column nested inside a
            # transformation doesn't preserve the grain the way naming it
            # directly does.
            partition_cols = {
                expr.identifier()
                for expr in func.over.partition_by
                if isinstance(expr, ast.Column)
            }
            for name in names:
                grain_sensitive.setdefault(name, []).append(partition_cols)
            continue
        accumulated |= names
    return accumulated, grain_sensitive


def _dimension_column(dimension: str) -> ast.Column:
    """
    A column node naming a dimension reference.

    Built rather than parsed. A grain entry is a dimension reference, not SQL,
    so interpolating it into a `SELECT` would both accept things that are not
    references (`a, b` silently truncating to `a`) and reject things that are:
    the role syntax in `v3.date.date_id[order]` is not valid SQL at all.
    """
    *namespace_parts, column = dimension.split(".")
    namespace: ast.Name | None = None
    for part in namespace_parts:
        namespace = ast.Name(part, namespace=namespace)
    return ast.Column(name=ast.Name(column, namespace=namespace))


def _broadcast_in_combiner(
    query_ast: ast.Query,
    component_name: str,
    merge_function: str,
    fixed_grain: list[str],
) -> None:
    """
    Re-merge the component's aggregate as a window, in place in the combiner.

    The merge call is replaced where it sits rather than the whole combiner being
    wrapped: `ROUND(SUM(x), 2)` broadcasts to `ROUND(SUM(SUM(x)) OVER (), 2)`, not
    to `SUM(ROUND(SUM(x), 2)) OVER ()`, which is a different number.

    Partitions name dimensions in their unresolved form. Each consumer resolves
    them for itself -- a CTE-qualified column for a query, a bare column for a
    cube -- through the same replacement every other dimension reference uses.
    """
    target = merge_function.upper()
    partition_by = [
        cast(ast.Expression, _dimension_column(dimension)) for dimension in fixed_grain
    ]
    for func in list(query_ast.find_all(ast.Function)):
        if func.name.name.upper() != target:
            continue
        if not any(
            col.name and col.name.name == component_name
            for col in func.find_all(ast.Column)
        ):
            continue
        # Mutated in place: `replace()` and `swap()` leave the projected tree
        # untouched, so substituting the node into its parent has no effect.
        func.args = [ast.Function(ast.Name(target), args=func.args)]
        func.over = ast.Over(partition_by=partition_by)
        return

    raise DJInvalidInputException(
        "Unsupported fixed_grain metric shape: could not find the component "
        "merge expression in the metric combiner.",
    )


def validate_fixed_grain_shape(query: str, fixed_grain: list[str]) -> None:
    """
    Raise if a base metric's own aggregate can't carry a declared fixed grain.

    For write-time use, so e.g. `COUNT(DISTINCT ...)` with a fixed_grain
    fails at creation rather than only when queried. A derived metric's
    query has no aggregate of its own -- that case is left to query time,
    where the error names the real problem (a grain belongs on the base).
    """
    query_ast = parse(query)
    has_aggregate = any(
        (dj_function := func.function()) and dj_function.is_aggregation
        for func in query_ast.find_all(ast.Function)
    )
    if has_aggregate:
        MetricComponentExtractor(0)._extract_base(
            query_ast,
            fixed_grain=fixed_grain,
        )


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

        query_ast = parse(metric_data.query)

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
        accumulated_by_window, grain_sensitive_windows = _names_accumulated_by_a_window(
            query_ast,
        )

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
                    _visited=_visited,
                )
            else:
                # True base metric - decompose aggregations
                base_ast = parse(base_metric.query)
                base_components, derived_ast = self._extract_base(
                    base_ast,
                    parse_reaggregate_spec(base_metric.reaggregate),
                    base_metric.fixed_grain,
                )

            # Checked on the extracted components, not this parent's own
            # declaration: decomposition recurses through derived parents, so a
            # grain declared two levels down still arrives on a component here.
            # The same applies to the AVG-class check below -- `base_metric`
            # itself may declare no grain and only inherit one this way.
            # A metric can combine components with *different* declared
            # grains (e.g. one global, one per-category) -- check every
            # distinct one, not just the first found, or an unsafe window on
            # a later component's grain would slip through unchecked.
            effective_fixed_grains = [
                comp.rule.fixed_grain
                for comp in base_components
                if comp.rule.fixed_grain is not None
            ]

            # A derived metric is only at risk when the frame actually
            # accumulates the parent; referencing it beside a window is fine.
            # A base metric has no reference to match, so the hazard is its own
            # aggregate already carrying a window — not a window anywhere in the
            # query, which `SUM(x - LAG(x) OVER (...))` has quite legitimately.
            if metric_data.is_derived:
                windowed_over_the_grain = (
                    base_metric.name in accumulated_by_window
                    or any(
                        not _window_covers_the_grain(
                            fixed_grain,
                            partition_cols,
                        )
                        for fixed_grain in effective_fixed_grains
                        for partition_cols in grain_sensitive_windows.get(
                            base_metric.name,
                            [],
                        )
                    )
                )
            else:
                windowed_over_the_grain = any(
                    _is_windowed_aggregate(func)
                    for func in parse(base_metric.query).find_all(ast.Function)
                )

            if windowed_over_the_grain and effective_fixed_grains:
                if metric_data.is_derived:
                    message = (
                        f"Metric `{base_metric.name}` declares `fixed_grain` (or "
                        "is derived from one), so a frame cannot accumulate it: "
                        "its value is the same on every row of the partition, and "
                        "the frame would add that same value once per row. "
                        "Referencing it beside a window is fine — for example "
                        "dividing a windowed metric by it."
                    )
                else:
                    message = (
                        f"Metric `{base_metric.name}` declares `fixed_grain` (or is "
                        "derived from one), so its own aggregate is already windowed "
                        "and cannot be broadcast on top of that."
                    )
                raise DJInvalidInputException(message)

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
            if metric_node.current.reaggregate:
                raise DJInvalidInputException(
                    "Semi-additive metrics must be base metrics in V1. "
                    f"Derived metric `{metric_node.name}` declares reaggregate.",
                )

            # `is not None`, not truthiness: `[]` is the global grain, and a
            # derived metric may not declare that either. Guards the cached
            # path, which serves `/sql/metrics/v3/` and bulk derivation.
            if metric_node.current.fixed_grain is not None:
                raise DJInvalidInputException(
                    "A grain must be declared on a base metric. Derived metric "
                    f"`{metric_node.name}` declares fixed_grain; declare it on the "
                    "metric being aggregated and divide by that instead.",
                )

            # Derived metric - base metrics are the parent metrics
            return MetricData(
                query=metric_node.current.query,
                is_derived=True,
                base_metrics=[
                    BaseMetricData(
                        name=parent_name,
                        query=nodes_cache[parent_name].current.query,
                        reaggregate=nodes_cache[parent_name].current.reaggregate,
                        fixed_grain=nodes_cache[parent_name].current.fixed_grain,
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
                        reaggregate=metric_node.current.reaggregate,
                        fixed_grain=metric_node.current.fixed_grain,
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
                NodeRevision.reaggregate.label("parent_reaggregate"),
                NodeRevision.fixed_grain.label("parent_fixed_grain"),
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
                NodeRevision.reaggregate,
                NodeRevision.fixed_grain,
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
            if this_row.reaggregate:
                raise DJInvalidInputException(
                    "Semi-additive metrics must be base metrics in V1. "
                    f"Derived metric `{this_row.name}` declares reaggregate.",
                )
            # Same rule on the uncached path, which serves `GET /metrics/{name}`
            # and the create-time background task. Neither guard covers the
            # other's callers, so both are load-bearing.
            if this_row.fixed_grain is not None:
                raise DJInvalidInputException(
                    "A grain must be declared on a base metric. Derived metric "
                    f"`{this_row.name}` declares fixed_grain; declare it on the "
                    "metric being aggregated and divide by that instead.",
                )

            # Derived metric - base metrics are the parents
            return MetricData(
                query=this_row.query,
                is_derived=True,
                base_metrics=[
                    BaseMetricData(
                        name=row.parent_name,
                        query=row.parent_query,
                        reaggregate=row.parent_reaggregate,
                        fixed_grain=row.parent_fixed_grain,
                    )
                    for row in parent_rows
                ],
            )
        else:
            # Base metric - base metric is itself
            return MetricData(
                query=this_row.query,
                is_derived=False,
                base_metrics=[
                    BaseMetricData(
                        name=this_row.name,
                        query=this_row.query,
                        reaggregate=this_row.reaggregate,
                        fixed_grain=this_row.fixed_grain,
                    ),
                ],
            )

    def _extract_base(
        self,
        query_ast: ast.Query,
        reaggregate: ReaggregateSpec | None = None,
        fixed_grain: list[str] | None = None,
    ) -> tuple[list[MetricComponent], ast.Query]:
        """
        Extract components from a base metric by decomposing aggregations.

        If any aggregation in the metric is non-decomposable (e.g. ``MIN_BY``,
        ``MAX_BY``), the whole metric is treated as non-decomposable — we
        return an empty components list and leave the AST untouched.  A
        mixed metric (some decomposable, some not) can't be safely
        pre-aggregated: the components that *did* decompose would lose
        track of columns referenced inside the non-decomposable functions,
        and the resulting measures table wouldn't contain everything the
        metric expression needs.

        Returns:
            Tuple of (components, modified_query_ast)
        """
        components: list[MetricComponent] = []
        components_tracker: set[str] = set()

        if query_ast.select.from_:  # pragma: no branch
            query_ast = self._normalize_aliases(query_ast)

            agg_funcs: list[tuple[ast.Function, type]] = []
            for func in query_ast.find_all(ast.Function):
                dj_function = func.function()
                if dj_function and dj_function.is_aggregation:
                    agg_funcs.append((func, dj_function))

            # If any aggregation is non-decomposable, abort decomposition
            # entirely — the metric is non-decomposable as a whole.
            if any(get_decomposition(dj_fn) is None for _, dj_fn in agg_funcs):
                if dimension_reaggregate_rules(reaggregate):
                    self._raise_unsupported_reaggregate_shape(
                        "dimension-specific reaggregation requires a "
                        "decomposable aggregation",
                    )
                if fixed_grain is not None:
                    # Ignoring the declaration would return the query-grain
                    # value under a name that claims otherwise.
                    self._raise_unsupported_fixed_grain_shape(
                        "a fixed grain requires a decomposable aggregate, and "
                        "this metric's aggregation cannot be split into measures",
                    )
                return [], query_ast

            for func, dj_function in agg_funcs:
                result = self._decompose(func, dj_function, query_ast)
                if result:  # pragma: no branch
                    # Apply combiner to AST
                    func.parent.replace(from_=func, to=result.combiner)  # type: ignore
                    # Collect unique components
                    for comp in sorted(result.components, key=lambda m: m.name):
                        if comp.name not in components_tracker:
                            components_tracker.add(comp.name)
                            components.append(comp)

            # Wrap user-authored divisions in the outer expression too.
            # _decompose only wraps the small combiners it builds for AVG /
            # variance / stddev / covariance; the user's top-level
            # expression (e.g. CAST(SUM(...)) / COUNT(*)) is unchanged
            # after sub-aggregation replacements and would otherwise emit
            # bare divisions.
            for proj in query_ast.select.projection:
                wrap_divisions_in_nullif(cast(ast.Expression, proj))

        if reaggregate is not None and dimension_reaggregate_rules(reaggregate):
            self._attach_reaggregate_spec(components, reaggregate)

        if fixed_grain is not None:
            self._attach_fixed_grain_spec(components, fixed_grain, query_ast)

        return components, query_ast

    def _attach_fixed_grain_spec(
        self,
        components: list[MetricComponent],
        fixed_grain: list[str],
        query_ast: ast.Query,
    ) -> None:
        """
        Broadcast the one measure that can carry a declared grain.

        The window goes into the combiner here rather than at SQL build time so
        that it travels with the decomposed metric. Everything downstream already
        resolves dimension references and preserves aggregate windows, so both
        the query path and the SQL stored on a materialized cube pick it up.

        Refusals raised here surface at deploy time for a deployment, but only
        at SQL build time for a metric created through the API — nothing
        decomposes it at write time.
        """
        if len(components) != 1:
            self._raise_unsupported_fixed_grain_shape(
                "a fixed grain requires exactly one component",
            )

        component = components[0]
        if (
            component.rule.type != Aggregability.FULL
            or component.aggregation is None
            or component.merge is None
        ):
            self._raise_unsupported_fixed_grain_shape(
                "a fixed grain requires one non-distinct fully-aggregatable "
                "component, because broadcasting re-applies the merge",
            )

        component.rule.fixed_grain = fixed_grain
        _broadcast_in_combiner(
            query_ast,
            component.name,
            cast(str, component.merge),
            fixed_grain,
        )

    @staticmethod
    def _raise_unsupported_fixed_grain_shape(reason: str) -> None:
        raise DJInvalidInputException(
            f"Unsupported fixed_grain metric shape: {reason}.",
        )

    def _attach_reaggregate_spec(
        self,
        components: list[MetricComponent],
        reaggregate: ReaggregateSpec,
    ) -> None:
        """
        Attach a dimension-specific reaggregation rule to one ordinary measure.
        """
        dimension_rules = dimension_reaggregate_rules(reaggregate)
        if len(dimension_rules) != 1:
            self._raise_unsupported_reaggregate_shape(
                "dimension-specific reaggregation requires exactly one rule",
            )

        if not is_supported_dimension_reaggregate_function(dimension_rules[0].fn):
            self._raise_unsupported_reaggregate_shape(
                f"unsupported dimension reaggregation function "
                f"`{dimension_rules[0].fn.value}`",
            )

        if len(components) != 1:
            self._raise_unsupported_reaggregate_shape(
                "dimension-specific reaggregation requires exactly one component",
            )

        component = components[0]
        if (
            component.rule.type != Aggregability.FULL
            or component.aggregation is None
            or component.merge is None
        ):
            self._raise_unsupported_reaggregate_shape(
                "dimension-specific reaggregation requires one non-distinct "
                "fully-aggregatable component",
            )

        component.rule.reaggregate = dimension_rules[0]

    @staticmethod
    def _raise_unsupported_reaggregate_shape(reason: str) -> None:
        raise DJInvalidInputException(
            f"Unsupported reaggregate metric shape: {reason}.",
        )

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

        if decomposition is None:  # pragma: no cover
            # Defensive: ``_extract_base`` filters non-decomposable
            # aggregations out before calling here, so this branch is
            # unreachable in practice.  Kept so direct callers (if any)
            # still get a sensible None return.
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
            # Decomposed AVG / variance / stddev / covariance all build
            # SUM(...) / SUM(count)-style combiners where the denominator
            # can legitimately be 0.  Wrap to produce NULL rather than
            # NaN/Infinity/error.
            combiner_ast = wrap_divisions_in_nullif(combiner_ast)

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

        # For plain-column DISTINCT (e.g. ``COUNT(DISTINCT account_id)``),
        # name the component after the bare column itself — there's no
        # expression ambiguity to disambiguate.  This keeps
        # ``component.name`` aligned with the column already projected
        # as a grain alias by measures.py: ``derived_expression`` and
        # the live measures SQL output reference the same identifier,
        # so external consumers (XP/ABlaze) can evaluate the persisted
        # expression directly against the materialized table.
        #
        # Complex DISTINCT (CASE/IF over a column) and non-DISTINCT
        # components still get a ``<base>_<hash>`` name — those need
        # the hash to distinguish different expressions that share a
        # column-name prefix.
        plain_distinct_col: str | None = None
        if is_distinct and comp_def.arg_index is not None:
            _arg = cast(ast.Expression, func.args[comp_def.arg_index])
            if isinstance(_arg, ast.Column):
                plain_distinct_col = _arg.name.name

        if plain_distinct_col is not None:
            component_name = plain_distinct_col
            grain_alias: str | None = plain_distinct_col
        else:
            # Build component name from columns in the expression
            if is_distinct:
                # DISTINCT uses column names + "_distinct"
                base_name = amenable_col_names(columns) + "_distinct"
            elif columns:
                # Normal case: column names + suffix
                base_name = amenable_col_names(columns) + comp_def.suffix
            else:
                # No columns (e.g., COUNT(*)) - use suffix without leading underscore
                base_name = comp_def.suffix.lstrip("_") or "count"

            short_hash = self._short_hash(expression, query_ast)
            component_name = f"{base_name}_{short_hash}"
            # Complex DISTINCT: grain alias equals the component name
            # so the projection alias stays a stable identifier.
            grain_alias = component_name if is_distinct else None

        # Build accumulate expression with template expansion
        accumulate_expr = self._expand_template(comp_def.accumulate, func.args)

        return MetricComponent(
            name=component_name,
            expression=expression,
            aggregation=None if is_distinct else accumulate_expr,
            merge=None if is_distinct else comp_def.merge,
            rule=AggregationRule(
                type=Aggregability.LIMITED if is_distinct else Aggregability.FULL,
                level=[str(a) for a in func.args] if is_distinct else None,
            ),
            grain_alias=grain_alias,
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
