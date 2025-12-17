"""Used for extracting components from metric definitions."""

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import lru_cache

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
        merge: Phase 2 function - combine pre-aggregated values (e.g., "SUM", "hll_union")
    """

    suffix: str
    accumulate: str
    merge: str


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
        return [ComponentDef("_hll", "hll_sketch_agg", "hll_union")]

    def combine(self, components: list[MetricComponent]):
        return make_func(
            "hll_sketch_estimate",
            make_func("hll_union", components[0].name),
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
        return self._make_var_samp(components)


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
        return make_func("SQRT", self._make_var_samp(components))


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


# =============================================================================
# Metric Component Extractor
# =============================================================================


class MetricComponentExtractor:
    """
    Extracts metric components from a metric definition and generates SQL derived
    from those components.
    """

    def __init__(self, query_ast: ast.Query):
        self._components: list[MetricComponent] = []
        self._components_tracker: set[str] = set()
        self._query_ast = query_ast
        self._extracted = False

    @classmethod
    @lru_cache(maxsize=128)
    def from_query_string(cls, metric_query: str):
        """Create metric component extractor from query string."""
        query_ast = parse(metric_query)
        return MetricComponentExtractor(query_ast=query_ast)

    @classmethod
    def from_query_ast(cls, query_ast: ast.Query):  # pragma: no cover
        """Create metric component extractor from query AST."""
        return MetricComponentExtractor(query_ast=query_ast)  # pragma: no cover

    def extract(self) -> tuple[list[MetricComponent], ast.Query]:
        """
        Decomposes the metric query into its constituent aggregatable components
        and constructs a SQL query derived from those components.
        """
        if not self._extracted and self._query_ast.select.from_:
            self._normalize_aliases()

            for func in self._query_ast.find_all(ast.Function):
                dj_function = func.function()
                if dj_function and dj_function.is_aggregation:
                    result = self._decompose(func, dj_function)
                    if result:
                        # Apply combiner to AST
                        func.parent.replace(from_=func, to=result.combiner)  # type: ignore
                        # Collect unique components
                        for comp in sorted(result.components, key=lambda m: m.name):
                            if comp.name not in self._components_tracker:
                                self._components_tracker.add(comp.name)
                                self._components.append(comp)

            self._extracted = True
        return self._components, self._query_ast

    def _normalize_aliases(self):
        """Remove table aliases from the query to normalize column references."""
        parent_node_alias = self._query_ast.select.from_.relations[  # type: ignore
            0
        ].primary.alias  # type: ignore
        if parent_node_alias:
            for col in self._query_ast.find_all(ast.Column):
                if col.namespace and col.namespace[0].name == parent_node_alias.name:
                    col.name = ast.Name(col.name.name)
            self._query_ast.select.from_.relations[0].primary.set_alias(None)  # type: ignore

    def _decompose(
        self,
        func: ast.Function,
        dj_function: type,
    ) -> DecompositionResult | None:
        """Decompose an aggregation function using the registry."""
        decomposition = get_decomposition(dj_function)

        if decomposition is None:
            # Not decomposable (e.g., MAX_BY, MIN_BY)
            return None

        # Build components from decomposition
        components = [
            self._make_component(func, comp_def)
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
    ) -> MetricComponent:
        """Create a MetricComponent from a function and component definition."""
        arg = func.args[0]
        expression = str(arg)

        # Build component name from columns in the expression
        columns = list(arg.find_all(ast.Column))
        is_distinct = func.quantifier == ast.SetQuantifier.Distinct

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

        short_hash = self._short_hash(expression)

        return MetricComponent(
            name=f"{base_name}_{short_hash}",
            expression=expression,
            aggregation=None if is_distinct else comp_def.accumulate,
            merge=None if is_distinct else comp_def.merge,
            rule=AggregationRule(
                type=Aggregability.LIMITED if is_distinct else Aggregability.FULL,
                level=[str(a) for a in func.args] if is_distinct else None,
            ),
        )

    def _short_hash(self, expression: str) -> str:
        """Generate a short hash for the given expression."""
        signature = expression + str(self._query_ast.select.from_)
        return hashlib.md5(signature.encode("utf-8")).hexdigest()[:8]
