"""Used for extracting components from metric definitions."""

import hashlib
from functools import lru_cache

from datajunction_server.models.cube_materialization import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.naming import amenable_name
from datajunction_server.sql import functions as dj_functions
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse


class MetricComponentExtractor:
    """
    Extracts metric components from a metric definition and generates SQL derived
    from those components.
    """

    def __init__(self, query_ast: ast.Query):
        self.handlers = {
            dj_functions.Sum: self._simple_associative_agg,
            dj_functions.Count: self._simple_associative_agg,
            dj_functions.CountIf: self._simple_associative_agg,
            dj_functions.Max: self._simple_associative_agg,
            dj_functions.MaxBy: self._max_min_by_agg,
            dj_functions.Min: self._simple_associative_agg,
            dj_functions.MinBy: self._max_min_by_agg,
            dj_functions.Avg: self._avg,
            dj_functions.AnyValue: self._simple_associative_agg,
        }

        # Outputs from decomposition
        self._components: list[MetricComponent] = []
        self._components_tracker: set[str] = set()
        self._query_ast = query_ast
        self._extracted = False

    @classmethod
    @lru_cache(maxsize=128)
    def from_query_string(cls, metric_query: str):
        """Create metric component extractor from query string"""
        query_ast = parse(metric_query)
        return MetricComponentExtractor(query_ast=query_ast)

    @classmethod
    def from_query_ast(cls, query_ast: ast.Query):  # pragma: no cover
        """Create metric component extractor from query AST"""
        return MetricComponentExtractor(query_ast=query_ast)  # pragma: no cover

    def extract(self) -> tuple[list[MetricComponent], ast.Query]:
        """
        Decomposes the metric query into its constituent aggregatable components and
        constructs a SQL query derived from those components.
        """
        if not self._extracted and self._query_ast.select.from_:
            # Normalize metric queries with aliases
            parent_node_alias = self._query_ast.select.from_.relations[  # type: ignore
                0
            ].primary.alias  # type: ignore
            if parent_node_alias:
                for col in self._query_ast.find_all(ast.Column):
                    if (
                        col.namespace
                        and col.namespace[0].name == parent_node_alias.name
                    ):
                        col.name = ast.Name(col.name.name)
                self._query_ast.select.from_.relations[0].primary.set_alias(None)  # type: ignore

            for func in self._query_ast.find_all(ast.Function):
                dj_function = func.function()
                handler = self.handlers.get(dj_function)
                if handler and dj_function.is_aggregation:
                    if func_components := handler(func):  # pragma: no cover
                        MetricComponentExtractor.update_ast(func, func_components)
                    for component in sorted(func_components, key=lambda m: m.name):
                        if component.name not in self._components_tracker:
                            self._components_tracker.add(component.name)
                            self._components.append(component)

            self._extracted = True
        return self._components, self._query_ast

    def _short_hash(self, expression: str) -> str:
        """
        Generates a short hash for the given expression.
        """
        signature = expression + str(self._query_ast.select.from_)
        return hashlib.md5(signature.encode("utf-8")).hexdigest()[:8]

    def _simple_associative_agg(self, func: ast.Function) -> list[MetricComponent]:
        """
        Handles decomposition for a single-argument associative aggregation function.
        Examples: SUM, MAX, MIN, COUNT
        """
        # Handle the case where the quantifier is DISTINCT, where we need to generate a
        # component that represents the dimension column with the distinct quantifier.
        arg = func.args[0]
        if func.quantifier == ast.SetQuantifier.Distinct:
            component_name = "_".join(
                [amenable_name(str(col)) for col in arg.find_all(ast.Column)]
                + ["distinct"],
            )
        else:
            component_name = "_".join(
                [amenable_name(str(col)) for col in arg.find_all(ast.Column)]
                + [func.name.name.lower()],
            )

        expression = str(arg)
        # TODO: we should only hash on the lowercase version of the expression, but we can't
        # do this migration until after we get versioned measures
        short_hash = self._short_hash(expression)

        return [
            MetricComponent(
                name=f"{component_name}_{short_hash}",
                expression=expression,
                aggregation=func.name.name.upper()
                if func.quantifier != ast.SetQuantifier.Distinct
                else None,
                rule=AggregationRule(
                    type=Aggregability.FULL
                    if func.quantifier != ast.SetQuantifier.Distinct
                    else Aggregability.LIMITED,
                    level=(
                        [str(arg) for arg in func.args]
                        if func.quantifier == ast.SetQuantifier.Distinct
                        else None
                    ),
                ),
            ),
        ]

    def _max_min_by_agg(self, func: ast.Function) -> list[MetricComponent]:
        """
        Handles decomposition for MAX_BY or MIN_BY aggregation functions.

        MAX_BY/MIN_BY functions cannot be pre-aggregated because they depend on finding
        the row with the maximum/minimum ordering value across the entire dataset.
        Pre-aggregating would lose the ordering relationship between measure and ordering
        values, making it impossible to correctly reconstruct the result.

        Example:
          MAX_BY(clicked, dateint) must be computed on the full dataset to find
          the 'clicked' value corresponding to the row with the highest 'dateint'.

        Args:
            func: The MAX_BY or MIN_BY function AST node

        Returns:
            An empty list which captures that this function cannot be decomposed.
        """
        return []

    def _avg(self, func: ast.Function) -> list[MetricComponent]:
        """
        Handles decomposition for AVG (it requires both the SUM and COUNT of
        the function's argument).
        """
        arg = func.args[0]
        component_name = "_".join([str(col) for col in arg.find_all(ast.Column)])
        expression = str(arg)
        short_hash = self._short_hash(expression)
        return [
            MetricComponent(
                name=f"{component_name}_{dj_functions.Sum.__name__.lower()}_{short_hash}",
                expression=expression,
                aggregation=dj_functions.Sum.__name__.upper(),
                rule=AggregationRule(
                    type=Aggregability.FULL
                    if func.quantifier != ast.SetQuantifier.Distinct
                    else Aggregability.LIMITED,
                    level=(
                        [str(arg) for arg in func.args]
                        if func.quantifier == ast.SetQuantifier.Distinct
                        else None
                    ),
                ),
            ),
            MetricComponent(
                name=f"{component_name}_{dj_functions.Count.__name__.lower()}_{short_hash}",
                expression=expression,
                aggregation=dj_functions.Count.__name__.upper(),
                rule=AggregationRule(
                    type=Aggregability.FULL
                    if func.quantifier != ast.SetQuantifier.Distinct
                    else Aggregability.LIMITED,
                    level=(
                        [str(arg) for arg in func.args]
                        if func.quantifier == ast.SetQuantifier.Distinct
                        else None
                    ),
                ),
            ),
        ]

    @staticmethod
    def update_ast(func: ast.Function, components: list[MetricComponent]):
        """
        Updates the query AST based on the metric components derived from the function.
        """
        function = func.function()
        if function == dj_functions.Avg:
            func.parent.replace(  # type: ignore
                from_=func,
                to=ast.BinaryOp(
                    op=ast.BinaryOpKind.Divide,
                    left=ast.Function(
                        ast.Name("SUM"),
                        args=[ast.Column(ast.Name(components[0].name))],
                    ),
                    right=ast.Function(
                        ast.Name("SUM"),
                        args=[ast.Column(ast.Name(components[1].name))],
                    ),
                ),
            )
        elif function in (dj_functions.Count, dj_functions.CountIf):
            if func.quantifier != ast.SetQuantifier.Distinct:
                func.name.name = "SUM"
                func.args = [
                    ast.Column(ast.Name(component.name)) for component in components
                ]
            else:
                func.args = [
                    ast.Column(ast.Name(component.name)) for component in components
                ]
        else:
            func.args = [
                ast.Column(ast.Name(component.name)) for component in components
            ]
