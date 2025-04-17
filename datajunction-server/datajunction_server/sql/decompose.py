"""Used for extracting measures form metric definitions."""

import hashlib
from functools import lru_cache

from datajunction_server.models.cube_materialization import (
    Aggregability,
    AggregationRule,
    Measure,
)
from datajunction_server.sql import functions as dj_functions
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse


class MeasureExtractor:
    """
    Extracts aggregatable measures from a metric definition and generates SQL
    derived from those measures.
    """

    def __init__(self, query_ast: ast.Query):
        self.handlers = {
            dj_functions.Sum: self._simple_associative_agg,
            dj_functions.Count: self._simple_associative_agg,
            dj_functions.CountIf: self._simple_associative_agg,
            dj_functions.Max: self._simple_associative_agg,
            dj_functions.MaxBy: self._simple_associative_agg,
            dj_functions.Min: self._simple_associative_agg,
            dj_functions.MinBy: self._simple_associative_agg,
            dj_functions.Avg: self._avg,
            dj_functions.AnyValue: self._simple_associative_agg,
        }

        # Outputs from decomposition
        self._measures: list[Measure] = []
        self._measures_tracker: set[str] = set()
        self._query_ast = query_ast
        self._extracted = False

    @classmethod
    @lru_cache(maxsize=128)
    def from_query_string(cls, metric_query: str):
        """Create measures extractor from query string"""
        query_ast = parse(metric_query)
        return MeasureExtractor(query_ast=query_ast)

    @classmethod
    def from_query_ast(cls, query_ast: ast.Query):  # pragma: no cover
        """Create measures extractor from query AST"""
        return MeasureExtractor(query_ast=query_ast)  # pragma: no cover

    def extract(self) -> tuple[list[Measure], ast.Query]:
        """
        Decomposes the metric query into its constituent aggregatable measures and
        constructs a SQL query derived from those measures.
        """
        if not self._extracted:
            # Normalize metric queries with aliases
            parent_node_alias = self._query_ast.select.from_.relations[  # type: ignore
                0
            ].primary.alias
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
                    if func_measures := handler(func):  # pragma: no cover
                        MeasureExtractor.update_ast(func, func_measures)

                    for measure in sorted(func_measures, key=lambda m: m.name):
                        if measure.name not in self._measures_tracker:
                            self._measures_tracker.add(measure.name)
                            self._measures.append(measure)

            self._extracted = True
        return self._measures, self._query_ast

    def _simple_associative_agg(self, func) -> list[Measure]:
        """
        Handles measures decomposition for a single-argument associative aggregation function.
        Examples: SUM, MAX, MIN, COUNT
        """
        arg = func.args[0]
        measure_name = "_".join(
            [str(col) for col in arg.find_all(ast.Column)] + [func.name.name.lower()],
        )
        expression = f"{func.quantifier} {arg}" if func.quantifier else str(arg)
        short_hash = hashlib.md5(expression.encode("utf-8")).hexdigest()[:8]

        return [
            Measure(
                name=f"{measure_name}_{short_hash}",
                expression=expression,
                aggregation=func.name.name.upper(),
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

    def _avg(self, func) -> list[Measure]:
        """
        Handles measures decomposition for AVG (it requires both the SUM and COUNT
        of the selected measure).
        """
        arg = func.args[0]
        measure_name = "_".join([str(col) for col in arg.find_all(ast.Column)])
        expression = str(arg)
        short_hash = hashlib.md5(expression.encode("utf-8")).hexdigest()[:8]
        return [
            Measure(
                name=f"{measure_name}_{dj_functions.Sum.__name__.lower()}_{short_hash}",
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
            Measure(
                name=f"{measure_name}_{dj_functions.Count.__name__.lower()}_{short_hash}",
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
    def update_ast(func, measures: list[Measure]):
        """
        Updates the query AST based on the measures derived from the function.
        """
        if func.function() == dj_functions.Avg:
            func.parent.replace(
                from_=func,
                to=ast.BinaryOp(
                    op=ast.BinaryOpKind.Divide,
                    left=ast.Function(
                        ast.Name("SUM"),
                        args=[ast.Column(ast.Name(measures[0].name))],
                    ),
                    right=ast.Function(
                        ast.Name("SUM"),
                        args=[ast.Column(ast.Name(measures[1].name))],
                    ),
                ),
            )
        elif (
            func.function() in (dj_functions.Count, dj_functions.CountIf)
            and func.quantifier != ast.SetQuantifier.Distinct
        ):
            func.name.name = "SUM"
            func.args = [ast.Column(ast.Name(measure.name)) for measure in measures]
        else:
            func.args = [ast.Column(ast.Name(measure.name)) for measure in measures]
