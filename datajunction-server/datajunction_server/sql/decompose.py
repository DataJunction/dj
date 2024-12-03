"""Used for extracting measures form metric definitions."""
from pydantic import BaseModel

from datajunction_server.enum import StrEnum
from datajunction_server.sql import functions as dj_functions
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse


class Aggregability(StrEnum):
    """
    Type of allowed aggregation for a given measure.
    """

    FULL = "full"
    LIMITED = "limited"
    NONE = "none"


class AggregationRule(BaseModel):
    """
    The aggregation rule for the measure. If the Aggregability type is LIMITED, the `level` should
    be specified to highlight the level at which the measure needs to be aggregated in order to
    support the specified aggregation function.

    For example, consider a metric like COUNT(DISTINCT user_id). It can be decomposed into a
    single measure with LIMITED aggregability, i.e., it is only aggregatable if the measure is
    calculated at the `user_id` level:
    - name: num_users
      expression: DISTINCT user_id
      aggregation: COUNT
      rule:
        type: LIMITED
        level: ["user_id"]
    """

    type: Aggregability = Aggregability.NONE
    level: list[str] | None = None


class Measure(BaseModel):
    """
    Measures are aggregated facts (e.g. SUM(view_secs)). They can be optionally combined
    to build derived metrics, e.g. SUM(clicks) / SUM(view_secs). Combining is optional because
    a stand-alone measure can itself be a metric.
    """

    name: str
    expression: str  # A SQL expression for defining the measure
    aggregation: str
    rule: AggregationRule


class MeasureExtractor:
    """
    Extracts aggregatable measures from a metric definition and generates SQL
    derived from those measures.
    """

    def __init__(self):
        """Register handlers for aggregation functions"""
        self.handlers = {
            dj_functions.Sum: self._simple_associative_agg,
            dj_functions.Count: self._simple_associative_agg,
            dj_functions.Max: self._simple_associative_agg,
            dj_functions.Min: self._simple_associative_agg,
            dj_functions.Avg: self._avg,
        }

    def extract_measures(self, metric_query: str) -> tuple[list[Measure], ast.Query]:
        """
        Decomposes the metric query into its constituent aggregatable measures and
        constructs a SQL query derived from those measures.
        """
        query_ast = parse(metric_query)
        measures = []

        for idx, func in enumerate(query_ast.find_all(ast.Function)):
            dj_function = func.function()
            handler = self.handlers.get(dj_function)
            if handler and dj_function.is_aggregation:
                if func_measures := handler(func, idx):  # pragma: no cover
                    MeasureExtractor.update_ast(func, func_measures)
                measures.extend(func_measures)

        return measures, query_ast

    def _simple_associative_agg(self, func, idx) -> list[Measure]:
        """
        Handles measures decomposition for a single-argument associative aggregation function.
        Examples: SUM, MAX, MIN, COUNT
        """
        arg = func.args[0]
        measure_name = "_".join(
            [str(col) for col in arg.find_all(ast.Column)] + [func.name.name.lower()],
        )
        return [
            Measure(
                name=f"{measure_name}_{idx}",
                expression=f"{func.quantifier} {arg}" if func.quantifier else str(arg),
                aggregation=func.name.name.upper(),
                rule=AggregationRule(
                    type=Aggregability.FULL
                    if func.quantifier != ast.SetQuantifier.Distinct
                    else Aggregability.LIMITED,
                ),
            ),
        ]

    def _avg(self, func, idx) -> list[Measure]:
        """
        Handles measures decomposition for AVG (it requires both the SUM and COUNT
        of the selected measure).
        """
        arg = func.args[0]
        measure_name = "_".join(
            [str(col) for col in arg.find_all(ast.Column)]
            + [dj_functions.Sum.__name__.lower()],
        )
        return [
            Measure(
                name=f"{measure_name}_{idx}",
                expression=str(arg),
                aggregation=dj_functions.Sum.__name__.upper(),
                rule=AggregationRule(
                    type=Aggregability.FULL
                    if func.quantifier != ast.SetQuantifier.Distinct
                    else Aggregability.LIMITED,
                ),
            ),
            Measure(
                name="count",
                expression="1",
                aggregation=dj_functions.Count.__name__.upper(),
                rule=AggregationRule(
                    type=Aggregability.FULL
                    if func.quantifier != ast.SetQuantifier.Distinct
                    else Aggregability.LIMITED,
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
                        ast.Name("COUNT"),
                        args=[ast.Column(ast.Name(measures[1].name))],
                    ),
                ),
            )
        elif (
            func.function() == dj_functions.Count
            and func.quantifier != ast.SetQuantifier.Distinct
        ):
            func.name.name = "SUM"
            func.args = [ast.Column(ast.Name(measure.name)) for measure in measures]
        else:
            func.args = [ast.Column(ast.Name(measure.name)) for measure in measures]


extractor = MeasureExtractor()
