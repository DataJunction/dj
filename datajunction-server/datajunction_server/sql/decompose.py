from pydantic import BaseModel
from datajunction_server.sql import functions as dj_functions
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse


class Measure(BaseModel):
    """
    Measures are components used to build metrics (e.g., sales_amount, revenue, user_count).
    """

    name: str
    expression: str  # A SQL expression for defining the measure
    aggregation: str


class MeasureExtractor:
    def __init__(self):
        """Register handlers for aggregation functions"""
        self.handlers = {
            dj_functions.Sum: self._generic_additive_agg,
            dj_functions.Count: self._generic_additive_agg,
            dj_functions.Max: self._generic_additive_agg,
            dj_functions.Min: self._generic_additive_agg,
            dj_functions.Avg: self._avg,
        }

    def extract_measures(self, metric_query: str) -> tuple[list[Measure], ast.Query]:
        """
        Decomposes the metric query into its constituent aggregatable measures and
        constructs a SQL query derived from those measures.
        """
        query_ast = parse(metric_query)
        measures = []

        for func in query_ast.find_all(ast.Function):
            handler = self.handlers.get(func.function())
            if handler:
                func_measures = handler(func)
                if func_measures:
                    self._update_ast(func, func_measures)
                measures.extend(func_measures)

        return measures, query_ast

    def _generic_additive_agg(self, func) -> list[Measure]:
        arg = func.args[0]
        measure_name = "_".join([str(col) for col in arg.find_all(ast.Column)])
        return [
            Measure(
                name=measure_name,
                expression=str(arg),
                aggregation=func.name.name,
            ),
        ]

    def _avg(self, func) -> list[Measure]:
        arg = func.args[0]
        measure_name = "_".join([str(col) for col in arg.find_all(ast.Column)])
        return [
            Measure(
                name=measure_name,
                expression=str(arg),
                aggregation=dj_functions.Sum,
            ),
            Measure(
                name="count",
                expression="1",
                aggregation=dj_functions.Count,
            ),
        ]

    def _update_ast(self, func, measures: list[Measure]):
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
        elif func.function() == dj_functions.Count:
            func.name.name = "SUM"
            func.args = [ast.Column(ast.Name(measure.name)) for measure in measures]
        else:
            func.args = [ast.Column(ast.Name(measure.name)) for measure in measures]


extractor = MeasureExtractor()
