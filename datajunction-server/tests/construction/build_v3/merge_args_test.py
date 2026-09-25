"""
Tests for fixed merge arguments on a component's Phase 2 call.

Sketch families need tuning passed to the merge as well as the accumulate --
`nflx_tdigest_agg(digest, compression)` -- but `merge` itself has to stay a bare
function name, because the Druid aggregator mapping and the semi-additive
rewrite both match on it. `merge_args` carries the rest.

The omission this guards against is silent: a sketch merge called without its
compression is accepted by the engine and returns a digest collapsed to a single
centroid, so every quantile comes back equal to the mean.
"""

from datajunction_server.construction.build_v3.decomposition import build_merge_call
from datajunction_server.models.decompose import (
    AggregationRule,
    Aggregability,
    MetricComponent,
)
from datajunction_server.sql.decompose import ComponentDef
from datajunction_server.sql.parsing import ast


def _column(name: str = "latency_tdigest_ab12") -> ast.Column:
    return ast.Column(name=ast.Name(name))


class TestBuildMergeCall:
    """The helper both emission sites use."""

    def test_no_args_is_the_plain_single_argument_call(self):
        # Every non-sketch aggregation takes this path and must be unchanged.
        expr = build_merge_call("SUM", [], _column("revenue_sum_ab12"))
        assert str(expr) == "SUM(revenue_sum_ab12)"

    def test_one_fixed_argument_is_appended_after_the_column(self):
        expr = build_merge_call(
            "nflx_tdigest_agg",
            ["CAST(200.0 AS DOUBLE)"],
            _column(),
        )
        assert str(expr) == (
            "nflx_tdigest_agg(latency_tdigest_ab12, CAST(200.0 AS DOUBLE))"
        )

    def test_several_fixed_arguments_keep_their_order(self):
        expr = build_merge_call("sketch_merge", ["12", "'HLL_4'"], _column("s_ab12"))
        assert str(expr) == "sketch_merge(s_ab12, 12, 'HLL_4')"

    def test_the_column_is_always_the_first_argument(self):
        expr = build_merge_call("f", ["1"], _column("c"))
        assert isinstance(expr, ast.Function)
        assert str(expr.args[0]) == "c"

    def test_the_merge_name_is_preserved_verbatim(self):
        # Matched on elsewhere, so casing must not be normalized.
        expr = build_merge_call("nflx_tdigest_agg", ["1.0"], _column("c"))
        assert expr.name.name == "nflx_tdigest_agg"

    def test_arguments_are_parsed_not_pasted(self):
        # A literal arrives as an expression node, not raw text, so it renders
        # through the same dialect machinery as the rest of the tree.
        expr = build_merge_call("f", ["1 + 2"], _column("c"))
        assert isinstance(expr.args[1], ast.Expression)
        assert str(expr) == "f(c, 1 + 2)"


class TestComponentDefDefaults:
    """Existing decompositions must be untouched by the new field."""

    def test_merge_args_defaults_to_empty(self):
        assert (
            ComponentDef(suffix="_sum", accumulate="SUM", merge="SUM").merge_args == ()
        )

    def test_merge_args_is_a_tuple_so_the_default_cannot_be_mutated(self):
        first = ComponentDef(suffix="_sum", accumulate="SUM", merge="SUM")
        second = ComponentDef(suffix="_cnt", accumulate="COUNT", merge="SUM")
        assert first.merge_args == second.merge_args == ()
        assert isinstance(first.merge_args, tuple)

    def test_merge_args_is_declared_on_the_component_def(self):
        comp = ComponentDef(
            suffix="_tdigest",
            accumulate="nflx_tdigest({}, CAST(200.0 AS DOUBLE))",
            merge="nflx_tdigest_agg",
            merge_args=("CAST(200.0 AS DOUBLE)",),
        )
        assert comp.merge == "nflx_tdigest_agg"
        assert comp.merge_args == ("CAST(200.0 AS DOUBLE)",)


class TestMetricComponentField:
    """`merge_args` survives the model, which is dumped to JSON in cube configs."""

    def _component(self, merge_args):
        return MetricComponent(
            name="latency_tdigest_ab12",
            expression="latency_ms",
            aggregation="nflx_tdigest",
            merge="nflx_tdigest_agg",
            merge_args=merge_args,
            rule=AggregationRule(type=Aggregability.FULL),
        )

    def test_defaults_to_empty_list(self):
        component = MetricComponent(
            name="revenue_sum_ab12",
            expression="revenue",
            aggregation="SUM",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        )
        assert component.merge_args == []

    def test_round_trips_through_json(self):
        # A list rather than a tuple: these models are serialized straight into
        # materialization configs.
        component = self._component(["CAST(200.0 AS DOUBLE)"])
        assert component.model_dump()["merge_args"] == ["CAST(200.0 AS DOUBLE)"]

    def test_feeds_the_merge_call(self):
        component = self._component(["CAST(200.0 AS DOUBLE)"])
        expr = build_merge_call(
            component.merge,
            component.merge_args,
            _column(component.name),
        )
        assert str(expr) == (
            "nflx_tdigest_agg(latency_tdigest_ab12, CAST(200.0 AS DOUBLE))"
        )
