"""
Tests for typing an accumulate whose outermost call takes several arguments.

Type inference feeds a component exactly one input type. That is right for
every aggregation DJ ships, including the templated ones -- ``SUM(POWER(x, 2))``
is still a single-argument ``SUM`` on the outside. A sketch is the first shape
that breaks it: ``nflx_tdigest(latency_ms, CAST(200.0 AS DOUBLE))`` needs two,
so inference raised ``TypeError``, the caller swallowed it, and the column was
recorded as the metric's own type.

That mattered beyond cosmetics. The recorded type is persisted on the
pre-aggregation row and forwarded to the query service, which uses it to
create the materialized table -- so a struct-valued sketch column was being
described as ``double``.

``POWER`` stands in for a sketch accumulate here: OSS registers no sketch
families, and it is the available two-argument function whose inferred result
differs from the fallback.
"""

from types import SimpleNamespace

import pytest

from datajunction_server.construction.build_v3.measures import (
    _multi_argument_accumulate_types,
    infer_component_type,
)
from datajunction_server.models.decompose import AggregationRule, MetricComponent
from datajunction_server.models.materialization import MaterializationTarget
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing import types as ct


def _parent(column_type: str = "double"):
    return SimpleNamespace(
        current=SimpleNamespace(
            columns=[SimpleNamespace(name="latency_ms", type=column_type)],
        ),
    )


def _component(aggregation: str, **kwargs) -> MetricComponent:
    return MetricComponent(
        name="latency_acc_abc123",
        expression="latency_ms",
        aggregation=aggregation,
        merge="SUM",
        rule=AggregationRule(),
        **kwargs,
    )


class TestMultiArgumentTypes:
    """The helper resolves every argument, or declines entirely."""

    def test_resolves_a_column_and_a_literal(self):
        types = _multi_argument_accumulate_types("POWER(latency_ms, 2)", _parent())
        assert [str(t) for t in types] == ["double", "int"]

    def test_resolves_a_cast_without_binding_it_to_a_table(self):
        # Literals and casts carry their own type; only columns need the parent.
        types = _multi_argument_accumulate_types(
            "POWER(latency_ms, CAST(200.0 AS DOUBLE))",
            _parent(),
        )
        assert [str(t) for t in types] == ["double", "double"]

    def test_declines_without_a_parent_to_resolve_columns_against(self):
        assert _multi_argument_accumulate_types("POWER(latency_ms, 2)", None) is None

    def test_declines_for_a_bare_function_name(self):
        # "SUM" is a name, not a call -- the overwhelmingly common shape.
        assert _multi_argument_accumulate_types("SUM", _parent()) is None

    def test_declines_for_a_single_argument_call(self):
        # Including templated ones, whose outermost call still takes one arg.
        assert (
            _multi_argument_accumulate_types("SUM(POWER(latency_ms, 2))", _parent())
            is None
        )

    def test_declines_when_a_column_type_is_unrecognized(self):
        # Better to fall back than to invent a type for the stored schema.
        assert (
            _multi_argument_accumulate_types(
                "POWER(latency_ms, 2)",
                _parent("some_unknown_type"),
            )
            is None
        )

    def test_declines_when_an_argument_has_multiple_possible_types(self, monkeypatch):
        """Table-valued/lambda-style results cannot type a scalar argument."""
        call = ast.Function(
            name=ast.Name("test_accumulate"),
            args=[
                SimpleNamespace(type=[ct.IntegerType(), ct.StringType()]),
                SimpleNamespace(type=ct.IntegerType()),
            ],
        )
        parsed = SimpleNamespace(select=SimpleNamespace(projection=[call]))
        monkeypatch.setattr(
            "datajunction_server.construction.build_v3.measures.parse",
            lambda _: parsed,
        )

        assert _multi_argument_accumulate_types("ignored", _parent()) is None


class TestInferredColumnType:
    """What the measures column ends up recorded as."""

    def test_multi_argument_accumulate_is_typed_from_its_arguments(self):
        # The regression: this used to return the metric type, because
        # POWER.infer_type(double) raises and the error was swallowed.
        assert (
            infer_component_type(
                _component("POWER(latency_ms, 2)"),
                "bigint",
                _parent(),
            )
            == "double"
        )

    def test_single_argument_accumulate_is_unchanged(self):
        assert infer_component_type(_component("SUM"), "bigint", _parent()) == "double"

    def test_unresolvable_multi_argument_accumulate_falls_back(self):
        assert (
            infer_component_type(
                _component("POWER(latency_ms, 2)"),
                "bigint",
                _parent("some_unknown_type"),
            )
            == "bigint"
        )

    @pytest.mark.parametrize("target", [None, MaterializationTarget.ICEBERG])
    def test_unconverted_targets_get_the_argument_derived_type(self, target):
        assert (
            infer_component_type(
                _component("POWER(latency_ms, 2)"),
                "bigint",
                _parent(),
                target,
            )
            == "double"
        )

    def test_declared_serialize_type_still_wins_for_its_target(self):
        """
        The conversion type takes precedence over the accumulated one.

        Druid stores the converted representation, so the column must be
        described as that -- the aggregator lookup keys on it.
        """
        component = _component(
            "POWER(latency_ms, 2)",
            serialize="to_druid_bytes({})",
            serialize_targets=[MaterializationTarget.DRUID],
            serialize_type="binary",
        )
        assert (
            infer_component_type(
                component,
                "bigint",
                _parent(),
                MaterializationTarget.DRUID,
            )
            == "binary"
        )
