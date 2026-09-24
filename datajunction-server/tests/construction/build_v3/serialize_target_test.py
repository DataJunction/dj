"""
Tests for the materialization-target serialize hook.

Some components must be written to destinations in a representation different
from their accumulated form. The fork keys off ``MaterializationTarget`` to
determine if the expression needs to be serialized during measures table writing.
"""

import pytest

from types import SimpleNamespace

from datajunction_server.construction.build_v3.decomposition import (
    build_component_expression,
)
from datajunction_server.construction.build_v3.measures import infer_component_type
from datajunction_server.models.decompose import AggregationRule, MetricComponent
from datajunction_server.models.materialization import MaterializationTarget


def _component(
    aggregation: str,
    serialize: str | None = None,
    serialize_targets: list[MaterializationTarget] | None = None,
) -> MetricComponent:
    return MetricComponent(
        name="latency_td_abc123",
        expression="latency_ms",
        aggregation=aggregation,
        merge="nflx_tdigest_agg",
        rule=AggregationRule(),
        serialize=serialize,
        serialize_targets=serialize_targets or [],
    )


DRUID = [MaterializationTarget.DRUID]


class TestSerializeIsApplied:
    """When the component declares a conversion and the target asks for it."""

    def test_simple_accumulate_is_wrapped(self):
        """A bare function-name accumulate gets the conversion."""
        component = _component(
            "nflx_tdigest",
            serialize="nflx_tdigest_sketch({})",
            serialize_targets=DRUID,
        )

        expr = build_component_expression(component, MaterializationTarget.DRUID)

        assert str(expr) == "nflx_tdigest_sketch(nflx_tdigest(latency_ms))"

    def test_template_accumulate_is_wrapped(self):
        """
        A pre-expanded template accumulate gets the conversion too.
        """
        component = _component(
            "nflx_tdigest(latency_ms, 200)",
            serialize="nflx_tdigest_sketch({})",
            serialize_targets=DRUID,
        )

        expr = build_component_expression(component, MaterializationTarget.DRUID)

        assert str(expr) == "nflx_tdigest_sketch(nflx_tdigest(latency_ms, 200))"


class TestSerializeIsSkipped:
    """Every path that must leave the accumulated expression untouched."""

    def test_no_target_means_no_conversion(self):
        """
        A query-time build passes no target and gets the unwrapped expression.
        """
        component = _component(
            "nflx_tdigest(latency_ms, 200)",
            serialize="nflx_tdigest_sketch({})",
            serialize_targets=DRUID,
        )

        expr = build_component_expression(component)

        assert str(expr) == "nflx_tdigest(latency_ms, 200)"

    def test_different_target_means_no_conversion(self):
        """
        An Iceberg pre-agg keeps the in-engine representation.
        """
        component = _component(
            "nflx_tdigest(latency_ms, 200)",
            serialize="nflx_tdigest_sketch({})",
            serialize_targets=DRUID,
        )

        expr = build_component_expression(component, MaterializationTarget.ICEBERG)

        assert str(expr) == "nflx_tdigest(latency_ms, 200)"

    def test_component_without_serialize_is_untouched(self):
        """
        A component declaring no conversion is unaffected even for Druid.
        """
        component = _component("SUM")

        expr = build_component_expression(component, MaterializationTarget.DRUID)

        assert str(expr) == "SUM(latency_ms)"

    @pytest.mark.parametrize(
        "target",
        [None, MaterializationTarget.DRUID, MaterializationTarget.ICEBERG],
    )
    def test_plain_sum_is_stable_across_targets(self, target):
        """
        An ordinary measure renders identically whatever the target.
        """
        expr = build_component_expression(_component("SUM"), target)

        assert str(expr) == "SUM(latency_ms)"


class TestSerializeDeclaration:
    """The declaration itself."""

    def test_targets_default_to_empty(self):
        """
        A component declaring a conversion but no targets never applies it.

        Defaulting to "no targets" rather than "all targets" prevents
        half-finished declarations from silently rewriting measures tables.
        """
        component = _component(
            "nflx_tdigest(latency_ms, 200)",
            serialize="nflx_tdigest_sketch({})",
        )

        expr = build_component_expression(component, MaterializationTarget.DRUID)

        assert str(expr) == "nflx_tdigest(latency_ms, 200)"


class TestSerializedColumnType:
    """
    The recorded column type must match the representation actually written.

    These are set in different places (SQL by ``build_component_expression``,
    type by ``infer_component_type``). A disagreement is silent and could cause
    Druid ingestion to miss the correct aggregator mappings.
    """

    @staticmethod
    def _parent():
        return SimpleNamespace(
            current=SimpleNamespace(
                columns=[SimpleNamespace(name="latency_ms", type="double")],
            ),
        )

    def _sketch_component(self, serialize_type: str | None = "binary"):
        """
        A component that converts on the way to Druid.
        """
        component = _component(
            "SUM",
            serialize="to_druid_bytes({})",
            serialize_targets=DRUID,
        )
        component.serialize_type = serialize_type
        return component

    def test_druid_target_records_the_converted_type(self):
        """The declared conversion type wins for the target that converts."""
        assert (
            infer_component_type(
                self._sketch_component(),
                "double",
                self._parent(),
                MaterializationTarget.DRUID,
            )
            == "binary"
        )

    @pytest.mark.parametrize(
        "target",
        [None, MaterializationTarget.ICEBERG],
    )
    def test_other_targets_keep_the_accumulated_type(self, target):
        """Query-time and Iceberg store the unconverted value, so keep its type."""
        assert (
            infer_component_type(
                self._sketch_component(),
                "double",
                self._parent(),
                target,
            )
            == "double"
        )

    def test_conversion_without_a_declared_type_falls_through(self):
        """
        Declaring a conversion but no type leaves inference in charge.
        """
        assert (
            infer_component_type(
                self._sketch_component(serialize_type=None),
                "double",
                self._parent(),
                MaterializationTarget.DRUID,
            )
            == "double"
        )

    def test_ordinary_component_is_unaffected(self):
        """A component with no conversion types exactly as it did before."""
        assert (
            infer_component_type(
                _component("SUM"),
                "double",
                self._parent(),
                MaterializationTarget.DRUID,
            )
            == "double"
        )


class TestSerializesFor:
    """The single predicate both the SQL and the type side consult."""

    @pytest.mark.parametrize(
        ("serialize", "targets", "target", "expected"),
        [
            ("f({})", DRUID, MaterializationTarget.DRUID, True),
            ("f({})", DRUID, MaterializationTarget.ICEBERG, False),
            ("f({})", DRUID, None, False),
            ("f({})", [], MaterializationTarget.DRUID, False),
            (None, DRUID, MaterializationTarget.DRUID, False),
        ],
    )
    def test_predicate(self, serialize, targets, target, expected):
        component = _component("SUM", serialize=serialize, serialize_targets=targets)

        assert component.serializes_for(target) is expected
