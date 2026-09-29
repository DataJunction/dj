"""
Regression tests for sketch measures reaching Druid ingestion.
"""

import pytest

from datajunction_server.api.cubes import _build_metrics_spec
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.cube_materialization import CombineMaterialization
from datajunction_server.models.decompose import AggregationRule, MetricComponent
from datajunction_server.models.materialization import MaterializationTarget
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.models.query import ColumnMetadata

TIMESTAMP_COLUMN = ColumnMetadata(name="order_date", type="int")


def _measure(
    name: str,
    aggregation: str | None,
    merge: str | None = None,
) -> MetricComponent:
    """A measure component as the decomposer would emit it."""
    return MetricComponent(
        name=name,
        expression=name,
        aggregation=aggregation,
        merge=merge,
        rule=AggregationRule(),
    )


def _combiner(
    measures: list[MetricComponent],
    measure_columns: list[ColumnMetadata],
) -> CombineMaterialization:
    """A combiner stage carrying real measures, ready to build a Druid spec."""
    return CombineMaterialization(
        node=NodeNameVersion(name="default.repairs_cube", version="v1.0"),
        columns=[TIMESTAMP_COLUMN, *measure_columns],
        grain=["order_date"],
        dimensions=["order_date"],
        measures=measures,
        timestamp_column="order_date",
        timestamp_format="yyyyMMdd",
        granularity=Granularity.DAY,
    )


class TestCombinerSketchMetricsSpec:
    """``CombineMaterialization.metrics_spec`` with measures that carry sketches."""

    def test_hll_measure_maps_to_hll_sketch_merge(self):
        """
        An HLL sketch measure reaches Druid as an HLLSketchMerge aggregator.
        """
        combiner = _combiner(
            measures=[
                _measure("customer_id_hll_23002251", "hll_sketch_agg", "hll_union_agg"),
            ],
            measure_columns=[
                ColumnMetadata(name="customer_id_hll_23002251", type="binary"),
            ],
        )

        assert combiner.metrics_spec() == [
            {
                "fieldName": "customer_id_hll_23002251",
                "name": "customer_id_hll_23002251",
                "type": "HLLSketchMerge",
                "lgK": 12,
                "tgtHllType": "HLL_4",
            },
        ]

    def test_hll_measure_is_neither_dropped_nor_downgraded(self):
        """
        The sketch measure survives as a sketch aggregator.
        """
        combiner = _combiner(
            measures=[
                _measure("customer_id_hll_23002251", "hll_sketch_agg", "hll_union_agg"),
            ],
            measure_columns=[
                ColumnMetadata(name="customer_id_hll_23002251", type="binary"),
            ],
        )

        spec = combiner.metrics_spec()

        assert len(spec) == 1, "sketch measure was dropped from the metricsSpec"
        assert spec[0]["type"] != "longSum"

    def test_plain_sum_measure_maps_to_long_sum(self):
        """A non-sketch measure still maps as before."""
        combiner = _combiner(
            measures=[_measure("total_repair_cost", "SUM", "SUM")],
            measure_columns=[
                ColumnMetadata(name="total_repair_cost", type="bigint"),
            ],
        )

        assert combiner.metrics_spec() == [
            {
                "fieldName": "total_repair_cost",
                "name": "total_repair_cost",
                "type": "longSum",
            },
        ]

    def test_merge_phase_takes_precedence_over_accumulate(self):
        """
        Pins which phase the aggregator lookup uses: ``merge``, not ``aggregation``.
        Druid ingests already-accumulated partials, so the merge function is the
        correct phase to key off.
        """
        combiner = _combiner(
            measures=[_measure("order_count", "COUNT", "SUM")],
            measure_columns=[ColumnMetadata(name="order_count", type="double")],
        )

        assert combiner.metrics_spec() == [
            {
                "fieldName": "order_count",
                "name": "order_count",
                "type": "doubleSum",
            },
        ]

    def test_percentile_measure_is_dropped_silently(self):
        """
        A percentile measure disappears from the ingestion spec without error.
        """
        combiner = _combiner(
            measures=[_measure("p95_latency", "approx_percentile")],
            measure_columns=[ColumnMetadata(name="p95_latency", type="double")],
        )

        assert combiner.metrics_spec() == []


class TestMetricsSpecBuildersAgree:
    """
    The two Druid metricsSpec builders, compared directly.

    ``CombineMaterialization.metrics_spec`` and ``api.cubes._build_metrics_spec``
    map measures for different entry points. They share ``get_druid_aggregator_spec``
    so the aggregator type and family config agree.
    """

    def test_builders_agree_on_hll_aggregator_type(self):
        """Both resolve an HLL sketch measure to the same aggregator type."""
        measure = _measure(
            "customer_id_hll_23002251",
            "hll_sketch_agg",
            "hll_union_agg",
        )
        measure_column = ColumnMetadata(
            name="customer_id_hll_23002251",
            type="binary",
        )

        v3_spec = _combiner([measure], [measure_column]).metrics_spec()
        cubes_api_spec = _build_metrics_spec([measure_column], [measure], {})

        assert v3_spec[0]["type"] == cubes_api_spec[0]["type"] == "HLLSketchMerge"

    def test_builders_agree_on_hll_tuning_config(self):
        """
        Both builders attach the same HLL tuning config.
        """
        measure = _measure(
            "customer_id_hll_23002251",
            "hll_sketch_agg",
            "hll_union_agg",
        )
        measure_column = ColumnMetadata(
            name="customer_id_hll_23002251",
            type="binary",
        )

        v3_entry = _combiner([measure], [measure_column]).metrics_spec()[0]
        cubes_api_entry = _build_metrics_spec([measure_column], [measure], {})[0]

        assert v3_entry["lgK"] == cubes_api_entry["lgK"] == 12
        assert v3_entry["tgtHllType"] == cubes_api_entry["tgtHllType"] == "HLL_4"

    def test_builders_disagree_on_unmappable_measures(self):
        """
        An unmappable measure is dropped by one builder and defaulted by the other.
        """
        measure = _measure("p95_latency", "approx_percentile")
        measure_column = ColumnMetadata(name="p95_latency", type="double")

        v3_spec = _combiner([measure], [measure_column]).metrics_spec()
        cubes_api_spec = _build_metrics_spec([measure_column], [measure], {})

        assert v3_spec == []
        assert cubes_api_spec[0]["type"] == "longSum"

    def test_unregistered_serialized_sketch_fails_in_both_builders(self):
        """A sketch cannot be silently omitted or ingested as a numeric sum."""
        measure = _measure("p95_latency", "custom_tdigest_agg", "custom_tdigest_agg")
        measure.params = {"compression": 200}
        measure.serialize = "custom_tdigest_sketch({})"
        measure.serialize_targets = [MaterializationTarget.DRUID]
        measure.serialize_type = "binary"
        column = ColumnMetadata(name="p95_latency", type="binary")

        with pytest.raises(DJInvalidInputException, match="No Druid aggregator"):
            _combiner([measure], [column]).metrics_spec()
        with pytest.raises(DJInvalidInputException, match="No Druid aggregator"):
            _build_metrics_spec([column], [measure], {})
