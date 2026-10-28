"""
Tests for decimal measures reaching Druid ingestion.
"""

import pytest

from datajunction_server.api.cubes import _build_metrics_spec
from datajunction_server.models.cube_materialization import CombineMaterialization
from datajunction_server.models.decompose import AggregationRule, MetricComponent
from datajunction_server.models.materialization import find_druid_aggregator
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.models.query import ColumnMetadata

TIMESTAMP_COLUMN = ColumnMetadata(name="order_date", type="int")


def _measure(name: str, aggregation: str) -> MetricComponent:
    """A measure component as the decomposer would emit it."""
    return MetricComponent(
        name=name,
        expression=name,
        aggregation=aggregation,
        merge=aggregation,
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


@pytest.mark.parametrize(
    "column_type, agg_func, expected",
    [
        ("decimal", "sum", "doubleSum"),
        ("decimal(19, 6)", "sum", "doubleSum"),
        ("decimal(19,6)", "SUM", "doubleSum"),
        ("decimal(8, 2)", "min", "doubleMin"),
        ("decimal(8, 2)", "max", "doubleMax"),
        ("bigint", "sum", "longSum"),
        ("varchar(10)", "sum", None),
        ("decimal(19, 6)", "approx_percentile", None),
    ],
)
def test_find_druid_aggregator(column_type, agg_func, expected):
    """Type parameters are ignored when looking up the aggregator."""
    assert find_druid_aggregator(column_type, agg_func) == expected


class TestCombinerDecimalMetricsSpec:
    """``CombineMaterialization.metrics_spec`` with decimal measures."""

    def test_decimal_sum_maps_to_double_sum(self):
        """A decimal sum is kept and ingested as a double."""
        combiner = _combiner(
            measures=[_measure("mvs_sum", "SUM")],
            measure_columns=[ColumnMetadata(name="mvs_sum", type="decimal(19, 6)")],
        )

        assert combiner.metrics_spec() == [
            {"fieldName": "mvs_sum", "name": "mvs_sum", "type": "doubleSum"},
        ]

    def test_decimal_measures_sit_beside_bigint_measures(self):
        """Bigint and decimal sums both reach the spec, in measure order."""
        combiner = _combiner(
            measures=[
                _measure("starter_cnt_sum", "SUM"),
                _measure("mvs_sum", "SUM"),
                _measure("view_hours_sum", "SUM"),
            ],
            measure_columns=[
                ColumnMetadata(name="starter_cnt_sum", type="bigint"),
                ColumnMetadata(name="mvs_sum", type="decimal(19, 6)"),
                ColumnMetadata(name="view_hours_sum", type="decimal(19, 6)"),
            ],
        )

        assert combiner.metrics_spec() == [
            {
                "fieldName": "starter_cnt_sum",
                "name": "starter_cnt_sum",
                "type": "longSum",
            },
            {"fieldName": "mvs_sum", "name": "mvs_sum", "type": "doubleSum"},
            {
                "fieldName": "view_hours_sum",
                "name": "view_hours_sum",
                "type": "doubleSum",
            },
        ]


def test_cube_api_maps_decimal_sum_to_double_sum():
    """The cube API no longer ingests a decimal sum as a long."""
    assert _build_metrics_spec(
        measure_columns=[ColumnMetadata(name="mvs_sum", type="decimal(19, 6)")],
        measure_components=[_measure("mvs_sum", "SUM")],
        component_aliases={},
    ) == [{"fieldName": "mvs_sum", "name": "mvs_sum", "type": "doubleSum"}]
