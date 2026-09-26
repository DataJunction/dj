"""
Tests for _build_metrics_spec function in cubes.py.

This function builds Druid metricsSpec from measure columns and their components.
"""

import pytest

from datajunction_server.api.cubes import _build_metrics_spec
from datajunction_server.models.cube_materialization import MetricMeasures
from datajunction_server.models.materialization import (
    DRUID_AGG_MAPPING,
    DRUID_SKETCH_CONFIG,
    DruidMeasuresCubeConfig,
    Measure,
    get_druid_aggregator_spec,
    register_druid_aggregator,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import AggregationRule, MetricComponent
from datajunction_server.models.query import ColumnMetadata


def _make_component(
    name: str,
    aggregation: str | None = None,
    merge: str | None = None,
) -> MetricComponent:
    """Helper to create a MetricComponent with minimal required fields."""
    return MetricComponent(
        name=name,
        expression=name,  # Simplified for testing
        aggregation=aggregation,
        merge=merge,
        rule=AggregationRule(),
    )


def _make_column(name: str, col_type: str = "bigint") -> ColumnMetadata:
    """Helper to create ColumnMetadata with minimal required fields."""
    return ColumnMetadata(name=name, type=col_type)


class TestBuildMetricsSpec:
    """Tests for _build_metrics_spec function."""

    def test_empty_inputs(self):
        """Empty inputs should return empty list."""
        result = _build_metrics_spec([], [], {})
        assert result == []

    def test_direct_component_lookup_with_sum(self):
        """Component name matches column name directly - SUM aggregation."""
        columns = [_make_column("total_revenue", "bigint")]
        components = [_make_component("total_revenue", aggregation="SUM", merge="SUM")]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert len(result) == 1
        assert result[0] == {
            "fieldName": "total_revenue",
            "name": "total_revenue",
            "type": "longSum",
        }

    def test_direct_component_lookup_with_count(self):
        """Component name matches column name - COUNT uses SUM for merge."""
        columns = [_make_column("order_count", "bigint")]
        components = [_make_component("order_count", aggregation="COUNT", merge="SUM")]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert len(result) == 1
        assert result[0]["type"] == "longSum"

    def test_alias_lookup(self):
        """Column name matches alias, needs to look up internal component name."""
        columns = [_make_column("approx_unique_users", "binary")]
        components = [
            _make_component(
                "user_id_hll_abc123",  # Internal hashed name
                aggregation="hll_sketch_agg",
                merge="hll_union_agg",
            ),
        ]
        # Alias maps internal name to output column name
        aliases = {"user_id_hll_abc123": "approx_unique_users"}

        result = _build_metrics_spec(columns, components, aliases)

        assert len(result) == 1
        assert result[0]["type"] == "HLLSketchMerge"
        assert result[0]["fieldName"] == "approx_unique_users"
        assert result[0]["name"] == "approx_unique_users"
        # HLL sketches should have extra config
        assert result[0]["lgK"] == 12
        assert result[0]["tgtHllType"] == "HLL_4"

    def test_component_not_found_fallback(self):
        """Component not found should fall back to longSum."""
        columns = [_make_column("unknown_metric", "bigint")]
        components = []  # No components
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert len(result) == 1
        assert result[0]["type"] == "longSum"

    def test_component_no_merge_no_aggregation(self):
        """Component found but has no merge or aggregation - fallback to longSum."""
        columns = [_make_column("raw_value", "bigint")]
        components = [
            _make_component("raw_value", aggregation=None, merge=None),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert len(result) == 1
        assert result[0]["type"] == "longSum"

    def test_merge_takes_precedence_over_aggregation(self):
        """Merge function should be used for pre-aggregated data if available."""
        columns = [_make_column("count_col", "bigint")]
        # COUNT in aggregation, but SUM for merge (this is correct for count rollup)
        components = [
            _make_component("count_col", aggregation="COUNT", merge="SUM"),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert len(result) == 1
        # Should use merge (SUM) not aggregation (COUNT)
        assert result[0]["type"] == "longSum"

    def test_aggregation_used_when_merge_is_none(self):
        """When merge is None, should fall back to aggregation function."""
        columns = [_make_column("sum_col", "double")]
        components = [
            _make_component("sum_col", aggregation="SUM", merge=None),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert len(result) == 1
        assert result[0]["type"] == "doubleSum"

    def test_hll_sketch_merge_has_extra_config(self):
        """HLLSketchMerge type should include lgK and tgtHllType."""
        columns = [_make_column("hll_col", "binary")]
        components = [
            _make_component(
                "hll_col",
                aggregation="hll_sketch_agg",
                merge="hll_union_agg",
            ),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert len(result) == 1
        assert result[0]["type"] == "HLLSketchMerge"
        assert "lgK" in result[0]
        assert result[0]["lgK"] == 12
        assert "tgtHllType" in result[0]
        assert result[0]["tgtHllType"] == "HLL_4"

    def test_double_sum(self):
        """Double type with SUM should produce doubleSum."""
        columns = [_make_column("amount", "double")]
        components = [_make_component("amount", aggregation="SUM", merge="SUM")]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "doubleSum"

    def test_float_sum(self):
        """Float type with SUM should produce floatSum."""
        columns = [_make_column("price", "float")]
        components = [_make_component("price", aggregation="SUM", merge="SUM")]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "floatSum"

    def test_double_min_max(self):
        """Double type with MIN/MAX should produce doubleMin/doubleMax."""
        columns = [
            _make_column("min_val", "double"),
            _make_column("max_val", "double"),
        ]
        components = [
            _make_component("min_val", aggregation="MIN", merge="MIN"),
            _make_component("max_val", aggregation="MAX", merge="MAX"),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "doubleMin"
        assert result[1]["type"] == "doubleMax"

    def test_int_sum(self):
        """Int type with SUM should produce longSum."""
        columns = [_make_column("int_col", "int")]
        components = [_make_component("int_col", aggregation="SUM", merge="SUM")]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "longSum"

    def test_bigint_min_max(self):
        """Bigint type with MIN/MAX should produce longMin/longMax."""
        columns = [
            _make_column("min_id", "bigint"),
            _make_column("max_id", "bigint"),
        ]
        components = [
            _make_component("min_id", aggregation="MIN", merge="MIN"),
            _make_component("max_id", aggregation="MAX", merge="MAX"),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "longMin"
        assert result[1]["type"] == "longMax"

    def test_mixed_types(self):
        """Multiple columns with different types and aggregations."""
        columns = [
            _make_column("revenue", "double"),
            _make_column("order_count", "bigint"),
            _make_column("hll_users", "binary"),
            _make_column("unknown_metric", "bigint"),
        ]
        components = [
            _make_component("revenue", aggregation="SUM", merge="SUM"),
            _make_component("order_count", aggregation="COUNT", merge="SUM"),
            _make_component(
                "hll_users_internal",
                aggregation="hll_sketch_agg",
                merge="hll_union_agg",
            ),
        ]
        aliases = {"hll_users_internal": "hll_users"}

        result = _build_metrics_spec(columns, components, aliases)

        assert len(result) == 4
        assert result[0]["type"] == "doubleSum"
        assert result[1]["type"] == "longSum"
        assert result[2]["type"] == "HLLSketchMerge"
        assert "lgK" in result[2]  # HLL has extra config
        assert result[3]["type"] == "longSum"  # Unknown falls back

    def test_case_insensitive_aggregation_lookup(self):
        """Aggregation function lookup should be case insensitive."""
        columns = [_make_column("col", "bigint")]
        components = [_make_component("col", aggregation="Sum", merge="SUM")]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "longSum"

    def test_unmapped_aggregation_type(self):
        """Unmapped (type, agg) combination should fall back to longSum."""
        columns = [_make_column("special_col", "varchar")]  # varchar not in mapping
        components = [_make_component("special_col", aggregation="SUM", merge="SUM")]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "longSum"

    def test_float_min_max(self):
        """Float type with MIN/MAX should produce floatMin/floatMax."""
        columns = [
            _make_column("min_price", "float"),
            _make_column("max_price", "float"),
        ]
        components = [
            _make_component("min_price", aggregation="MIN", merge="MIN"),
            _make_component("max_price", aggregation="MAX", merge="MAX"),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "floatMin"
        assert result[1]["type"] == "floatMax"

    def test_int_min_max(self):
        """Int type with MIN/MAX should produce longMin/longMax."""
        columns = [
            _make_column("min_qty", "int"),
            _make_column("max_qty", "int"),
        ]
        components = [
            _make_component("min_qty", aggregation="MIN", merge="MIN"),
            _make_component("max_qty", aggregation="MAX", merge="MAX"),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "longMin"
        assert result[1]["type"] == "longMax"

    def test_count_types_use_longsum_for_merge(self):
        """COUNT columns of various types should use longSum for merge."""
        # COUNT produces integers regardless of input type
        columns = [
            _make_column("count_bigint", "bigint"),
            _make_column("count_int", "int"),
            _make_column("count_double", "double"),
            _make_column("count_float", "float"),
        ]
        components = [
            _make_component("count_bigint", aggregation="COUNT", merge="COUNT"),
            _make_component("count_int", aggregation="COUNT", merge="COUNT"),
            _make_component("count_double", aggregation="COUNT", merge="COUNT"),
            _make_component("count_float", aggregation="COUNT", merge="COUNT"),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        # All COUNT merges should map to longSum
        for r in result:
            assert r["type"] == "longSum"

    def test_hll_sketch_agg_binary_type(self):
        """Binary type with hll_sketch_agg should produce HLLSketchMerge."""
        columns = [_make_column("sketch_col", "binary")]
        components = [
            _make_component(
                "sketch_col",
                aggregation="hll_sketch_agg",
                merge="hll_sketch_agg",
            ),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert result[0]["type"] == "HLLSketchMerge"
        assert result[0]["lgK"] == 12
        assert result[0]["tgtHllType"] == "HLL_4"

    def test_non_hll_sketch_type_no_extra_config(self):
        """Non-HLL types should NOT have lgK or tgtHllType."""
        columns = [_make_column("sum_col", "bigint")]
        components = [_make_component("sum_col", aggregation="SUM", merge="SUM")]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert "lgK" not in result[0]
        assert "tgtHllType" not in result[0]

    def test_preserves_column_order(self):
        """Output should preserve input column order."""
        columns = [
            _make_column("z_col", "bigint"),
            _make_column("a_col", "bigint"),
            _make_column("m_col", "bigint"),
        ]
        components = [
            _make_component("z_col", aggregation="SUM", merge="SUM"),
            _make_component("a_col", aggregation="SUM", merge="SUM"),
            _make_component("m_col", aggregation="SUM", merge="SUM"),
        ]
        aliases = {}

        result = _build_metrics_spec(columns, components, aliases)

        assert [r["name"] for r in result] == ["z_col", "a_col", "m_col"]


class TestDruidAggregatorSpec:
    """
    Tests for the shared aggregator helper behind both metricsSpec builders.
    """

    def test_merge_is_preferred_over_accumulate(self):
        """
        Ingestion merges stored partials, so the merge function keys the lookup.
        """
        spec = get_druid_aggregator_spec(
            column_name="orders_count",
            column_type="bigint",
            aggregation="COUNT",
            merge="SUM",
        )
        assert spec == {
            "fieldName": "orders_count",
            "name": "orders_count",
            "type": "longSum",
        }

    def test_falls_back_to_accumulate_without_merge(self):
        """A component with no merge phase keys off its accumulate."""
        spec = get_druid_aggregator_spec(
            column_name="revenue_sum",
            column_type="double",
            aggregation="SUM",
            merge=None,
        )
        assert spec is not None
        assert spec["type"] == "doubleSum"

    def test_unmappable_returns_none(self):
        """
        Return None for unmappable measures so callers determine fallback behavior.
        """
        assert (
            get_druid_aggregator_spec(
                column_name="x",
                column_type="varchar",
                aggregation="SUM",
                merge="SUM",
            )
            is None
        )
        assert (
            get_druid_aggregator_spec(
                column_name="x",
                column_type=None,
                aggregation="SUM",
                merge="SUM",
            )
            is None
        )
        assert (
            get_druid_aggregator_spec(
                column_name="x",
                column_type="bigint",
                aggregation=None,
                merge=None,
            )
            is None
        )

    def test_sketch_gets_default_config(self):
        """An HLL sketch carries its precision defaults into the spec."""
        spec = get_druid_aggregator_spec(
            column_name="accounts_hll",
            column_type="binary",
            aggregation="hll_sketch_agg",
            merge="hll_union_agg",
        )
        assert spec == {
            "fieldName": "accounts_hll",
            "name": "accounts_hll",
            "type": "HLLSketchMerge",
            "lgK": 12,
            "tgtHllType": "HLL_4",
        }

    def test_declared_params_override_defaults_partially(self):
        """
        Metric-level params override defaults for specific keys while retaining others.
        """
        spec = get_druid_aggregator_spec(
            column_name="accounts_hll",
            column_type="binary",
            aggregation="hll_sketch_agg",
            merge="hll_union_agg",
            params={"lgK": 17},
        )
        assert spec is not None
        assert spec["lgK"] == 17
        assert spec["tgtHllType"] == "HLL_4"

    def test_params_cannot_overwrite_structural_keys(self):
        """
        `params` must not overwrite structural keys like `type` or `fieldName`.
        """
        with pytest.raises(DJInvalidInputException) as excinfo:
            get_druid_aggregator_spec(
                column_name="accounts_hll",
                column_type="binary",
                aggregation="hll_sketch_agg",
                merge="hll_union_agg",
                params={"type": "doubleSum", "fieldName": "some_other_column"},
            )
        assert "does not accept" in str(excinfo.value)
        assert "`fieldName`" in str(excinfo.value)
        assert "`type`" in str(excinfo.value)

    def test_unknown_param_is_rejected(self):
        """Unknown parameters are rejected loudly."""
        with pytest.raises(DJInvalidInputException) as excinfo:
            get_druid_aggregator_spec(
                column_name="accounts_hll",
                column_type="binary",
                aggregation="hll_sketch_agg",
                merge="hll_union_agg",
                params={"lgk": 17},  # lowercase k
            )
        assert "lgk" in str(excinfo.value)
        assert "lgK" in str(excinfo.value)  # the supported spelling is named

    def test_params_rejected_for_unparameterized_aggregator(self):
        """Params passed to an unparameterized aggregator raise an error."""
        with pytest.raises(DJInvalidInputException) as excinfo:
            get_druid_aggregator_spec(
                column_name="orders_count",
                column_type="bigint",
                aggregation="COUNT",
                merge="SUM",
                params={"compression": 200},
            )
        assert "takes no parameters" in str(excinfo.value)


class TestRegisterDruidAggregator:
    """
    Tests for registering custom sketch families.
    """

    def test_register_and_use(self):
        """A registered family is mappable and carries its default config."""
        try:
            register_druid_aggregator(
                column_type="binary",
                merge_func="test_tdigest_agg",
                aggregator="testTDigestSketch",
                default_config={"compression": 200},
            )
            spec = get_druid_aggregator_spec(
                column_name="latency_tdigest",
                column_type="binary",
                aggregation="test_tdigest",
                merge="test_tdigest_agg",
            )
            assert spec == {
                "fieldName": "latency_tdigest",
                "name": "latency_tdigest",
                "type": "testTDigestSketch",
                "compression": 200,
            }
            # A metric's own params win over the family default.
            tuned = get_druid_aggregator_spec(
                column_name="latency_tdigest",
                column_type="binary",
                aggregation="test_tdigest",
                merge="test_tdigest_agg",
                params={"compression": 1000},
            )
            assert tuned is not None
            assert tuned["compression"] == 1000
            assert "testTDigestSketch" in DRUID_SKETCH_CONFIG
        finally:
            DRUID_AGG_MAPPING.pop(("binary", "test_tdigest_agg"), None)
            DRUID_SKETCH_CONFIG.pop("testTDigestSketch", None)

    def test_register_without_config(self):
        """A family with nothing to tune registers without extra config keys."""
        try:
            register_druid_aggregator(
                column_type="bigint",
                merge_func="test_plain_merge",
                aggregator="testPlainAgg",
            )
            spec = get_druid_aggregator_spec(
                column_name="c",
                column_type="bigint",
                aggregation="test_plain",
                merge="test_plain_merge",
            )
            assert spec == {
                "fieldName": "c",
                "name": "c",
                "type": "testPlainAgg",
            }
        finally:
            DRUID_AGG_MAPPING.pop(("bigint", "test_plain_merge"), None)

    @pytest.mark.parametrize("bad_value", ["broken", True, float("nan"), 10**1000])
    def test_rejects_invalid_compression(self, bad_value):
        """Invalid tuning values fail before a Druid ingestion job is submitted."""
        try:
            register_druid_aggregator(
                column_type="binary",
                merge_func="test_checked_tdigest_agg",
                aggregator="testCheckedTDigestSketch",
                default_config={"compression": 200},
            )
            with pytest.raises(DJInvalidInputException, match="compression"):
                get_druid_aggregator_spec(
                    column_name="latency_tdigest",
                    column_type="binary",
                    aggregation="test_checked_tdigest_agg",
                    merge="test_checked_tdigest_agg",
                    params={"compression": bad_value},
                )
        finally:
            DRUID_AGG_MAPPING.pop(("binary", "test_checked_tdigest_agg"), None)
            DRUID_SKETCH_CONFIG.pop("testCheckedTDigestSketch", None)

    def test_conflicting_defaults_are_rejected(self):
        """
        Re-registering an aggregator with different defaults raises an error.
        """
        try:
            register_druid_aggregator(
                column_type="binary",
                merge_func="test_conflict_agg",
                aggregator="testConflictSketch",
                default_config={"compression": 100},
            )
            with pytest.raises(DJInvalidInputException) as excinfo:
                register_druid_aggregator(
                    column_type="binary",
                    merge_func="test_conflict_agg",
                    aggregator="testConflictSketch",
                    default_config={"compression": 200},
                )
            assert "already registered with different defaults" in str(excinfo.value)
            # Re-registering exactly what is there is a no-op.
            register_druid_aggregator(
                column_type="binary",
                merge_func="test_conflict_agg",
                aggregator="testConflictSketch",
                default_config={"compression": 100},
            )
            assert DRUID_SKETCH_CONFIG["testConflictSketch"] == {"compression": 100}
        finally:
            DRUID_AGG_MAPPING.pop(("binary", "test_conflict_agg"), None)
            DRUID_SKETCH_CONFIG.pop("testConflictSketch", None)

    def test_conflicting_aggregator_is_rejected(self):
        """Two families cannot claim the same (column type, merge func) pair."""
        try:
            register_druid_aggregator(
                column_type="binary",
                merge_func="test_claimed",
                aggregator="testFirstSketch",
            )
            with pytest.raises(DJInvalidInputException) as excinfo:
                register_druid_aggregator(
                    column_type="binary",
                    merge_func="test_claimed",
                    aggregator="testSecondSketch",
                )
            assert "already registered as" in str(excinfo.value)
        finally:
            DRUID_AGG_MAPPING.pop(("binary", "test_claimed"), None)

    def test_v2_measures_cube_config_gets_family_defaults(self):
        """
        The V2 path uses the aggregator fallback with `merge=None` to pull correct specs.
        """
        try:
            register_druid_aggregator(
                column_type="binary",
                merge_func="test_v2_tdigest",
                aggregator="testV2Sketch",
                default_config={"compression": 200},
            )
            config = DruidMeasuresCubeConfig.model_construct(
                dimensions=[],
                measures={
                    "some.metric": MetricMeasures.model_construct(
                        metric="some.metric",
                        combiner="x",
                        measures=[
                            Measure(
                                name="latency_tdigest",
                                field_name="latency_tdigest",
                                agg="test_v2_tdigest",
                                type="binary",
                            ),
                        ],
                    ),
                },
            )
            assert config.metrics_spec() == {
                "latency_tdigest": {
                    "fieldName": "latency_tdigest",
                    "name": "latency_tdigest",
                    "type": "testV2Sketch",
                    "compression": 200,
                },
            }
        finally:
            DRUID_AGG_MAPPING.pop(("binary", "test_v2_tdigest"), None)
            DRUID_SKETCH_CONFIG.pop("testV2Sketch", None)
