"""
Tests for _build_metrics_spec function in cubes.py.

This function builds Druid metricsSpec from measure columns and their components.
"""

from datajunction_server.api.cubes import _build_metrics_spec
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
