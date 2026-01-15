"""Tests for cube materialization models."""

import pytest

from datajunction_server.models.cube_materialization import (
    DruidCubeV3Config,
    PreAggTableInfo,
)
from datajunction_server.models.decompose import (
    AggregationRule,
    Aggregability,
    MetricComponent,
)
from datajunction_server.models.query import ColumnMetadata


class TestDruidCubeV3ConfigDruidCubeConfigCompatibility:
    """Test DruidCubeConfig compatibility computed properties."""

    @pytest.fixture
    def sample_config(self):
        """Create a sample DruidCubeV3Config for testing."""
        return DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[
                PreAggTableInfo(
                    table_ref="catalog.schema.preagg_table",
                    parent_node="default.orders",
                    grain=["date_id", "country"],
                ),
            ],
            combined_sql="SELECT * FROM preagg_table",
            combined_columns=[
                ColumnMetadata(
                    name="date_id",
                    type="int",
                    semantic_entity="default.date_dim.date_id",
                ),
                ColumnMetadata(
                    name="country",
                    type="string",
                    semantic_entity="default.country_dim.country",
                ),
                ColumnMetadata(
                    name="revenue_sum",
                    type="double",
                    semantic_entity="default.revenue",
                ),
                ColumnMetadata(
                    name="order_count",
                    type="bigint",
                    semantic_entity="default.orders",
                ),
            ],
            combined_grain=["date_id", "country"],
            measure_components=[
                MetricComponent(
                    name="revenue_sum",
                    expression="revenue",
                    aggregation="SUM",
                    merge="SUM",
                    rule=AggregationRule(type=Aggregability.FULL),
                ),
                MetricComponent(
                    name="order_count",
                    expression="1",
                    aggregation="COUNT",
                    merge="SUM",
                    rule=AggregationRule(type=Aggregability.FULL),
                ),
            ],
            component_aliases={
                "revenue_sum": "total_revenue",
                "order_count": "num_orders",
            },
            cube_metrics=[
                "default.total_revenue",
                "default.num_orders",
            ],
            # metrics is now explicitly populated (no longer computed)
            metrics=[
                {
                    "node": "default.total_revenue",
                    "name": "total_revenue",
                    "metric_expression": "SUM(revenue_sum)",
                    "metric": {
                        "name": "default.total_revenue",
                        "display_name": "Total Revenue",
                    },
                },
                {
                    "node": "default.num_orders",
                    "name": "num_orders",
                    "metric_expression": "SUM(order_count)",
                    "metric": {
                        "name": "default.num_orders",
                        "display_name": "Num Orders",
                    },
                },
            ],
            timestamp_column="date_id",
            timestamp_format="yyyyMMdd",
        )

    def test_dimensions_property(self, sample_config):
        """Test that dimensions property returns combined_grain."""
        assert sample_config.dimensions == ["date_id", "country"]
        assert sample_config.dimensions == sample_config.combined_grain

    def test_metrics_property_with_cube_metrics(self, sample_config):
        """
        Test that metrics property returns DruidCubeConfig-compatible
        format with cube_metrics.
        """
        metrics = sample_config.metrics

        assert len(metrics) == 2

        # Check first metric
        assert metrics[0]["node"] == "default.total_revenue"
        assert metrics[0]["name"] == "total_revenue"
        assert metrics[0]["metric"]["name"] == "default.total_revenue"
        assert metrics[0]["metric"]["display_name"] == "Total Revenue"
        # Verify metric_expression is set from matching measure_component merge function
        assert metrics[0]["metric_expression"] == "SUM(revenue_sum)"

        # Check second metric
        assert metrics[1]["node"] == "default.num_orders"
        assert metrics[1]["name"] == "num_orders"
        assert metrics[1]["metric"]["name"] == "default.num_orders"
        assert metrics[1]["metric"]["display_name"] == "Num Orders"
        # Verify metric_expression is set from matching measure_component merge function
        assert metrics[1]["metric_expression"] == "SUM(order_count)"

    def test_metrics_property_fallback_to_components(self):
        """Test that metrics field stores explicit metrics list."""
        config = DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[],
            combined_sql="SELECT * FROM table",
            combined_columns=[
                ColumnMetadata(name="revenue_sum", type="double"),
            ],
            combined_grain=["date_id"],
            measure_components=[
                MetricComponent(
                    name="revenue_sum",
                    expression="revenue",
                    aggregation="SUM",
                    merge="SUM",
                    rule=AggregationRule(type=Aggregability.FULL),
                ),
            ],
            component_aliases={"revenue_sum": "total_revenue"},
            cube_metrics=[],
            # metrics is now explicitly populated
            metrics=[
                {
                    "name": "total_revenue",
                    "metric_expression": "SUM(revenue_sum)",
                },
            ],
            timestamp_column="date_id",
        )

        metrics = config.metrics

        assert len(metrics) == 1
        assert metrics[0]["name"] == "total_revenue"
        assert metrics[0]["metric_expression"] == "SUM(revenue_sum)"

    def test_combiners_property(self, sample_config):
        """
        Test that combiners property returns columns in DruidCubeConfig
        expected format.
        """
        combiners = sample_config.combiners

        assert len(combiners) == 1
        assert "columns" in combiners[0]

        columns = combiners[0]["columns"]
        assert len(columns) == 4

        # Check column structure
        assert columns[0]["name"] == "date_id"
        assert columns[0]["column"] == "default.date_dim.date_id"

        assert columns[1]["name"] == "country"
        assert columns[1]["column"] == "default.country_dim.country"

    def test_model_dump_includes_computed_fields(self, sample_config):
        """
        Test that model_dump() includes the DruidCubeConfig
        compatibility fields.
        """
        data = sample_config.model_dump()

        assert "dimensions" in data
        assert "metrics" in data
        assert "combiners" in data

        assert data["dimensions"] == ["date_id", "country"]
        assert len(data["metrics"]) == 2
        assert len(data["combiners"]) == 1

    def test_empty_dimensions(self):
        """Test dimensions with empty combined_grain."""
        config = DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[],
            combined_sql="SELECT 1",
            combined_columns=[],
            combined_grain=[],
            timestamp_column="date_id",
        )
        assert config.dimensions == []

    def test_metrics_with_no_merge_function(self):
        """Test metrics can be stored with SUM fallback expression."""
        config = DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[],
            combined_sql="SELECT * FROM table",
            combined_columns=[],
            combined_grain=["date_id"],
            measure_components=[
                MetricComponent(
                    name="some_metric",
                    expression="value",
                    aggregation="SUM",
                    merge=None,  # No merge function
                    rule=AggregationRule(type=Aggregability.FULL),
                ),
            ],
            component_aliases={},
            cube_metrics=[],
            # metrics is now explicitly populated (endpoint provides SUM fallback)
            metrics=[
                {
                    "name": "some_metric",
                    "metric_expression": "SUM(some_metric)",
                },
            ],
            timestamp_column="date_id",
        )

        metrics = config.metrics
        assert len(metrics) == 1
        # Should have SUM(name) expression
        assert metrics[0]["metric_expression"] == "SUM(some_metric)"

    def test_metrics_with_hll_merge_function(self):
        """Test metrics can store HLL (HyperLogLog) merge expressions."""
        config = DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[],
            combined_sql="SELECT * FROM table",
            combined_columns=[],
            combined_grain=["date_id"],
            measure_components=[
                MetricComponent(
                    name="user_hll",
                    expression="user_id",
                    aggregation="hll_sketch_agg",
                    merge="hll_union",
                    rule=AggregationRule(type=Aggregability.FULL),
                ),
            ],
            component_aliases={"user_hll": "unique_users"},
            cube_metrics=[],
            # metrics is now explicitly populated
            metrics=[
                {
                    "name": "unique_users",
                    "metric_expression": "hll_union(user_hll)",
                },
            ],
            timestamp_column="date_id",
        )

        metrics = config.metrics
        assert len(metrics) == 1
        assert metrics[0]["name"] == "unique_users"
        assert metrics[0]["metric_expression"] == "hll_union(user_hll)"

    def test_combiners_with_missing_semantic_entity(self):
        """Test combiners when semantic_entity is None."""
        config = DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[],
            combined_sql="SELECT * FROM table",
            combined_columns=[
                ColumnMetadata(name="col1", type="int", semantic_entity=None),
                ColumnMetadata(
                    name="col2",
                    type="string",
                    semantic_entity="some.entity",
                ),
            ],
            combined_grain=["col1"],
            timestamp_column="col1",
        )

        combiners = config.combiners
        columns = combiners[0]["columns"]

        # Should fall back to column name when semantic_entity is None
        assert columns[0]["name"] == "col1"
        assert columns[0]["column"] == "col1"

        assert columns[1]["name"] == "col2"
        assert columns[1]["column"] == "some.entity"

    def test_metrics_display_name_formatting(self):
        """Test that display_name can be properly formatted in stored metrics."""
        config = DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[],
            combined_sql="SELECT * FROM table",
            combined_columns=[],
            combined_grain=["date_id"],
            cube_metrics=[
                "default.my_complex_metric_name",
                "sales.total_revenue_usd",
            ],
            # metrics is now explicitly populated (endpoint formats display_name)
            metrics=[
                {
                    "node": "default.my_complex_metric_name",
                    "name": "my_complex_metric_name",
                    "metric_expression": "SUM(some_component)",
                    "metric": {
                        "name": "default.my_complex_metric_name",
                        "display_name": "My Complex Metric Name",
                    },
                },
                {
                    "node": "sales.total_revenue_usd",
                    "name": "total_revenue_usd",
                    "metric_expression": "SUM(revenue)",
                    "metric": {
                        "name": "sales.total_revenue_usd",
                        "display_name": "Total Revenue Usd",
                    },
                },
            ],
            timestamp_column="date_id",
        )

        metrics = config.metrics

        assert metrics[0]["name"] == "my_complex_metric_name"
        assert metrics[0]["metric"]["display_name"] == "My Complex Metric Name"

        assert metrics[1]["name"] == "total_revenue_usd"
        assert metrics[1]["metric"]["display_name"] == "Total Revenue Usd"

    def test_json_serialization_roundtrip(self, sample_config):
        """Test that config can be serialized to JSON and back."""
        import json

        # Serialize to JSON
        json_str = sample_config.model_dump_json()
        data = json.loads(json_str)

        # Verify backwards compatibility fields are present
        assert "dimensions" in data
        assert "metrics" in data
        assert "combiners" in data

        # Verify data integrity
        assert data["druid_datasource"] == "dj_test_cube_v1_0"
        assert data["cube_metrics"] == ["default.total_revenue", "default.num_orders"]

    def test_multiple_preagg_tables(self):
        """Test config with multiple pre-agg tables."""
        config = DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[
                PreAggTableInfo(
                    table_ref="catalog.schema.preagg_1",
                    parent_node="default.orders",
                    grain=["date_id"],
                ),
                PreAggTableInfo(
                    table_ref="catalog.schema.preagg_2",
                    parent_node="default.users",
                    grain=["date_id", "user_id"],
                ),
            ],
            combined_sql="SELECT * FROM preagg_1 JOIN preagg_2",
            combined_columns=[
                ColumnMetadata(name="date_id", type="int"),
            ],
            combined_grain=["date_id"],
            timestamp_column="date_id",
        )

        assert len(config.preagg_tables) == 2
        assert config.dimensions == ["date_id"]

    def test_urls_backwards_compatibility(self):
        """Test that urls property aliases workflow_urls for old UI compatibility."""
        config = DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[],
            combined_sql="SELECT 1",
            combined_columns=[],
            combined_grain=["date_id"],
            timestamp_column="date_id",
            workflow_urls=[
                "http://workflow/scheduled",
                "http://workflow/backfill",
            ],
        )

        # urls should alias workflow_urls
        assert config.urls == config.workflow_urls
        assert config.urls == ["http://workflow/scheduled", "http://workflow/backfill"]

        # Both should be in model_dump
        data = config.model_dump()
        assert "urls" in data
        assert "workflow_urls" in data
        assert data["urls"] == data["workflow_urls"]

    def test_urls_empty_when_no_workflow(self):
        """Test that urls is empty when workflow_urls is empty."""
        config = DruidCubeV3Config(
            druid_datasource="dj_test_cube_v1_0",
            preagg_tables=[],
            combined_sql="SELECT 1",
            combined_columns=[],
            combined_grain=["date_id"],
            timestamp_column="date_id",
            workflow_urls=[],
        )

        assert config.urls == []
        assert config.workflow_urls == []
