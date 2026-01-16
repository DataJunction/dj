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
