"""Tests for cube materialization models."""

from types import SimpleNamespace

import pytest

from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.cube_materialization import (
    DruidCubeV3Config,
    MeasuresMaterialization,
    PreAggTableInfo,
)
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
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


class TestFromMeasuresQueryRoleResolution:
    """``MeasuresMaterialization.from_measures_query`` must reconstruct the
    role-qualified dimension reference from the cube column's ``name`` +
    ``dimension_column`` so it can match the v3 measures query's
    ``semantic_entity`` (which embeds the role).

    Regression: cubes whose temporal partition uses a role-qualified dimension
    raised ``IndexError`` because the comparison was a bare ``name`` equality.
    """

    @pytest.fixture
    def measures_query(self):
        """A v3-style measures query with a role-qualified date dimension."""
        return SimpleNamespace(
            node=NodeNameVersion(name="default.cube", version="v1.0"),
            grain=["default_DOT_date_DOT_dateint_LBRACKET_reporting_date_RBRACKET"],
            columns=[
                ColumnMetadata(
                    name="dateint",
                    type="int",
                    semantic_entity="default.date.dateint[reporting_date]",
                    semantic_type="dimension",
                ),
                ColumnMetadata(
                    name="amount_sum",
                    type="double",
                    semantic_entity="default.amount",
                    semantic_type="metric",
                ),
            ],
            metrics={
                "default.total_amount": (
                    [
                        MetricComponent(
                            name="amount_sum",
                            expression="amount",
                            aggregation="SUM",
                            merge="SUM",
                            rule=AggregationRule(type=Aggregability.FULL),
                        ),
                    ],
                    "amount_sum",
                ),
            },
            sql="SELECT 1",
            spark_conf={},
            upstream_tables=["default.facts"],
        )

    def test_role_qualified_partition_resolves(self, measures_query):
        """Cube column with ``dimension_column='[reporting_date]'`` must
        match the role-qualified ``semantic_entity`` in the measures query."""
        # Mimics a database Column for a role-qualified temporal partition.
        # Cube columns store the role separately in ``dimension_column``.
        temporal_partition = SimpleNamespace(
            name="default.date.dateint",
            dimension_column="[reporting_date]",
            partition=SimpleNamespace(format="yyyyMMdd", granularity=Granularity.DAY),
        )

        result = MeasuresMaterialization.from_measures_query(
            measures_query,
            temporal_partition,
        )

        assert result.timestamp_column == "dateint"
        assert result.timestamp_format == "yyyyMMdd"
        assert result.granularity == Granularity.DAY

    def test_unqualified_partition_still_resolves(self, measures_query):
        """A cube without a role on its temporal partition must still match."""
        measures_query.columns[0].semantic_entity = "default.date.dateint"
        temporal_partition = SimpleNamespace(
            name="default.date.dateint",
            dimension_column=None,
            partition=SimpleNamespace(format="yyyyMMdd", granularity=Granularity.DAY),
        )

        result = MeasuresMaterialization.from_measures_query(
            measures_query,
            temporal_partition,
        )

        assert result.timestamp_column == "dateint"

    def test_missing_partition_raises_clear_error(self, measures_query):
        """If no measures column matches the partition, raise a clear error
        instead of an opaque ``IndexError``."""
        temporal_partition = SimpleNamespace(
            name="default.unrelated.column",
            dimension_column=None,
            partition=SimpleNamespace(format="yyyyMMdd", granularity=Granularity.DAY),
        )

        with pytest.raises(DJInvalidInputException, match="Could not find timestamp"):
            MeasuresMaterialization.from_measures_query(
                measures_query,
                temporal_partition,
            )
