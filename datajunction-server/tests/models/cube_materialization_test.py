"""Tests for cube materialization models."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from datajunction_server.errors import DJInvalidInputException
from datajunction_server.materialization.jobs.cube_materialization import (
    DruidCubeMaterializationJob,
)
from datajunction_server.models.cube_materialization import (
    DruidCubeConfig,
    DruidCubeMaterializationInput,
    DruidCubeV3Config,
    MeasuresMaterialization,
    PreAggTableInfo,
    materialized_table_name,
    version_from_materialized_table,
)
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.utils import get_settings
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.models.decompose import (
    AggregationRule,
    Aggregability,
    MetricComponent,
)
from datajunction_server.models.query import ColumnMetadata


def test_druid_cube_materialization_job_passes_lookback_window():
    """
    The legacy Druid cube scheduler should preserve the configured lookback
    window when it hands off to the query service.
    """
    cube_config = DruidCubeConfig(
        cube=NodeNameVersion(name="default.repairs_cube", version="v1.0"),
        dimensions=[],
        metrics=[],
        measures_materializations=[],
        combiners=[],
        lookback_window="7 DAY",
    )
    materialization = SimpleNamespace(
        name="druid_cube__incremental_time__default.repair_orders_fact.order_date",
        config=cube_config.model_dump(),
        strategy=MaterializationStrategy.INCREMENTAL_TIME,
        schedule="@daily",
        job="DruidCubeMaterializationJob",
    )
    query_service_client = Mock()

    DruidCubeMaterializationJob().schedule(materialization, query_service_client)

    materialization_input = query_service_client.materialize_cube.call_args.kwargs[
        "materialization_input"
    ]
    assert isinstance(materialization_input, DruidCubeMaterializationInput)
    assert materialization_input.lookback_window == "7 DAY"


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


class TestMaterializedTableNameRoundTrip:
    """`materialized_table_name` and `version_from_materialized_table` must stay
    inverses — the availability endpoint derives the node version from the table
    name, so a drift between the two would silently misroute availability."""

    def test_round_trip_unprefixed(self):
        table = materialized_table_name("foo.bar.baz", "v2.1", "abc123def4560000")
        assert table == "foo_bar_baz_v2_1_abc123def4560000"
        assert version_from_materialized_table(table, "foo.bar.baz") == "v2.1"

    def test_round_trip_with_druid_prefix(self):
        prefix = get_settings().druid_datasource_prefix
        table = prefix + materialized_table_name(
            "cs.main.perf",
            "v1.0",
            "deadbeefdeadbeef",
        )
        assert version_from_materialized_table(table, "cs.main.perf") == "v1.0"

    def test_non_materialized_names_return_none(self):
        # A bare source table.
        assert version_from_materialized_table("pmts", "default.revenue") is None
        # A datasource for a different node.
        assert (
            version_from_materialized_table(
                "dj__other_v1_0_abcd1234",
                "default.revenue",
            )
            is None
        )
        # Matches the node stem but has no <version>_<hash> suffix.
        assert (
            version_from_materialized_table("dj__default_revenue_v1", "default.revenue")
            is None
        )

    def test_round_trip_multi_digit_versions(self):
        # Only the trailing hash token is peeled off, never a version digit.
        for version in ("v1.10", "v10.2", "v10.10"):
            table = materialized_table_name("a.b", version, "0011223344556677")
            assert version_from_materialized_table(table, "a.b") == version
            prefixed = get_settings().druid_datasource_prefix + table
            assert version_from_materialized_table(prefixed, "a.b") == version

    def test_round_trip_node_name_with_version_like_suffix(self):
        # Anchoring on the full node stem handles node names that themselves
        # look version-ish or contain underscores.
        node_name = "default.foo_v2"
        table = get_settings().druid_datasource_prefix + materialized_table_name(
            node_name,
            "v1.0",
            "abcd1234abcd1234",
        )
        assert version_from_materialized_table(table, node_name) == "v1.0"

    def test_boundary_inputs_return_none(self):
        prefix = get_settings().druid_datasource_prefix
        # Just the prefix, nothing else.
        assert version_from_materialized_table(prefix, "default.x") is None
        # Node stem present but nothing after it.
        assert (
            version_from_materialized_table(f"{prefix}default_x_", "default.x") is None
        )
