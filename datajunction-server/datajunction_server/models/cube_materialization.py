"""Models related to cube materialization"""

import hashlib
from typing import Any, Dict, List, Optional, Union, Literal

from pydantic import BaseModel, Field, field_validator, computed_field

from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.column import SemanticType
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    DecomposedMetric,
    MetricComponent,
)
from datajunction_server.models.materialization import (
    DRUID_AGG_MAPPING,
    MaterializationJobTypeEnum,
    MaterializationStrategy,
)
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.models.query import ColumnMetadata

# Re-export for backward compatibility
__all__ = [
    "Aggregability",
    "AggregationRule",
    "DecomposedMetric",
    "MetricComponent",
]


class MetricMeasures(BaseModel):
    """
    Represent a metric as a set of measures, along with the expression for
    combining the measures to make the metric.
    """

    metric: str
    measures: List[MetricComponent]
    combiner: str


class DruidSpec(BaseModel):
    """
    Represents Druid-specific configuration for a MeasuresMaterialization.
    """

    datasource: str = Field(description="The Druid datasource name.")
    ingestion_spec: Dict = Field(
        description="Druid ingestion spec for the materialization.",
    )


class MeasuresMaterialization(BaseModel):
    """
    Represents a single pre-aggregation transform query for materializing a partition.
    """

    node: NodeNameVersion = Field(description="The node being materialized")
    grain: list[str] = Field(
        description="The grain at which the node is being materialized.",
    )
    dimensions: list[str] = Field(
        description="List of dimensions included in this materialization.",
    )
    measures: list[MetricComponent] = Field(
        description="List of measures included in this materialization.",
    )
    query: str = Field(
        description="The query used for each materialization run.",
    )

    columns: list[ColumnMetadata]

    timestamp_column: str | None = Field(description="Timestamp column name")
    timestamp_format: str | None = Field(
        description="Timestamp format. Example: `yyyyMMdd`",
    )

    granularity: Granularity | None = Field(
        description="The time granularity for each materialization run. Examples: DAY, HOUR",
    )

    spark_conf: Dict[str, str] | None = Field(
        description="Spark config for this materialization.",
    )
    upstream_tables: list[str] = Field(
        description="List of upstream tables used in this materialization.",
    )

    def table_ast(self):
        """
        Generate a unique output table name based on the parameters.
        """
        from datajunction_server.sql.parsing import ast

        return ast.Table(name=ast.Name(self.output_table_name))

    @computed_field  # type: ignore[misc]
    @property
    def output_table_name(self) -> str:
        """
        Generate a unique output table name based on the parameters.
        """
        unique_string = "|".join(
            [
                self.node.name,
                str(self.node.version),
                self.timestamp_column or "",
                ",".join(sorted(self.grain)),
                ",".join(sorted(self.dimensions)),
                ",".join(sorted([measure.name for measure in self.measures])),
            ],
        )
        unique_hash = hashlib.sha256(unique_string.encode()).hexdigest()[:16]
        return f"{self.node.name}_{self.node.version}_{unique_hash}".replace(".", "_")

    def model_dump(self, **kwargs):  # pragma: no cover
        base = super().model_dump(**kwargs)
        base["output_table_name"] = self.output_table_name
        return base

    @classmethod
    def from_measures_query(cls, measures_query, temporal_partition):
        """
        Builds a MeasuresMaterialization object from a measures query.
        """
        metric_components = list(
            {
                component.name: component
                for metric, (components, combiner) in measures_query.metrics.items()
                for component in components
            }.values(),
        )
        dimensional_metric_components = [
            component.name
            for component in metric_components
            if not component.aggregation
        ]
        return MeasuresMaterialization(
            node=measures_query.node,
            grain=measures_query.grain,
            columns=measures_query.columns,
            timestamp_column=[
                col.name
                for col in measures_query.columns
                if col.semantic_entity == temporal_partition.name
            ][0],
            timestamp_format=temporal_partition.partition.format,
            granularity=temporal_partition.partition.granularity,
            query=measures_query.sql,
            dimensions=[
                col.name
                for col in measures_query.columns  # type: ignore
                if col.semantic_type == SemanticType.DIMENSION
            ]
            + dimensional_metric_components,
            measures=metric_components,
            spark_conf=measures_query.spark_conf,
            upstream_tables=measures_query.upstream_tables,
        )


class MeasureKey(BaseModel):
    """
    Lookup key for a measure
    """

    node: NodeNameVersion
    measure_name: str


class CubeMetric(BaseModel):
    """
    Represents a metric belonging to a cube.
    """

    metric: NodeNameVersion = Field(description="The name and version of the metric.")
    required_measures: list[MeasureKey] = Field(
        description="List of measures required by this metric.",
    )
    derived_expression: str = Field(
        description=(
            "The query for rewriting the original metric query "
            "using the materialized measures."
        ),
    )
    metric_expression: str = Field(
        description=(
            "SQL expression for rewriting the original metric query "
            "using the materialized measures."
        ),
    )


class UpsertCubeMaterialization(BaseModel):
    """
    An upsert object for cube materializations
    """

    # For cubes this is the only materialization type we support
    job: Literal["druid_cube"]

    # Only FULL or INCREMENTAL_TIME is available for cubes
    strategy: MaterializationStrategy = MaterializationStrategy.INCREMENTAL_TIME

    # Cron schedule
    schedule: str

    # Configuration for the materialization (optional for compatibility)
    config: Dict[str, Any] | None = None

    # Lookback window, only relevant if materialization strategy is INCREMENTAL_TIME
    lookback_window: str | None = "1 DAY"

    @field_validator("job")
    def validate_job(
        cls,
        job: Union[str, MaterializationJobTypeEnum],
    ) -> MaterializationJobTypeEnum:
        """
        Validates the `job` field. Converts to an enum if `job` is a string.
        """
        if isinstance(job, str):  # pragma: no cover
            job_name = job.upper()
            options = MaterializationJobTypeEnum._member_names_
            if job_name not in options:
                raise DJInvalidInputException(
                    http_status_code=404,
                    message=f"Materialization job type `{job.upper()}` not found. "
                    f"Available job types: {[job.name for job in MaterializationJobTypeEnum]}",
                )
            return MaterializationJobTypeEnum[job_name]
        return job  # pragma: no cover


class CombineMaterialization(BaseModel):
    """
    Stage for combining measures datasets at their shared grain and ingesting to Druid.
    Note that if there is only one upstream measures dataset, the Spark combining stage will
    be skipped and we ingest the aggregated measures directly to Druid.
    """

    node: NodeNameVersion
    query: str | None = None
    columns: List[ColumnMetadata]
    grain: list[str] = Field(
        description="The grain at which the node is being materialized.",
    )
    dimensions: list[str] = Field(
        description="List of dimensions included in this materialization.",
    )
    measures: list[MetricComponent] = Field(
        description="List of measures included in this materialization.",
    )

    timestamp_column: str | None = Field(
        description="Timestamp column name",
        default=None,
    )
    timestamp_format: str | None = Field(
        description="Timestamp format. Example: `yyyyMMdd`",
        default=None,
    )

    granularity: Granularity | None = Field(
        description="The time granularity for each materialization run. Examples: DAY, HOUR",
        default=None,
    )
    upstream_tables: list[str] = Field(default_factory=list)

    @computed_field  # type: ignore[misc]
    @property
    def output_table_name(self) -> str:
        """
        Builds an output table name based on the node and a hash of its unique key.
        """
        unique_string = "|".join(
            [
                self.node.name,
                str(self.node.version),
                self.timestamp_column or "",
                ",".join(sorted(self.grain)),
                ",".join(sorted(self.dimensions)),
                ",".join(sorted([measure.name for measure in self.measures])),
            ],
        )
        unique_hash = hashlib.sha256(unique_string.encode()).hexdigest()[:16]
        return f"{self.node.name}_{self.node.version}_{unique_hash}".replace(".", "_")

    def metrics_spec(self) -> list[dict[str, Any]]:
        """
        Returns the Druid metrics spec for ingestion
        """
        column_mapping = {col.name: col.type for col in self.columns}  # type: ignore
        return [
            {
                "fieldName": measure.name,
                "name": measure.name,
                "type": DRUID_AGG_MAPPING[
                    (column_mapping[measure.name], measure.aggregation.lower())
                ],
            }
            for measure in self.measures
            if measure.aggregation
            and (
                column_mapping.get(measure.name),
                measure.aggregation.lower(),
            )
            in DRUID_AGG_MAPPING
        ]

    @computed_field  # type: ignore[misc]
    @property
    def druid_spec(self) -> str:
        """
        Builds the Druid ingestion spec based on the materialization config.
        """
        return self.build_druid_spec()

    def build_druid_spec(self):
        """
        Builds the Druid ingestion spec from a materialization config.
        """
        # A temporal partition should be configured on the cube, raise an error if not
        if not self.timestamp_column:
            raise DJInvalidInputException(  # pragma: no cover
                "There must be at least one time-based partition configured"
                " on this cube or it cannot be materialized to Druid.",
            )

        druid_datasource_name = f"dj__{self.output_table_name}"

        # if there are categorical partitions, we can additionally include one of them
        # in the partitionDimension field under partitionsSpec
        druid_spec: Dict = {
            "dataSchema": {
                "dataSource": druid_datasource_name,
                "parser": {
                    "parseSpec": {
                        "format": "parquet",
                        "dimensionsSpec": {
                            "dimensions": sorted(
                                list(set(self.dimensions)),  # type: ignore
                            ),
                        },
                        "timestampSpec": {
                            "column": self.timestamp_column,
                            "format": self.timestamp_format,
                        },
                    },
                },
                "metricsSpec": self.metrics_spec(),
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": str(self.granularity).upper(),
                    "intervals": [],  # this should be set at runtime
                },
            },
            "tuningConfig": {
                "partitionsSpec": {
                    "targetPartitionSize": 5000000,
                    "type": "hashed",
                },
                "useCombiner": True,
                "type": "hadoop",
            },
        }
        return druid_spec

    def model_dump(self, **kwargs):  # pragma: no cover
        base = super().model_dump(**kwargs)
        base["druid_spec"] = self.build_druid_spec()
        base["output_table_name"] = self.output_table_name
        return base


class DruidCubeConfig(BaseModel):
    """
    Represents a DruidCube job that contains multiple MeasuresMaterialization configurations.
    """

    cube: NodeNameVersion

    dimensions: list[str]
    metrics: list[CubeMetric]

    # List of MeasuresMaterialization configurations.
    measures_materializations: List[MeasuresMaterialization]

    # List of materializations used to combine measures outputs. For hyper efficient
    # Druid queries, there should ideally only be a single one, but this may not be
    # possible for metrics at different levels.
    combiners: list[CombineMaterialization]


class DruidCubeMaterializationInput(BaseModel):
    """
    Materialization info as passed to the query service.
    """

    name: str

    # Frozen cube info at the time of materialization
    cube: NodeNameVersion
    dimensions: list[str]
    metrics: list[CubeMetric]

    # Materialization metadata
    strategy: MaterializationStrategy
    schedule: str
    job: str
    lookback_window: Optional[str] = "1 DAY"

    # List of measures materializations
    measures_materializations: List[MeasuresMaterialization]

    # List of materializations used to combine measures outputs. For hyper efficient
    # Druid queries, there should ideally only be a single one, but this may not be
    # possible for metrics at different levels.
    combiners: list[CombineMaterialization]


# =============================================================================
# V2: Pre-agg based cube materialization
# =============================================================================


class CubeMaterializeRequest(BaseModel):
    """
    Request for creating a cube materialization workflow.

    This creates a Druid workflow that:
    1. Waits for pre-agg tables to be available (VTTS)
    2. Runs combined SQL that reads from pre-agg tables
    3. Ingests the combined data into Druid
    """

    schedule: str = Field(
        description="Cron schedule for the materialization (e.g., '0 0 * * *' for daily)",
    )
    strategy: MaterializationStrategy = Field(
        default=MaterializationStrategy.INCREMENTAL_TIME,
        description="Materialization strategy (FULL or INCREMENTAL_TIME)",
    )
    lookback_window: str = Field(
        default="1 DAY",
        description="Lookback window for incremental materialization",
    )
    druid_datasource: Optional[str] = Field(
        default=None,
        description="Custom Druid datasource name. Defaults to 'dj__{cube_name}'",
    )
    run_backfill: bool = Field(
        default=True,
        description="Whether to run an initial backfill",
    )


class PreAggTableInfo(BaseModel):
    """Information about a pre-agg table used by the cube."""

    table_ref: str = Field(
        description="Full table reference (catalog.schema.table)",
    )
    parent_node: str = Field(
        description="Parent node name this pre-agg is derived from",
    )
    grain: List[str] = Field(
        description="Grain columns for this pre-agg",
    )


class CubeMaterializeResponse(BaseModel):
    """
    Response from cube materialization endpoint.

    Contains all information needed to create and execute the Druid cube workflow:
    - Pre-agg table dependencies for VTTS waits
    - Combined SQL for Druid ingestion
    - Druid spec for ingestion configuration
    """

    # Cube info
    cube: NodeNameVersion
    druid_datasource: str

    # Pre-agg dependencies - the Druid workflow should wait for these
    preagg_tables: List[PreAggTableInfo]

    # Combined SQL that reads from pre-agg tables
    combined_sql: str
    combined_columns: List[ColumnMetadata]
    combined_grain: List[str]

    # Druid ingestion spec
    druid_spec: Dict

    # Materialization config
    strategy: MaterializationStrategy
    schedule: str
    lookback_window: str

    # Workflow info
    workflow_urls: List[str] = Field(
        default_factory=list,
        description="URLs to the created workflows (if any)",
    )

    # Status
    message: str


class CubeMaterializationV2Input(BaseModel):
    """
    Input for creating a v2 cube materialization workflow (sent to query service).

    This creates a workflow that:
    1. Waits for pre-agg tables to be available (via VTTS)
    2. Runs combined SQL that reads from pre-agg tables
    3. Ingests the result to Druid
    """

    # Cube identity
    cube_name: str = Field(description="Full cube name (e.g., 'default.my_cube')")
    cube_version: str = Field(description="Cube version")

    # Pre-agg table dependencies
    preagg_tables: List[PreAggTableInfo] = Field(
        description="List of pre-agg tables the Druid ingestion depends on",
    )

    # Combined SQL
    combined_sql: str = Field(
        description="SQL that combines pre-agg tables (FULL OUTER JOIN + COALESCE)",
    )
    combined_columns: List[ColumnMetadata] = Field(
        description="Output columns of the combined SQL",
    )
    combined_grain: List[str] = Field(
        description="Shared grain of the combined query",
    )

    # Druid config
    druid_datasource: str = Field(
        description="Target Druid datasource name",
    )
    druid_spec: Dict = Field(
        description="Druid ingestion spec",
    )

    # Temporal partition info (for incremental)
    timestamp_column: str = Field(
        description="Name of the timestamp/partition column",
    )
    timestamp_format: str = Field(
        default="yyyyMMdd",
        description="Format of the timestamp column",
    )

    # Materialization config
    strategy: MaterializationStrategy = Field(
        default=MaterializationStrategy.INCREMENTAL_TIME,
    )
    schedule: str = Field(
        description="Cron schedule (e.g., '0 0 * * *' for daily)",
    )
    lookback_window: str = Field(
        default="1 DAY",
        description="Lookback window for incremental",
    )


class DruidCubeV3Config(BaseModel):
    """
    V3 Druid cube materialization config.

    This config is stored in Materialization.config for cubes using the V3
    build path (pre-aggregation based). It's distinct from V2's DruidMeasuresCubeConfig.

    Key differences from V2:
    - Uses pre-aggregation tables as intermediate storage
    - Stores metric components with their merge functions
    - Includes the combined SQL that joins pre-agg tables

    Backwards compatibility:
    - `dimensions` computed property aliases `combined_grain`
    - `metrics` computed property transforms measure components
    - `combiners` computed property wraps columns
    """

    version: Literal["v3"] = Field(
        default="v3",
        description="Config version discriminator",
    )

    # Druid target
    druid_datasource: str = Field(
        description="Target Druid datasource name",
    )

    # Pre-agg table dependencies
    preagg_tables: List[PreAggTableInfo] = Field(
        description="Pre-agg tables the Druid ingestion reads from",
    )

    # Combined SQL info
    combined_sql: str = Field(
        description="SQL that combines pre-agg tables",
    )
    combined_columns: List[ColumnMetadata] = Field(
        description="Output columns of the combined SQL",
    )
    combined_grain: List[str] = Field(
        description="Shared grain columns of the cube",
    )

    # Metric components for Druid metricsSpec
    measure_components: List[MetricComponent] = Field(
        default_factory=list,
        description="Metric components with aggregation/merge info",
    )
    component_aliases: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from component name to output column alias",
    )

    # Cube's metric node names (deprecated - use metrics field instead)
    cube_metrics: List[str] = Field(
        default_factory=list,
        description="List of metric node names in the cube",
    )

    # Metrics with their combiner expressions
    # This replaces the computed `metrics` property with stored values
    metrics: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of metrics with metric_expression for querying the materialized cube",
    )

    # Temporal partition info
    timestamp_column: str = Field(
        description="Name of the timestamp/partition column",
    )
    timestamp_format: str = Field(
        default="yyyyMMdd",
        description="Format of the timestamp column",
    )

    # Workflow tracking
    workflow_urls: List[str] = Field(
        default_factory=list,
        description="URLs for the materialization workflow",
    )

    @computed_field  # type: ignore[misc]
    @property
    def dimensions(self) -> List[str]:
        """
        Backwards compatibility: Returns dimensions (alias for combined_grain).

        DruidCubeConfig expects `config.dimensions` to get the list of
        dimension columns for the cube.
        """
        return self.combined_grain

    @computed_field  # type: ignore[misc]
    @property
    def combiners(self) -> List[Dict[str, Any]]:
        """
        Returns combiners with columns in expected format.

        DruidCubeConfig expects `config.combiners[0].columns` to get column
        metadata for building the options.columns mapping.
        """
        return [
            {
                "columns": [
                    {
                        "name": col.name,
                        "column": col.semantic_entity or col.name,
                    }
                    for col in self.combined_columns
                ],
            },
        ]

    # =========================================================================
    # Old UI Compatibility: Alias for workflow_urls
    # =========================================================================

    @computed_field  # type: ignore[misc]
    @property
    def urls(self) -> List[str]:
        """
        Old UI compatibility: Alias for workflow_urls.

        The old materialization UI looks for `config.urls` to display workflow links.
        This computed property provides backwards compatibility.
        """
        return self.workflow_urls
