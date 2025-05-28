"""Models related to cube materialization"""

import hashlib
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator

from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.column import SemanticType
from datajunction_server.models.materialization import (
    DRUID_AGG_MAPPING,
    MaterializationJobTypeEnum,
    MaterializationStrategy,
)
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.models.query import ColumnMetadata


class Aggregability(StrEnum):
    """
    Type of allowed aggregation for a given metric component.
    """

    FULL = "full"
    LIMITED = "limited"
    NONE = "none"


class AggregationRule(BaseModel):
    """
    The aggregation rule for the metric component. If the Aggregability type is LIMITED, the
    `level` should be specified to highlight the level at which the metric component needs to
    be aggregated in order to support the specified aggregation function.

    For example, consider a metric like COUNT(DISTINCT user_id). It can be decomposed into a
    single metric component with LIMITED aggregability, i.e., it is only aggregatable if the
    component is calculated at the `user_id` level:
    - name: num_users
      expression: DISTINCT user_id
      aggregation: COUNT
      rule:
        type: LIMITED
        level: ["user_id"]
    """

    type: Aggregability = Aggregability.NONE
    level: list[str] | None = None


class MetricComponent(BaseModel):
    """
    A reusable, named building block of a metric definition.

    A MetricComponent represents a SQL expression that can serve as an input to building
    a metric. It may be an aggregatable fact (e.g. `view_secs` in `SUM(view_secs)`),
    a conditional (e.g., `IF(x, y, z)` in `SUM(IF(x, y, z))`) or a distinct expression
    (e.g. `DISTINCT IF(x, y, z)`), or any derived input used in computing metrics.

    Components may be:
    - Aggregated directly to form a simple metric (e.g. `SUM(view_secs)`)
    - Combined with others to define derived metrics (e.g. `SUM(clicks) / SUM(view_secs)`)
    - Reused across multiple metrics

    Not all components require aggregation — some may be passed through as-is or grouped by,
    with the group-by grain defined by the aggregation rule.

    Attributes:
        name: A unique name for the component, typically derived from its expression.
        expression: The raw SQL expression that defines the component.
        aggregation: The aggregation function to apply (e.g. 'SUM', 'COUNT'), or None if unaggregated.
        rule: Aggregation rules that define how and when the component can be aggregated
            (e.g., full or limited), and at what grain it can be aggregated.
    """

    name: str
    expression: str  # A SQL expression for defining the measure
    aggregation: str | None
    rule: AggregationRule


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

    def dict(self, **kwargs):
        base = super().dict(**kwargs)
        base["output_table_name"] = self.output_table_name
        # base["druid_spec"] = self.build_druid_spec()
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
    job: MaterializationJobTypeEnum = MaterializationJobTypeEnum.DRUID_CUBE

    # Only FULL or INCREMENTAL_TIME is available for cubes
    strategy: MaterializationStrategy = MaterializationStrategy.INCREMENTAL_TIME

    # Cron schedule
    schedule: str

    # Lookback window, only relevant if materialization strategy is INCREMENTAL_TIME
    lookback_window: str | None = "1 DAY"

    @validator("job", pre=True)
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
        return job


class CombineMaterialization(BaseModel):
    """
    Stage for combining measures datasets at their shared grain and ingesting to Druid.
    Note that if there is only one upstream measures dataset, the Spark combining stage will
    be skipped and we ingest the aggregated measures directly to Druid.
    """

    node: NodeNameVersion
    query: str | None
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

    timestamp_column: str | None = Field(description="Timestamp column name")
    timestamp_format: str | None = Field(
        description="Timestamp format. Example: `yyyyMMdd`",
    )

    granularity: Granularity | None = Field(
        description="The time granularity for each materialization run. Examples: DAY, HOUR",
    )
    upstream_tables: list[str] = []

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

    def dict(self, **kwargs):
        base = super().dict(**kwargs)
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
