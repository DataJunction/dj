"""Models for materialization"""

import enum
from collections.abc import Callable
from datetime import date
from typing import TYPE_CHECKING, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    field_validator,
    model_serializer,
    model_validator,
)

from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import (
    BackfillOutput,
    Granularity,
    PartitionColumnOutput,
    PartitionType,
)
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.naming import amenable_name
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.database.node import NodeRevision

DRUID_AGG_MAPPING = {
    ("bigint", "sum"): "longSum",
    ("int", "sum"): "longSum",
    ("double", "sum"): "doubleSum",
    ("float", "sum"): "floatSum",
    ("double", "min"): "doubleMin",
    ("double", "max"): "doubleMax",
    ("float", "min"): "floatMin",
    ("float", "max"): "floatMax",
    ("bigint", "min"): "longMin",
    ("int", "min"): "longMin",
    ("bigint", "max"): "longMax",
    ("int", "max"): "longMax",
    ("bigint", "count"): "longSum",
    ("int", "count"): "longSum",
    ("double", "count"): "longSum",
    ("float", "count"): "longSum",
    # HLL Sketch support (Druid DataSketches extension)
    ("binary", "hll_union_agg"): "HLLSketchMerge",
    ("binary", "hll_sketch_agg"): "HLLSketchMerge",
}

# Aggregation types that need special handling (extra config parameters)
DRUID_SKETCH_TYPES = {"HLLSketchMerge"}

# How long ingested cube data is kept in Druid. Without an explicit rule a datasource
# inherits the cluster default, which is unrelated to the span the cube holds, so an
# ingest reaching further back fails at load time with a retention rule violation.
# 400 days covers a full year of history plus room for late-arriving restatements.
DEFAULT_CUBE_RETENTION = "400 DAYS"


class CoverageSpec(BaseModel):
    """
    The span a cube's materialization should serve.

    Either a fixed span or a rolling one, never both:

      coverage: {from: 2024-01-01, to: 2024-06-30}
      coverage: {window: 800 DAYS}

    Omitting `to` leaves the span ongoing.
    """

    # Inclusive, matching FROM_PARTITION and TO_PARTITION.
    # `from` is a keyword, so alias it.
    from_: date | None = Field(default=None, alias="from")
    to: date | None = None

    # Free-form, like `lookback_window`, which parses nothing.
    window: str | None = None

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def check_form(self) -> "CoverageSpec":
        """
        Enforce that either a fixed span is declared or a rolling window is
        declared, but not both.
        """
        # Collapse whitespace so equal spans compare equal.
        if self.window is not None:
            self.window = " ".join(self.window.split())
        if self.window and (self.from_ or self.to):
            raise DJInvalidInputException(
                message="Declare a fixed span or a window, not both.",
            )
        if self.to and not self.from_:
            raise DJInvalidInputException(
                message="Coverage needs a `from` to go with `to`.",
            )
        if self.from_ and self.to and self.to < self.from_:
            raise DJInvalidInputException(
                message=(
                    f"Coverage ends before it starts: {self.to.isoformat()} "
                    f"precedes {self.from_.isoformat()}."
                ),
            )
        return self

    @model_serializer(mode="wrap")
    def serialize(self, handler: Callable[["CoverageSpec"], dict[str, Any]]) -> dict:
        """Emit `from`, and dates as strings, for JSON and YAML."""
        return {
            ("from" if key == "from_" else key): (
                value.isoformat() if isinstance(value, date) else value
            )
            for key, value in handler(self).items()
        }


class MaterializationStrategy(StrEnum):
    """
    Materialization strategies
    """

    # Replace the target dataset in full. Typically used for smaller transforms tables
    # -> Availability state: single table
    FULL = "full"

    # Full materialization into a new snapshot for each scheduled materialization run.
    # DJ will manage the location of the snapshot tables and return the appropriate snapshot
    # when requested
    # -> Availability state: multiple tables, one per snapshot. The snapshot key should be based on
    # when the upstream tables were last updated, with a new snapshot generated only if they were
    # updated
    SNAPSHOT = "snapshot"

    # Snapshot the query as a new partition in the target dataset, stamped with the current
    # date/time. This may be used to produce dimensional snapshot tables. The materialized table
    # will contain an appropriate snapshot column.
    # -> Availability state: single table.
    SNAPSHOT_PARTITION = "snapshot_partition"

    # Materialize incrementally based on the temporal partition column(s). This strategy will
    # process the last N periods (defined by lookback_window) of data and load the results into
    # corresponding target partitions. Requires a time column.
    # -> Availability state: single table
    INCREMENTAL_TIME = "incremental_time"

    # Create a database view from the query, doesn't materialize any data
    # -> Availability state: single view
    VIEW = "view"

    # An externally-built pre-aggregation table adopted via /preaggs/register.
    # DJ does not generate, run, or refresh it; it only routes queries to it.
    # -> Availability state: single table, reported by the external pipeline
    EXTERNAL = "external"


class GenericMaterializationInput(BaseModel):
    """
    The input when calling the query service's materialization
    API endpoint for a generic node.
    """

    name: str
    node_name: str
    node_version: str
    node_type: str
    job: str
    strategy: MaterializationStrategy
    schedule: str
    query: str
    upstream_tables: list[str]
    spark_conf: dict | None = None
    partitions: list[PartitionColumnOutput] | None = None
    columns: list[ColumnMetadata]
    lookback_window: str | None = "1 DAY"


class DruidMaterializationInput(GenericMaterializationInput):
    """
    The input when calling the query service's materialization
    API endpoint for a cube node.
    """

    druid_spec: dict


class MaterializationInfo(BaseModel):
    """
    The output when calling the query service's materialization
    API endpoint for a cube node.
    """

    output_tables: list[str]
    urls: list[str]
    workflow_names: list[str] = []


class MaterializationConfigOutput(BaseModel):
    """
    Output for materialization config.
    """

    node_revision_id: int
    name: str | None
    config: dict
    schedule: str
    job: str | None
    backfills: list[BackfillOutput]
    strategy: str | None
    deactivated_at: UTCDatetime | None

    model_config = ConfigDict(from_attributes=True)


class MaterializationConfigInfoUnified(
    MaterializationInfo,
    MaterializationConfigOutput,
):
    """
    Materialization config + info
    """


class SparkConf(RootModel):
    """Spark configuration"""

    root: dict[str, str] = {}


class GenericMaterializationConfigInput(BaseModel):
    """
    User-input portions of the materialization config
    """

    # Spark config
    spark: SparkConf | None = Field(default_factory=dict)

    # The time window to lookback when overwriting materialized datasets
    # This will only be used if a time partition was set on the node and
    # the materialization strategy is INCREMENTAL_TIME
    lookback_window: str | None = None


class GenericMaterializationConfig(GenericMaterializationConfigInput):
    """
    Generic node materialization config needed by any materialization choices
    and engine combinations
    """

    query: str | None = None
    columns: list[ColumnMetadata] | None = None
    upstream_tables: list[str] | None = None

    def temporal_partition(
        self,
        node_revision: "NodeRevision",
    ) -> list[PartitionColumnOutput]:
        """
        The temporal partition column names on the intermediate measures table
        """
        user_defined_temporal_columns = node_revision.temporal_partition_columns()
        if not user_defined_temporal_columns:
            return []
        user_defined_temporal_column = user_defined_temporal_columns[0]
        partition_ref = (
            user_defined_temporal_column.cube_element_name
            if node_revision.type == NodeType.CUBE
            else user_defined_temporal_column.name
        )
        return [
            PartitionColumnOutput(
                name=col.name,
                format=user_defined_temporal_column.partition.format,
                type_=user_defined_temporal_column.partition.type_,
                expression=str(
                    user_defined_temporal_column.partition.temporal_expression(),
                ),
            )
            for col in self.columns  # type: ignore
            if partition_ref in (col.semantic_entity, col.name)
        ]

    def categorical_partitions(
        self,
        node_revision: "NodeRevision",
    ) -> list[PartitionColumnOutput]:
        """
        The categorical partition column names on the intermediate measures table
        """
        user_defined_categorical_columns = {
            col.cube_element_name if node_revision.type == NodeType.CUBE else col.name
            for col in node_revision.categorical_partition_columns()
        }
        return [
            PartitionColumnOutput(
                name=col.name,
                type_=PartitionType.CATEGORICAL,
            )  # type: ignore
            for col in self.columns  # type: ignore
            if col.semantic_entity in user_defined_categorical_columns
        ]


class DruidConf(BaseModel):
    """Druid configuration"""

    granularity: str | None = None
    intervals: list[str] | None = None
    timestamp_column: str | None = None
    timestamp_format: str | None = None
    parse_spec_format: str | None = None


class Measure(BaseModel):
    """
    A measure with a simple aggregation
    """

    name: str
    field_name: str
    agg: str
    type: str

    def __eq__(self, other):
        return tuple(self.__dict__.items()) == tuple(
            other.__dict__.items(),
        )  # pragma: no cover

    def __hash__(self):
        return hash(tuple(self.__dict__.items()))  # pragma: no cover


class MetricMeasures(BaseModel):
    """
    Represent a metric as a set of measures, along with the expression for
    combining the measures to make the metric.
    """

    metric: str
    measures: list[Measure]
    combiner: str


class GenericCubeConfigInput(GenericMaterializationConfigInput):
    """
    Generic cube materialization config fields that require user input
    """

    dimensions: list[str] | None = None
    measures: dict[str, MetricMeasures] | None = None
    metrics: list[ColumnMetadata] | None = None


class GenericCubeConfig(GenericCubeConfigInput, GenericMaterializationConfig):
    """
    Generic cube materialization config needed by any materialization
    choices and engine combinations
    """


class DruidCubeConfigInput(GenericCubeConfigInput):
    """
    Specific Druid cube materialization fields that require user input
    """

    prefix: str | None = ""
    suffix: str | None = ""
    druid: DruidConf | None = None


class DruidMeasuresCubeConfig(DruidCubeConfigInput, GenericCubeConfig):
    """
    Specific cube materialization implementation with Spark and Druid ingestion and
    optional prefix and/or suffix to include with the materialized entity's name.
    """

    def metrics_spec(self) -> dict:
        """
        Returns the Druid metrics spec for ingestion
        """
        self.dimensions += [  # type: ignore
            measure.field_name
            for measure_group in self.measures.values()  # type: ignore
            for measure in measure_group.measures
            if (measure.type.lower(), measure.agg.lower()) not in DRUID_AGG_MAPPING
        ]
        return {
            measure.name: {
                "fieldName": measure.field_name,
                "name": measure.field_name,
                "type": DRUID_AGG_MAPPING[(measure.type.lower(), measure.agg.lower())],
            }
            for measure_group in self.measures.values()  # type: ignore
            for measure in measure_group.measures
            if (measure.type.lower(), measure.agg.lower()) in DRUID_AGG_MAPPING
        }

    def build_druid_spec(self, node_revision: "NodeRevision"):
        """
        Builds the Druid ingestion spec from a materialization config.
        """
        node_name = node_revision.name
        metrics_spec = list(self.metrics_spec().values())

        # A temporal partition should be configured on the cube, raise an error if not
        user_defined_temporal_partitions = node_revision.temporal_partition_columns()
        if not user_defined_temporal_partitions:
            raise DJInvalidInputException(  # pragma: no cover
                "There must be at least one time-based partition configured"
                " on this cube or it cannot be materialized to Druid.",
            )

        # Use the user-defined temporal partition if it exists
        user_defined_temporal_partition = None
        user_defined_temporal_partition = user_defined_temporal_partitions[0]
        # The cube column stores the role separately in ``dimension_column`` as
        # ``[role]``. Reconstruct the role-qualified form to match the v3
        # measures-query ``semantic_entity`` (e.g. ``node.col[role]``).
        partition_ref = user_defined_temporal_partition.cube_element_name
        timestamp_column = [
            col.name
            for col in self.columns  # type: ignore
            if col.semantic_entity == partition_ref
        ][0]

        druid_datasource_name = (
            self.prefix  # type: ignore
            + amenable_name(node_name)  # type: ignore
            + self.suffix  # type: ignore
        )
        # if there are categorical partitions, we can additionally include one of them
        # in the partitionDimension field under partitionsSpec
        druid_spec: dict = {
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
                            "column": timestamp_column,
                            "format": (
                                user_defined_temporal_partition.partition.format
                                if user_defined_temporal_partition
                                else "millis"
                            ),
                        },
                    },
                },
                "metricsSpec": metrics_spec,
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": str(
                        user_defined_temporal_partition.partition.granularity
                        if user_defined_temporal_partition
                        else Granularity.DAY,
                    ).upper(),
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


class DruidMetricsCubeConfig(DruidMeasuresCubeConfig):
    """
    Specific cube materialization implementation with Spark and Druid ingestion and
    optional prefix and/or suffix to include with the materialized entity's name.
    """

    def metrics_spec(self) -> dict:
        """
        Returns the Druid metrics spec for ingestion
        """
        return {
            metric.name: {
                "fieldName": metric.name,
                "name": metric.name,
                "type": DRUID_AGG_MAPPING[(metric.type.lower(), "sum")],
            }
            for metric in self.metrics  # type: ignore
            if (metric.type.lower(), "sum") in DRUID_AGG_MAPPING
        }


class MaterializationJobType(BaseModel):
    """
    Materialization job types. These job types will map to their implementations
    under the subclasses of `MaterializationJob`.
    """

    name: str
    label: str
    description: str

    # Node types that can be materialized with this job type
    allowed_node_types: list[NodeType]

    # The class that implements this job type, must subclass `MaterializationJob`
    job_class: str


class MaterializationJobTypeEnum(enum.Enum):
    """
    Available materialization job types
    """

    SPARK_SQL = MaterializationJobType(
        name="spark_sql",
        label="Spark SQL",
        description="Spark SQL materialization job",
        allowed_node_types=[NodeType.TRANSFORM, NodeType.DIMENSION, NodeType.CUBE],
        job_class="SparkSqlMaterializationJob",
    )

    DRUID_MEASURES_CUBE = MaterializationJobType(
        name="druid_measures_cube",
        label="Druid Measures Cube (Pre-Agg Cube)",
        description=(
            "Used to materialize a cube's measures to Druid for low-latency access to a set of "
            "metrics and dimensions. While the logical cube definition is at the level of metrics "
            "and dimensions, this materialized Druid cube will contain measures and dimensions,"
            " with rollup configured on the measures where appropriate."
        ),
        allowed_node_types=[NodeType.CUBE],
        job_class="DruidMeasuresCubeMaterializationJob",
    )

    DRUID_METRICS_CUBE = MaterializationJobType(
        name="druid_metrics_cube",
        label="Druid Metrics Cube (Post-Agg Cube)",
        description=(
            "Used to materialize a cube of metrics and dimensions to Druid for low-latency access."
            " The materialized cube is at the metric level, meaning that all metrics will be "
            "aggregated to the level of the cube's dimensions."
        ),
        allowed_node_types=[NodeType.CUBE],
        job_class="DruidMetricsCubeMaterializationJob",
    )

    DRUID_CUBE = MaterializationJobType(
        name="druid_cube",
        label="Druid Cube",
        description=(
            "Used to materialize a cube of metrics and dimensions to Druid for low-latency access."
            "Will replace the other cube materialization types."
        ),
        allowed_node_types=[NodeType.CUBE],
        job_class="DruidCubeMaterializationJob",
    )

    @classmethod
    def find_match(cls, job_name: str) -> "MaterializationJobTypeEnum":
        """Find a matching enum value for the given job name"""
        return [job_type for job_type in cls if job_type.value.job_class == job_name][0]


class UpsertMaterialization(BaseModel):
    """
    An upsert object for materialization configs
    """

    name: str | None = None
    job: Literal[
        "spark_sql",
        "druid_measures_cube",
        "druid_metrics_cube",
    ]
    config: (
        DruidCubeConfigInput
        | GenericCubeConfigInput
        | GenericMaterializationConfigInput
        | None
    ) = None
    schedule: str
    strategy: MaterializationStrategy

    @field_validator("job")
    def validate_job(
        cls,
        job: str | MaterializationJobTypeEnum,
    ) -> MaterializationJobTypeEnum:
        """
        Validates the `job` field. Converts to an enum if `job` is a string.
        """
        if isinstance(job, str):
            job_name = job.upper()
            options = MaterializationJobTypeEnum._member_names_
            if job_name not in options:
                raise DJInvalidInputException(  # pragma: no cover
                    http_status_code=404,
                    message=f"Materialization job type `{job.upper()}` not found. "
                    f"Available job types: {[job.name for job in MaterializationJobTypeEnum]}",
                )
            return MaterializationJobTypeEnum[job_name]
        return job  # pragma: no cover
