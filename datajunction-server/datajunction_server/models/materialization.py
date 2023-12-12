"""Models for materialization"""
import enum
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, BaseModel, validator
from sqlalchemy import JSON
from sqlalchemy import Column as SqlaColumn
from sqlalchemy import DateTime, String
from sqlalchemy.types import Enum
from sqlmodel import Field, Relationship, SQLModel, UniqueConstraint

from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.base import BaseSQLModel
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import (
    Backfill,
    BackfillOutput,
    PartitionColumnOutput,
    PartitionType,
)
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.models import NodeRevision

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
    upstream_tables: List[str]
    spark_conf: Optional[Dict] = None
    partitions: Optional[List[Dict]] = None
    columns: List[ColumnMetadata]


class DruidMaterializationInput(GenericMaterializationInput):
    """
    The input when calling the query service's materialization
    API endpoint for a cube node.
    """

    druid_spec: Dict


class MaterializationInfo(BaseModel):
    """
    The output when calling the query service's materialization
    API endpoint for a cube node.
    """

    output_tables: List[str]
    urls: List[AnyHttpUrl]


class MaterializationConfigOutput(SQLModel):
    """
    Output for materialization config.
    """

    name: Optional[str]
    config: Dict
    schedule: str
    job: Optional[str]
    backfills: List[BackfillOutput]
    strategy: Optional[str]


class MaterializationConfigInfoUnified(
    MaterializationInfo,
    MaterializationConfigOutput,
):
    """
    Materialization config + info
    """


class SparkConf(BaseSQLModel):
    """Spark configuration"""

    __root__: Dict[str, str] = {}


class GenericMaterializationConfigInput(BaseModel):
    """
    User-input portions of the materialization config
    """

    # Spark config
    spark: Optional[SparkConf]

    # The time window to lookback when overwriting materialized datasets
    # This will only be used if a time partition was set on the node and
    # the materialization strategy is INCREMENTAL_TIME
    lookback_window: Optional[str]


class GenericMaterializationConfig(GenericMaterializationConfigInput):
    """
    Generic node materialization config needed by any materialization choices
    and engine combinations
    """

    query: Optional[str]
    columns: Optional[List[ColumnMetadata]]
    upstream_tables: Optional[List[str]]

    def temporal_partition(
        self,
        node_revision: "NodeRevision",
    ) -> List[PartitionColumnOutput]:
        """
        The temporal partition column names on the intermediate measures table
        """
        user_defined_temporal_columns = node_revision.temporal_partition_columns()
        if not user_defined_temporal_columns:
            return []
        user_defined_temporal_column = user_defined_temporal_columns[0]
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
            if user_defined_temporal_column.name in (col.semantic_entity, col.name)
        ]

    def categorical_partitions(
        self,
        node_revision: "NodeRevision",
    ) -> List[PartitionColumnOutput]:
        """
        The categorical partition column names on the intermediate measures table
        """
        user_defined_categorical_columns = {
            col.name for col in node_revision.categorical_partition_columns()
        }
        return [
            PartitionColumnOutput(
                name=col.name,
                type_=PartitionType.CATEGORICAL,
            )
            for col in self.columns  # type: ignore
            if col.column in user_defined_categorical_columns
        ]


class DruidConf(BaseSQLModel):
    """Druid configuration"""

    granularity: Optional[str]
    intervals: Optional[List[str]]
    timestamp_column: Optional[str]
    timestamp_format: Optional[str]
    parse_spec_format: Optional[str]


class Measure(SQLModel):
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


class MetricMeasures(SQLModel):
    """
    Represent a metric as a set of measures, along with the expression for
    combining the measures to make the metric.
    """

    metric: str
    measures: List[Measure]  #
    combiner: str


class GenericCubeConfigInput(GenericMaterializationConfigInput):
    """
    Generic cube materialization config fields that require user input
    """

    dimensions: Optional[List[str]]
    measures: Optional[Dict[str, MetricMeasures]]


class GenericCubeConfig(GenericCubeConfigInput, GenericMaterializationConfig):
    """
    Generic cube materialization config needed by any materialization
    choices and engine combinations
    """


class DruidCubeConfigInput(GenericCubeConfigInput):
    """
    Specific Druid cube materialization fields that require user input
    """

    prefix: Optional[str] = ""
    suffix: Optional[str] = ""
    druid: Optional[DruidConf]


class DruidCubeConfig(DruidCubeConfigInput, GenericCubeConfig):
    """
    Specific cube materialization implementation with Spark and Druid ingestion and
    optional prefix and/or suffix to include with the materialized entity's name.
    """

    def metrics_spec(self) -> Dict:
        """
        Returns the Druid metrics spec for ingestion
        """
        return {
            measure.name: {
                "fieldName": measure.field_name,
                "name": measure.field_name,
                "type": DRUID_AGG_MAPPING[(measure.type.lower(), measure.agg.lower())],
            }
            for measure_group in self.measures.values()  # type: ignore
            for measure in measure_group.measures
        }

    def build_druid_spec(self, node_revision: "NodeRevision"):
        """
        Builds the Druid ingestion spec from a materialization config.
        """
        from datajunction_server.utils import (  # pylint: disable=import-outside-toplevel
            amenable_name,
        )

        node_name = node_revision.name
        metrics_spec = list(self.metrics_spec().values())
        user_defined_temporal_partitions = node_revision.temporal_partition_columns()
        user_defined_temporal_partition = user_defined_temporal_partitions[0]
        output_temporal_partition_column = [
            col.name
            for col in self.columns  # type: ignore
            if col.semantic_entity == user_defined_temporal_partition.name
        ][0]
        druid_datasource_name = (
            self.prefix  # type: ignore
            + amenable_name(node_name)  # type: ignore
            + self.suffix  # type: ignore
        )
        # if there are categorical partitions, we can additionally include one of them
        # in the partitionDimension field under partitionsSpec
        druid_spec: Dict = {
            "dataSchema": {
                "dataSource": druid_datasource_name,
                "parser": {
                    "parseSpec": {
                        "format": "parquet",
                        "dimensionsSpec": {"dimensions": self.dimensions},
                        "timestampSpec": {
                            "column": output_temporal_partition_column,
                            "format": user_defined_temporal_partition.partition.format,
                        },
                    },
                },
                "metricsSpec": metrics_spec,
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": user_defined_temporal_partition.partition.granularity,
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


class Materialization(BaseSQLModel, table=True):  # type: ignore
    """
    Materialization configured for a node.
    """

    __table_args__ = (
        UniqueConstraint(
            "name",
            "node_revision_id",
            name="name_node_revision_uniq",
        ),
    )

    id: Optional[int] = Field(
        default=None,
        primary_key=True,
        sa_column_kwargs={
            "autoincrement": True,
        },
    )

    node_revision_id: int = Field(foreign_key="noderevision.id")
    node_revision: "NodeRevision" = Relationship(
        back_populates="materializations",
    )

    name: str

    strategy: MaterializationStrategy = Field(
        sa_column=SqlaColumn(Enum(MaterializationStrategy)),
    )

    # A cron schedule to materialize this node by
    schedule: str

    # Arbitrary config relevant to the materialization job
    config: Union[GenericMaterializationConfig, DruidCubeConfig] = Field(
        default={},
        sa_column=SqlaColumn(JSON),
    )

    # The name of the plugin that handles materialization, if any
    job: str = Field(
        default="MaterializationJob",
        sa_column=SqlaColumn("job", String),
    )

    deactivated_at: UTCDatetime = Field(
        nullable=True,
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default=None,
    )

    backfills: List[Backfill] = Relationship(
        back_populates="materialization",
        sa_relationship_kwargs={
            "primaryjoin": "Materialization.id==Backfill.materialization_id",
            "cascade": "all, delete",
        },
    )

    @validator("config")
    def config_validator(cls, value):  # pylint: disable=no-self-argument
        """Changes `config` to a dict prior to saving"""
        return value.dict()


class MaterializationJobType(BaseSQLModel):
    """
    Materialization job types. These job types will map to their implementations
    under the subclasses of `MaterializationJob`.
    """

    name: str
    label: str
    description: str

    # Node types that can be materialized with this job type
    allowed_node_types: List[NodeType]

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

    DRUID_CUBE = MaterializationJobType(
        name="druid_cube",
        label="Druid Cube",
        description=(
            "Used to materialize a cube to Druid for low-latency access to a set of metrics "
            "and dimensions. While the logical cube definition is at the level of metrics "
            "and dimensions, a materialized Druid cube will reference measures and dimensions,"
            " with rollup configured on the measures where appropriate."
        ),
        allowed_node_types=[NodeType.CUBE],
        job_class="DruidCubeMaterializationJob",
    )

    @classmethod
    def find_match(cls, job_name: str) -> "MaterializationJobTypeEnum":
        """Find a matching enum value for the given job name"""
        return [job_type for job_type in cls if job_type.value.job_class == job_name][0]


class UpsertMaterialization(BaseSQLModel):
    """
    An upsert object for materialization configs
    """

    name: Optional[str]
    job: MaterializationJobTypeEnum
    config: Union[
        DruidCubeConfigInput,
        GenericCubeConfigInput,
        GenericMaterializationConfigInput,
    ]
    schedule: str
    strategy: MaterializationStrategy

    @validator("job", pre=True)
    def validate_job(  # pylint: disable=no-self-argument
        cls,
        job: Union[str, MaterializationJobTypeEnum],
    ) -> MaterializationJobTypeEnum:
        """
        Validates the `job` field. Converts to an enum if `job` is a string.
        """
        if isinstance(job, str):
            job_name = job.upper()
            options = (
                MaterializationJobTypeEnum._member_names_  # pylint: disable=protected-access,no-member
            )
            if job_name not in options:
                raise DJInvalidInputException(
                    http_status_code=404,
                    message=f"Materialization job type `{job.upper()}` not found. "
                    f"Available job types: {[job.name for job in MaterializationJobTypeEnum]}",
                )
            return MaterializationJobTypeEnum[job_name]
        return job
