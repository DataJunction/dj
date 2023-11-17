"""Models for materialization"""
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, BaseModel, validator
from sqlalchemy import JSON
from sqlalchemy import Column as SqlaColumn
from sqlalchemy import DateTime, String
from sqlmodel import Field, Relationship, SQLModel, UniqueConstraint

from datajunction_server.models.base import BaseSQLModel
from datajunction_server.models.engine import Engine, EngineInfo, EngineRef
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


class GenericMaterializationInput(BaseModel):
    """
    The input when calling the query service's materialization
    API endpoint for a generic node.
    """

    name: str
    node_name: str
    node_version: str
    node_type: str
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
    engine: EngineInfo
    config: Dict
    schedule: str
    job: str
    backfills: List[BackfillOutput]


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


class GenericMaterializationConfig(GenericMaterializationConfigInput):
    """
    Generic node materialization config needed by any materialization choices
    and engine combinations
    """

    query: Optional[str]
    columns: Optional[List[ColumnMetadata]]
    upstream_tables: Optional[List[str]]


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

    def temporal_partition(
        self,
        node_revision: "NodeRevision",
    ) -> List[PartitionColumnOutput]:
        """
        The temporal partition column names on the intermediate measures table
        """
        user_defined_temporal_columns = node_revision.temporal_partition_columns()
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
            if col.semantic_entity == user_defined_temporal_column.name
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
            "engine_id",
            name="node_revision_engine_uniq",
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

    engine_id: int = Field(foreign_key="engine.id")
    engine: Engine = Relationship()

    name: str

    # A cron schedule to materialize this node by
    schedule: str

    # Arbitrary config relevant to the materialization engine
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


class UpsertMaterialization(BaseSQLModel):
    """
    An upsert object for materialization configs
    """

    name: Optional[str]
    engine: EngineRef
    config: Union[
        DruidCubeConfigInput,
        GenericCubeConfigInput,
        GenericMaterializationConfigInput,
    ]
    schedule: str
