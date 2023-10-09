"""Models for materialization"""
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, BaseModel, validator
from sqlalchemy import JSON
from sqlalchemy import Column as SqlaColumn
from sqlalchemy import DateTime, String
from sqlmodel import Field, Relationship, SQLModel, UniqueConstraint

from datajunction_server.models.base import BaseSQLModel
from datajunction_server.models.engine import Engine, EngineInfo, EngineRef
from datajunction_server.models.partition import Backfill, BackfillOutput
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.models import NodeRevision


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
