"""
Model for nodes.
"""
# pylint: disable=too-many-instance-attributes
import enum
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Extra
from pydantic import Field as PydanticField
from pydantic import root_validator
from sqlalchemy import JSON, DateTime, String
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.sql.schema import UniqueConstraint
from sqlalchemy.types import Enum
from sqlmodel import Field, Relationship, SQLModel
from typing_extensions import TypedDict

from dj.errors import DJInvalidInputException
from dj.models.base import BaseSQLModel, NodeColumns, generate_display_name
from dj.models.catalog import Catalog
from dj.models.column import Column, ColumnYAML
from dj.models.database import Database
from dj.models.engine import Dialect, Engine, EngineInfo, EngineRef
from dj.models.tag import Tag, TagNodeRelationship
from dj.sql.parsing.types import ColumnType
from dj.typing import UTCDatetime
from dj.utils import Version, amenable_name

DEFAULT_DRAFT_VERSION = Version(major=0, minor=1)
DEFAULT_PUBLISHED_VERSION = Version(major=1, minor=0)


@dataclass(frozen=True)
class BuildCriteria:
    """
    Criterion used for building
        - used to deterimine whether to use an availability state
    """

    timestamp: Optional[UTCDatetime] = None
    dialect: Dialect = Dialect.SPARK


class NodeRelationship(BaseSQLModel, table=True):  # type: ignore
    """
    Join table for self-referential many-to-many relationships between nodes.
    """

    parent_id: Optional[int] = Field(
        default=None,
        foreign_key="node.id",
        primary_key=True,
    )

    # This will default to `latest`, which points to the current version of the node,
    # or it can be a specific version.
    parent_version: Optional[str] = Field(
        default="latest",
    )

    child_id: Optional[int] = Field(
        default=None,
        foreign_key="noderevision.id",
        primary_key=True,
    )


class CubeRelationship(BaseSQLModel, table=True):  # type: ignore
    """
    Join table for many-to-many relationships between cube nodes and metric/dimension nodes.
    """

    __tablename__ = "cube"

    cube_id: Optional[int] = Field(
        default=None,
        foreign_key="noderevision.id",
        primary_key=True,
    )

    cube_element_id: Optional[int] = Field(
        default=None,
        foreign_key="column.id",
        primary_key=True,
    )


class NodeType(str, enum.Enum):
    """
    Node type.

    A node can have 4 types, currently:

    1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.
    2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.
    3. METRIC nodes are leaves in the DAG, and have a single aggregation query.
    4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.
    5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.
    """

    SOURCE = "source"
    TRANSFORM = "transform"
    METRIC = "metric"
    DIMENSION = "dimension"
    CUBE = "cube"


class NodeMode(str, enum.Enum):
    """
    Node mode.

    A node can be in one of the following modes:

    1. PUBLISHED - Must be valid and not cause any child nodes to be invalid
    2. DRAFT - Can be invalid, have invalid parents, and include dangling references
    """

    PUBLISHED = "published"
    DRAFT = "draft"


class NodeStatus(str, enum.Enum):
    """
    Node status.

    A node can have one of the following statuses:

    1. VALID - All references to other nodes and node columns are valid
    2. INVALID - One or more parent nodes are incompatible or do not exist
    """

    VALID = "valid"
    INVALID = "invalid"


class NodeYAML(TypedDict, total=False):
    """
    Schema of a node in the YAML file.
    """

    description: str
    display_name: str
    type: NodeType
    query: str
    columns: Dict[str, ColumnYAML]


class NodeBase(BaseSQLModel):
    """
    A base node.
    """

    name: str = Field(sa_column=SqlaColumn("name", String, unique=True))
    type: NodeType = Field(sa_column=SqlaColumn(Enum(NodeType)))
    display_name: Optional[str] = Field(
        sa_column=SqlaColumn(
            "display_name",
            String,
            default=generate_display_name("name"),
        ),
        max_length=100,
    )


class NodeRevisionBase(BaseSQLModel):
    """
    A base node revision.
    """

    name: str = Field(
        sa_column=SqlaColumn("name", String, unique=False),
        foreign_key="node.name",
    )
    display_name: Optional[str] = Field(
        sa_column=SqlaColumn(
            "display_name",
            String,
            default=generate_display_name("name"),
        ),
    )
    type: NodeType = Field(sa_column=SqlaColumn(Enum(NodeType)))
    description: str = ""
    query: Optional[str] = None
    mode: NodeMode = NodeMode.PUBLISHED


class MissingParent(BaseSQLModel, table=True):  # type: ignore
    """
    A missing parent node
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(sa_column=SqlaColumn("name", String))
    created_at: UTCDatetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )


class NodeMissingParents(BaseSQLModel, table=True):  # type: ignore
    """
    Join table for missing parents
    """

    missing_parent_id: Optional[int] = Field(
        default=None,
        foreign_key="missingparent.id",
        primary_key=True,
    )
    referencing_node_id: Optional[int] = Field(
        default=None,
        foreign_key="noderevision.id",
        primary_key=True,
    )


class AvailabilityStateBase(BaseSQLModel):
    """
    An availability state base
    """

    catalog: str
    schema_: Optional[str] = Field(default=None)
    table: str
    valid_through_ts: int
    max_partition: List[str] = Field(sa_column=SqlaColumn(JSON))
    min_partition: List[str] = Field(sa_column=SqlaColumn(JSON))


class AvailabilityState(AvailabilityStateBase, table=True):  # type: ignore
    """
    The availability of materialized data for a node
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    updated_at: UTCDatetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )

    def is_available(
        self,
        criteria: Optional[BuildCriteria] = None,  # pylint: disable=unused-argument
    ) -> bool:  # pragma: no cover
        """
        Determine whether an availability state is useable given criteria
        """
        # Criteria to determine if an availability state should be used needs to be added
        return True


class NodeAvailabilityState(BaseSQLModel, table=True):  # type: ignore
    """
    Join table for availability state
    """

    availability_id: Optional[int] = Field(
        default=None,
        foreign_key="availabilitystate.id",
        primary_key=True,
    )
    node_id: Optional[int] = Field(
        default=None,
        foreign_key="noderevision.id",
        primary_key=True,
    )


class NodeNamespace(SQLModel, table=True):  # type: ignore
    """
    A node namespace
    """

    namespace: str = Field(nullable=False, unique=True, primary_key=True)


class Node(NodeBase, table=True):  # type: ignore
    """
    Node that acts as an umbrella for all node revisions
    """

    __table_args__ = (
        UniqueConstraint("name", "namespace", name="unique_node_namespace_name"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    namespace: Optional[str] = "default"
    current_version: str = Field(default=str(DEFAULT_DRAFT_VERSION))
    created_at: UTCDatetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )
    deactivated_at: UTCDatetime = Field(
        nullable=True,
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default=None,
    )

    revisions: List["NodeRevision"] = Relationship(back_populates="node")
    current: "NodeRevision" = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "and_(Node.id==NodeRevision.node_id, "
            "Node.current_version == NodeRevision.version)",
            "viewonly": True,
            "uselist": False,
        },
    )

    children: List["NodeRevision"] = Relationship(
        back_populates="parents",
        link_model=NodeRelationship,
        sa_relationship_kwargs={
            "primaryjoin": "Node.id==NodeRelationship.parent_id",
            "secondaryjoin": "NodeRevision.id==NodeRelationship.child_id",
        },
    )

    tags: List["Tag"] = Relationship(
        back_populates="nodes",
        link_model=TagNodeRelationship,
        sa_relationship_kwargs={
            "primaryjoin": "TagNodeRelationship.node_id==Node.id",
            "secondaryjoin": "TagNodeRelationship.tag_id==Tag.id",
        },
    )

    def __hash__(self) -> int:
        return hash(self.id)


class MaterializationConfig(BaseSQLModel, table=True):  # type: ignore
    """
    Materialization configuration for a node and specific engines.
    """

    node_revision_id: int = Field(foreign_key="noderevision.id", primary_key=True)
    node_revision: "NodeRevision" = Relationship(
        back_populates="materialization_configs",
    )

    engine_id: int = Field(foreign_key="engine.id", primary_key=True)
    engine: Engine = Relationship()

    # A cron schedule to materialize this node by
    schedule: str

    # Arbitrary config relevant to the materialization engine
    config: Dict = Field(default={}, sa_column=SqlaColumn(JSON))

    # The name of the plugin that handles materialization, if any
    job: str = Field(
        default="MaterializationJob",
        sa_column=SqlaColumn("job", String),
    )


class NodeRevision(NodeRevisionBase, table=True):  # type: ignore
    """
    A node revision.
    """

    __table_args__ = (UniqueConstraint("version", "node_id"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    version: Optional[str] = Field(default=str(DEFAULT_DRAFT_VERSION))
    node_id: Optional[int] = Field(foreign_key="node.id")
    node: Node = Relationship(back_populates="revisions")
    catalog_id: int = Field(default=None, foreign_key="catalog.id")
    catalog: Catalog = Relationship(
        back_populates="node_revisions",
        sa_relationship_kwargs={
            "lazy": "joined",
        },
    )
    schema_: Optional[str] = None
    table: Optional[str] = None

    # A list of metric columns and dimension columns, only used by cube nodes
    cube_elements: List["Column"] = Relationship(
        link_model=CubeRelationship,
        sa_relationship_kwargs={
            "primaryjoin": "NodeRevision.id==CubeRelationship.cube_id",
            "secondaryjoin": "Column.id==CubeRelationship.cube_element_id",
            "lazy": "joined",
        },
    )
    status: NodeStatus = NodeStatus.INVALID
    updated_at: UTCDatetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )

    parents: List["Node"] = Relationship(
        back_populates="children",
        link_model=NodeRelationship,
        sa_relationship_kwargs={
            "primaryjoin": "NodeRevision.id==NodeRelationship.child_id",
            "secondaryjoin": "Node.id==NodeRelationship.parent_id",
        },
    )

    parent_links: List[NodeRelationship] = Relationship()

    missing_parents: List[MissingParent] = Relationship(
        link_model=NodeMissingParents,
        sa_relationship_kwargs={
            "primaryjoin": "NodeRevision.id==NodeMissingParents.referencing_node_id",
            "secondaryjoin": "MissingParent.id==NodeMissingParents.missing_parent_id",
            "cascade": "all, delete",
        },
    )

    columns: List[Column] = Relationship(
        link_model=NodeColumns,
        sa_relationship_kwargs={
            "primaryjoin": "NodeRevision.id==NodeColumns.node_id",
            "secondaryjoin": "Column.id==NodeColumns.column_id",
            "cascade": "all, delete",
        },
    )

    # The availability of materialized data needs to be stored on the NodeRevision
    # level in order to support pinned versions, where a node owner wants to pin
    # to a particular upstream node version.
    availability: Optional[AvailabilityState] = Relationship(
        link_model=NodeAvailabilityState,
        sa_relationship_kwargs={
            "primaryjoin": "NodeRevision.id==NodeAvailabilityState.node_id",
            "secondaryjoin": "AvailabilityState.id==NodeAvailabilityState.availability_id",
            "cascade": "all, delete",
            "uselist": False,
        },
    )

    # Nodes of type SOURCE will not have this property as their materialization
    # is not managed as a part of this service
    materialization_configs: List[MaterializationConfig] = Relationship(
        back_populates="node_revision",
    )

    def __hash__(self) -> int:
        return hash(self.id)

    def primary_key(self) -> List[Column]:
        """
        Returns the primary key columns of this node.
        """
        primary_key_columns = []
        for col in self.columns:  # pylint: disable=not-an-iterable
            if col.has_primary_key_attribute():
                primary_key_columns.append(col)
        return primary_key_columns

    @staticmethod
    def format_metric_alias(query: str, name: str) -> str:
        """
        Metric aliases must have the same name as the node.
        """
        from dj.sql.parsing import ast  # pylint: disable=import-outside-toplevel
        from dj.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
            parse,
        )

        tree = parse(query)
        projection_0 = tree.select.projection[0]
        # the expression has to be something we can get a name out of
        if not hasattr(projection_0, "alias_or_name"):
            raise DJInvalidInputException(
                f"Metric expression of type {type(projection_0).__name__} "
                f"must be aliased as the node name: `{amenable_name(name)}`",
            )
        # if the name is not what we expect, check if it is an alias
        # if it is an alias, we will raise because the user will have
        # deliberately named this expression
        # otherwise, we will just add the alias we want e.g. the node name
        expr_name: ast.Name = projection_0.alias_or_name  # type: ignore
        if expr_name.name != amenable_name(name):
            if expr_name.parent_key == "alias":
                raise DJInvalidInputException(
                    "Invalid Metric. The expression in the projection "
                    "cannot have alias different from the node name. Got "
                    f"`{expr_name}` but expected `{amenable_name(name)}`",
                )
            tree.select.projection[0] = projection_0.set_alias(
                ast.Name(amenable_name(name)),
            )
        return str(tree)

    def check_metric(self):
        """
        Check if the Node defines a metric.

        The Node SQL query should have a single expression in its
        projections and it should be an aggregation function.
        """
        from dj.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
            parse,
        )

        # must have a single expression
        tree = parse(self.query)
        if len(tree.select.projection) != 1:
            raise DJInvalidInputException(
                "Metric queries can only have a single "
                f"expression, found {len(tree.select.projection)}",
            )
        projection_0 = tree.select.projection[0]

        if (
            not hasattr(projection_0, "is_aggregation")
            or not projection_0.is_aggregation()  # type: ignore
        ):
            raise DJInvalidInputException(
                f"Metric {self.name} has an invalid query, "
                "should have a single aggregation",
            )

    def extra_validation(self) -> None:
        """
        Extra validation for node data.
        """
        if self.type in (NodeType.SOURCE,):
            if self.query:
                raise DJInvalidInputException(
                    f"Node {self.name} of type {self.type} should not have a query",
                )

        if self.type in {NodeType.TRANSFORM, NodeType.METRIC, NodeType.DIMENSION}:
            if not self.query:
                raise DJInvalidInputException(
                    f"Node {self.name} of type {self.type} needs a query",
                )

        if self.type == NodeType.METRIC:
            self.check_metric()

        if self.type == NodeType.CUBE:
            if not self.cube_elements:
                raise DJInvalidInputException(
                    f"Node {self.name} of type cube node needs cube elements",
                )

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        extra = Extra.allow


class ImmutableNodeFields(BaseSQLModel):
    """
    Node fields that cannot be changed
    """

    name: str
    namespace: str = "default"


class MutableNodeFields(BaseSQLModel):
    """
    Node fields that can be changed.
    """

    display_name: Optional[str]
    description: str
    mode: NodeMode
    primary_key: Optional[List[str]]


class MutableNodeQueryField(BaseSQLModel):
    """
    Query field for node.
    """

    query: str


class NodeNameOutput(SQLModel):
    """
    Node name only
    """

    name: str


class NodeNameList(SQLModel):
    """
    List of node names
    """

    __root__: List[str]


class AttributeTypeName(BaseSQLModel):
    """
    Attribute type name.
    """

    namespace: str
    name: str


class AttributeOutput(BaseSQLModel):
    """
    Column attribute output.
    """

    attribute_type: AttributeTypeName


class ColumnOutput(SQLModel):
    """
    A simplified column schema, without ID or dimensions.
    """

    name: str
    type: ColumnType
    attributes: Optional[List[AttributeOutput]]
    dimension: Optional[NodeNameOutput]

    class Config:  # pylint: disable=too-few-public-methods
        """
        Should perform validation on assignment
        """

        validate_assignment = True

    @root_validator
    def type_string(cls, values):  # pylint: disable=no-self-argument
        """
        Extracts the type as a string
        """
        values["type"] = str(values.get("type"))
        return values


class SourceColumnOutput(SQLModel):
    """
    A column used in creation of a source node
    """

    name: str
    type: ColumnType
    attributes: Optional[List[AttributeOutput]]
    dimension: Optional[str]

    class Config:  # pylint: disable=too-few-public-methods
        """
        Should perform validation on assignment
        """

        validate_assignment = True

    @root_validator
    def type_string(cls, values):  # pylint: disable=no-self-argument
        """
        Extracts the type as a string
        """
        values["type"] = str(values.get("type"))
        return values


class SourceNodeFields(BaseSQLModel):
    """
    Source node fields that can be changed.
    """

    catalog: str
    schema_: str
    table: str
    columns: Optional[List["SourceColumnOutput"]] = []


class CubeNodeFields(BaseSQLModel):
    """
    Cube node fields that can be changed
    """

    display_name: Optional[str]
    metrics: List[str]
    dimensions: List[str]
    filters: Optional[List[str]]
    description: str
    mode: NodeMode


#
# Create and Update objects
#


class CreateNode(ImmutableNodeFields, MutableNodeFields, MutableNodeQueryField):
    """
    Create non-source node object.
    """


class CreateSourceNode(ImmutableNodeFields, MutableNodeFields, SourceNodeFields):
    """
    A create object for source nodes
    """


class UpsertMaterializationConfig(BaseSQLModel):
    """
    An upsert object for materialization configs
    """

    engine: EngineRef
    config: Dict
    schedule: str


class SparkConf(BaseSQLModel):
    """Spark configuration"""

    __root__: Dict[str, str]


class DruidConf(BaseSQLModel):
    """Druid configuration"""

    granularity: str
    intervals: Optional[List[str]]
    timestamp_column: str
    parse_spec_format: Optional[str]


class GenericCubeConfig(BaseModel):
    """
    Generic cube materialization config needed by any materialization
    choices and engine combinations
    """

    node_name: Optional[str]
    query: Optional[str]
    dimensions: Optional[List[str]]
    measures: Optional[Dict[str, List[Dict[str, str]]]]
    partitions: Optional[List[str]]


class DruidCubeConfig(GenericCubeConfig):
    """
    Specific cube materialization implementation with Spark and Druid ingestion and
    optional prefix and/or suffix to include with the materialized entity's name.
    """

    prefix: Optional[str] = ""
    suffix: Optional[str] = ""
    spark: Optional[SparkConf]
    druid: Optional[DruidConf]


class UpsertCubeMaterializationConfig(BaseSQLModel):
    """
    An upsert object for cube materialization configs
    """

    engine: EngineRef
    config: Union[DruidCubeConfig, GenericCubeConfig]
    schedule: str


class CreateCubeNode(ImmutableNodeFields, CubeNodeFields):
    """
    A create object for cube nodes
    """

    materialization_configs: Optional[List[UpsertCubeMaterializationConfig]] = []


class UpdateNode(MutableNodeFields, SourceNodeFields):
    """
    Update node object where all fields are optional
    """

    __annotations__ = {
        k: Optional[v]
        for k, v in {
            **SourceNodeFields.__annotations__,  # pylint: disable=E1101
            **MutableNodeFields.__annotations__,  # pylint: disable=E1101
            **MutableNodeQueryField.__annotations__,  # pylint: disable=E1101
        }.items()
    }

    class Config:  # pylint: disable=too-few-public-methods
        """
        Do not allow fields other than the ones defined here.
        """

        extra = Extra.forbid


#
# Response output objects
#


class OutputModel(BaseModel):
    """
    An output model with the ability to flatten fields. When fields are created with
    `Field(flatten=True)`, the field's values will be automatically flattened into the
    parent output model.
    """

    def _iter(self, *args, to_dict: bool = False, **kwargs):
        for dict_key, value in super()._iter(to_dict, *args, **kwargs):
            if to_dict and self.__fields__[dict_key].field_info.extra.get(
                "flatten",
                False,
            ):
                assert isinstance(value, dict)
                for key, val in value.items():
                    yield key, val
            else:
                yield dict_key, value


class TableOutput(SQLModel):
    """
    Output for table information.
    """

    id: Optional[int]
    catalog: Optional[Catalog]
    schema_: Optional[str]
    table: Optional[str]
    database: Optional[Database]


class MaterializationConfigOutput(SQLModel):
    """
    Output for materialization config.
    """

    engine: EngineInfo
    config: Dict
    schedule: str
    job: str


class NodeRevisionOutput(SQLModel):
    """
    Output for a node revision with information about columns and if it is a metric.
    """

    id: int = Field(alias="node_revision_id")
    node_id: int
    type: NodeType
    name: str
    display_name: str
    version: str
    status: NodeStatus
    mode: NodeMode
    catalog: Optional[Catalog]
    schema_: Optional[str]
    table: Optional[str]
    description: str = ""
    query: Optional[str] = None
    availability: Optional[AvailabilityState] = None
    columns: List[ColumnOutput]
    updated_at: UTCDatetime
    materialization_configs: List[MaterializationConfigOutput]
    parents: List[NodeNameOutput]

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        allow_population_by_field_name = True


class NodeOutput(OutputModel):
    """
    Output for a node that shows the current revision.
    """

    namespace: str
    current: NodeRevisionOutput = PydanticField(flatten=True)
    created_at: UTCDatetime
    tags: List["Tag"] = []


class NodeValidation(SQLModel):
    """
    A validation of a provided node definition
    """

    message: str
    status: NodeStatus
    node_revision: NodeRevision
    dependencies: List[NodeRevisionOutput]
    columns: List[Column]
