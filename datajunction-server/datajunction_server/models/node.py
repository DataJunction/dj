# pylint: disable=too-many-instance-attributes,too-many-lines,too-many-ancestors
"""
Model for nodes.
"""
import enum
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from pydantic import BaseModel, Extra, root_validator, validator
from pydantic.fields import Field
from pydantic.utils import GetterDict
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.types import Enum
from typing_extensions import TypedDict

from datajunction_server.api.graphql.scalars import Cursor
from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJError
from datajunction_server.models.base import labelize
from datajunction_server.models.catalog import CatalogInfo
from datajunction_server.models.column import ColumnYAML
from datajunction_server.models.database import DatabaseOutput
from datajunction_server.models.dimensionlink import LinkDimensionOutput
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import MaterializationConfigOutput
from datajunction_server.models.node_type import NodeNameOutput, NodeType
from datajunction_server.models.partition import PartitionOutput
from datajunction_server.models.tag import TagMinimum, TagOutput
from datajunction_server.models.user import UserNameOnly
from datajunction_server.sql.parsing.types import ColumnType
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import Version

DEFAULT_DRAFT_VERSION = Version(major=0, minor=1)
DEFAULT_PUBLISHED_VERSION = Version(major=1, minor=0)
MIN_VALID_THROUGH_TS = -sys.maxsize - 1


@dataclass(frozen=True)
class BuildCriteria:
    """
    Criterion used for building
        - used to deterimine whether to use an availability state
    """

    timestamp: Optional[UTCDatetime] = None
    dialect: Dialect = Dialect.SPARK
    target_node_name: Optional[str] = None


class NodeMode(StrEnum):
    """
    Node mode.

    A node can be in one of the following modes:

    1. PUBLISHED - Must be valid and not cause any child nodes to be invalid
    2. DRAFT - Can be invalid, have invalid parents, and include dangling references
    """

    PUBLISHED = "published"
    DRAFT = "draft"


class NodeStatus(StrEnum):
    """
    Node status.

    A node can have one of the following statuses:

    1. VALID - All references to other nodes and node columns are valid
    2. INVALID - One or more parent nodes are incompatible or do not exist
    """

    VALID = "valid"
    INVALID = "invalid"


class NodeValidationError(BaseModel):
    """
    Validation error
    """

    type: str
    message: str


class NodeStatusDetails(BaseModel):
    """
    Node status details. Contains a list of node errors or an empty list of the node status is valid
    """

    status: NodeStatus
    errors: List[NodeValidationError]


class NodeYAML(TypedDict, total=False):
    """
    Schema of a node in the YAML file.
    """

    description: str
    display_name: str
    type: NodeType
    query: str
    columns: Dict[str, ColumnYAML]


class NodeBase(BaseModel):
    """
    A base node.
    """

    name: str
    type: NodeType
    display_name: Optional[str] = Field(max_length=100)


class NodeRevisionBase(BaseModel):
    """
    A base node revision.
    """

    name: str
    display_name: Optional[str]
    type: NodeType
    description: Optional[str] = ""
    query: Optional[str] = None
    mode: NodeMode = NodeMode.PUBLISHED


class TemporalPartitionRange(BaseModel):
    """
    Any temporal partition range with a min and max partition.
    """

    min_temporal_partition: Optional[List[str]] = None
    max_temporal_partition: Optional[List[str]] = None

    def is_outside(self, other) -> bool:
        """
        Whether this temporal range is outside the time bounds of the other temporal range
        """
        return (
            self.min_temporal_partition < other.min_temporal_partition
            or self.max_temporal_partition > other.max_temporal_partition
        )


class PartitionAvailability(TemporalPartitionRange):
    """
    Partition-level availability
    """

    # This list maps to the ordered list of categorical partitions at the node level.
    # For example, if the node's `categorical_partitions` are configured as ["country", "group_id"],
    # a valid entry for `value` may be ["DE", null].
    value: List[Optional[str]]

    # Valid through timestamp
    valid_through_ts: Optional[int]


class AvailabilityNode(TemporalPartitionRange):
    """A node in the availability trie tracker"""

    children: Dict = {}
    valid_through_ts: Optional[int] = Field(default=MIN_VALID_THROUGH_TS)

    def merge_temporal(self, other: "AvailabilityNode"):
        """
        Merge the temporal ranges with each other by saving the largest
        possible time range.
        """
        self.min_temporal_partition = min(  # type: ignore
            self.min_temporal_partition,
            other.min_temporal_partition,
        )
        self.max_temporal_partition = max(  # type: ignore
            self.max_temporal_partition,
            other.max_temporal_partition,
        )
        self.valid_through_ts = max(  # type: ignore
            self.valid_through_ts,
            other.valid_through_ts,
        )


class AvailabilityTracker:
    """
    Tracks the availability of the partitions in a trie, which is used for merging
    availability states across categorical and temporal partitions.
    """

    def __init__(self):
        self.root = AvailabilityNode()

    def insert(self, partition):
        """
        Inserts the partition availability into the availability tracker trie.
        """
        current = self.root
        for value in partition.value:
            next_item = AvailabilityNode(
                min_temporal_partition=partition.min_temporal_partition,
                max_temporal_partition=partition.max_temporal_partition,
                valid_through_ts=partition.valid_through_ts,
            )
            # If a wildcard is found, only add this specific partition's
            # time range if it's wider than the range of the wildcard
            if None in current.children and partition.is_outside(
                current.children[None],
            ):
                wildcard_partition = current.children[None]
                next_item.merge_temporal(wildcard_partition)
                current.children[value] = next_item
            else:
                # Add if partition doesn't match any existing, otherwise merge with existing
                if value not in current.children:
                    current.children[value] = next_item
                else:
                    next_item = current.children[value]
                    next_item.merge_temporal(partition)

                # Remove extraneous partitions at this level if this partition value is a wildcard
                if value is None:
                    child_keys = [key for key in current.children if key is not None]
                    for child in child_keys:
                        child_partition = current.children[child]
                        if not child_partition.is_outside(partition):
                            del current.children[child]
            current = next_item

    def get_partition_range(self) -> List[PartitionAvailability]:
        """
        Gets the final set of merged partitions.
        """
        candidates: List[Tuple[AvailabilityNode, List[str]]] = [(self.root, [])]
        final_partitions = []
        while candidates:
            current, partition_list = candidates.pop()
            if current.children:
                for key, value in current.children.items():
                    candidates.append((value, partition_list + [key]))
            else:
                final_partitions.append(
                    PartitionAvailability(
                        value=partition_list,
                        min_temporal_partition=current.min_temporal_partition,
                        max_temporal_partition=current.max_temporal_partition,
                        valid_through_ts=current.valid_through_ts,
                    ),
                )
        return final_partitions


class AvailabilityStateBase(TemporalPartitionRange):
    """
    An availability state base
    """

    catalog: str
    schema_: Optional[str] = Field(default=None)
    table: str
    valid_through_ts: int
    url: Optional[str]
    links: Optional[Dict[str, Any]] = Field(default={})

    # An ordered list of categorical partitions like ["country", "group_id"]
    # or ["region_id", "age_group"]
    categorical_partitions: Optional[List[str]] = Field(default=[])

    # An ordered list of temporal partitions like ["date", "hour"] or ["date"]
    temporal_partitions: Optional[List[str]] = Field(default=[])

    # Node-level temporal ranges
    min_temporal_partition: Optional[List[str]] = Field(default=[])
    max_temporal_partition: Optional[List[str]] = Field(default=[])

    # Partition-level availabilities
    partitions: Optional[List[PartitionAvailability]] = Field(default=[])

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True

    @validator("partitions")
    def validate_partitions(cls, partitions):  # pylint: disable=no-self-argument
        """
        Validator for partitions
        """
        return [partition.dict() for partition in partitions] if partitions else []

    def merge(self, other: "AvailabilityStateBase"):
        """
        Merge this availability state with another.
        """
        all_partitions = [
            PartitionAvailability(**partition)
            if isinstance(partition, dict)
            else partition
            for partition in self.partitions + other.partitions  # type: ignore
        ]
        min_range = [
            x for x in (self.min_temporal_partition, other.min_temporal_partition) if x
        ]
        max_range = [
            x for x in (self.max_temporal_partition, other.max_temporal_partition) if x
        ]
        top_level_partition = PartitionAvailability(
            value=[None for _ in other.categorical_partitions]
            if other.categorical_partitions
            else [],
            min_temporal_partition=min(min_range) if min_range else None,
            max_temporal_partition=max(max_range) if max_range else None,
            valid_through_ts=max(self.valid_through_ts, other.valid_through_ts),
        )
        all_partitions += [top_level_partition]

        tracker = AvailabilityTracker()
        for partition in all_partitions:
            tracker.insert(partition)
        final_partitions = tracker.get_partition_range()

        self.partitions = [
            partition
            for partition in final_partitions
            if not all(val is None for val in partition.value)
        ]
        merged_top_level = [
            partition
            for partition in final_partitions
            if all(val is None for val in partition.value)
        ]

        if merged_top_level:  # pragma: no cover
            self.min_temporal_partition = (
                top_level_partition.min_temporal_partition
                or merged_top_level[0].min_temporal_partition
            )
            self.max_temporal_partition = (
                top_level_partition.max_temporal_partition
                or merged_top_level[0].max_temporal_partition
            )
            self.valid_through_ts = (
                top_level_partition.valid_through_ts
                or merged_top_level[0].valid_through_ts
                or MIN_VALID_THROUGH_TS
            )

        return self


class MetricDirection(StrEnum):
    """
    The direction of the metric that's considered good, i.e., higher is better
    """

    HIGHER_IS_BETTER = "higher_is_better"
    LOWER_IS_BETTER = "lower_is_better"
    NEUTRAL = "neutral"


class Unit(BaseModel):
    """
    Metric unit
    """

    name: str
    label: Optional[str]
    category: Optional[str]
    abbreviation: Optional[str]
    description: Optional[str]

    def __str__(self):
        return self.name  # pragma: no cover

    def __repr__(self):
        return self.name

    @validator("label", always=True)
    def get_label(  # pylint: disable=no-self-argument
        cls,
        label: str,
        values: Dict[str, Any],
    ) -> str:
        """Generate a default label if one was not provided."""
        if not label and values:
            return labelize(values["name"])
        return label

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class MetricUnit(enum.Enum):
    """
    Available units of measure for metrics
    TODO: Eventually this can be recorded in a database,   # pylint: disable=fixme
    since measurement units can be customized depending on the metric
    (i.e., clicks/hour). For the time being, this enum provides some basic units.
    """

    UNKNOWN = Unit(name="unknown", category="")
    UNITLESS = Unit(name="unitless", category="")

    PERCENTAGE = Unit(
        name="percentage",
        category="",
        abbreviation="%",
        description="A ratio expressed as a number out of 100. Values range from 0 to 100.",
    )

    PROPORTION = Unit(
        name="proportion",
        category="",
        abbreviation="",
        description="A ratio that compares a part to a whole. Values range from 0 to 1.",
    )

    # Monetary
    DOLLAR = Unit(name="dollar", label="Dollar", category="currency", abbreviation="$")

    # Time
    SECOND = Unit(name="second", category="time", abbreviation="s")
    MINUTE = Unit(name="minute", category="time", abbreviation="m")
    HOUR = Unit(name="hour", category="time", abbreviation="h")
    DAY = Unit(name="day", category="time", abbreviation="d")
    WEEK = Unit(name="week", category="time", abbreviation="w")
    MONTH = Unit(name="month", category="time", abbreviation="mo")
    YEAR = Unit(name="year", category="time", abbreviation="y")


class MetricMetadataOptions(BaseModel):
    """
    Metric metadata options list
    """

    directions: List[MetricDirection]
    units: List[Unit]


class MetricMetadataBase(BaseModel):  # type: ignore
    """
    Base class for additional metric metadata
    """

    direction: Optional[MetricDirection] = Field(
        sa_column=SqlaColumn(Enum(MetricDirection)),
        default=MetricDirection.NEUTRAL,
    )
    unit: Optional[MetricUnit] = Field(
        sa_column=SqlaColumn(Enum(MetricUnit)),
        default=MetricUnit.UNKNOWN,
    )


class MetricMetadataOutput(BaseModel):
    """
    Metric metadata output
    """

    direction: Optional[MetricDirection]
    unit: Optional[Unit]

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class MetricMetadataInput(BaseModel):
    """
    Metric metadata output
    """

    direction: Optional[MetricDirection]
    unit: Optional[str]


class ImmutableNodeFields(BaseModel):
    """
    Node fields that cannot be changed
    """

    name: str
    namespace: str = "default"


class MutableNodeFields(BaseModel):
    """
    Node fields that can be changed.
    """

    display_name: Optional[str]
    description: str
    mode: NodeMode
    primary_key: Optional[List[str]]


class MutableNodeQueryField(BaseModel):
    """
    Query field for node.
    """

    query: str


class NodeNameList(BaseModel):
    """
    List of node names
    """

    __root__: List[str]


class NodeIndexItem(BaseModel):
    """
    Node details used for indexing purposes
    """

    name: str
    display_name: str
    description: str
    type: NodeType


class NodeMinimumDetail(BaseModel):
    """
    List of high level node details
    """

    name: str
    display_name: str
    description: str
    version: str
    type: NodeType
    status: NodeStatus
    mode: NodeMode
    updated_at: UTCDatetime
    tags: Optional[List[TagMinimum]]
    edited_by: Optional[List[str]]

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class AttributeTypeName(BaseModel):
    """
    Attribute type name.
    """

    namespace: str
    name: str

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class AttributeOutput(BaseModel):
    """
    Column attribute output.
    """

    attribute_type: AttributeTypeName

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class DimensionAttributeOutput(BaseModel):
    """
    Dimension attribute output should include the name and type
    """

    name: str
    node_name: Optional[str]
    node_display_name: Optional[str]
    is_primary_key: bool
    type: str
    path: List[str]
    filter_only: bool = False


class ColumnOutput(BaseModel):
    """
    A simplified column schema, without ID or dimensions.
    """

    name: str
    display_name: Optional[str]
    type: str
    attributes: Optional[List[AttributeOutput]]
    dimension: Optional[NodeNameOutput]
    partition: Optional[PartitionOutput]
    # order: Optional[int]

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        """
        Should perform validation on assignment
        """

        orm_mode = True
        validate_assignment = True

    _extract_type = validator("type", pre=True, allow_reuse=True)(
        lambda raw: str(raw),  # pylint: disable=unnecessary-lambda
    )


class SourceColumnOutput(BaseModel):
    """
    A column used in creation of a source node
    """

    name: str
    type: ColumnType
    attributes: Optional[List[AttributeOutput]]
    dimension: Optional[str]

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
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


class SourceNodeFields(BaseModel):
    """
    Source node fields that can be changed.
    """

    catalog: str
    schema_: str
    table: str
    columns: List["SourceColumnOutput"]
    missing_table: bool = False


class CubeNodeFields(BaseModel):
    """
    Cube-specific fields that can be changed
    """

    metrics: List[str]
    dimensions: List[str]
    filters: Optional[List[str]]
    orderby: Optional[List[str]]
    limit: Optional[int]
    description: str
    mode: NodeMode


class MetricNodeFields(BaseModel):
    """
    Metric node fields that can be changed
    """

    required_dimensions: Optional[List[str]]
    metric_metadata: Optional[MetricMetadataInput]


#
# Create and Update objects
#


class CreateNode(
    ImmutableNodeFields,
    MutableNodeFields,
    MutableNodeQueryField,
    MetricNodeFields,
):
    """
    Create non-source node object.
    """


class CreateSourceNode(ImmutableNodeFields, MutableNodeFields, SourceNodeFields):
    """
    A create object for source nodes
    """

    query: Optional[str] = None


class CreateCubeNode(ImmutableNodeFields, MutableNodeFields, CubeNodeFields):
    """
    A create object for cube nodes
    """


class UpdateNode(
    MutableNodeFields,
    SourceNodeFields,
    MutableNodeQueryField,
    MetricNodeFields,
    CubeNodeFields,
):
    """
    Update node object where all fields are optional
    """

    __annotations__ = {
        k: Optional[v]
        for k, v in {
            **SourceNodeFields.__annotations__,  # pylint: disable=E1101
            **MutableNodeFields.__annotations__,  # pylint: disable=E1101
            **MutableNodeQueryField.__annotations__,  # pylint: disable=E1101
            **CubeNodeFields.__annotations__,  # pylint: disable=E1101
        }.items()
    }

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        """
        Do not allow fields other than the ones defined here.
        """

        extra = Extra.forbid


#
# Response output objects
#


class GenericNodeOutputModel(BaseModel):
    """
    A generic node output model that flattens the current node revision info
    into the top-level fields on the output model.
    """

    @root_validator(pre=True)
    def flatten_current(  # pylint: disable=no-self-argument
        cls,
        values: GetterDict,
    ) -> Union[GetterDict, Dict[str, Any]]:
        """
        Flatten the current node revision into top-level fields.
        """
        current = values.get("current")
        if current is None:
            return values
        current_dict = dict(current.__dict__.items())
        final_dict = {
            "namespace": values.get("namespace"),
            "created_at": values.get("created_at"),
            "deactivated_at": values.get("deactivated_at"),
            "current_version": values.get("current_version"),
            "catalog": values.get("catalog"),
            "missing_table": values.get("missing_table"),
            "tags": values.get("tags"),
            "created_by": values.get("created_by").__dict__,
        }
        for k, v in current_dict.items():
            final_dict[k] = v
        final_dict["node_revision_id"] = final_dict["id"]
        return final_dict


class TableOutput(BaseModel):
    """
    Output for table information.
    """

    id: Optional[int]
    catalog: Optional[CatalogInfo]
    schema_: Optional[str]
    table: Optional[str]
    database: Optional[DatabaseOutput]


class NodeRevisionOutput(BaseModel):
    """
    Output for a node revision with information about columns and if it is a metric.
    """

    id: int
    node_id: int
    type: NodeType
    name: str
    display_name: str
    version: str
    status: NodeStatus
    mode: NodeMode
    catalog: Optional[CatalogInfo]
    schema_: Optional[str]
    table: Optional[str]
    description: str = ""
    query: Optional[str] = None
    availability: Optional[AvailabilityStateBase] = None
    columns: List[ColumnOutput]
    updated_at: UTCDatetime
    materializations: List[MaterializationConfigOutput]
    parents: List[NodeNameOutput]
    metric_metadata: Optional[MetricMetadataOutput] = None
    dimension_links: Optional[List[LinkDimensionOutput]]

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        orm_mode = True


class NodeOutput(GenericNodeOutputModel):
    """
    Output for a node that shows the current revision.
    """

    namespace: str
    id: int = Field(alias="node_revision_id")
    node_id: int
    type: NodeType
    name: str
    display_name: str
    version: str
    status: NodeStatus
    mode: NodeMode
    catalog: Optional[CatalogInfo]
    schema_: Optional[str]
    table: Optional[str]
    description: str = ""
    query: Optional[str] = None
    availability: Optional[AvailabilityStateBase] = None
    columns: List[ColumnOutput]
    updated_at: UTCDatetime
    materializations: List[MaterializationConfigOutput]
    parents: List[NodeNameOutput]
    metric_metadata: Optional[MetricMetadataOutput] = None
    dimension_links: Optional[List[LinkDimensionOutput]]
    created_at: UTCDatetime
    created_by: UserNameOnly
    tags: List[TagOutput] = []
    current_version: str
    missing_table: Optional[bool] = False

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        orm_mode = True

    @classmethod
    def load_options(cls):
        """
        ORM options to successfully load this object
        """
        from datajunction_server.database.node import (  # pylint: disable=import-outside-toplevel
            Node,
            NodeRevision,
        )

        return [
            selectinload(Node.current).options(*NodeRevision.default_load_options()),
            joinedload(Node.tags),
            selectinload(Node.created_by),
        ]


class DAGNodeRevisionOutput(BaseModel):
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
    catalog: Optional[CatalogInfo]
    schema_: Optional[str]
    table: Optional[str]
    description: str = ""
    columns: List[ColumnOutput]
    updated_at: UTCDatetime
    parents: List[NodeNameOutput]
    dimension_links: List[LinkDimensionOutput]

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        allow_population_by_field_name = True
        orm_mode = True


class DAGNodeOutput(GenericNodeOutputModel):
    """
    Output for a node in another node's DAG
    """

    namespace: str
    id: int = Field(alias="node_revision_id")
    node_id: int
    type: NodeType
    name: str
    display_name: str
    version: str
    status: NodeStatus
    mode: NodeMode
    catalog: Optional[CatalogInfo]
    schema_: Optional[str]
    table: Optional[str]
    description: str = ""
    columns: List[ColumnOutput]
    updated_at: UTCDatetime
    parents: List[NodeNameOutput]
    dimension_links: List[LinkDimensionOutput]
    created_at: UTCDatetime
    tags: List[TagOutput] = []
    current_version: str

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class NodeValidation(BaseModel):
    """
    A validation of a provided node definition
    """

    message: str
    status: NodeStatus
    dependencies: List[NodeRevisionOutput]
    columns: List[ColumnOutput]
    errors: List[DJError]
    missing_parents: List[str]


class LineageColumn(BaseModel):
    """
    Column in lineage graph
    """

    column_name: str
    node_name: Optional[str] = None
    node_type: Optional[str] = None
    display_name: Optional[str] = None
    lineage: Optional[List["LineageColumn"]] = None


LineageColumn.update_forward_refs()


class NamespaceOutput(BaseModel):
    """
    Output for a namespace that includes the number of nodes
    """

    namespace: str
    num_nodes: int


class NodeIndegreeOutput(BaseModel):
    """
    Node indegree output
    """

    name: str
    indegree: int


@dataclass
class NodeCursor(Cursor):
    """Cursor that represents a node in a paginated list."""

    created_at: UTCDatetime
    id: int

    @classmethod
    def decode(cls, serialized: str) -> "NodeCursor":
        return cast(NodeCursor, super().decode(serialized))
