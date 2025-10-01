"""
Model for nodes.
"""

import enum
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    RootModel,
    ConfigDict,
    create_model,
)
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.types import Enum
from typing_extensions import TypedDict

from datajunction_server.api.graphql.scalars import Cursor
from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJError
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
    display_name: str | None = Field(max_length=100, default=None)


class NodeRevisionBase(BaseModel):
    """
    A base node revision.
    """

    name: str
    display_name: str | None = None
    type: NodeType
    description: str | None = None
    query: str | None = None
    mode: NodeMode = NodeMode.PUBLISHED


class TemporalPartitionRange(BaseModel):
    """
    Any temporal partition range with a min and max partition.
    """

    min_temporal_partition: list[str | int] | None = None
    max_temporal_partition: list[str | int] | None = None

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
    valid_through_ts: int | None = None


class AvailabilityNode(TemporalPartitionRange):
    """A node in the availability trie tracker"""

    children: Dict = {}
    valid_through_ts: int | None = Field(default=MIN_VALID_THROUGH_TS)

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
    schema_: str | None = Field(default=None)
    table: str
    valid_through_ts: int
    url: str | None = Field(default=None)
    links: Dict[str, Any] | None = Field(default_factory=dict)

    # An ordered list of categorical partitions like ["country", "group_id"]
    # or ["region_id", "age_group"]
    categorical_partitions: list[str] | None = Field(default_factory=list)

    # An ordered list of temporal partitions like ["date", "hour"] or ["date"]
    temporal_partitions: list[str] | None = Field(default_factory=list)

    # Node-level temporal ranges
    min_temporal_partition: list[str | int] | None = Field(default_factory=list)
    max_temporal_partition: list[str | int] | None = Field(default_factory=list)

    # Partition-level availabilities
    partitions: list[PartitionAvailability] | None = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)

    @field_validator("partitions")
    def validate_partitions(cls, partitions):
        """
        Validator for partitions
        """
        return (
            [partition.model_dump() for partition in partitions] if partitions else []
        )

    @field_validator("min_temporal_partition", mode="before")
    def convert_min_temporal_partition(cls, min_temporal_partition):
        """
        Validator for min_temporal_partition
        """
        return (
            [str(part) for part in min_temporal_partition]
            if min_temporal_partition
            else []
        )

    @field_validator("max_temporal_partition", mode="before")
    def convert_max_temporal_partition(cls, max_temporal_partition):
        """
        Validator for max_temporal_partition
        """
        return (
            [str(part) for part in max_temporal_partition]
            if max_temporal_partition
            else []
        )

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


class AvailabilityStateInfo(AvailabilityStateBase):
    """
    Availability state information for a node
    """

    id: int
    updated_at: str
    node_revision_id: int
    node_version: str


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
    label: Optional[str] = None
    category: Optional[str] = None
    abbreviation: Optional[str] = None
    description: Optional[str] = None

    def __str__(self):
        return self.name  # pragma: no cover

    def __repr__(self):
        return self.name


class MetricUnit(enum.Enum):
    """
    Available units of measure for metrics
    TODO: Eventually this can be recorded in a database,
    since measurement units can be customized depending on the metric
    (i.e., clicks/hour). For the time being, this enum provides some basic units.
    """

    UNKNOWN = Unit(
        name="unknown",
        label="Unknown",
        category="",
        abbreviation=None,
        description=None,
    )
    UNITLESS = Unit(
        name="unitless",
        label="Unitless",
        category="",
        abbreviation=None,
        description=None,
    )

    PERCENTAGE = Unit(
        name="percentage",
        label="Percentage",
        category="",
        abbreviation="%",
        description="A ratio expressed as a number out of 100. Values range from 0 to 100.",
    )

    PROPORTION = Unit(
        name="proportion",
        label="Proportion",
        category="",
        abbreviation="",
        description="A ratio that compares a part to a whole. Values range from 0 to 1.",
    )

    # Monetary
    DOLLAR = Unit(
        name="dollar",
        label="Dollar",
        category="currency",
        abbreviation="$",
        description=None,
    )

    # Time
    SECOND = Unit(
        name="second",
        label="Second",
        category="time",
        abbreviation="s",
        description=None,
    )
    MINUTE = Unit(
        name="minute",
        label="Minute",
        category="time",
        abbreviation="m",
        description=None,
    )
    HOUR = Unit(
        name="hour",
        label="Hour",
        category="time",
        abbreviation="h",
        description=None,
    )
    DAY = Unit(
        name="day",
        label="Day",
        category="time",
        abbreviation="d",
        description=None,
    )
    WEEK = Unit(
        name="week",
        label="Week",
        category="time",
        abbreviation="w",
        description=None,
    )
    MONTH = Unit(
        name="month",
        label="Month",
        category="time",
        abbreviation="mo",
        description=None,
    )
    YEAR = Unit(
        name="year",
        label="Year",
        category="time",
        abbreviation="y",
        description=None,
    )


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

    direction: MetricDirection | None = None
    unit: Unit | None = None
    significant_digits: int | None = None
    min_decimal_exponent: int | None = None
    max_decimal_exponent: int | None = None

    model_config = ConfigDict(from_attributes=True)

    @field_validator("unit", mode="before")
    def validate_unit(cls, value):
        if isinstance(value, MetricUnit):
            return Unit(
                name=value.value.name,
                label=value.value.label,
                category=value.value.category,
                abbreviation=value.value.abbreviation,
                description=value.value.description,
            )
        return value  # pragma: no cover


class MetricMetadataInput(BaseModel):
    """
    Metric metadata output
    """

    direction: MetricDirection | None = None
    unit: str | None = None
    significant_digits: int | None = None
    min_decimal_exponent: int | None = None
    max_decimal_exponent: int | None = None


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

    display_name: str | None = None
    description: str | None = None
    mode: NodeMode = NodeMode.PUBLISHED
    primary_key: list[str] | None = None
    custom_metadata: dict | None = None
    owners: list[str] | None = None


class MutableNodeQueryField(BaseModel):
    """
    Query field for node.
    """

    query: str


class NodeNameList(RootModel):
    """
    List of node names
    """

    root: List[str]


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
    tags: list[TagMinimum] = Field(default_factory=list)
    edited_by: list[str] | None = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class AttributeTypeName(BaseModel):
    """
    Attribute type name.
    """

    namespace: str
    name: str

    model_config = ConfigDict(from_attributes=True)


class AttributeOutput(BaseModel):
    """
    Column attribute output.
    """

    attribute_type: AttributeTypeName

    model_config = ConfigDict(from_attributes=True)


class DimensionAttributeOutput(BaseModel):
    """
    Dimension attribute output should include the name and type
    """

    name: str
    node_name: str | None
    node_display_name: str | None
    properties: list[str] | None
    type: str | None
    path: list[str]
    filter_only: bool = False


class ColumnOutput(BaseModel):
    """
    A simplified column schema, without ID or dimensions.
    """

    name: str
    display_name: Optional[str] = None
    type: str
    description: Optional[str] = None
    attributes: Optional[List[AttributeOutput]] = None
    dimension: Optional[NodeNameOutput] = None
    partition: Optional[PartitionOutput] = None

    model_config = ConfigDict(from_attributes=True, validate_assignment=True)

    @field_validator("type", mode="before")
    def extract_type(cls, raw):
        return str(raw)


class SourceColumnOutput(BaseModel):
    """
    A column used in creation of a source node
    """

    name: str
    type: ColumnType
    attributes: Optional[List[AttributeOutput]] = None
    dimension: Optional[str] = None

    model_config = ConfigDict(validate_assignment=True, from_attributes=True)

    @field_validator("type", mode="before")
    def validate_column_type(cls, value):
        """
        Convert string type to ColumnType object
        """
        if isinstance(value, str):
            return ColumnType.validate(value)
        return value


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

    metrics: List[str] | None = None
    dimensions: List[str] | None = None
    filters: Optional[List[str]] = None
    orderby: Optional[List[str]] = None
    limit: Optional[int] = None
    description: Optional[str] = None
    mode: NodeMode


class MetricNodeFields(BaseModel):
    """
    Metric node fields that can be changed
    """

    required_dimensions: Optional[List[str]] = None
    metric_metadata: Optional[MetricMetadataInput] = None


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


def build_update_node_fields():
    """
    Build the fields for the UpdateNode model by collecting fields from parent classes
    and making them optional.
    """
    update_node_fields = {}
    for parent_class in [
        MutableNodeFields,
        SourceNodeFields,
        MutableNodeQueryField,
        MetricNodeFields,
        CubeNodeFields,
    ]:
        for field_name, field_info in parent_class.model_fields.items():
            # Make the field optional by adding None to the union type
            original_type = field_info.annotation
            if (
                hasattr(original_type, "__origin__")
                and original_type.__origin__ is Union
            ):
                # Already a union type, add None if not present
                if type(None) not in original_type.__args__:
                    optional_type = original_type | None  # pragma: no cover
                else:
                    optional_type = original_type
            else:
                # Not a union, make it optional
                optional_type = original_type | None
            update_node_fields[field_name] = (optional_type, None)
    return update_node_fields


# Create the UpdateNode class with all optional fields
UpdateNode = create_model(
    "UpdateNode",
    **build_update_node_fields(),
    __config__=ConfigDict(extra="forbid"),
    __doc__="Update node object where all fields are optional",
)


#
# Response output objects
#


class GenericNodeOutputModel(BaseModel):
    """
    A generic node output model that flattens the current node revision info
    into the top-level fields on the output model.
    """

    @model_validator(mode="before")
    def flatten_current(
        cls,
        values: Any,
    ) -> Dict[str, Any]:
        """
        Flatten the current node revision into top-level fields.
        """
        current = values.current
        if current is None:
            return values  # pragma: no cover
        final_dict = {
            "namespace": values.namespace,
            "created_at": values.created_at,
            "deactivated_at": values.deactivated_at,
            "current_version": values.current_version,
            "catalog": values.current.catalog,
            "missing_table": values.missing_table,
            "tags": values.tags,
            "created_by": values.created_by,
            "owners": [owner for owner in values.owners or []],
        }
        current_dict = dict(current.__dict__.items())
        for k, v in current_dict.items():
            final_dict[k] = v

        final_dict["dimension_links"] = [
            link
            for link in final_dict["dimension_links"]  # type: ignore
            if link.dimension.deactivated_at is None  # type: ignore
        ]
        final_dict["node_revision_id"] = final_dict["id"]
        return final_dict


class TableOutput(BaseModel):
    """
    Output for table information.
    """

    id: int | None = None
    catalog: CatalogInfo | None = None
    schema_: str | None = None
    table: str | None = None
    database: DatabaseOutput | None = None


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
    catalog: CatalogInfo | None = None
    schema_: str | None = None
    table: str | None = None
    description: str = ""
    query: str | None = None
    availability: AvailabilityStateBase | None = None
    columns: List[ColumnOutput]
    updated_at: UTCDatetime
    materializations: List[MaterializationConfigOutput]
    parents: List[NodeNameOutput]
    metric_metadata: MetricMetadataOutput | None = None
    dimension_links: list[LinkDimensionOutput] | None = None
    custom_metadata: dict | None = None

    model_config = ConfigDict(from_attributes=True)


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
    catalog: CatalogInfo | None = None
    schema_: str | None = None
    table: str | None = None
    description: str = ""
    query: str | None = None
    availability: AvailabilityStateBase | None = None
    columns: list[ColumnOutput]
    updated_at: UTCDatetime
    materializations: list[MaterializationConfigOutput]
    parents: list[NodeNameOutput]
    metric_metadata: MetricMetadataOutput | None = None
    dimension_links: list[LinkDimensionOutput] = Field(default_factory=list)
    created_at: UTCDatetime
    created_by: UserNameOnly
    tags: list[TagOutput] = Field(default_factory=list)
    current_version: str
    missing_table: bool | None = False
    custom_metadata: dict | None = None
    owners: list[UserNameOnly] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)

    @classmethod
    def load_options(cls):
        """
        ORM options to successfully load this object
        """
        from datajunction_server.database.node import (
            Node,
            NodeRevision,
        )

        return [
            selectinload(Node.current).options(*NodeRevision.default_load_options()),
            joinedload(Node.tags),
            selectinload(Node.created_by),
            selectinload(Node.owners),
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
    catalog: CatalogInfo | None = None
    schema_: str | None = None
    table: str | None = None
    description: str = ""
    columns: list[ColumnOutput] | None = None
    updated_at: UTCDatetime
    parents: list[NodeNameOutput] | None = None
    dimension_links: list[LinkDimensionOutput] | None = None

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
    )


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
    catalog: CatalogInfo | None = None
    schema_: str | None = None
    table: str | None = None
    description: str = ""
    columns: list[ColumnOutput]
    updated_at: UTCDatetime
    parents: List[NodeNameOutput]
    dimension_links: List[LinkDimensionOutput]
    created_at: UTCDatetime
    tags: List[TagOutput] = []
    current_version: str

    model_config = ConfigDict(from_attributes=True)


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

    model_config = ConfigDict(defer_initialization=True)


# Forward reference resolution for the self-referencing model
LineageColumn.model_rebuild()


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
