# pylint: disable=too-many-instance-attributes,too-many-lines,too-many-ancestors
"""
Model for nodes.
"""
import enum
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Extra
from pydantic import Field as PydanticField
from pydantic import root_validator, validator
from sqlalchemy import JSON, DateTime, String
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.sql.schema import UniqueConstraint
from sqlalchemy.types import Enum
from sqlmodel import Field, Relationship, SQLModel
from typing_extensions import TypedDict

from datajunction_server.errors import DJError, DJInvalidInputException
from datajunction_server.models.base import (
    BaseSQLModel,
    NodeColumns,
    generate_display_name,
    labelize,
)
from datajunction_server.models.catalog import Catalog
from datajunction_server.models.column import Column, ColumnYAML
from datajunction_server.models.database import Database
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import (
    Materialization,
    MaterializationConfigOutput,
)
from datajunction_server.models.partition import PartitionOutput, PartitionType
from datajunction_server.models.tag import Tag, TagNodeRelationship
from datajunction_server.sql.parsing.types import ColumnType
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import SEPARATOR, Version, amenable_name

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
    for_materialization: bool = False


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


class BoundDimensionsRelationship(BaseSQLModel, table=True):  # type: ignore
    """
    Join table for many-to-many relationships between metric nodes
    and parent nodes for dimensions that are required.
    """

    __tablename__ = "metric_required_dimensions"

    metric_id: Optional[int] = Field(
        default=None,
        foreign_key="noderevision.id",
        primary_key=True,
    )

    bound_dimension_id: Optional[int] = Field(
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


class TemporalPartitionRange(BaseSQLModel):
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

    # An ordered list of categorical partitions like ["country", "group_id"]
    # or ["region_id", "age_group"]
    categorical_partitions: Optional[List[str]] = Field(
        sa_column=SqlaColumn("categorical_partitions", JSON),
        default=[],
    )

    # An ordered list of temporal partitions like ["date", "hour"] or ["date"]
    temporal_partitions: Optional[List[str]] = Field(
        sa_column=SqlaColumn("temporal_partitions", JSON),
        default=[],
    )

    # Node-level temporal ranges
    min_temporal_partition: Optional[List[str]] = Field(
        sa_column=SqlaColumn(JSON),
        default=[],
    )
    max_temporal_partition: Optional[List[str]] = Field(
        sa_column=SqlaColumn(JSON),
        default=[],
    )

    # Partition-level availabilities
    partitions: Optional[List[PartitionAvailability]] = Field(
        sa_column=SqlaColumn("partitions", JSON),
        default=[],
    )

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


class MetricDirection(str, enum.Enum):
    """
    The direction of the metric that's considered good, i.e., higher is better
    """

    HIGHER_IS_BETTER = "higher_is_better"
    LOWER_IS_BETTER = "lower_is_better"
    NEUTRAL = "neutral"


class Unit(BaseSQLModel):
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


class MetricMetadataOptions(BaseSQLModel):
    """
    Metric metadata options list
    """

    directions: List[MetricDirection]
    units: List[Unit]


class MetricMetadataBase(BaseSQLModel):  # type: ignore
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


class MetricMetadata(MetricMetadataBase, table=True):  # type: ignore
    """
    Additional metric metadata
    """

    id: Optional[int] = Field(default=None, primary_key=True)

    @classmethod
    def from_input(cls, input_data: "MetricMetadataInput") -> "MetricMetadata":
        """
        Parses a MetricMetadataInput object to a MetricMetadata object
        """
        return MetricMetadata(
            direction=input_data.direction,
            unit=MetricUnit[input_data.unit.upper()] if input_data.unit else None,
        )


class MetricMetadataOutput(BaseSQLModel):
    """
    Metric metadata output
    """

    direction: Optional[MetricDirection]
    unit: Optional[Unit]


class MetricMetadataInput(BaseSQLModel):
    """
    Metric metadata output
    """

    direction: Optional[MetricDirection]
    unit: Optional[str]


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
    deactivated_at: UTCDatetime = Field(
        nullable=True,
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default=None,
    )


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

    # A list of columns from the metric's parent that
    # are required for grouping when using the metric
    required_dimensions: List["Column"] = Relationship(
        link_model=BoundDimensionsRelationship,
        sa_relationship_kwargs={
            "primaryjoin": "NodeRevision.id==BoundDimensionsRelationship.metric_id",
            "secondaryjoin": "Column.id==BoundDimensionsRelationship.bound_dimension_id",
        },
    )

    metric_metadata_id: Optional[int] = Field(
        default=None,
        foreign_key="metricmetadata.id",
    )
    metric_metadata: Optional[MetricMetadata] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "NodeRevision.metric_metadata_id==MetricMetadata.id",
            "cascade": "all, delete",
            "uselist": False,
        },
    )

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

    parent_links: List[NodeRelationship] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "NodeRevision.id==NodeRelationship.child_id",
            "cascade": "all, delete",
        },
    )

    missing_parents: List[MissingParent] = Relationship(
        link_model=NodeMissingParents,
        sa_relationship_kwargs={
            "primaryjoin": "NodeRevision.id==NodeMissingParents.referencing_node_id",
            "secondaryjoin": "MissingParent.id==NodeMissingParents.missing_parent_id",
            "cascade": "all, delete",
        },
    )

    columns: List["Column"] = Relationship(
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
    materializations: List[Materialization] = Relationship(
        back_populates="node_revision",
        sa_relationship_kwargs={
            "cascade": "all, delete-orphan",
        },
    )

    lineage: List[Dict] = Field(
        default=[],
        sa_column=SqlaColumn(JSON),
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
        Return a metric query with the metric aliases reassigned to
        have the same name as the node, if they aren't already matching.
        """
        from datajunction_server.sql.parsing import (  # pylint: disable=import-outside-toplevel
            ast,
        )
        from datajunction_server.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
            parse,
        )

        tree = parse(query)
        projection_0 = tree.select.projection[0]
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
        from datajunction_server.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
            parse,
        )

        # must have a single expression
        tree = parse(self.query)
        if len(tree.select.projection) != 1:
            raise DJInvalidInputException(
                http_status_code=HTTPStatus.BAD_REQUEST,
                message="Metric queries can only have a single "
                f"expression, found {len(tree.select.projection)}",
            )
        projection_0 = tree.select.projection[0]

        # must have an aggregation
        if (
            not hasattr(projection_0, "is_aggregation")
            or not projection_0.is_aggregation()  # type: ignore
        ):
            raise DJInvalidInputException(
                http_status_code=HTTPStatus.BAD_REQUEST,
                message=f"Metric {self.name} has an invalid query, "
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

        if self.type != NodeType.METRIC and self.required_dimensions:
            raise DJInvalidInputException(
                f"Node {self.name} of type {self.type} cannot have "
                "bound dimensions which are only for metrics.",
            )

        if self.type == NodeType.METRIC:
            self.check_metric()

        if self.type == NodeType.CUBE:
            if not self.cube_elements:
                raise DJInvalidInputException(
                    f"Node {self.name} of type cube node needs cube elements",
                )

    def copy_dimension_links_from_revision(self, old_revision: "NodeRevision"):
        """
        Copy dimension links and attributes from another node revision if the column names match
        """
        old_columns_mapping = {col.name: col for col in old_revision.columns}
        for col in self.columns:  # pylint: disable=not-an-iterable
            if col.name in old_columns_mapping:
                col.dimension_id = old_columns_mapping[col.name].dimension_id
                col.attributes = old_columns_mapping[col.name].attributes or []
        return self

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        extra = Extra.allow

    def has_available_materialization(self, build_criteria: BuildCriteria) -> bool:
        """
        Has a materialization available
        """
        return (
            self.availability is not None  # pragma: no cover
            and self.availability.is_available(  # pylint: disable=no-member
                criteria=build_criteria,
            )
        )

    def cube_elements_with_nodes(self) -> List[Tuple[Column, Optional["NodeRevision"]]]:
        """
        Cube elements along with their nodes
        """
        return [
            (element, element.node_revision())
            for element in self.cube_elements  # pylint: disable=not-an-iterable
        ]

    def cube_metrics(self) -> List[Node]:
        """
        Cube node's metrics
        """
        if self.type != NodeType.CUBE:
            raise DJInvalidInputException(  # pragma: no cover
                message="Cannot retrieve metrics for a non-cube node!",
            )

        return [
            node_revision.node  # type: ignore
            for element, node_revision in self.cube_elements_with_nodes()
            if node_revision and node_revision.type == NodeType.METRIC
        ]

    def cube_dimensions(self) -> List[str]:
        """
        Cube node's dimension attributes
        """
        if self.type != NodeType.CUBE:
            raise DJInvalidInputException(  # pragma: no cover
                "Cannot retrieve dimensions for a non-cube node!",
            )
        return [
            node_revision.name + SEPARATOR + element.name
            for element, node_revision in self.cube_elements_with_nodes()
            if node_revision and node_revision.type != NodeType.METRIC
        ]

    def temporal_partition_columns(self) -> List[Column]:
        """
        The node's temporal partition columns, if any
        """
        return [
            col
            for col in self.columns  # pylint: disable=not-an-iterable
            if col.partition and col.partition.type_ == PartitionType.TEMPORAL
        ]

    def categorical_partition_columns(self) -> List[Column]:
        """
        The node's categorical partition columns, if any
        """
        return [
            col
            for col in self.columns  # pylint: disable=not-an-iterable
            if col.partition and col.partition.type_ == PartitionType.CATEGORICAL
        ]

    def __deepcopy__(self, memo):
        """
        Note: We should not use copy or deepcopy to copy any SQLAlchemy objects.
        This is implemented here to make copying of AST structures easier, but does
        not actually copy anything
        """
        return None


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


class NodeIndexItem(SQLModel):
    """
    Node details used for indexing purposes
    """

    name: str
    display_name: str
    description: str
    type: NodeType


class NodeMinimumDetail(SQLModel):
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


class DimensionAttributeOutput(SQLModel):
    """
    Dimension attribute output should include the name and type
    """

    name: str
    type: ColumnType
    path: List[str]

    @root_validator
    def type_string(cls, values):  # pylint: disable=no-self-argument
        """
        Extracts the type as a string
        """
        values["type"] = str(values.get("type"))
        return values


class ColumnOutput(BaseSQLModel):
    """
    A simplified column schema, without ID or dimensions.
    """

    name: str
    display_name: Optional[str]
    type: ColumnType
    attributes: Optional[List[AttributeOutput]]
    dimension: Optional[NodeNameOutput]
    partition: Optional[PartitionOutput]

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
    columns: List["SourceColumnOutput"]


class CubeNodeFields(BaseSQLModel):
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


class MetricNodeFields(BaseSQLModel):
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
    materializations: List[MaterializationConfigOutput]
    parents: List[NodeNameOutput]
    metric_metadata: Optional[MetricMetadataOutput] = None

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
    current_version: str


class NodeValidation(SQLModel):
    """
    A validation of a provided node definition
    """

    message: str
    status: NodeStatus
    dependencies: List[NodeRevisionOutput]
    columns: List[Column]
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


class NamespaceOutput(OutputModel):
    """
    Output for a namespace that includes the number of nodes
    """

    namespace: str
    num_nodes: int
