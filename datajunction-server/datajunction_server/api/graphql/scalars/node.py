"""Node-related scalars."""

import datetime
from typing import List, Optional

import strawberry
from strawberry.scalars import JSON

from datajunction_server.api.graphql.scalars import BigInt
from datajunction_server.database.dimensionlink import (
    JoinCardinality as JoinCardinality_,
)
from datajunction_server.database.dimensionlink import JoinType as JoinType_
from datajunction_server.database.node import NodeRevision as DBNodeRevision
from datajunction_server.models.node import Dialect as Dialect_
from datajunction_server.models.node import MetricDirection as MetricDirection_
from datajunction_server.models.node import NodeMode as NodeMode_
from datajunction_server.models.node import NodeStatus as NodeStatus_
from datajunction_server.models.node import NodeType as NodeType_
from datajunction_server.models.partition import PartitionType as PartitionType_

# Import all existing enums as strawberry enums
NodeType = strawberry.enum(NodeType_)
NodeStatus = strawberry.enum(NodeStatus_)
NodeMode = strawberry.enum(NodeMode_)
JoinType = strawberry.enum(JoinType_)
JoinCardinality = strawberry.enum(JoinCardinality_)
MetricDirection = strawberry.enum(MetricDirection_)
PartitionType = strawberry.enum(PartitionType_)
Dialect = strawberry.enum(Dialect_)


@strawberry.type
class PartitionAvailability:  # pylint: disable=too-few-public-methods
    """
    Partition-level availability
    """

    min_temporal_partition: Optional[List[str]]
    max_temporal_partition: Optional[List[str]]

    # This list maps to the ordered list of categorical partitions at the node level.
    # For example, if the node's `categorical_partitions` are configured as ["country", "group_id"],
    # a valid entry for `value` may be ["DE", null].
    value: List[Optional[str]]

    # Valid through timestamp
    valid_through_ts: Optional[int]


@strawberry.type
class AvailabilityState:  # pylint: disable=too-few-public-methods
    """
    A materialized table that is available for the node
    """

    catalog: str
    schema_: Optional[str]
    table: str
    valid_through_ts: int
    url: Optional[str]

    # An ordered list of categorical partitions like ["country", "group_id"]
    # or ["region_id", "age_group"]
    categorical_partitions: Optional[List[str]]

    # An ordered list of temporal partitions like ["date", "hour"] or ["date"]
    temporal_partitions: Optional[List[str]]

    # Node-level temporal ranges
    min_temporal_partition: Optional[List[str]]
    max_temporal_partition: Optional[List[str]]

    # Partition-level availabilities
    partitions: Optional[List[PartitionAvailability]]


@strawberry.type
class AttributeTypeName:  # pylint: disable=too-few-public-methods
    """
    Attribute type name.
    """

    namespace: str
    name: str


@strawberry.type
class Attribute:  # pylint: disable=too-few-public-methods
    """
    Column attribute
    """

    attribute_type: AttributeTypeName


@strawberry.type
class NodeName:  # pylint: disable=too-few-public-methods
    """
    Node name
    """

    name: str


@strawberry.type
class Partition:  # pylint: disable=too-few-public-methods
    """
    A partition configuration for a column
    """

    type_: PartitionType  # type: ignore
    format: Optional[str]
    granularity: Optional[str]
    expression: Optional[str]


@strawberry.type
class Column:  # pylint: disable=too-few-public-methods
    """
    A column on a node
    """

    name: str
    display_name: Optional[str]
    type: str
    attributes: Optional[List[Attribute]]
    dimension: Optional[NodeName]
    partition: Optional[Partition]
    # order: Optional[int]


@strawberry.type
class Engine:  # pylint: disable=too-few-public-methods
    """
    Database engine
    """

    name: str
    version: str
    uri: Optional[str]
    dialect: Optional[Dialect]  # type: ignore


@strawberry.type
class Catalog:  # pylint: disable=too-few-public-methods
    """
    Catalog
    """

    name: str
    engines: Optional[List[Engine]]


@strawberry.type
class DimensionLink:  # pylint: disable=too-few-public-methods
    """
    A dimension link between a dimension and a node
    """

    dimension: NodeName
    join_type: JoinType  # type: ignore
    join_sql: str
    join_cardinality: Optional[JoinCardinality]  # type: ignore
    role: Optional[str]
    foreign_keys: JSON


@strawberry.type
class PartitionBackfill:  # pylint: disable=too-few-public-methods
    """
    Used for setting backfilled values
    """

    column_name: str

    # Backfilled values and range. Most temporal partitions will just use `range`, but some may
    # optionally use `values` to specify specific values
    # Ex: values: [20230901]
    #     range: [20230901, 20231001]
    values: Optional[List[str]]
    range: Optional[List[str]]


@strawberry.type
class Backfill:  # pylint: disable=too-few-public-methods
    """
    Materialization job backfill
    """

    spec: Optional[List[PartitionBackfill]]
    urls: Optional[List[str]]


@strawberry.type
class MaterializationConfig:  # pylint: disable=too-few-public-methods
    """
    Materialization config
    """

    name: Optional[str]
    config: JSON
    schedule: str
    job: Optional[str]
    backfills: List[Backfill]
    strategy: Optional[str]


@strawberry.type
class Unit:  # pylint: disable=too-few-public-methods
    """
    Metric unit
    """

    name: str
    label: Optional[str]
    category: Optional[str]
    abbreviation: Optional[str]
    description: Optional[str]


@strawberry.type
class MetricMetadata:  # pylint: disable=too-few-public-methods
    """
    Metric metadata output
    """

    direction: Optional[MetricDirection]  # type: ignore
    unit: Optional[Unit]


@strawberry.type
class CubeElement:  # pylint: disable=too-few-public-methods
    """
    An element in a cube, either a metric or dimension
    """

    name: str
    display_name: str
    type: str
    partition: Optional[Partition]


@strawberry.type
class DimensionAttribute:  # pylint: disable=too-few-public-methods
    """
    A dimensional column attribute
    """

    name: str
    attribute: str
    role: str
    dimension_node: "NodeRevision"


@strawberry.type
class NodeRevision:
    """
    The base fields of a node revision, which does not include joined in entities.
    """

    id: BigInt
    type: NodeType  # type: ignore
    name: str
    display_name: Optional[str]
    version: str
    status: NodeStatus  # type: ignore
    mode: Optional[NodeMode]  # type: ignore
    description: str = ""
    updated_at: datetime.datetime
    catalog: Optional[Catalog]

    query: Optional[str] = None
    columns: List[Column]

    # Dimensions and data graph-related outputs
    dimension_links: List[DimensionLink]
    parents: List[NodeName]

    # Materialization-related outputs
    availability: Optional[AvailabilityState] = None
    materializations: Optional[List[MaterializationConfig]]

    # Only source nodes will have this
    schema_: Optional[str]
    table: Optional[str]

    # Only metrics will have this field
    metric_metadata: Optional[MetricMetadata] = None

    # Only cubes will have these fields
    @strawberry.field
    def cube_metrics(self, root: "DBNodeRevision") -> List["NodeRevision"]:
        """
        Metrics for a cube node
        """
        if root.type != NodeType.CUBE:
            return []
        ordering = root.ordering()
        return sorted(
            [
                node_revision
                for _, node_revision in root.cube_elements_with_nodes()
                if node_revision and node_revision.type == NodeType.METRIC
            ],
            key=lambda x: ordering[x.name],
        )

    @strawberry.field
    def cube_dimensions(self, root: "DBNodeRevision") -> List[DimensionAttribute]:
        """
        Dimensions for a cube node
        """
        if root.type != NodeType.CUBE:
            return []
        dimension_to_roles = {col.name: col.dimension_column for col in root.columns}
        ordering = root.ordering()
        return sorted(
            [
                DimensionAttribute(  # type: ignore
                    name=(
                        node_revision.name
                        + "."
                        + element.name
                        + dimension_to_roles.get(element.name, "")
                    ),
                    attribute=element.name,
                    role=dimension_to_roles.get(element.name, ""),
                    dimension_node=node_revision,
                )
                for element, node_revision in root.cube_elements_with_nodes()
                if node_revision and node_revision.type != NodeType.METRIC
            ],
            key=lambda x: ordering[x.name],
        )


@strawberry.type
class Tag:  # pylint: disable=too-few-public-methods
    """
    Tag metadata
    """

    name: str
    description: str = ""
    tag_type: str
    tag_metadata: JSON


@strawberry.type
class Node:  # pylint: disable=too-few-public-methods
    """
    A DJ node
    """

    id: BigInt
    name: str
    type: NodeType  # type: ignore
    current_version: str
    created_at: datetime.datetime
    deactivated_at: Optional[datetime.datetime]

    current: NodeRevision
    revisions: List[NodeRevision]

    tags: List[Tag]
