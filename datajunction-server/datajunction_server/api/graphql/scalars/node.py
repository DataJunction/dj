"""Node-related scalars."""
import datetime
from typing import List, Optional

import strawberry
from strawberry.scalars import JSON

from datajunction_server.api.graphql.scalars import BigInt
from datajunction_server.api.graphql.scalars.availabilitystate import AvailabilityState
from datajunction_server.api.graphql.scalars.catalog_engine import Catalog
from datajunction_server.api.graphql.scalars.column import Column, NodeName, Partition
from datajunction_server.api.graphql.scalars.materialization import (
    MaterializationConfig,
)
from datajunction_server.api.graphql.scalars.metricmetadata import MetricMetadata
from datajunction_server.api.graphql.scalars.user import User
from datajunction_server.database.dimensionlink import (
    JoinCardinality as JoinCardinality_,
)
from datajunction_server.database.dimensionlink import JoinType as JoinType_
from datajunction_server.database.node import Node as DBNode
from datajunction_server.database.node import NodeRevision as DBNodeRevision
from datajunction_server.models.node import NodeMode as NodeMode_
from datajunction_server.models.node import NodeStatus as NodeStatus_
from datajunction_server.models.node import NodeType as NodeType_

NodeType = strawberry.enum(NodeType_)
NodeStatus = strawberry.enum(NodeStatus_)
NodeMode = strawberry.enum(NodeMode_)
JoinType = strawberry.enum(JoinType_)
JoinCardinality = strawberry.enum(JoinCardinality_)


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
class DimensionAttribute:  # pylint: disable=too-few-public-methods
    """
    A dimensional column attribute
    """

    name: str
    attribute: str
    role: str
    dimension_node: "NodeRevision"


@strawberry.type
class Tag:  # pylint: disable=too-few-public-methods
    """
    Tag metadata
    """

    name: str
    description: Optional[str]
    tag_type: str
    tag_metadata: JSON


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
    materializations: Optional[List[MaterializationConfig]] = None

    # Only source nodes will have this
    schema_: Optional[str]
    table: Optional[str]

    # Only metrics will have this field
    metric_metadata: Optional[MetricMetadata] = None
    required_dimensions: Optional[List[Column]] = None

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
    created_by: User

    @strawberry.field
    def edited_by(self, root: "DBNode") -> List[str]:
        """
        The users who edited this node
        """
        return root.edited_by
