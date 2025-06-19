"""Node-related scalars."""

import datetime
from typing import List, Optional

import strawberry
from strawberry.scalars import JSON
from strawberry.types import Info

from datajunction_server.api.graphql.scalars import BigInt
from datajunction_server.api.graphql.scalars.availabilitystate import AvailabilityState
from datajunction_server.api.graphql.scalars.catalog_engine import Catalog
from datajunction_server.api.graphql.scalars.column import Column, NodeName, Partition
from datajunction_server.api.graphql.scalars.materialization import (
    MaterializationConfig,
)
from datajunction_server.api.graphql.scalars.metricmetadata import (
    DecomposedMetric,
    MetricMetadata,
)
from datajunction_server.api.graphql.scalars.user import User
from datajunction_server.api.graphql.utils import extract_fields
from datajunction_server.database.dimensionlink import (
    JoinCardinality as JoinCardinality_,
)
from datajunction_server.database.dimensionlink import JoinType as JoinType_
from datajunction_server.database.node import Node as DBNode
from datajunction_server.database.node import NodeRevision as DBNodeRevision
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node import NodeMode as NodeMode_
from datajunction_server.models.node import NodeStatus as NodeStatus_
from datajunction_server.models.node import NodeType as NodeType_
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse

NodeType = strawberry.enum(NodeType_)
NodeStatus = strawberry.enum(NodeStatus_)
NodeMode = strawberry.enum(NodeMode_)
JoinType = strawberry.enum(JoinType_)
JoinCardinality = strawberry.enum(JoinCardinality_)


@strawberry.type
class CubeElement:
    """
    An element in a cube, either a metric or dimension
    """

    name: str
    display_name: str
    type: str
    partition: Optional[Partition]


@strawberry.type
class DimensionLink:
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
class DimensionAttribute:
    """
    A dimensional column attribute
    """

    name: str
    attribute: str | None
    role: str | None = None
    properties: list[str]
    type: str

    _dimension_node: Optional["Node"] = None

    @strawberry.field(description="The dimension node this attribute belongs to")
    async def dimension_node(self, info: Info) -> "Node":
        """
        Lazy load the dimension node when queried.
        """
        if self._dimension_node:
            return self._dimension_node

        from datajunction_server.api.graphql.resolvers.nodes import get_node_by_name

        dimension_node_name = self.name.rsplit(".", 1)[0]
        fields = extract_fields(info)
        return await get_node_by_name(  # type: ignore
            session=info.context["session"],
            fields=fields,
            name=dimension_node_name,
        )


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
    custom_metadata: Optional[JSON] = strawberry.field(default_factory=dict)

    @strawberry.field
    def catalog(self, root: "DBNodeRevision") -> Optional[Catalog]:
        """
        Catalog for the node
        """
        return Catalog.from_pydantic(root.catalog)  # type: ignore

    query: Optional[str] = None

    @strawberry.field
    def columns(
        self,
        root: "DBNodeRevision",
        attributes: list[str] | None = None,
    ) -> list[Column]:
        """
        The columns of the node
        """
        return [
            Column(  # type: ignore
                name=col.name,
                display_name=col.display_name,
                type=col.type,
                attributes=col.attributes,
                dimension=(
                    NodeName(name=col.dimension.name)  # type: ignore
                    if col.dimension
                    else None
                ),
                partition=Partition(
                    type_=col.partition.type_,  # type: ignore
                    format=col.partition.format,
                    granularity=col.partition.granularity,
                    expression=col.partition.temporal_expression(),
                )
                if col.partition
                else None,
            )
            for col in root.columns
            if (
                any(col.has_attribute(attr) for attr in attributes)
                if attributes
                else True
            )
        ]

    # Dimensions and data graph-related outputs
    dimension_links: List[DimensionLink]
    parents: List[NodeName]

    # Materialization-related outputs
    availability: Optional[AvailabilityState] = None
    materializations: Optional[List[MaterializationConfig]] = None

    # Only source nodes will have these fields
    schema_: Optional[str]
    table: Optional[str]

    # Only metrics will have these fields
    required_dimensions: List[Column] | None = None

    @strawberry.field
    def primary_key(self, root: "DBNodeRevision") -> list[str]:
        """
        The primary key of the node
        """
        return [col.name for col in root.primary_key()]

    @strawberry.field
    def metric_metadata(self, root: "DBNodeRevision") -> MetricMetadata | None:
        """
        Metric metadata
        """
        if root.type != NodeType.METRIC:
            return None

        query_ast = parse(root.query)
        functions = [func.function() for func in query_ast.find_all(ast.Function)]
        return MetricMetadata(  # type: ignore
            direction=root.metric_metadata.direction if root.metric_metadata else None,
            unit=root.metric_metadata.unit if root.metric_metadata else None,
            significant_digits=root.metric_metadata.significant_digits
            if root.metric_metadata
            else None,
            min_decimal_exponent=root.metric_metadata.min_decimal_exponent
            if root.metric_metadata
            else None,
            max_decimal_exponent=root.metric_metadata.max_decimal_exponent
            if root.metric_metadata
            else None,
            expression=str(query_ast.select.projection[0]),
            incompatible_druid_functions={
                func.__name__.upper()
                for func in functions
                if Dialect.DRUID not in func.dialects
            },
        )

    @strawberry.field
    def extracted_measures(self, root: "DBNodeRevision") -> DecomposedMetric | None:
        """
        A list of metric components for a metric node
        """
        if root.type != NodeType.METRIC:
            return None
        extractor = MetricComponentExtractor.from_query_string(root.query)
        components, derived_ast = extractor.extract()
        return DecomposedMetric(  # type: ignore
            components=components,
            derived_query=str(derived_ast),
            derived_expression=str(derived_ast.select.projection[0]),
        )

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
                    _dimension_node=node_revision,
                    type=element.type,
                    properties=element.attribute_names(),
                )
                for element, node_revision in root.cube_elements_with_nodes()
                if node_revision and node_revision.type != NodeType.METRIC
            ],
            key=lambda x: ordering[x.name],
        )


@strawberry.type
class TagBase:
    """
    A DJ node tag without any referential fields
    """

    name: str
    tag_type: str
    description: str | None
    display_name: str | None
    tag_metadata: JSON | None = strawberry.field(default_factory=dict)


@strawberry.type
class Node:
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

    tags: List[TagBase]
    created_by: User

    @strawberry.field
    def edited_by(self, root: "DBNode") -> List[str]:
        """
        The users who edited this node
        """
        return root.edited_by
