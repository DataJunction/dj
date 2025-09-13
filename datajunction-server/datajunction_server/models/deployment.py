from enum import Enum
from pydantic import BaseModel, Field, validator, PrivateAttr

from typing import Any, Literal, Union

from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.dimensionlink import JoinType, LinkType
from datajunction_server.models.node import (
    MetricDirection,
    MetricUnit,
    NodeMode,
    NodeType,
)
from datajunction_server.utils import SEPARATOR


class DeploymentStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    FAILED = "failed"
    SUCCESS = "success"


class TagSpec(BaseModel):
    """
    Specification for a tag
    """

    name: str
    description: str = ""
    tag_type: str = ""
    tag_metadata: dict | None = None


class ColumnSpec(BaseModel):
    """
    Represents a column
    """

    name: str
    type: str
    display_name: str | None = None
    description: str | None = None
    attributes: list[str] = Field(default_factory=list)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ColumnSpec):
            return False

        return (
            self.name == other.name
            and self.type == other.type
            and (self.display_name == other.display_name or self.display_name is None)
            and (self.description == other.description or self.description is None)
            and set(self.attributes) == set(other.attributes)
        )


class DimensionLinkSpec(BaseModel):
    """
    Specification for a dimension link
    """

    type: LinkType
    role: str | None = None
    namespace: str | None = Field(default=None, exclude=True)

    def __eq__(self, other: Any) -> bool:
        return self.type == other.type and self.role == other.role


class DimensionJoinLinkSpec(DimensionLinkSpec):
    """
    Specification for a dimension join link

    If a custom `join_on` clause is not specified, DJ will automatically set
    this clause to be on the selected column and the dimension node's primary key
    """

    dimension_node: str
    type: LinkType = LinkType.JOIN

    node_column: str | None = None
    join_type: JoinType = JoinType.LEFT
    join_on: str | None = None

    @property
    def rendered_dimension_node(self) -> str:
        return (
            render_prefixes(self.dimension_node, self.namespace)
            if self.namespace
            else self.dimension_node
        )

    @property
    def rendered_join_on(self) -> str | None:
        return (
            render_prefixes(self.join_on, self.namespace or "")
            if self.join_on
            else None
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.type,
                self.role,
                self.rendered_dimension_node,
                self.join_type,
                self.rendered_join_on,
                self.node_column,
            ),
        )

    def __eq__(self, other: Any) -> bool:
        return (
            super().__eq__(other)
            and self.rendered_dimension_node == other.rendered_dimension_node
            and self.join_type == other.join_type
            and self.rendered_join_on == other.rendered_join_on
            and (
                self.node_column == other.node_column
                or (self.node_column is None and other.node_column is None)
            )
        )


class DimensionReferenceLinkSpec(DimensionLinkSpec):
    """
    Specification for a dimension reference link

    The `dimension` input should be a fully qualified dimension attribute name,
    e.g., "<dimension_node>.<column>"
    """

    node_column: str
    dimension: str
    type: LinkType = LinkType.REFERENCE

    @property
    def rendered_dimension_node(self) -> str:
        raw_dim_node = self.dimension.rsplit(".", 1)[0]
        return (
            render_prefixes(raw_dim_node, self.namespace)
            if self.namespace
            else raw_dim_node
        )

    @property
    def dimension_attribute(self) -> str:
        return self.dimension.rsplit(".", 1)[-1]

    def __hash__(self) -> int:
        return hash(
            (
                self.rendered_dimension_node,
                self.dimension_attribute,
                self.node_column,
            ),
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DimensionReferenceLinkSpec):
            return False
        return (
            super().__eq__(other)
            and self.rendered_dimension_node == other.rendered_dimension_node
            and self.dimension_attribute == other.dimension_attribute
            and self.node_column == other.node_column
        )


def render_prefixes(parameterized_string: str, prefix: str | None = None) -> str:
    """
    Replaces ${prefix} in a string
    """
    return parameterized_string.replace(
        "${prefix}",
        f"{prefix}{SEPARATOR}" if prefix else "",
    )


class NodeSpec(BaseModel):
    """
    Specification of a node as declared in a deployment.
    The name is relative and will be hydrated with the deployment namespace.
    """

    name: str

    # Not user-supplied, gets injected
    namespace: str | None = Field(default=None, exclude=True)

    node_type: NodeType
    owners: list[str] = Field(default_factory=list)
    display_name: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    mode: NodeMode = NodeMode.PUBLISHED
    custom_metadata: dict | None = None

    _query_ast: Any | None = PrivateAttr(default=None)

    @property
    def rendered_name(self) -> str:
        if self.namespace:
            return f"{self.namespace}{SEPARATOR}{self.name}"
        return self.name

    @property
    def rendered_query(self) -> str | None:
        if hasattr(self, "query"):
            query = getattr(self, "query")
            if query:
                return render_prefixes(query, self.namespace)
        return None

    @property
    def query_ast(self):
        """
        Lazily parse and cache the rendered query as an AST.
        """
        from datajunction_server.sql.parsing.backends.antlr4 import parse

        if self._query_ast is None and self.rendered_query:
            self._query_ast = parse(self.rendered_query)
        return self._query_ast

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, NodeSpec):
            return False
        return (
            self.rendered_name == other.rendered_name
            and self.node_type == other.node_type
            and (self.display_name == other.display_name or self.display_name is None)
            and (self.description == other.description)
            and set(self.owners) == set(other.owners)
            and set(self.tags) == set(other.tags)
            and self.mode == other.mode
            and (
                self.custom_metadata == other.custom_metadata
                or self.custom_metadata is None
                and other.custom_metadata == {}
            )
        )


class LinkableNodeSpec(NodeSpec):
    """
    Specification for a node type that can be linked to dimension nodes:
    e.g., source, transform, dimension
    """

    columns: list[ColumnSpec] | None = None
    dimension_links: list[DimensionJoinLinkSpec | DimensionReferenceLinkSpec] | None = (
        None
    )
    primary_key: list[str] = Field(default_factory=list)

    @validator("dimension_links", pre=True, each_item=True)
    def coerce_dimension_links(cls, value, values):
        if isinstance(value, dict):
            link_type = value.get("type")
            mapping = {
                "join": DimensionJoinLinkSpec,
                "reference": DimensionReferenceLinkSpec,
            }
            if link_type not in mapping:
                raise ValueError(f"Unknown node_type: {link_type}")
            deployment_ns = values.get("namespace")
            return mapping[link_type](**value, namespace=deployment_ns)
        value.namespace = values.get("namespace")
        return value

    def __eq__(self, other: Any) -> bool:
        return (
            super().__eq__(other)
            and (self.columns or []) == (other.columns or [])
            and sorted(
                self.dimension_links or [],
                key=lambda link: link.rendered_dimension_node,
            )
            == sorted(
                other.dimension_links or [],
                key=lambda link: link.rendered_dimension_node,
            )
        )


class SourceSpec(LinkableNodeSpec):
    """
    Specification for a source node
    """

    node_type: Literal[NodeType.SOURCE] = NodeType.SOURCE
    table: str

    def __post_init__(self):
        """
        Validate that the table name is fully qualified
        """
        if (
            self.table.count(".") != 2
            or not self.table.replace(".", "").replace("_", "").isalnum()
        ):
            raise DJInvalidInputException(
                f"Invalid table name {self.table}: table name must be fully qualified: "
                "<catalog>.<schema>.<table>",
            )

    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other) and self.table == other.table


class TransformSpec(LinkableNodeSpec):
    """
    Specification for a transform node
    """

    node_type: Literal[NodeType.TRANSFORM] = NodeType.TRANSFORM
    query: str

    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other) and self.query_ast.compare(other.query_ast)


class DimensionSpec(LinkableNodeSpec):
    """
    Specification for a dimension node
    """

    node_type: Literal[NodeType.DIMENSION] = NodeType.DIMENSION
    query: str

    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other) and self.query_ast.compare(other.query_ast)


class MetricSpec(NodeSpec):
    """
    Specification for a metric node
    """

    node_type: Literal[NodeType.METRIC] = NodeType.METRIC
    query: str
    required_dimensions: list[str] | None = None
    direction: MetricDirection | None = None
    unit: MetricUnit | None = None
    significant_digits: int | None = None
    min_decimal_exponent: int | None
    max_decimal_exponent: int | None

    def __eq__(self, other: Any) -> bool:
        return (
            super().__eq__(other)
            and self.query_ast.compare(other.query_ast)
            and (self.required_dimensions or []) == (other.required_dimensions or [])
            and self.direction == other.direction
            and self.unit == other.unit
            and self.significant_digits == other.significant_digits
            and self.min_decimal_exponent == other.min_decimal_exponent
            and self.max_decimal_exponent == other.max_decimal_exponent
        )


class CubeSpec(NodeSpec):
    """
    Specification for a cube node
    """

    node_type: Literal[NodeType.CUBE] = NodeType.CUBE
    metrics: list[str]
    dimensions: list[str] = Field(default_factory=dict)
    filters: list[str] | None = None

    @property
    def rendered_metrics(self) -> list[str]:
        return [render_prefixes(metric, self.namespace) for metric in self.metrics]

    @property
    def rendered_dimensions(self) -> list[str]:
        return [render_prefixes(dim, self.namespace) for dim in self.dimensions]

    @property
    def rendered_filters(self) -> list[str]:
        return [
            render_prefixes(filter_, self.namespace) for filter_ in self.filters or []
        ]

    def __eq__(self, other: Any) -> bool:
        return (
            super().__eq__(other)
            and set(self.rendered_metrics) == set(other.rendered_metrics)
            and set(self.rendered_dimensions) == set(other.rendered_dimensions)
            and (self.rendered_filters or []) == (other.rendered_filters or [])
        )


NodeUnion = Union[
    SourceSpec,
    TransformSpec,
    DimensionSpec,
    MetricSpec,
    CubeSpec,
]


class DeploymentSpec(BaseModel):
    """
    Specification of a full deployment (namespace, nodes, tags, and add'l metadata).
    Typically hydrated from a project manifest (YAML/JSON/etc).
    """

    namespace: str
    nodes: list[NodeUnion] = Field(default_factory=list)
    tags: list[TagSpec] = Field(default_factory=list)

    @validator("nodes", pre=True, each_item=True)
    def coerce_nodes(cls, value, values):
        if isinstance(value, dict):
            node_type = value.get("node_type")
            mapping = {
                "source": SourceSpec,
                "transform": TransformSpec,
                "dimension": DimensionSpec,
                "metric": MetricSpec,
                "cube": CubeSpec,
            }
            if node_type not in mapping:
                raise ValueError(f"Unknown node_type: {node_type}")
            deployment_ns = values.get("namespace")
            return mapping[node_type](**value, namespace=deployment_ns)
        return value


class VersionedNode(BaseModel):
    """
    Node name and version
    """

    name: str
    current_version: str

    class Config:
        orm_mode = True


class DeploymentResult(BaseModel):
    """
    Result of deploying a single node, link, or tag
    """

    class Status(str, Enum):
        SUCCESS = "success"
        NOOP = "noop"
        FAILED = "failed"
        SKIPPED = "skipped"

    class Type(str, Enum):
        NODE = "node"
        LINK = "link"
        TAG = "tag"
        GENERAL = "general"

    name: str
    deploy_type: Type
    status: Status
    message: str = ""


class DeploymentInfo(BaseModel):
    """
    Information about a deployment
    """

    uuid: str
    namespace: str
    status: DeploymentStatus
    results: list[DeploymentResult] = Field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(
            result.status == DeploymentResult.Status.SUCCESS for result in self.results
        )
