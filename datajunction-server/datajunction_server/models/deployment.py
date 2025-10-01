from enum import Enum
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    PrivateAttr,
    ConfigDict,
    model_validator,
)

from typing import Annotated, Any, Literal, Union

from datajunction_server.models.partition import Granularity, PartitionType
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.base import labelize
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


class PartitionSpec(BaseModel):
    """
    Represents a partition
    """

    type: PartitionType
    granularity: Granularity | None = None
    format: str | None = None


class ColumnSpec(BaseModel):
    """
    Represents a column
    """

    name: str
    type: str
    display_name: str | None = None
    description: str | None = None
    attributes: list[str] = Field(default_factory=list)
    partition: PartitionSpec | None = None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ColumnSpec):
            return False
        return (
            self.name == other.name
            and self.type == other.type
            and (self.display_name == other.display_name or self.display_name is None)
            and self.description == other.description
            and set(self.attributes) == set(other.attributes)
            and self.partition == other.partition
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
    type: Literal[LinkType.JOIN] = LinkType.JOIN

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
        if not isinstance(other, DimensionJoinLinkSpec):
            return False
        return (
            super().__eq__(other)
            and self.rendered_dimension_node == other.rendered_dimension_node
            and self.join_type == other.join_type
            and self.rendered_join_on == other.rendered_join_on
            and self.node_column == other.node_column
        )


class DimensionReferenceLinkSpec(DimensionLinkSpec):
    """
    Specification for a dimension reference link

    The `dimension` input should be a fully qualified dimension attribute name,
    e.g., "<dimension_node>.<column>"
    """

    node_column: str
    dimension: str
    type: Literal[LinkType.REFERENCE] = LinkType.REFERENCE

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
            if "${prefix}" in self.name:  # pragma: no cover
                return render_prefixes(self.name, self.namespace)
            return f"{self.namespace}{SEPARATOR}{self.name}"
        return self.name

    @property
    def rendered_query(self) -> str | None:
        if hasattr(self, "query") and self.query:
            query = getattr(self, "query")
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
            return False  # pragma: no cover
        return (
            self.rendered_name == other.rendered_name
            and self.node_type == other.node_type
            and (self.display_name == other.display_name or self.display_name is None)
            and (self.description == other.description)
            and set(self.owners) == set(other.owners)
            and set(self.tags) == set(other.tags)
            and self.mode == other.mode
            and eq_or_fallback(self.custom_metadata, other.custom_metadata, {})
        )

    def diff(self, other: "NodeSpec") -> list[str]:
        """
        Return a list of fields that differ between this and another NodeSpec.
        """
        return diff(
            self,
            other,
            ignore_fields=["name", "namespace", "query", "columns"],
        )


class LinkableNodeSpec(NodeSpec):
    """
    Specification for a node type that can be linked to dimension nodes:
    e.g., source, transform, dimension
    """

    columns: list[ColumnSpec] | None = None
    dimension_links: list[
        Annotated[
            DimensionJoinLinkSpec | DimensionReferenceLinkSpec,
            Field(discriminator="type"),
        ]
    ] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def set_namespaces(self):
        """
        Set namespace on all dimension links
        """
        if self.namespace:
            for link in self.dimension_links:
                link.namespace = self.namespace
        return self

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LinkableNodeSpec):
            return False
        dimension_links_equal = sorted(
            self.dimension_links or [],
            key=lambda link: link.rendered_dimension_node,
        ) == sorted(
            other.dimension_links or [],
            key=lambda link: link.rendered_dimension_node,
        )
        return (
            super().__eq__(other)
            and eq_columns(self.columns, other.columns)
            and dimension_links_equal
        )


class SourceSpec(LinkableNodeSpec):
    """
    Specification for a source node
    """

    node_type: Literal[NodeType.SOURCE] = NodeType.SOURCE
    table: str

    @field_validator("table")
    def validate_table(cls, value) -> str:
        """
        Validate that the table name is fully qualified
        """
        if (
            value.count(".") != 2
            or not value.replace(".", "").replace("_", "").isalnum()
        ):
            raise DJInvalidInputException(
                f"Invalid table name {value}: table name must be fully qualified: "
                "<catalog>.<schema>.<table>",
            )
        return value

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
    unit_enum: MetricUnit | None = Field(default=None, exclude=True)

    significant_digits: int | None = None
    min_decimal_exponent: int | None = None
    max_decimal_exponent: int | None = None

    def __init__(self, **data: Any):
        unit = data.pop("unit", None)
        if unit:
            try:
                if isinstance(unit, MetricUnit):
                    data["unit_enum"] = unit
                else:
                    data["unit_enum"] = MetricUnit[  # pragma: no cover
                        unit.strip().upper()
                    ]
            except KeyError:  # pragma: no cover
                raise DJInvalidInputException(f"Invalid metric unit: {unit}")
        super().__init__(**data)

    @property
    def unit(self) -> str | None:
        """Return lowercased unit name for JSON serialization."""
        if self.unit_enum is None:  # pragma: no cover
            return None
        return self.unit_enum.value.name.lower()

    def model_dump(self, **kwargs):  # pragma: no cover
        base = super().model_dump(**kwargs)
        base["unit"] = self.unit
        return base

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MetricSpec):
            return False
        return (
            super().__eq__(other)
            and self.query_ast.compare(other.query_ast)
            and (self.required_dimensions or []) == (other.required_dimensions or [])
            and eq_or_fallback(self.direction, other.direction, MetricDirection.NEUTRAL)
            and eq_or_fallback(self.unit, other.unit, MetricUnit.UNKNOWN.value.name)
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
    columns: list[ColumnSpec] | None = None

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
            and eq_columns(self.columns, other.columns)
            and set(self.rendered_metrics) == set(other.rendered_metrics)
            and set(self.rendered_dimensions) == set(other.rendered_dimensions)
            and (self.rendered_filters or []) == (other.rendered_filters or [])
        )


NodeUnion = Annotated[
    Union[
        SourceSpec,
        TransformSpec,
        DimensionSpec,
        MetricSpec,
        CubeSpec,
    ],
    Field(discriminator="node_type"),
]


def diff(one: BaseModel, two: BaseModel, ignore_fields: list[str] = None) -> list[str]:
    """
    Compare two Pydantic models and return a list of fields that have changed.
    """
    changed_fields = [
        field
        for field in one.__fields__.keys()
        if field not in (ignore_fields or [])
        and hasattr(one, field)
        and hasattr(two, field)
        and (
            (
                isinstance(getattr(one, field), (list, dict))
                and set(getattr(one, field) or []) != set(getattr(two, field) or [])
            )
            or (
                not isinstance(getattr(one, field), (list, dict))
                and getattr(one, field) != getattr(two, field)
            )
        )
    ]
    return changed_fields


class DeploymentSpec(BaseModel):
    """
    Specification of a full deployment (namespace, nodes, tags, and add'l metadata).
    Typically hydrated from a project manifest (YAML/JSON/etc).
    """

    namespace: str
    nodes: list[NodeUnion] = Field(default_factory=list)
    tags: list[TagSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def set_namespaces(self):
        """
        Set namespace on all node specs and their dimension links
        """
        if (  # pragma: no cover
            hasattr(self, "nodes") and hasattr(self, "namespace") and self.namespace
        ):
            for node in self.nodes:
                # Set namespace on the node itself
                if hasattr(node, "namespace") and not node.namespace:
                    node.namespace = self.namespace

                # Set namespace on dimension links (for LinkableNodeSpec subclasses)
                if hasattr(node, "dimension_links") and node.dimension_links:
                    for link in node.dimension_links:
                        if not link.namespace:
                            link.namespace = self.namespace
        return self


class VersionedNode(BaseModel):
    """
    Node name and version
    """

    name: str
    current_version: str

    model_config = ConfigDict(from_attributes=True)


class DeploymentResult(BaseModel):
    """
    Result of deploying a single node, link, or tag
    """

    class Status(str, Enum):
        SUCCESS = "success"
        FAILED = "failed"
        SKIPPED = "skipped"

    class Operation(str, Enum):
        CREATE = "create"
        UPDATE = "update"
        DELETE = "delete"
        NOOP = "noop"
        UNKNOWN = "unknown"

    class Type(str, Enum):
        NODE = "node"
        LINK = "link"
        TAG = "tag"
        GENERAL = "general"

    name: str
    deploy_type: Type
    status: Status
    operation: Operation
    message: str = ""


class DeploymentInfo(BaseModel):
    """
    Information about a deployment
    """

    uuid: str
    namespace: str
    status: DeploymentStatus
    results: list[DeploymentResult] = Field(default_factory=list)


def eq_or_fallback(a, b, fallback):
    """
    Helper to compare two values that may be None, with a fallback value
    """
    return a == b or (a is None and b == fallback)


def eq_columns(a: list[ColumnSpec] | None, b: list[ColumnSpec] | None) -> bool:
    """
    Compare two lists of ColumnSpec objects (or None) with special rules:
      - None or [] is considered equivalent to a list where every column only has 'primary_key'
        in attributes and partition is None.
    """
    a_map = {col.name: col for col in a or []}
    b_map = {col.name: col for col in b or []}
    a_cols, b_cols = [], []
    for col_name in set(a_map.keys()).union(set(b_map.keys())):
        a_col = a_map.get(col_name)
        b_col = b_map.get(col_name)
        if not a_col:
            a_col = ColumnSpec(
                name=col_name,
                display_name=labelize(col_name),
                type=b_col.type if b_col else "",
                attributes=[],
            )
        if not a_col.display_name:
            a_col.display_name = labelize(col_name)
        if not a_col.description:
            a_col.description = ""
        if not b_col:
            b_col = ColumnSpec(  # pragma: no cover
                name=col_name,
                display_name=labelize(col_name),
                type=a_col.type if a_col else "",
                attributes=[],
            )
        if not b_col.display_name:
            b_col.display_name = labelize(col_name)
        if not b_col.description:  # pragma: no cover
            b_col.description = ""
        if "primary_key" in a_col.attributes:
            a_col.attributes = list(set(a_col.attributes) - {"primary_key"})
        if "primary_key" in b_col.attributes:
            b_col.attributes = list(set(b_col.attributes) - {"primary_key"})
        a_cols.append(a_col)
        b_cols.append(b_col)
    return a_cols == b_cols
