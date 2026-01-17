from enum import Enum
from pydantic import (
    BaseModel,
    Field,
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


class DeploymentSourceType(str, Enum):
    """Type of deployment source - git-managed or local/adhoc."""

    GIT = "git"
    LOCAL = "local"


class TagSpec(BaseModel):
    """
    Specification for a tag
    """

    name: str
    display_name: str
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
    Represents a column.

    The `type` field is optional - if not provided, DJ will infer the column
    type from the query or source definition. This is useful when you only
    want to specify metadata (display_name, attributes, description) without
    hardcoding the type.
    """

    name: str
    type: str | None = None  # Optional - DJ infers from query/source if not provided
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
            return False  # pragma: no cover
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

    model_config = ConfigDict(truncate_errors=False)

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
            and (
                self.description == other.description
                or not self.description
                and not other.description
            )
            and set(self.owners) == set(other.owners)
            and set(self.tags) == set(other.tags)
            and self.mode == other.mode
            and eq_or_fallback(self.custom_metadata, other.custom_metadata, {})
        )

    def diff(self, other: "NodeSpec") -> list[str]:
        """
        Return a list of fields that differ between this and another NodeSpec.
        Compares user-provided fields and returns names of fields that changed.
        """
        changed = []

        # Compare display_name
        if self.display_name != other.display_name:
            changed.append("display_name")

        # Compare description
        if self.description != other.description:
            changed.append("description")

        # Compare owners (as sets for order-independence)
        if set(self.owners or []) != set(other.owners or []):
            changed.append("owners")

        # Compare tags (as sets for order-independence)
        if set(self.tags or []) != set(other.tags or []):
            changed.append("tags")

        # Compare mode
        if self.mode != other.mode:
            changed.append("mode")

        # Compare custom_metadata
        if (self.custom_metadata or {}) != (other.custom_metadata or {}):
            changed.append("custom_metadata")

        # Compare dimension_links for LinkableNodeSpec
        if hasattr(self, "dimension_links") and hasattr(other, "dimension_links"):
            self_links = sorted(
                [link.model_dump() for link in (self.dimension_links or [])],
                key=lambda x: (
                    x.get("type", ""),
                    x.get("dimension_node", "") or x.get("dimension", ""),
                    x.get("role", ""),
                ),
            )
            other_links = sorted(
                [link.model_dump() for link in (other.dimension_links or [])],
                key=lambda x: (
                    x.get("type", ""),
                    x.get("dimension_node", "") or x.get("dimension", ""),
                    x.get("role", ""),
                ),
            )
            if self_links != other_links:
                changed.append("dimension_links")

        # Compare primary_key for LinkableNodeSpec
        if hasattr(self, "primary_key") and hasattr(other, "primary_key"):
            if set(self.primary_key or []) != set(other.primary_key or []):
                changed.append("primary_key")

        # Type-specific comparisons
        if self.node_type == NodeType.SOURCE:
            if hasattr(self, "catalog") and hasattr(other, "catalog"):
                if self.catalog != other.catalog:
                    changed.append("catalog")
            if hasattr(self, "schema_") and hasattr(other, "schema_"):
                if self.schema_ != other.schema_:
                    changed.append("schema_")
            if hasattr(self, "table") and hasattr(other, "table"):
                if self.table != other.table:
                    changed.append("table")

        elif self.node_type == NodeType.METRIC:
            if hasattr(self, "required_dimensions") and hasattr(
                other,
                "required_dimensions",
            ):
                if set(self.required_dimensions or []) != set(
                    other.required_dimensions or [],
                ):
                    changed.append("required_dimensions")
            if hasattr(self, "direction") and hasattr(other, "direction"):
                if self.direction != other.direction:
                    changed.append("direction")
            if hasattr(self, "unit_enum") and hasattr(other, "unit_enum"):
                if self.unit_enum != other.unit_enum:
                    changed.append("unit_enum")

        elif self.node_type == NodeType.CUBE:
            if hasattr(self, "metrics") and hasattr(other, "metrics"):
                if set(self.metrics or []) != set(other.metrics or []):
                    changed.append("metrics")
            if hasattr(self, "dimensions") and hasattr(other, "dimensions"):
                if set(self.dimensions or []) != set(other.dimensions or []):
                    changed.append("dimensions")

        return changed


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

    @property
    def links_mapping(self) -> dict[tuple[str, str | None], DimensionLinkSpec]:
        """Map dimension links by (dimension_node, role)"""
        return {
            (link.rendered_dimension_node, link.role): link
            for link in self.dimension_links
        }

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LinkableNodeSpec):
            return False
        dimension_links_equal = sorted(
            self.dimension_links or [],
            key=lambda link: (link.rendered_dimension_node, link.role or ""),
        ) == sorted(
            other.dimension_links or [],
            key=lambda link: (link.rendered_dimension_node, link.role or ""),
        )
        return (
            super().__eq__(other)
            and eq_columns(
                self.columns,
                other.columns,
                compare_types=True if self.node_type == NodeType.SOURCE else False,
            )
            and dimension_links_equal
        )


class SourceSpec(LinkableNodeSpec):
    """
    Specification for a source node
    """

    node_type: Literal[NodeType.SOURCE] = NodeType.SOURCE
    catalog: str
    schema_: str | None = Field(alias="schema")
    table: str

    model_config = ConfigDict(populate_by_name=True)

    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other) and (
            self.catalog == other.catalog
            and self.schema_ == other.schema_
            and self.table == other.table
        )


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
    required_dimensions: list[str] | None = None  # Field(default_factory=list)
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

    @property
    def rendered_columns(self) -> list[ColumnSpec]:
        """Render column names with namespace prefixes for comparison"""
        if not self.columns:
            return []
        rendered = []
        for col in self.columns:
            rendered_col = col.model_copy()
            rendered_col.name = render_prefixes(col.name, self.namespace)
            rendered.append(rendered_col)
        return rendered

    def __eq__(self, other: Any) -> bool:
        return (
            super().__eq__(other)
            and eq_columns(self.rendered_columns, other.rendered_columns)
            or self.rendered_columns == []
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


def _normalize_for_comparison(value):
    """
    Normalize a value for comparison. Converts lists to frozensets for
    order-independent comparison, and handles nested structures.
    """
    if isinstance(value, list):
        # Convert list items to comparable format
        normalized_items = []
        for item in value:
            if isinstance(item, BaseModel):
                # Convert BaseModel to a hashable tuple of sorted items
                normalized_items.append(
                    tuple(sorted(_normalize_for_comparison(item.model_dump()).items()))
                    if isinstance(_normalize_for_comparison(item.model_dump()), dict)
                    else _normalize_for_comparison(item.model_dump()),
                )
            elif isinstance(item, dict):
                normalized_items.append(
                    tuple(sorted(_normalize_for_comparison(item).items())),
                )
            else:
                normalized_items.append(item)
        return frozenset(normalized_items)
    elif isinstance(value, dict):
        return {k: _normalize_for_comparison(v) for k, v in value.items()}
    else:
        return value


def diff(one: BaseModel, two: BaseModel, ignore_fields: list[str] = None) -> list[str]:
    """
    Compare two Pydantic models and return a list of fields that have changed.

    Uses model_dump() to get all fields including inherited ones, then compares
    values with proper handling for lists (order-independent) and nested objects.
    """
    ignore = set(ignore_fields or [])

    # Use model_dump to get all fields including inherited ones
    one_dict = one.model_dump()
    two_dict = two.model_dump()

    changed_fields = []
    # Check all fields from both models
    all_fields = set(one_dict.keys()) | set(two_dict.keys())

    for field in all_fields:
        if field in ignore:
            continue

        one_value = one_dict.get(field)
        two_value = two_dict.get(field)

        # Normalize values for comparison (handles lists as sets, etc.)
        one_normalized = _normalize_for_comparison(one_value)
        two_normalized = _normalize_for_comparison(two_value)

        if one_normalized != two_normalized:
            changed_fields.append(field)

    return changed_fields


class GitDeploymentSource(BaseModel):
    """
    Deployment from a tracked git repository.
    Indicates the source of truth is in version control with CI/CD automation.
    """

    type: Literal[DeploymentSourceType.GIT] = DeploymentSourceType.GIT
    repository: str  # e.g., "github.com/org/repo"
    branch: str | None = None  # e.g., "main", "feature/xyz"
    commit_sha: str | None = None  # e.g., "abc123def456"
    ci_system: str | None = None  # e.g., "jenkins", "github-actions"
    ci_run_url: str | None = None  # Link to the CI build/run


class LocalDeploymentSource(BaseModel):
    """
    Adhoc deployment without a git repository context.
    Could be from CLI, direct API calls, scripts, or development/testing.
    """

    type: Literal[DeploymentSourceType.LOCAL] = DeploymentSourceType.LOCAL
    # Optional context about the adhoc deployment
    hostname: str | None = None  # Machine it was run from
    reason: str | None = None  # Why this adhoc deployment?


# Discriminated union - Pydantic will use the 'type' field to determine which model to use
DeploymentSource = Annotated[
    GitDeploymentSource | LocalDeploymentSource,
    Field(discriminator="type"),
]


class NamespaceSourcesResponse(BaseModel):
    """
    Response for the /namespaces/{namespace}/sources endpoint.
    Shows the primary deployment source for a namespace.
    """

    namespace: str
    primary_source: GitDeploymentSource | LocalDeploymentSource | None = None
    total_deployments: int = 0


class BulkNamespaceSourcesRequest(BaseModel):
    """
    Request body for fetching sources for multiple namespaces at once.
    """

    namespaces: list[str] = Field(
        ...,
        description="List of namespace names to fetch sources for",
    )


class BulkNamespaceSourcesResponse(BaseModel):
    """
    Response for bulk fetching namespace sources.
    Maps namespace names to their deployment source info.
    """

    sources: dict[str, NamespaceSourcesResponse] = Field(default_factory=dict)


class DeploymentSpec(BaseModel):
    """
    Specification of a full deployment (namespace, nodes, tags, and add'l metadata).
    Typically hydrated from a project manifest (YAML/JSON/etc).
    """

    namespace: str
    nodes: list[NodeUnion] = Field(default_factory=list)
    tags: list[TagSpec] = Field(default_factory=list)
    source: DeploymentSource | None = None  # CI/CD provenance tracking

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
    created_at: str | None = None  # ISO datetime
    created_by: str | None = None  # Username
    source: GitDeploymentSource | LocalDeploymentSource | None = None


def eq_or_fallback(a, b, fallback):
    """
    Helper to compare two values that may be None, with a fallback value
    """
    return a == b or (a is None and b == fallback)


def eq_columns(
    a: list[ColumnSpec] | None,
    b: list[ColumnSpec] | None,
    compare_types: bool = True,
) -> bool:
    """
    Compare two lists of ColumnSpec objects (or None) with special rules:
      - None or [] is considered equivalent to a list where every column only has 'primary_key'
        in attributes and partition is None.
      - If a column is missing display_name or description, it's treated as empty string.
    If the compare_types flag is False, the column types will not be compared.
    """
    a_map = {col.name: col for col in a or []}
    b_map = {col.name: col for col in b or []}
    a_cols, b_cols = [], []
    for col_name in set(a_map.keys()).union(set(b_map.keys())):
        a_col = a_map.get(col_name).model_copy() if a_map.get(col_name) else None  # type: ignore
        b_col = b_map.get(col_name).model_copy() if b_map.get(col_name) else None  # type: ignore
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
        if not compare_types:
            a_col.type = ""
            b_col.type = ""
        # Remove primary_key from copies for comparison
        if "primary_key" in a_col.attributes:
            a_col.attributes = list(set(a_col.attributes) - {"primary_key"})
        if "primary_key" in b_col.attributes:
            b_col.attributes = list(set(b_col.attributes) - {"primary_key"})
        a_cols.append(a_col)
        b_cols.append(b_col)
    return a_cols == b_cols
