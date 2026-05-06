from enum import Enum
from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
    ConfigDict,
    model_validator,
)

from typing import Annotated, Any, Literal, Optional, Union
from datajunction_server.models.partition import Granularity, PartitionType
from datajunction_server.errors import (
    DJInvalidDeploymentConfig,
    DJInvalidInputException,
)
from datajunction_server.models.base import labelize
from datajunction_server.models.dimensionlink import (
    JoinType,
    LinkType,
    SparkJoinStrategy,
)
from datajunction_server.models.impact import DownstreamImpact
from datajunction_server.models.node import (
    MetricDirection,
    MetricUnit,
    NodeMode,
    NodeStatus,
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
    order: int | None = Field(
        default=None,
        exclude=True,
    )  # Internal use only, not serialized

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
    default_value: str | None = None
    spark_hints: Optional[SparkJoinStrategy] = None

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
                self.default_value,
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
            and self.default_value == other.default_value
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
    # Internal: marks specs from already-validated sources (e.g., branch copies)
    # that can skip expensive SQL parsing and validation
    _skip_validation: bool = PrivateAttr(default=False)
    # Internal: pre-computed upstream dependency names (with ${prefix} placeholders).
    # Populated during export from source node DB parents to avoid re-parsing SQL
    # during the copy fast-path. None means "not populated; fall back to SQL parsing".
    _upstream_names: list[str] | None = PrivateAttr(default=None)
    # Internal: source node status carried through the copy fast-path so a branch
    # copy preserves VALID/INVALID instead of unconditionally marking nodes VALID.
    # None means "not populated; fall back to NodeStatus.VALID".
    _source_status: NodeStatus | None = PrivateAttr(default=None)

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

    def rendered_spec(self) -> "NodeSpec":
        """
        Return a copy of this spec with all ${prefix} placeholders resolved.
        Needed for accurate diffs against existing specs that store fully-qualified names.
        """
        import json

        raw = self.model_dump(mode="json")
        prefix = f"{self.namespace}{SEPARATOR}" if self.namespace else ""
        rendered_json = json.dumps(raw).replace("${prefix}", prefix)
        return self.__class__.model_validate_json(rendered_json)

    def diff(self, other: "NodeSpec") -> list[str]:
        """
        Return a list of fields that differ between this and another NodeSpec.
        Renders ${prefix} placeholders in `other` before comparing so that
        specs with unresolved prefixes don't produce false positives.
        """
        return diff(
            self,
            other.rendered_spec(),
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
            and set(self.primary_key or []) == set(other.primary_key or [])
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
    # Internal only - used for validation skip optimization when copying from valid nodes.
    # Excluded from serialization so it's never exported.
    columns: list[ColumnSpec] | None = Field(default=None, exclude=True)
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
    # Both default to empty so a malformed cube spec (e.g., one whose metrics
    # were dropped during YAML round-trip, or a cube authored with zero
    # metrics) still parses. Downstream validation flags the cube as INVALID
    # rather than failing the whole deployment at pydantic-parse time.
    metrics: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
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
        if not isinstance(other, CubeSpec):
            return False
        if not super().__eq__(other):
            return False
        if not (
            set(self.rendered_metrics) == set(other.rendered_metrics)
            and set(self.rendered_dimensions) == set(other.rendered_dimensions)
            and (self.rendered_filters or []) == (other.rendered_filters or [])
        ):
            return False
        # Compare only partition config for user-specified columns.
        # Cube element columns (types, order, attributes) are auto-derived and ignored.
        incoming_partitions = {
            col.name: col.partition for col in self.rendered_columns if col.partition
        }
        existing_partitions = {
            col.name: col.partition
            for col in (other.rendered_columns or [])
            if col.partition
        }
        return incoming_partitions == existing_partitions


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


def _norm(v: Any) -> Any:
    """Normalize falsy string/None to None so that '' and None compare equal."""
    if isinstance(v, str):
        return v or None
    return v


def diff(one: BaseModel, two: BaseModel, ignore_fields: list[str] = None) -> list[str]:
    """
    Compare two Pydantic models and return a list of fields that have changed.
    """
    changed_fields = [
        field
        for field in one.model_fields.keys()
        if field not in (ignore_fields or [])
        and hasattr(one, field)
        and hasattr(two, field)
        and (
            (
                isinstance(getattr(one, field) or getattr(two, field), (list, dict))
                and {
                    tuple(sorted(item.model_dump().items()))
                    if isinstance(item, BaseModel)
                    else item
                    for item in getattr(one, field) or []
                }
                != {
                    tuple(sorted(item.model_dump().items()))
                    if isinstance(item, BaseModel)
                    else item
                    for item in getattr(two, field) or []
                }
            )
            or (
                not isinstance(getattr(one, field) or getattr(two, field), (list, dict))
                and _norm(getattr(one, field)) != _norm(getattr(two, field))
            )
        )
    ]
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
    commit_author_email: str | None = None  # e.g., "alice@example.com"
    commit_author_name: str | None = None  # e.g., "Alice Smith"
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


class NamespaceGitConfig(BaseModel):
    """
    Git configuration for a namespace, enabling git-backed branch management.
    When set, users can create branches and sync changes to GitHub from the UI.
    """

    github_repo_path: str | None = None  # e.g., "owner/repo"
    git_branch: str | None = None  # e.g., "main" or "feature-x"
    git_path: str | None = None  # e.g., "definitions/" - subdirectory within repo
    default_branch: str | None = None  # Default branch for git roots (e.g., "main")
    parent_namespace: str | None = None  # Links branch namespaces to parent
    git_only: bool | None = None  # If True, UI edits blocked; must edit via git


class DeploymentSpec(BaseModel):
    """
    Specification of a full deployment (namespace, nodes, tags, and add'l metadata).
    Typically hydrated from a project manifest (YAML/JSON/etc).
    """

    namespace: str
    nodes: list[NodeUnion] = Field(default_factory=list)
    tags: list[TagSpec] = Field(default_factory=list)
    source: DeploymentSource | None = None  # CI/CD provenance tracking
    git_config: NamespaceGitConfig | None = None  # Git branch management config
    force: bool = Field(
        default=False,
        description=(
            "If True, all nodes are treated as changed and will be updated even if "
            "they are identical to the existing version. Useful for forcing a full "
            "re-deployment (e.g., after infrastructure changes)."
        ),
    )
    auto_register_sources: bool = Field(
        default=True,
        description=(
            "If True, automatically register missing source nodes by introspecting "
            "catalog tables. Missing nodes that match the pattern catalog.schema.table "
            "will be looked up in the specified catalog and auto-created as source nodes."
        ),
    )
    default_catalog: str | None = Field(
        default=None,
        description=(
            "Default catalog name to use for non-source nodes when the catalog cannot be "
            "inferred from parent nodes (e.g., for invalid nodes with unresolvable parents). "
            "Falls back to the virtual catalog if not set."
        ),
    )

    @model_validator(mode="after")
    def set_namespaces(self):
        """
        Require a non-empty namespace and propagate it to every node spec and
        dimension link.

        An empty namespace used to silently no-op here, producing specs whose
        ``${prefix}`` placeholders stayed unrendered all the way down to
        ``DimensionLink.parse_join_sql`` — which then failed with a cryptic
        ANTLR ``mismatched input '$'`` error. Failing here turns that into a
        clear "namespace is required" error at the edge.
        """
        if not self.namespace:
            raise DJInvalidDeploymentConfig(
                message=(
                    "DeploymentSpec.namespace is required and must be non-empty. "
                    "Pass --namespace on the CLI or set `namespace:` in dj.yaml."
                ),
            )
        for node in self.nodes:
            if not node.namespace:
                node.namespace = self.namespace
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
        INVALID = "invalid"  # deployed but node status is INVALID

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
    changed_fields: list[str] = Field(default_factory=list)


class DeploymentInfo(BaseModel):
    """
    Information about a deployment
    """

    uuid: str
    namespace: str
    status: DeploymentStatus
    results: list[DeploymentResult] = Field(default_factory=list)
    downstream_impacts: list[DownstreamImpact] = Field(default_factory=list)
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
    # For source nodes (compare_types=True), column additions and removals from
    # an explicit column list are changes. Only applies when both sides are non-empty
    # (None/[] means "unspecified — don't compare").
    if compare_types and a and b and set(a_map.keys()) != set(b_map.keys()):
        return False
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
