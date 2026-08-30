import json
from collections.abc import Iterable
from enum import Enum, IntEnum
from typing import Annotated, Any, ClassVar, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    TypeAdapter,
    field_validator,
    model_validator,
)

from datajunction_server.errors import (
    DJError,
    DJInvalidDeploymentConfig,
    DJInvalidInputException,
)
from datajunction_server.models.base import labelize
from datajunction_server.models.dimensionlink import (
    JoinCardinality,
    JoinType,
    LinkType,
    SparkJoinStrategy,
)
from datajunction_server.models.impact import DownstreamImpact
from datajunction_server.models.materialization import (
    DEFAULT_CUBE_RETENTION,
    CoverageSpec,
    MaterializationStrategy,
    SparkSpec,
)
from datajunction_server.models.node import (
    MetricDirection,
    MetricUnit,
    NodeMode,
    NodeStatus,
    NodeType,
)
from datajunction_server.models.partition import Granularity, PartitionType
from datajunction_server.models.preagg_binding import (
    SPEC_SURFACE,
    validate_column_bindings,
)
from datajunction_server.models.unit import (
    Unit,
    legacy_unit_to_structured,
    unit_to_dict,
)
from datajunction_server.utils import SEPARATOR, Version


class ChangeTier(IntEnum):
    """
    How significant a change to a node is, and so what kind of new version it earns.

    A different question from `NodeSpec.__eq__`, which says only whether anything
    changed at all. Both the deployment path and `PATCH /nodes/{name}` classify
    significance through this enum, so the two cannot drift apart.

    Values are spaced rather than consecutive so a future tier -- the node changed
    but earns no new revision, which is what materialization config will need --
    slots between two existing tiers without renumbering. Only the ordering matters:
    `fold_change_tiers` takes a maximum and nothing reads the numbers.
    """

    NONE = 0
    MINOR = 10
    MAJOR = 20


def fold_change_tiers(tiers: Iterable[ChangeTier]) -> ChangeTier:
    """
    Reduce the tiers of several individual changes to the tier of the change as a
    whole: the most significant one wins, and no changes at all is NONE.
    """
    return max(tiers, default=ChangeTier.NONE)


def bump_version(version: str, tier: ChangeTier) -> str:
    """
    Apply a change tier to a version string. NONE leaves the version alone, so
    re-processing an unchanged node -- a forced re-deploy, or a re-deploy of a node
    stuck in INVALID -- does not invent a version for it.
    """
    parsed = Version.parse(version)
    if tier >= ChangeTier.MAJOR:
        return str(parsed.next_major_version())
    if tier >= ChangeTier.MINOR:
        return str(parsed.next_minor_version())
    return str(parsed)


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
    # Optional: the deployment falls back to a labelized version of the name.
    display_name: str | None = None
    description: str = ""
    tag_type: str = ""
    # The metadata bag on a tag. Node specs call their bag `custom_metadata`, so
    # both that and a bare `metadata:` are accepted as aliases to avoid forcing
    # users to rewrite existing YAML.
    tag_metadata: dict | None = Field(
        default=None,
        validation_alias=AliasChoices("tag_metadata", "metadata", "custom_metadata"),
    )

    model_config = ConfigDict(populate_by_name=True)


class HierarchyLevelSpec(BaseModel):
    """
    Specification for a single level within a hierarchy.
    """

    name: str
    dimension_node: str
    grain_columns: list[str] | None = None

    def rendered_dimension_node(self, namespace: str | None) -> str:
        """Return the dimension_node name with namespace prefix applied."""
        return (
            render_prefixes(self.dimension_node, namespace)
            if namespace
            else self.dimension_node
        )


class NamespacedSpec(BaseModel):
    """
    Base class for specs that carry a namespace and need a rendered_name.

    Both HierarchySpec and NodeSpec share identical name/namespace fields and
    rendered_name logic; this base class deduplts that to avoid drift.
    """

    name: str

    # Not user-supplied, gets injected by DeploymentSpec.set_namespaces
    namespace: str | None = Field(default=None, exclude=True)

    @property
    def rendered_name(self) -> str:
        if self.namespace:
            if "${prefix}" in self.name:
                return render_prefixes(self.name, self.namespace)
            prefix = f"{self.namespace}{SEPARATOR}"
            if self.name.startswith(prefix):
                return self.name  # already fully qualified
            return f"{prefix}{self.name}"
        return self.name


class HierarchySpec(NamespacedSpec):
    """
    Specification for a dimensional hierarchy.
    """

    display_name: str | None = None
    description: str | None = None
    levels: list[HierarchyLevelSpec] = Field(default_factory=list, min_length=2)


class PreAggSpec(NamespacedSpec):
    """
    Specification for an externally-built pre-aggregation table adopted at deploy
    time (equivalent to POST /preaggs/register). ``name`` is a stable handle used
    for reconciliation and availability callbacks. Metric/dimension references may
    use ``${prefix}`` or be fully qualified; they are rendered against the
    deployment namespace.

    Every metric and every dimension is declared together with the physical
    column of the external table that holds it, as a map::

        metrics:
          ${prefix}paid_members: paid_members_sum
        dimensions:
          ${prefix}country_dim.country_iso: country
          ${prefix}date_dim.utc_date: utc_date

    Both maps require a value for every key -- including a dimension whose
    physical column happens to match its DJ column name, which is written out
    rather than left empty. An optional value would make the map not really a
    mapping, a trailing colon is easy to write by accident, and spelling the
    physical name out documents the table in the file that declares it.

    What goes under ``metrics`` are the measures the table stores. A derived
    metric (a ratio of two others, say) is not listed and cannot be: it has no
    column of its own. It is covered anyway, because both registration and
    query-time matching work on decomposed measure identities rather than metric
    names, so any metric that decomposes into the stored measures resolves to
    this table.

    The earlier four-field form -- ``metrics``/``dimensions`` as lists alongside
    separate ``measure_columns``/``dimension_columns`` maps -- is no longer
    accepted, and a spec still using it is rejected with a message describing
    what to write instead.

    ``POST /preaggs/register`` takes the same declaration in JSON, and both
    surfaces enforce these rules from
    :mod:`datajunction_server.models.preagg_binding` so they cannot drift apart.
    """

    # Metric/dimension reference -> the physical column of the external table
    # holding it. The `rendered_*` properties below split these back into the
    # references-plus-bindings shape the registration internals take.
    metrics: dict[str, str] = Field(default_factory=dict)
    dimensions: dict[str, str] = Field(default_factory=dict)
    catalog: str
    schema_: str = Field(alias="schema")
    table: str
    valid_through_ts: int | None = None

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def require_column_bindings(cls, data: Any) -> Any:
        """
        Hold the author to the map form: every metric and dimension names the
        physical column that holds it, and the fields that used to carry those
        bindings separately are gone. Shared with ``POST /preaggs/register``,
        which enforces the identical rules against a JSON body.
        """
        return validate_column_bindings(data, SPEC_SURFACE)

    @property
    def rendered_metrics(self) -> dict[str, str]:
        """Each metric reference rendered against the namespace, still bound to
        its physical column."""
        return {
            render_prefixes(metric, self.namespace): column
            for metric, column in self.metrics.items()
        }

    @property
    def rendered_dimensions(self) -> dict[str, str]:
        """Each dimension reference rendered against the namespace, still bound to
        its physical column."""
        return {
            render_prefixes(dimension, self.namespace): column
            for dimension, column in self.dimensions.items()
        }


class PartitionSpec(BaseModel):
    """
    Represents a partition
    """

    type: PartitionType
    granularity: Granularity | None = None
    format: str | None = None


class MaterializationSpec(BaseModel):
    """
    Declarative materialization config for a cube.

    Deliberately carries only what the author decides: when to build, how, how far
    back to look, how much of history to serve, how long the result is kept, and the
    Druid, Spark and platform settings their own site needs. The backend that runs it
    (and everything it derives -- the measures queries, combiner SQL, the rest of the
    Druid spec, output tables) is DJ's choice, so no `job` field is exposed here and
    the block stays portable if that choice changes.
    """

    schedule: str
    strategy: MaterializationStrategy = MaterializationStrategy.INCREMENTAL_TIME
    lookback_window: str | None = "1 DAY"

    # Unlike `lookback_window`, this is meaningful under both strategies: a full
    # rebuild is exactly the case that loads the widest span of history.
    retention: str | None = DEFAULT_CUBE_RETENTION

    coverage: CoverageSpec | None = None

    # Deep-merged into the Druid ingestion spec DJ generates.
    druid: dict[str, Any] | None = None

    # Spark config for this materialization's stages.
    spark: SparkSpec | None = None

    # Carried to the query service verbatim. DJ reads no key out of it.
    platform: dict[str, Any] | None = None

    def rendered(self, namespace: str | None) -> "MaterializationSpec":
        """A copy with `${prefix}` resolved in the `spark.measures` keys."""
        if not self.spark or not self.spark.measures:
            return self
        return self.model_copy(
            update={
                "spark": self.spark.model_copy(
                    update={
                        "measures": {
                            render_prefixes(parent, namespace): conf
                            for parent, conf in self.spark.measures.items()
                        },
                    },
                ),
            },
        )

    @model_validator(mode="after")
    def clear_empty_coverage(self) -> "MaterializationSpec":
        """Treat an empty coverage block as absent."""
        if self.coverage == CoverageSpec():
            self.coverage = None
        return self

    @model_validator(mode="after")
    def clear_lookback_for_full(self) -> "MaterializationSpec":
        """
        `lookback_window` only means something for INCREMENTAL_TIME. Normalize it
        away for FULL so an ignored value can't make two equivalent specs compare
        unequal and churn a live workflow on deploy.
        """
        if self.strategy != MaterializationStrategy.INCREMENTAL_TIME:
            self.lookback_window = None
        return self


class MaterializationAction(str, Enum):
    """
    What a cube's `materialization:` block can say instead of naming a schedule.

    `NONE` asks for whatever the cube has materialized to be torn down. It is a
    value rather than an absent key on purpose: "not managed in YAML" and "should
    not be materialized" have to stay distinguishable after a round trip through
    `model_dump`, which emits every optional field explicitly and so cannot
    preserve which keys an author actually wrote.
    """

    NONE = "none"


def declared_materialization_blocks(
    materialization: MaterializationSpec
    | list[MaterializationSpec]
    | MaterializationAction
    | None,
) -> list[MaterializationSpec]:
    """
    A cube's `materialization:` value as the list of blocks it declares.

    The scalar and the list form mean the same thing to everything past parsing, and
    neither an absent block nor the teardown sentinel declares anything to build, so
    reducing all four shapes to a list is what keeps the rest of the deployment path
    from caring which one the manifest was written in. Which of the two empty cases
    is which is read off `materialization` directly.
    """
    if isinstance(materialization, MaterializationSpec):
        return [materialization]
    if isinstance(materialization, list):
        return list(materialization)
    return []


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
    unit: Unit | None = None
    order: int | None = Field(
        default=None,
        exclude=True,
    )  # Internal use only, not serialized

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ColumnSpec):
            return False
        return (
            self.name == other.name
            and self.type == other.type
            and (self.display_name == other.display_name or self.display_name is None)
            and self.description == other.description
            and set(self.attributes) == set(other.attributes)
            and self.partition == other.partition
            and self.unit == other.unit
        )


class DimensionLinkSpec(BaseModel):
    """
    Specification for a dimension link
    """

    type: LinkType
    role: str | None = None
    namespace: str | None = Field(default=None, exclude=True)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DimensionLinkSpec):
            return False  # pragma: no cover
        return self.type == other.type and self.role == other.role


class DimensionJoinLinkSpec(DimensionLinkSpec):
    """
    Specification for a dimension join link

    `join_on` is required for every join type except CROSS, and carries the whole
    join. A spec says nothing about which of this node's columns is the foreign
    key, so the clause cannot be derived the way `link_dimension` derives it from
    a column the caller names explicitly. `node_column` applies only to reference
    links and is rejected here.
    """

    dimension_node: str
    type: Literal[LinkType.JOIN] = LinkType.JOIN

    node_column: str | None = None
    join_type: JoinType = JoinType.LEFT
    join_cardinality: JoinCardinality = JoinCardinality.MANY_TO_ONE
    join_on: str | None = None
    default_value: str | None = None
    spark_hints: SparkJoinStrategy | None = None

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
                self.join_cardinality,
                self.rendered_join_on,
                self.node_column,
                self.default_value,
            ),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DimensionJoinLinkSpec):
            return False  # pragma: no cover
        return (
            super().__eq__(other)
            and self.rendered_dimension_node == other.rendered_dimension_node
            and self.join_type == other.join_type
            and self.join_cardinality == other.join_cardinality
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

    def __hash__(self) -> int:
        return hash(
            (
                self.type,
                self.role,
                self.rendered_dimension_node,
                self.dimension_attribute,
                self.node_column,
            ),
        )

    def __eq__(self, other: object) -> bool:
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


class NodeSpec(NamespacedSpec):
    """
    Specification of a node as declared in a deployment.
    The name is relative and will be hydrated with the deployment namespace.
    """

    node_type: NodeType
    owners: list[str] = Field(default_factory=list)
    display_name: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    mode: NodeMode = NodeMode.PUBLISHED
    custom_metadata: dict | None = None

    # How significant a change to each field is. Data rather than a chain of
    # conditionals so that adding a field forces someone to classify it: a test
    # walks `model_fields` on every subclass and fails on anything missing here.
    # Each class declares only the fields it introduces; lookup walks the MRO.
    # An unclassified field falls back to MAJOR, so the failure mode is an
    # over-eager version bump rather than a silently swallowed change.
    FIELD_CHANGE_TIERS: ClassVar[dict[str, ChangeTier]] = {
        # Identity, not content: a different name is a different node. `diff()`
        # ignores both fields, so neither ever reaches the classifier.
        "name": ChangeTier.NONE,
        "namespace": ChangeTier.NONE,
        # Never changes in place; MAJOR so that if it somehow did, it could not
        # slip through as a minor bump.
        "node_type": ChangeTier.MAJOR,
        # Metadata only: leaves the query, the columns and the built output alone.
        "owners": ChangeTier.MINOR,
        "display_name": ChangeTier.MINOR,
        "description": ChangeTier.MINOR,
        "tags": ChangeTier.MINOR,
        "mode": ChangeTier.MINOR,
        "custom_metadata": ChangeTier.MINOR,
    }

    # How significant it is to reorder a list field without adding or removing
    # anything. Fields absent here are not order-sensitive, so reordering them is
    # not a change at all. `diff()` compares list fields as sets and cannot see a
    # reorder on its own, which is why `order_diff()` exists alongside it.
    FIELD_ORDER_CHANGE_TIERS: ClassVar[dict[str, ChangeTier]] = {}

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
            prefix = f"{self.namespace}{SEPARATOR}"
            if self.name.startswith(prefix):
                return self.name  # already fully qualified
            return f"{prefix}{self.name}"
        return self.name

    @property
    def rendered_query(self) -> str | None:
        if hasattr(self, "query") and self.query:
            query = self.query
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

    def __eq__(self, other: object) -> bool:
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

    @classmethod
    def _declared_tier(
        cls,
        mapping_name: str,
        field: str,
    ) -> ChangeTier | None:
        """
        Look a field up in one of the tier mappings, walking the MRO so that each
        spec class only has to declare the fields it introduces. Returns None
        when no class in the MRO classifies the field.
        """
        for klass in cls.__mro__:
            mapping = klass.__dict__.get(mapping_name)
            if mapping and field in mapping:
                return mapping[field]
        return None

    @classmethod
    def field_change_tier(cls, field: str) -> ChangeTier:
        """
        Tier of a change to `field`'s value. Unclassified fields are MAJOR so an
        unclassified field can never be treated as trivial.
        """
        declared = cls._declared_tier("FIELD_CHANGE_TIERS", field)
        return ChangeTier.MAJOR if declared is None else declared

    @classmethod
    def field_order_change_tier(cls, field: str) -> ChangeTier:
        """
        Tier of a reorder of `field` that leaves its contents intact. Fields that
        no class classifies carry no meaning in their ordering.
        """
        declared = cls._declared_tier("FIELD_ORDER_CHANGE_TIERS", field)
        return ChangeTier.NONE if declared is None else declared

    @classmethod
    def has_explicit_change_tier(cls, field: str) -> bool:
        """Whether some class in the MRO classifies `field`."""
        return cls._declared_tier("FIELD_CHANGE_TIERS", field) is not None

    @classmethod
    def unclassified_fields(cls) -> list[str]:
        """Fields on this spec class that nobody classified. Should always be empty."""
        return [
            field
            for field in cls.model_fields
            if not cls.has_explicit_change_tier(field)
        ]

    @classmethod
    def order_sensitive_fields(cls) -> list[str]:
        """Fields for which some class in the MRO classifies a reorder."""
        fields: dict[str, None] = {}
        for klass in cls.__mro__:
            for field in klass.__dict__.get("FIELD_ORDER_CHANGE_TIERS", {}):
                fields.setdefault(field, None)
        return list(fields)

    @classmethod
    def change_tier(
        cls,
        changed_fields: Iterable[str],
        reordered_fields: Iterable[str] = (),
    ) -> ChangeTier:
        """
        Classify how significant a set of changes is. `changed_fields` are fields
        whose value differs; `reordered_fields` are fields whose contents are the
        same but whose ordering differs.
        """
        return fold_change_tiers(
            [cls.field_change_tier(field) for field in changed_fields]
            + [cls.field_order_change_tier(field) for field in reordered_fields],
        )

    def order_diff(self, other: "NodeSpec") -> list[str]:
        """
        Return the order-sensitive fields whose ordering differs between this and
        another NodeSpec while their contents stay the same.

        `diff()` compares list fields as sets, so a pure reorder is invisible to
        it. Renders `${prefix}` placeholders in `other` first, exactly as `diff()`
        does, so unresolved prefixes don't produce false positives.
        """
        rendered = other.rendered_spec()
        reordered = []
        for field in self.order_sensitive_fields():
            mine = list(getattr(self, field, None) or [])
            theirs = list(getattr(rendered, field, None) or [])
            if mine != theirs and set(mine) == set(theirs):
                reordered.append(field)
        return reordered


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

    # All three change the node's shape or how it joins, so all three are major.
    FIELD_CHANGE_TIERS: ClassVar[dict[str, ChangeTier]] = {
        "columns": ChangeTier.MAJOR,
        "dimension_links": ChangeTier.MAJOR,
        "primary_key": ChangeTier.MAJOR,
    }

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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LinkableNodeSpec):
            return False  # pragma: no cover
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

    # The source node points at a different physical table.
    FIELD_CHANGE_TIERS: ClassVar[dict[str, ChangeTier]] = {
        "catalog": ChangeTier.MAJOR,
        "schema_": ChangeTier.MAJOR,
        "table": ChangeTier.MAJOR,
    }

    model_config = ConfigDict(populate_by_name=True)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SourceSpec):
            return False  # pragma: no cover
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

    FIELD_CHANGE_TIERS: ClassVar[dict[str, ChangeTier]] = {
        "query": ChangeTier.MAJOR,
    }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TransformSpec):
            return False
        return super().__eq__(other) and self.query_ast.compare(other.query_ast)


class DimensionSpec(LinkableNodeSpec):
    """
    Specification for a dimension node
    """

    node_type: Literal[NodeType.DIMENSION] = NodeType.DIMENSION
    query: str

    FIELD_CHANGE_TIERS: ClassVar[dict[str, ChangeTier]] = {
        "query": ChangeTier.MAJOR,
    }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DimensionSpec):
            return False  # pragma: no cover
        return super().__eq__(other) and self.query_ast.compare(other.query_ast)


class MetricSpec(NodeSpec):
    """
    Specification for a metric node.

    The `unit` input field accepts either of two shapes:
      - **Legacy flat string** (`unit: dollar`) — translated via the
        `MetricUnit` enum. Bounded to legacy values.
      - **Structured dict** (`unit: {kind: currency, code: USD}` or
        `unit: {numerator: ..., denominator: ...}`) — the same shape as
        `ColumnSpec.unit`, but authored at the metric level so users don't
        need to know the metric's output column name.

    Internally these are stored separately (`unit_enum`, `unit_structured`)
    and reconciled at deploy time onto the metric's single output column.
    """

    node_type: Literal[NodeType.METRIC] = NodeType.METRIC
    query: str
    # Internal only - used for validation skip optimization when copying from valid nodes.
    # Excluded from serialization so it's never exported.
    columns: list[ColumnSpec] | None = Field(default=None, exclude=True)
    required_dimensions: list[str] | None = None  # Field(default_factory=list)
    direction: MetricDirection | None = None
    unit_enum: MetricUnit | None = Field(default=None, exclude=True)
    # Structured unit form at the metric level — peer of `unit_enum`.
    # Only one of `unit_enum` / `unit_structured` is set per spec (the
    # __init__ dispatches by input shape).
    unit_structured: Unit | None = Field(default=None, exclude=True)

    significant_digits: int | None = None
    min_decimal_exponent: int | None = None
    max_decimal_exponent: int | None = None

    FIELD_CHANGE_TIERS: ClassVar[dict[str, ChangeTier]] = {
        "query": ChangeTier.MAJOR,
        "columns": ChangeTier.MAJOR,
        # Required dimensions constrain which queries the metric can answer.
        "required_dimensions": ChangeTier.MAJOR,
        # Everything below is presentation metadata on the metric's single output
        # column -- the same set the PATCH path already treats as minor via
        # `metric_metadata` in `create_new_revision_from_existing`.
        "direction": ChangeTier.MINOR,
        "unit_enum": ChangeTier.MINOR,
        "unit_structured": ChangeTier.MINOR,
        "significant_digits": ChangeTier.MINOR,
        "min_decimal_exponent": ChangeTier.MINOR,
        "max_decimal_exponent": ChangeTier.MINOR,
    }

    # Class-level adapter used by __init__ to eagerly validate structured
    # unit input. `ClassVar` keeps Pydantic from treating it as a field.
    _unit_adapter: ClassVar[TypeAdapter] = TypeAdapter(Unit)

    def __init__(self, **data: Any):
        unit = data.pop("unit", None)
        # Empty string and empty dict both mean "no unit authored" — match
        # the historical `if unit:` permissive gate so existing YAML files
        # with `unit: ""` or `unit: {}` (e.g. template residue) keep parsing.
        if unit is None or unit == "" or unit == {}:
            pass
        elif isinstance(unit, MetricUnit):
            data["unit_enum"] = unit
        elif isinstance(unit, str):
            try:
                data["unit_enum"] = MetricUnit[unit.strip().upper()]
            except KeyError:
                raise DJInvalidInputException(f"Invalid metric unit: {unit}")
        elif isinstance(unit, dict):
            # Validate eagerly so users get a clean DJInvalidInputException
            # (with the offending dict echoed) instead of a noisy Pydantic
            # ValidationError that leaks internal field names like
            # `unit_structured.atomic.kind`.
            try:
                MetricSpec._unit_adapter.validate_python(unit)
            except Exception as exc:
                raise DJInvalidInputException(
                    f"Invalid metric unit {unit!r}: {exc}",
                ) from exc
            data["unit_structured"] = unit
        else:
            raise DJInvalidInputException(
                f"Metric unit must be a string or a structured dict; "
                f"got {type(unit).__name__}",
            )
        super().__init__(**data)

    @property
    def unit(self) -> str | dict | None:
        """
        Return the canonical metric unit value for serialization.

        Returns:
          - `None` if no unit is set.
          - A structured dict if the metric was authored with a structured
            unit at the spec level.
          - The legacy lowercase enum name (e.g. `"dollar"`) otherwise.

        Output consumers that need a structured value regardless of input
        shape should read `column.unit` on the metric's output column.
        """
        if self.unit_structured is not None:
            # Canonical dict shape (JSON-friendly, no None values).
            return unit_to_dict(self.unit_structured)
        if self.unit_enum is None or self.unit_enum == MetricUnit.UNKNOWN:
            return None
        return self.unit_enum.value.name.lower()

    @property
    def rendered_required_dimensions(self) -> list[str]:
        """
        Required dimensions with `${prefix}` resolved to this spec's namespace.

        Full `node.column` paths that were parameterized on export (in-deploy
        nodes) map back to the target namespace here; bare column names and raw
        external paths contain no `${prefix}` and pass through unchanged.
        """
        return [
            render_prefixes(required_dim, self.namespace)
            if "${prefix}" in required_dim
            else required_dim
            for required_dim in (self.required_dimensions or [])
        ]

    def model_dump(self, **kwargs):  # pragma: no cover
        base = super().model_dump(**kwargs)
        base["unit"] = self.unit
        return base

    def _canonical_unit(self) -> "Unit | None":
        """
        Reduce both legacy and structured inputs to the same canonical Unit
        instance for equality comparisons. Returns None when the metric has
        no unit (or only the UNKNOWN sentinel). Two specs that author the
        same conceptual unit via different input shapes (`unit: dollar` vs
        `unit: {kind: currency, code: USD}`) produce equal frozen Unit
        instances — so __eq__ doesn't falsely report drift between YAML and
        DB-roundtripped specs.
        """
        if self.unit_structured is not None:
            return self.unit_structured
        if self.unit_enum is None or self.unit_enum == MetricUnit.UNKNOWN:
            return None
        return legacy_unit_to_structured(self.unit_enum)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MetricSpec):
            return False
        return (
            super().__eq__(other)
            and self.query_ast.compare(other.query_ast)
            and (self.required_dimensions or []) == (other.required_dimensions or [])
            and eq_or_fallback(self.direction, other.direction, MetricDirection.NEUTRAL)
            and self._canonical_unit() == other._canonical_unit()
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
    # Tri-state. A spec materializes the cube on the schedule it names; the `none`
    # sentinel tears down whatever the cube has materialized; absent -- and null,
    # which is what serializing an absent field produces -- means the cube's
    # materialization is not managed here and whatever exists is left alone. Only a
    # value can carry intent through serialization, so removal is spelled
    # `materialization: none` rather than inferred from a key being present.
    #
    # A list declares more than one, which a cube legitimately needs: an
    # `incremental_time` build for freshness alongside a periodic `full` rebuild
    # that corrects late-arriving data, out-of-order events and dimension
    # backfills. `strategy` is what tells two entries apart -- `job` is not
    # authorable (see `MaterializationSpec`) and everything else is a knob rather
    # than an identity -- so two entries sharing one are rejected below. The scalar
    # form stays valid and means exactly what it always did; nothing has to be
    # rewritten as a one-element list.
    materialization: (
        MaterializationSpec | list[MaterializationSpec] | MaterializationAction | None
    ) = None

    FIELD_CHANGE_TIERS: ClassVar[dict[str, ChangeTier]] = {
        # Adding or removing a metric or a dimension changes the cube's columns.
        "metrics": ChangeTier.MAJOR,
        "dimensions": ChangeTier.MAJOR,
        # Filters decide which rows the cube contains, so a filter change
        # invalidates any table materialized from the previous revision --
        # `is_non_trivial_cube_change` already treats it that way, and it would be
        # absurd for that to coexist with a mere minor version bump.
        "filters": ChangeTier.MAJOR,
        # Only user-authored partition config is compared here (see __eq__);
        # everything else about cube columns is auto-derived.
        "columns": ChangeTier.MAJOR,
        # Materialization is not part of a cube's definition. Configuring one
        # through `POST /nodes/{name}/materialization/` has never created a node
        # revision, and a YAML-declared schedule has to behave the same way, or
        # the same edit would cut a version through one door and not the other.
        # Because `materialization` is also excluded from `__eq__`, a
        # materialization-only edit leaves the cube in the deployment's skip list
        # and never reaches the classifier at all; NONE records that intent rather
        # than describing a reachable code path. Nothing is lost by the exclusion:
        # reconciling the declared block against the persisted config is a separate
        # concern that runs over every declared cube whether or not the node
        # changed, which is also what catches a materialization changed outside
        # YAML.
        "materialization": ChangeTier.NONE,
    }

    FIELD_ORDER_CHANGE_TIERS: ClassVar[dict[str, ChangeTier]] = {
        # Metric and dimension ordering determines the cube's column order via
        # `NodeRevision.cube_dimensions()`, which is the shape of the result sets
        # users see. A reorder is therefore a real change worth recording, just
        # not one that moves any value -- so it earns a minor version.
        "metrics": ChangeTier.MINOR,
        "dimensions": ChangeTier.MINOR,
        # Filters are ANDed together, so their ordering carries no meaning at all
        # and reordering them is genuinely a no-op.
        "filters": ChangeTier.NONE,
    }

    @field_validator("materialization", mode="before")
    @classmethod
    def normalize_materialization_sentinel(cls, value: Any) -> Any:
        """
        Case-fold the sentinel, since `None` is what a Python author reaches for and
        `NONE` what a YAML one does, and both mean `none`.
        """
        return value.lower() if isinstance(value, str) else value

    @model_validator(mode="after")
    def validate_materialization(self) -> "CubeSpec":
        """
        An INCREMENTAL_TIME materialization has nothing to increment over without a
        temporal partition, so require one up front rather than failing later in the
        build. The partition is read from the cube's own columns (see ColumnSpec.
        partition) -- it is not restated on the materialization block, so that a cube
        carrying the same dimension in two roles can say which role partitions it.

        A list is checked entry by entry, and additionally for the two shapes that
        have no meaning: an empty one, which cannot be told from the teardown
        sentinel, and repeated strategies, which leave no way to say which declared
        entry a given materialization answers to.
        """
        if isinstance(self.materialization, list):
            if not self.materialization:
                raise DJInvalidDeploymentConfig(
                    message=(
                        f"Cube `{self.name}` declares an empty `materialization:` "
                        "list. Use `materialization: none` to remove the cube's "
                        "materialization, or omit the key to leave it alone."
                    ),
                )
            seen: set[MaterializationStrategy] = set()
            for block in self.materialization:
                if block.strategy in seen:
                    raise DJInvalidDeploymentConfig(
                        message=(
                            f"Cube `{self.name}` declares more than one "
                            f"`{block.strategy.value}` materialization. Strategies "
                            "identify a cube's materializations, so each one may "
                            "appear at most once."
                        ),
                    )
                seen.add(block.strategy)

        if any(
            block.strategy == MaterializationStrategy.INCREMENTAL_TIME
            for block in self.declared_materializations
        ) and not any(
            col.partition and col.partition.type == PartitionType.TEMPORAL
            for col in self.columns or []
        ):
            raise DJInvalidDeploymentConfig(
                message=(
                    f"Cube `{self.name}` declares an `incremental_time` "
                    "materialization but no temporal partition. Add `partition: "
                    "{type: temporal, ...}` to the cube column that partitions it, "
                    "or use `strategy: full`."
                ),
            )
        return self

    @property
    def declared_materializations(self) -> list[MaterializationSpec]:
        """The materialization blocks this cube declares, scalar and list alike."""
        return declared_materialization_blocks(self.materialization)

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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CubeSpec):
            return False
        if not super().__eq__(other):
            return False
        # Metrics and dimensions are compared in order: their ordering sets the
        # cube's column order, so a reorder is a change the deployment path has to
        # be able to see (compared as sets, a YAML reorder was silently ignored).
        # Filters are compared as sets: they are ANDed, so reordering them is
        # cosmetic and must not trigger a redeploy.
        if not (
            self.rendered_metrics == other.rendered_metrics
            and self.rendered_dimensions == other.rendered_dimensions
            and set(self.rendered_filters or []) == set(other.rendered_filters or [])
        ):
            return False
        # `materialization` is deliberately absent from this comparison. Equality
        # here decides whether the cube's definition changed, and any inequality
        # routes the cube to the update path, which always writes a new revision and
        # so mints at least a minor version. Rescheduling a build is not a definition
        # change and must not mint one (nor trip the workflow stop that a material
        # cube change triggers); see `materialization` in FIELD_CHANGE_TIERS. It is
        # reconciled separately, against the persisted config rather than this spec.

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
    SourceSpec | TransformSpec | DimensionSpec | MetricSpec | CubeSpec,
    Field(discriminator="node_type"),
]


def _norm(v: Any) -> Any:
    """Normalize falsy string/None to None so that '' and None compare equal."""
    if isinstance(v, str):
        return v or None
    return v


def _as_comparable_set(value: Any) -> set:
    """
    Reduce a list field to a set of hashable, comparable items.

    A model is reduced to its canonical JSON rather than to a tuple of its fields,
    because a field of its own can hold a nested model or a dict -- a
    materialization's `coverage`, a column's `partition` -- and a tuple carrying one
    of those cannot go into a set at all.
    """
    return {
        json.dumps(item.model_dump(mode="json"), sort_keys=True)
        if isinstance(item, BaseModel)
        else item
        for item in value or []
    }


def _values_differ(left: Any, right: Any) -> bool:
    """
    Whether two field values differ.

    Dicts are compared by value: iterating a dict yields only its keys, so the
    set comparison lists use would miss a changed value (`custom_metadata` going
    from `{"tier": "1"}` to `{"tier": "2"}` looked identical). Lists are compared
    as sets, so reordering one is not a difference -- see `NodeSpec.order_diff`
    for the fields where ordering does carry meaning.
    """
    if isinstance(left or right, dict):
        return (left or {}) != (right or {})
    if isinstance(left or right, list):
        return _as_comparable_set(left) != _as_comparable_set(right)
    return _norm(left) != _norm(right)


def diff(
    one: BaseModel,
    two: BaseModel,
    ignore_fields: list[str] | None = None,
) -> list[str]:
    """
    Compare two Pydantic models and return a list of fields that have changed.
    """
    return [
        field
        for field in one.model_fields.keys()
        if field not in (ignore_fields or [])
        and hasattr(one, field)
        and hasattr(two, field)
        and _values_differ(getattr(one, field), getattr(two, field))
    ]


class CustomMetadataSchemaSpec(BaseModel):
    """
    Specification for a custom_metadata JSON Schema to register for a namespace.

    The namespace defaults to the enclosing DeploymentSpec's. Naming one narrows the
    schema to a sub-namespace, which is how a vocabulary can be rolled out to part of
    a repo's graph -- conformed dimensions first, say -- before it governs all of it.
    A namespace outside the deploying one is rejected: a manifest may scope narrower
    than itself, never wider.
    """

    key: str
    node_type: NodeType | None = None
    namespace: str | None = None
    json_schema: dict
    filterable: bool = True
    description: str | None = None


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
    # Name of the branch namespace this namespace lives under (equals the
    # namespace itself when it IS the branch). Lets the UI show branch
    # controls (Git Settings / Sync to Git / Create PR) on subnamespaces
    # too, scoped to the branch.
    branch_namespace: str | None = None
    # The namespace that owns the repo path. Lets clients tell apart
    # the git root from descendants that inherit it.
    git_root_namespace: str | None = None


class DeploymentSpec(BaseModel):
    """
    Specification of a full deployment (namespace, nodes, tags, and add'l metadata).
    Typically hydrated from a project manifest (YAML/JSON/etc).
    """

    namespace: str
    nodes: list[NodeUnion] = Field(default_factory=list)
    tags: list[TagSpec] = Field(default_factory=list)
    hierarchies: list[HierarchySpec] = Field(default_factory=list)
    preaggregations: list[PreAggSpec] = Field(default_factory=list)
    # None and [] mean different things: None is "this manifest does not manage
    # schemas", [] is "it manages them and declares none", which retires the
    # namespace's rows. A list default would make every deployment that omits
    # the section look like the latter.
    custom_metadata_schemas: list[CustomMetadataSchemaSpec] | None = None
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
    allow_empty: bool = Field(
        default=False,
        description=(
            "If True, permit a deployment whose spec contains zero nodes to "
            "soft-delete the existing nodes in the target namespace. Guards "
            "against an accidental empty push (e.g. a mistyped `directory` "
            "argument that resolves to no node files) silently wiping a "
            "namespace."
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
        for hierarchy in self.hierarchies:
            if not hierarchy.namespace:
                hierarchy.namespace = self.namespace
        for preagg in self.preaggregations:
            if not preagg.namespace:
                preagg.namespace = self.namespace
        for schema in self.custom_metadata_schemas or []:
            if not schema.namespace:
                schema.namespace = self.namespace
            elif schema.namespace != self.namespace and not schema.namespace.startswith(
                f"{self.namespace}.",
            ):
                # Narrower than the deploying namespace is a rollout choice;
                # wider, or sideways, would let one repo govern another's nodes.
                raise DJInvalidDeploymentConfig(
                    message=(
                        f"custom_metadata schema '{schema.key}' declares namespace "
                        f"'{schema.namespace}', which is not '{self.namespace}' or "
                        "beneath it. A deployment may scope a schema to its own "
                        "namespace or a sub-namespace, never to another."
                    ),
                )
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
        WARNING = "warning"  # deployed successfully, but something needs attention

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
        NAMESPACE = "namespace"
        PREAGG = "preaggregation"
        MATERIALIZATION = "materialization"
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
    warnings: list[DJError] = Field(default_factory=list)
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
