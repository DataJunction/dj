"""
Project a deployment into the bindings the check engine evaluates, one per
entity, from the node spec and the plan's `existing_specs` and `node_graph`.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, get_args, get_origin

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only, noload

from datajunction_server.database.tag import Tag

from datajunction_server.enum import StrEnum
from datajunction_server.internal.checks.context import (
    Bindings,
    Change,
    DeclaredProperties,
    LinkProjection,
    NodeProjection,
    TagProjection,
    TagTypes,
    custom_metadata,
)
from datajunction_server.internal.checks.engine import CheckResult
from datajunction_server.internal.checks.manifest import ResolvedRuleset
from datajunction_server.internal.checks.validator import MalformedCheck
from datajunction_server.internal.custom_metadata import resolve_schemas
from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentResult,
    DimensionJoinLinkSpec,
    DimensionLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    NodeSpec,
    TagSpec,
    TransformSpec,
)
from datajunction_server.models.node_type import NodeType


def _is_sequence_field(annotation: Any) -> bool:
    """True when a field holds a list, including behind an Optional or a union."""
    if get_origin(annotation) is list:
        return True
    return any(get_origin(arg) is list for arg in get_args(annotation))


def _spec_defaults(base: type[BaseModel], skip: frozenset[str]) -> dict[str, Any]:
    """
    Set default values for CEL reads.
    """
    defaults: dict[str, Any] = {}
    pending = [base]
    while pending:
        cls = pending.pop()
        for name, info in cls.model_fields.items():
            if name in skip:
                continue
            empty: Any = [] if _is_sequence_field(info.annotation) else ""
            # A list default wins: one subclass typing it as a list is enough.
            if defaults.get(name) != []:
                defaults[name] = empty
        pending.extend(cls.__subclasses__())
    return defaults


# custom_metadata and tags are projected per node instead.
_NODE_DEFAULTS = _spec_defaults(NodeSpec, frozenset({"custom_metadata", "tags"}))
_LINK_DEFAULTS = _spec_defaults(DimensionLinkSpec, frozenset())

_UNCHANGED_OPERATIONS = frozenset(
    {DeploymentResult.Operation.NOOP, DeploymentResult.Operation.DELETE},
)


def _cel_safe(value: Any, empty: Any = "") -> Any:
    """Coerce a dumped value into something CEL can read."""
    if value is None:
        return empty
    if isinstance(value, (StrEnum, NodeType)):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _cel_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_cel_safe(item) for item in value]
    return value


@dataclass
class DeclaredSchemas:
    """The custom_metadata vocabulary a deploy's checks are compiled against."""

    properties: dict[str, tuple[str, ...]] = field(default_factory=dict)
    placeholders: dict[str, dict[str, Any]] = field(default_factory=dict)


async def resolve_declared_schemas(
    session: AsyncSession,
    namespace: str | None,
    node_types: Iterable[NodeType],
) -> DeclaredSchemas:
    """
    Union the declared property names per metadata key across the deploy's node
    types.
    """
    declared = DeclaredSchemas()
    for node_type in sorted(set(node_types)):
        schemas = await resolve_schemas(session, namespace, node_type)
        for key, json_schema in schemas.items():
            properties = (json_schema or {}).get("properties") or {}
            placeholders = declared.placeholders.setdefault(key, {})
            for name, subschema in properties.items():
                placeholders.setdefault(name, _placeholder(subschema))
            declared.properties[key] = tuple(sorted(placeholders))
    return declared


async def resolve_tag_types(
    session: AsyncSession,
    tag_specs: Iterable[TagSpec],
    used: Iterable[str],
) -> dict[str, str]:
    """
    Map each tag name the deploy's nodes use to its type.
    """
    declared = {spec.name: str(spec.tag_type or "") for spec in tag_specs}
    names = set(used) | set(declared)
    if not names:
        return {}
    stored = await Tag.find_tags(
        session,
        tag_names=sorted(names),
        options=[
            load_only(Tag.id, Tag.name, Tag.tag_type),
            noload(Tag.created_by),
            noload(Tag.nodes),
        ],
    )
    return {**{tag.name: str(tag.tag_type or "") for tag in stored}, **declared}


def _placeholder(subschema: Any) -> Any:
    """A representative value for a declared property."""
    kind = str(subschema.get("type")) if isinstance(subschema, Mapping) else ""
    return {
        "integer": 1,
        "number": 1,
        "boolean": True,
        "array": [],
        "object": {},
    }.get(kind, "placeholder")


def project_link(link: DimensionLinkSpec) -> LinkProjection:
    """One dimension link that unions together fields across join links and
    reference links."""
    dumped = link.model_dump()
    projected = {
        name: _cel_safe(dumped.get(name), empty)
        for name, empty in _LINK_DEFAULTS.items()
    }
    # A reference link's `dimension` is an attribute, `<node>.<column>`, so the
    # target node is read through `dimension_node` on both kinds.
    projected["dimension_node"] = link.rendered_dimension_node
    return projected


def project_tags(names: Sequence[str], tag_types: TagTypes) -> list[TagProjection]:
    """A node's tags, each with the type the deploy resolved for it."""
    return [
        TagProjection(name=name, tag_type=tag_types.get(name, "")) for name in names
    ]


def project_node(
    spec: NodeSpec | None,
    declared: DeclaredProperties,
    tag_types: TagTypes,
) -> NodeProjection:
    """
    Project one node spec into the `node` binding.
    """
    dumped = spec.model_dump() if spec is not None else {}
    projected = {
        name: _cel_safe(dumped.get(name), empty)
        for name, empty in _NODE_DEFAULTS.items()
    }
    # Both are excluded from the dump, so they are read off the spec.
    projected["name"] = spec.rendered_name if spec is not None else ""
    projected["namespace"] = (spec.namespace or "") if spec is not None else ""
    projected["custom_metadata"] = custom_metadata(
        dumped.get("custom_metadata"),
        declared,
    )
    projected["tags"] = project_tags(dumped.get("tags") or [], tag_types)
    projected["dimension_links"] = [
        project_link(link) for link in getattr(spec, "dimension_links", None) or []
    ]
    return projected


def project_dependency(
    name: str,
    spec: NodeSpec | None,
    declared: DeclaredProperties,
    tag_types: TagTypes,
) -> NodeProjection:
    """
    One upstream, in the same shape as `node`. An upstream the deployment does
    not contain has no spec, so only its name is filled in.
    """
    return dict(project_node(spec, declared, tag_types), name=name)


def build_bindings(
    spec: NodeSpec | None,
    *,
    previous: NodeSpec | None,
    dependencies: Sequence[tuple[str, NodeSpec | None]],
    operation: DeploymentResult.Operation,
    declared: DeclaredProperties,
    tag_types: TagTypes,
) -> Bindings:
    node = project_node(spec, declared, tag_types)
    # An unchanged or removed node is its own previous state, so project once.
    projected_previous = (
        node
        if operation in _UNCHANGED_OPERATIONS
        else project_node(previous, declared, tag_types)
    )
    return Bindings(
        node=node,
        previous=projected_previous,
        dependencies=[
            project_dependency(name, dependency, declared, tag_types)
            for name, dependency in dependencies
        ],
        change=Change(kind=str(operation.value)),
    )


def build_fixtures(declared: DeclaredSchemas) -> list[Bindings]:
    """
    Two synthetic nodes for `load_checks` to evaluate every clause against.
    """
    populated_metadata = {
        key: dict(placeholders)
        for key, placeholders in declared.placeholders.items()
        if placeholders
    }
    bare = TransformSpec(
        name="bare",
        namespace="fixture",
        query="SELECT 1",
    )
    populated = DimensionSpec(
        name="populated",
        namespace="fixture",
        query="SELECT 1",
        display_name="Populated",
        description="A fully populated fixture.",
        owners=["fixture_owner"],
        tags=["fixture_tag"],
        columns=[
            ColumnSpec(
                name="first",
                type="string",
                display_name="First",
                description="The first column.",
                attributes=["primary_key"],
            ),
        ],
        primary_key=["first"],
        # One of each kind, so a clause reading join fields is exercised
        # against a reference link too.
        dimension_links=[
            DimensionReferenceLinkSpec(
                node_column="first",
                dimension="fixture.other.attribute",
            ),
            DimensionJoinLinkSpec(
                dimension_node="fixture.other",
                node_column="first",
            ),
        ],
        custom_metadata=populated_metadata,
    )
    tag_types = {"fixture_tag": "fixture_tag_type"}
    return [
        build_bindings(
            bare,
            previous=None,
            dependencies=[],
            operation=DeploymentResult.Operation.CREATE,
            declared=declared.properties,
            tag_types=tag_types,
        ),
        build_bindings(
            populated,
            previous=populated,
            dependencies=[("fixture.parent", bare)],
            operation=DeploymentResult.Operation.DELETE,
            declared=declared.properties,
            tag_types=tag_types,
        ),
    ]


class RulesetVerdict(StrEnum):
    """One ruleset's roll-up over one entity."""

    PASSED = "passed"
    FAILED = "failed"
    # Every member was skipped, so the bundle asserted nothing. Not the same
    # as vacuously passing.
    NOT_APPLICABLE = "not_applicable"


@dataclass
class RulesetOutcome:
    """A ruleset's verdict for one entity, and the member checks behind it."""

    ruleset: str
    verdict: RulesetVerdict
    ran: tuple[str, ...]
    failed: tuple[str, ...]


def roll_up(
    rulesets: Mapping[str, ResolvedRuleset],
    results: Sequence[CheckResult],
) -> list[RulesetOutcome]:
    """
    Aggregate one entity's check results into a verdict per ruleset. Membership
    is the flat set the manifest resolved, so `includes` is already expanded.
    """
    outcomes = []
    for name, ruleset in rulesets.items():
        ran = tuple(
            result.check
            for result in results
            if result.check in ruleset.checks and not result.skipped
        )
        failed = tuple(
            result.check
            for result in results
            if result.check in ran and not result.passed
        )
        if not ran:
            verdict = RulesetVerdict.NOT_APPLICABLE
        else:
            verdict = RulesetVerdict.FAILED if failed else RulesetVerdict.PASSED
        outcomes.append(RulesetOutcome(name, verdict, ran, failed))
    return outcomes


def unsupported_ruleset_guards(
    rulesets: Mapping[str, ResolvedRuleset],
) -> list[MalformedCheck]:
    """Refuse a ruleset `when`, since nothing applies that scoping yet."""
    return [
        MalformedCheck(
            check=f"ruleset {name}",
            clause="when",
            problem="ruleset `when` guards are not yet supported",
        )
        for name, ruleset in sorted(rulesets.items())
        if ruleset.when is not None
    ]
