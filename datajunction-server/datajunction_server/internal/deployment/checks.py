"""
Project a deployment into the activations the check engine evaluates, one per
entity, from the node spec and the plan's `existing_specs` and `node_graph`.

Fixtures are built through the same projection, so a check that loads cannot
then break on a real node.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, get_args, get_origin

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.enum import StrEnum
from datajunction_server.internal.checks.context import (
    Activation,
    DeclaredProperties,
    NodeProjection,
    custom_metadata,
)
from datajunction_server.internal.checks.engine import CheckResult
from datajunction_server.internal.checks.manifest import ResolvedRuleset
from datajunction_server.internal.checks.validator import MalformedCheck
from datajunction_server.internal.custom_metadata import resolve_schemas
from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentResult,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    NodeSpec,
    TransformSpec,
)
from datajunction_server.models.node_type import NodeType


def _is_sequence_field(annotation: Any) -> bool:
    """True when a field holds a list, including behind an Optional or a union."""
    if get_origin(annotation) is list:
        return True
    return any(get_origin(arg) is list for arg in get_args(annotation))


def _node_spec_defaults() -> dict[str, Any]:
    """
    Every field on any node spec, with the empty value CEL should read when this
    subclass does not have it.

    A list field has to default to a list: a metric has no `dimension_links`,
    and a check calling `.all()` on the empty string would fail where it should
    trivially pass.
    """
    defaults: dict[str, Any] = {}
    pending = [NodeSpec]
    while pending:
        cls = pending.pop()
        for name, info in cls.model_fields.items():
            if name == "custom_metadata":
                continue
            empty: Any = [] if _is_sequence_field(info.annotation) else ""
            # A list default wins: one subclass typing it as a list is enough.
            if defaults.get(name) != []:
                defaults[name] = empty
        pending.extend(cls.__subclasses__())
    return defaults


_NODE_DEFAULTS = _node_spec_defaults()

# A node the deploy leaves alone or removes is identical to its deployed state,
# so `previous` is the same projection.
_UNCHANGED_OPERATIONS = frozenset(
    {DeploymentResult.Operation.NOOP, DeploymentResult.Operation.DELETE},
)


def _cel_safe(value: Any, empty: Any = "") -> Any:
    """
    Coerce a dumped value into something CEL can read.

    None becomes the field's empty value rather than null, so a check reading an
    unset field does not have to guard every one.
    """
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
    # Typed from the schema, so a numeric comparison survives the fixture.
    placeholders: dict[str, dict[str, Any]] = field(default_factory=dict)


async def resolve_declared_schemas(
    session: AsyncSession,
    namespace: str | None,
    node_types: Iterable[NodeType],
) -> DeclaredSchemas:
    """
    Union the declared property names per metadata key across the deploy's node
    types. Checks compile once per deploy, so the vocabulary has to be one
    thing; a property declared only for metrics reads as null elsewhere.
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


def _placeholder(subschema: Any) -> Any:
    """A representative value for a declared property, typed from its schema."""
    kind = str(subschema.get("type")) if isinstance(subschema, Mapping) else ""
    return {
        "integer": 1,
        "number": 1,
        "boolean": True,
        "array": [],
        "object": {},
    }.get(kind, "placeholder")


def project_node(spec: NodeSpec | None, declared: DeclaredProperties) -> NodeProjection:
    """
    Project one node spec into the `node` binding. None means no such node.

    Every field of every node spec is carried, defaulted when this subclass has
    none, so a check never reads a key that is missing -- CEL treats that as an
    error rather than an empty value.
    """
    dumped = spec.model_dump() if spec is not None else {}
    projected = {
        name: _cel_safe(dumped.get(name), empty)
        for name, empty in _NODE_DEFAULTS.items()
    }
    projected["name"] = spec.rendered_name if spec is not None else ""
    projected["custom_metadata"] = custom_metadata(
        dumped.get("custom_metadata"),
        declared,
    )
    return projected


def project_dependency(
    name: str,
    spec: NodeSpec | None,
    declared: DeclaredProperties,
) -> NodeProjection:
    """
    An upstream, projected like `node` so the same rule reads either.

    An upstream outside the deployment namespace has no spec, so only its name
    is known.
    """
    return dict(project_node(spec, declared), name=name)


def build_activation(
    spec: NodeSpec | None,
    *,
    previous: NodeSpec | None,
    dependencies: Sequence[tuple[str, NodeSpec | None]],
    operation: DeploymentResult.Operation,
    declared: DeclaredProperties,
) -> Activation:
    node = project_node(spec, declared)
    # An unchanged or removed node is its own previous state, so project once.
    projected_previous = (
        node if operation in _UNCHANGED_OPERATIONS else project_node(previous, declared)
    )
    return {
        "node": node,
        "dependencies": [
            project_dependency(name, dependency, declared)
            for name, dependency in dependencies
        ],
        "previous": projected_previous,
        "change": {"kind": str(operation.value)},
    }


def build_fixtures(declared: DeclaredSchemas) -> list[Activation]:
    """One empty node and one fully populated, so both branches of a clause are
    exercised at load."""
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
        dimension_links=[
            DimensionReferenceLinkSpec(
                node_column="first",
                dimension="fixture.other.attribute",
            ),
        ],
        custom_metadata=populated_metadata,
    )
    return [
        build_activation(
            bare,
            previous=None,
            dependencies=[],
            operation=DeploymentResult.Operation.CREATE,
            declared=declared.properties,
        ),
        build_activation(
            populated,
            previous=populated,
            dependencies=[("fixture.parent", bare)],
            operation=DeploymentResult.Operation.DELETE,
            declared=declared.properties,
        ),
    ]


class RulesetVerdict(StrEnum):
    """One ruleset's roll-up over one entity."""

    PASSED = "passed"
    FAILED = "failed"
    # Every member was skipped, so the bundle asserted nothing. Not the same
    # as vacuously passing.
    NOT_APPLICABLE = "not applicable"


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
