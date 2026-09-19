"""
Project a deployment into the activations the check engine evaluates, one per
entity, from the node spec and the plan's `existing_specs` and `node_graph`.

Fixtures are built through the same projection, so a check that loads cannot
then break on a real node.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.enum import StrEnum
from datajunction_server.internal.checks.context import (
    DeclaredProperties,
    custom_metadata,
)
from datajunction_server.internal.checks.engine import CheckResult
from datajunction_server.internal.checks.manifest import ResolvedRuleset
from datajunction_server.internal.checks.validator import Activation, MalformedCheck
from datajunction_server.internal.custom_metadata import resolve_schemas
from datajunction_server.models.deployment import (
    ColumnSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    NodeSpec,
    TransformSpec,
)
from datajunction_server.models.node_type import NodeType

# Every projection carries these, so reading one off a missing node gives an
# empty value rather than an evaluation error.
_ABSENT_NODE: dict[str, Any] = {
    "name": "",
    "type": "",
    "namespace": "",
    "display_name": "",
    "description": "",
    "mode": "",
    "owners": [],
    "tags": [],
    "columns": [],
    "primary_key": [],
    "dimension_links": [],
}


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


def project_node(spec: NodeSpec | None, declared: DeclaredProperties) -> dict[str, Any]:
    """Project one node spec into the `node` binding. None means no such node."""
    if spec is None:
        return dict(_ABSENT_NODE, custom_metadata=custom_metadata(None, declared))
    return {
        "name": spec.rendered_name,
        "type": str(spec.node_type),
        "namespace": spec.namespace or "",
        "display_name": spec.display_name or "",
        "description": spec.description or "",
        "mode": str(spec.mode),
        "owners": list(spec.owners),
        "tags": list(spec.tags),
        "columns": [
            {
                "name": column.name,
                "type": column.type or "",
                "display_name": column.display_name or "",
                "description": column.description or "",
                "attributes": list(column.attributes),
            }
            for column in getattr(spec, "columns", None) or []
        ],
        "primary_key": list(getattr(spec, "primary_key", None) or []),
        "dimension_links": [
            {
                "type": str(link.type),
                "role": link.role or "",
                "dimension_node": link.rendered_dimension_node,
            }
            for link in getattr(spec, "dimension_links", None) or []
        ],
        "custom_metadata": custom_metadata(spec.custom_metadata, declared),
    }


def project_previous(
    spec: NodeSpec | None,
    declared: DeclaredProperties,
) -> dict[str, Any]:
    """The deployed state, projected like `node` so a check reads either."""
    return dict(project_node(spec, declared), exists=spec is not None)


def project_dependency(
    name: str,
    spec: NodeSpec | None,
    declared: DeclaredProperties,
) -> dict[str, Any]:
    # An upstream outside the deployment namespace has no spec to project.
    return {
        "name": name,
        "type": str(spec.node_type) if spec is not None else "",
        "custom_metadata": custom_metadata(
            spec.custom_metadata if spec is not None else None,
            declared,
        ),
    }


def build_activation(
    spec: NodeSpec | None,
    *,
    previous: NodeSpec | None,
    dependencies: Sequence[tuple[str, NodeSpec | None]],
    change: Mapping[str, bool],
    declared: DeclaredProperties,
) -> Activation:
    return {
        "node": project_node(spec, declared),
        "dependencies": [
            project_dependency(name, dependency, declared)
            for name, dependency in dependencies
        ],
        "previous": project_previous(previous, declared),
        "change": dict(change),
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
            change={"is_new": True, "is_removal": False},
            declared=declared.properties,
        ),
        build_activation(
            populated,
            previous=populated,
            dependencies=[("fixture.parent", bare)],
            change={"is_new": False, "is_removal": True},
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
