"""
Project a deployment into the activations the check engine evaluates.

The engine takes one activation per entity; a deploy already carries the shapes
it needs — the node spec, the plan's `existing_specs` and `node_graph` — so the
work here is projecting them. The same projection builds the fixtures the checks
are compiled against, so a check that loads cannot then break on a real node.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.internal.checks.context import (
    DeclaredProperties,
    custom_metadata,
)
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

# Keys every node projection carries, so a check reading one against a node that
# has no such field gets an empty value rather than an evaluation error.
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
    # One representative value per declared property, typed from its schema, so
    # a numeric comparison survives the populated fixture.
    placeholders: dict[str, dict[str, Any]] = field(default_factory=dict)


async def resolve_declared_schemas(
    session: AsyncSession,
    namespace: str | None,
    node_types: Iterable[NodeType],
) -> DeclaredSchemas:
    """
    Union the declared property names per metadata key across the deploy's node
    types. Checks compile once for the whole deploy, so the vocabulary has to be
    one thing: a property declared only for metrics simply reads as null on a
    dimension, which is the right "not set" answer.
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
    """
    One bare node and one with every field populated, so both branches of a
    clause are exercised at load. Built through the projection a real deploy
    uses, so the two shapes cannot drift.
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


def unsupported_ruleset_guards(
    rulesets: Mapping[str, ResolvedRuleset],
) -> list[MalformedCheck]:
    """
    A ruleset `when` scopes the bundle to part of the graph, and nothing here
    applies that scoping yet. Refusing beats quietly enforcing the ruleset's
    checks everywhere, or nowhere.
    """
    return [
        MalformedCheck(
            check=f"ruleset {name}",
            clause="when",
            problem="ruleset `when` guards are not yet supported",
        )
        for name, ruleset in sorted(rulesets.items())
        if ruleset.when is not None
    ]
