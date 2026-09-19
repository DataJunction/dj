"""
The CEL environment and the values registered in it. A check referencing
anything not declared here fails to compile.
"""

from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

from cel_expr_python import cel

_STR_MAP = cel.Type.Map(cel.Type.STRING, cel.Type.DYN)

VARIABLES: dict[str, Any] = {
    "node": _STR_MAP,
    "dependencies": cel.Type.List(_STR_MAP),
    "previous": _STR_MAP,
    "change": cel.Type.Map(cel.Type.STRING, cel.Type.STRING),
}

# One node projected for CEL. Its keys are every field of every node spec, so
# they are not spelled out here.
NodeProjection = dict[str, Any]

# One dimension link, its keys unioned across the link specs.
LinkProjection = dict[str, Any]

# Tag name -> the type the deploy resolved for it.
TagTypes = Mapping[str, str]


class TagProjection(TypedDict):
    """One of a node's tags."""

    name: str
    tag_type: str


class Change(TypedDict):
    """What the deploy is doing to the node."""

    kind: str


class Activation(TypedDict):
    """
    What one node's check evaluates against.

        node          the node as the manifest declares it
        previous      the same node as currently deployed
        dependencies  its upstreams, each projected like `node`
        change.kind   create, update, delete or noop

    A delete or a noop produces no new version, so `previous` repeats `node`.
    Reading `change.kind` is the only way to tell those two apart.
    """

    node: NodeProjection
    previous: NodeProjection
    dependencies: list[NodeProjection]
    change: Change


# Read from the registered custom_metadata schemas.
DeclaredProperties = Mapping[str, Sequence[str]]


def build_env() -> cel.Env:
    return cel.NewEnv(variables=VARIABLES)


def custom_metadata(
    raw: Mapping[str, Any] | None,
    declared_properties: DeclaredProperties,
) -> dict[str, Any]:
    """
    Prefill declared custom_metadata properties to null. Prefilling makes the
    `!= null` check safe, since an absent key evaluates to an error rather than
    false.

    Unschematized keys pass through untouched and need has() guards.
    """
    normalized = dict(raw or {})
    for key, properties in declared_properties.items():
        block = normalized.get(key)
        if not isinstance(block, Mapping):
            block = {}
        normalized[key] = {prop: block.get(prop) for prop in properties}
    return normalized
