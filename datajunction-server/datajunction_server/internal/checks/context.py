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
    # Typed STRING so a guard like `change.kind == 'delete'` is known to
    # compare strings at compile time.
    "change": cel.Type.Map(cel.Type.STRING, cel.Type.STRING),
}

# One node projected for CEL. Its keys are every field of every node spec, so
# they are not spelled out here.
NodeProjection = dict[str, Any]


class Activation(TypedDict):
    """
    The values a check evaluates against: one per name in VARIABLES.

    `previous` is the node as last deployed, and `change.kind` is the deploy's
    operation on it: create, update, delete or noop. A node the deploy leaves
    alone or removes is its own previous state, so `change` is the only way to
    tell a removal from an untouched node.
    """

    node: NodeProjection
    previous: NodeProjection
    dependencies: list[NodeProjection]
    change: dict[str, str]


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
