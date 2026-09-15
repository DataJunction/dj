"""
What a check may name: the CEL environment and the values bound into it.
Anything not declared here is a compile error.
"""

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from cel_expr_python import cel

_STR_MAP = cel.Type.Map(cel.Type.STRING, cel.Type.DYN)

VARIABLES: dict[str, Any] = {
    "node": _STR_MAP,
    "dependencies": cel.Type.List(_STR_MAP),
    "previous": _STR_MAP,
    # BOOL rather than DYN so `change.is_removal` type-checks as a guard.
    "change": cel.Type.Map(cel.Type.STRING, cel.Type.BOOL),
}

# Metadata key -> the properties its registered schema declares. Must come from
# the real schemas: a stale list prefills a renamed property to null, and its
# check then reports "not set" forever.
DeclaredProperties = Mapping[str, Sequence[str]]


def build_env() -> cel.Env:
    """The one environment every check compiles and evaluates against."""
    return cel.NewEnv(variables=VARIABLES)


def custom_metadata(
    raw: Mapping[str, Any] | None,
    declared_properties: DeclaredProperties,
) -> dict[str, Any]:
    """
    custom_metadata as authored, with every declared property prefilled to null.

    An absent CEL map key evaluates to an error rather than false, so prefilling
    is what makes `!= null` safe. Unschematised keys pass through untouched and
    need has() guards.
    """
    normalized = dict(raw or {})
    for key, properties in declared_properties.items():
        block = normalized.get(key)
        if not isinstance(block, Mapping):
            block = {}
        normalized[key] = {prop: block.get(prop) for prop in properties}
    return normalized


def node_snapshot(node: Any, declared_properties: DeclaredProperties) -> dict[str, Any]:
    """
    Flatten a node and its current revision into plain values.

    Relationships must already be eager-loaded: CEL evaluates synchronously, so
    a lazy load here would raise MissingGreenlet.
    """
    revision = node.current
    return {
        "name": node.name,
        "type": _plain(node.type),
        "namespace": node.namespace,
        "custom_metadata": custom_metadata(
            revision.custom_metadata,
            declared_properties,
        ),
        "description": revision.description or "",
        "mode": _plain(revision.mode),
        "owners": [
            {"username": owner.username, "email": owner.email} for owner in node.owners
        ],
        "tags": [{"name": tag.name, "tag_type": tag.tag_type} for tag in node.tags],
        "columns": [
            {"name": column.name, "description": column.description}
            for column in revision.columns
        ],
        # Read off the primary_key column attribute, not a column named as such.
        "primary_key": [column.name for column in revision.primary_key()],
        "dimension_links": [
            {
                "role": link.role,
                "join_type": _plain(link.join_type) or "left",
                "default_value": link.default_value,
                "dimension": {
                    "name": link.dimension.name,
                    "custom_metadata": custom_metadata(
                        link.dimension.current.custom_metadata,
                        declared_properties,
                    ),
                },
            }
            for link in revision.dimension_links
        ],
    }


def dependency_snapshot(
    parent: Any,
    declared_properties: DeclaredProperties,
) -> dict[str, Any]:
    """Upstream entities carry only what checks need."""
    return {
        "name": parent.name,
        "type": _plain(parent.type),
        "custom_metadata": custom_metadata(
            parent.current.custom_metadata,
            declared_properties,
        ),
    }


def activation(
    node: Any,
    declared_properties: DeclaredProperties,
    *,
    dependencies: Iterable[Any] = (),
    previous: Any | None = None,
    is_new: bool = False,
    is_removal: bool = False,
) -> dict[str, Any]:
    """The value map bound into CEL for one entity."""
    return {
        "node": node_snapshot(node, declared_properties),
        "dependencies": [
            dependency_snapshot(parent, declared_properties) for parent in dependencies
        ],
        "previous": {
            "exists": previous is not None,
            "custom_metadata": custom_metadata(
                previous.custom_metadata if previous else None,
                declared_properties,
            ),
        },
        "change": {"is_new": is_new, "is_removal": is_removal},
    }


def _plain(value: Any) -> Any:
    """Enum members bind as their value; CEL has no Python enums."""
    if value is None:
        return None
    return str(value.value if hasattr(value, "value") else value)
