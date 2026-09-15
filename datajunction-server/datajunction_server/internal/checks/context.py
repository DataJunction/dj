"""
The surface a check author writes against: the CEL environment and the values
bound into it.

Everything a check can name is declared here; anything else is a compile error.
Cost lives here too -- a check is free, a binding is not.
"""

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from cel_expr_python import cel

_STR_MAP = cel.Type.Map(cel.Type.STRING, cel.Type.DYN)

# Root bindings. Adding one here is a deliberate, reviewable cost decision.
VARIABLES: dict[str, Any] = {
    # The entity under evaluation, including its custom_metadata.
    "node": _STR_MAP,
    # Upstream entities, each {name, type, custom_metadata}.
    "dependencies": cel.Type.List(_STR_MAP),
    # The currently deployed revision: {exists, custom_metadata}.
    "previous": _STR_MAP,
    # What the deploy is doing to this entity: {is_new, is_removal}. Typed BOOL
    # rather than DYN so `change.is_removal` type-checks as a boolean guard.
    "change": cel.Type.Map(cel.Type.STRING, cel.Type.BOOL),
}

# Custom-metadata key -> the property names its registered schema declares. Each
# declared property is null-prefilled below, so callers must pass the real
# schemas rather than a hardcoded list: a property renamed in the schema but not
# here would be prefilled to null and its check would report "not set" forever.
DeclaredProperties = Mapping[str, Sequence[str]]


def build_env() -> cel.Env:
    """The one environment every check compiles and evaluates against."""
    return cel.NewEnv(variables=VARIABLES)


def custom_metadata(
    raw: Mapping[str, Any] | None,
    declared_properties: DeclaredProperties,
) -> dict[str, Any]:
    """
    custom_metadata as authored, with every schema-declared property prefilled.

    Reading an absent key from a CEL map yields an error value rather than
    false, so each declared property is filled with null to make
    `node.custom_metadata.<key>.<property> != null` safe. Normalizing to a dict
    matters as well: a revision may store JSON null here, and selecting a field
    from null is a different CEL error from a missing key.

    Keys with no registered schema pass through untouched, so a check can still
    walk any path -- it just has to guard with has() where a key may be absent.
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

    Deliberately not ORM objects: DJ runs on AsyncSession and CEL evaluation is
    synchronous, so a lazy load inside an expression would raise MissingGreenlet.
    Every relationship read here must already be eager-loaded by the caller.
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
    """Upstream entities carry only what checks need, not a full snapshot."""
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
    """Enum members bind as their value; CEL has no notion of a Python enum."""
    if value is None:
        return None
    return str(value.value if hasattr(value, "value") else value)
