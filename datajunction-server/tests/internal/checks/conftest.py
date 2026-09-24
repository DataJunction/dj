"""Invented custom_metadata vocabulary for the check-engine tests."""

from collections.abc import Iterable

from datajunction_server.internal.checks.context import Bindings
from datajunction_server.internal.deployment import checks
from datajunction_server.models.node_type import NodeType

# One custom-metadata key with three declared properties, as a registered schema
# would supply them.
DECLARED_PROPERTIES: dict[str, tuple[str, ...]] = {
    "sample": ("color", "size", "shape"),
}

# A clause guarded to one node type still needs a node of that type to be
# evaluated against, so the tests cover all of them.
NODE_TYPES = tuple(NodeType)


def _declared(properties: dict[str, tuple[str, ...]]) -> checks.DeclaredSchemas:
    return checks.DeclaredSchemas(
        properties=properties,
        placeholders={
            key: {prop: f"fixture-{prop}" for prop in props}
            for key, props in properties.items()
        },
    )


def build_fixtures(
    declared_properties: dict[str, tuple[str, ...]],
    node_types: Iterable[NodeType] = NODE_TYPES,
) -> list[Bindings]:
    """The load-time fixtures, built the way a deploy builds them."""
    return checks.build_fixtures(_declared(declared_properties), node_types)


def fixture_pair(
    declared_properties: dict[str, tuple[str, ...]] = DECLARED_PROPERTIES,
    node_type: NodeType = NodeType.DIMENSION,
) -> list[Bindings]:
    """The bare and populated fixtures for one node type."""
    return build_fixtures(declared_properties, [node_type])
