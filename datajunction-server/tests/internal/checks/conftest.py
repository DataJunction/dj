"""Invented custom_metadata vocabulary for the check-engine tests."""

from datajunction_server.internal.checks.context import Bindings
from datajunction_server.internal.deployment import checks

# One custom-metadata key with three declared properties, as a registered schema
# would supply them.
DECLARED_PROPERTIES = {"sample": ("color", "size", "shape")}


def build_fixtures(
    declared_properties: dict[str, tuple[str, ...]],
) -> list[Bindings]:
    """The load-time fixtures, built the way a deploy builds them."""
    return checks.build_fixtures(
        checks.DeclaredSchemas(
            properties=declared_properties,
            placeholders={
                key: {prop: f"fixture-{prop}" for prop in properties}
                for key, properties in declared_properties.items()
            },
        ),
    )
