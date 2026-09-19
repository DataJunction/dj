"""Invented vocabulary and fixture activations for the check-engine tests."""

from typing import Any

from datajunction_server.internal.checks.context import (
    DeclaredProperties,
    custom_metadata,
)

# One custom-metadata key with three declared properties, as a registered schema
# would supply them.
DECLARED_PROPERTIES = {"sample": ("color", "size", "shape")}


def build_fixtures(declared_properties: DeclaredProperties) -> list[dict[str, Any]]:
    """
    One empty entity and one fully populated, enough to force every clause down
    both branches. Built from the declared schemas, so a property no schema
    declares fails validation rather than reading null forever.
    """
    populated = {
        key: {prop: f"fixture-{prop}" for prop in properties}
        for key, properties in declared_properties.items()
    }
    return [
        {
            "node": {
                "name": "fixture.empty",
                "node_type": "dimension",
                "namespace": "fixture",
                "custom_metadata": custom_metadata(None, declared_properties),
                "description": "",
                "mode": "published",
                "owners": [],
                "tags": [],
                "columns": [],
                "primary_key": [],
                "dimension_links": [],
            },
            "dependencies": [],
            "previous": {
                "custom_metadata": custom_metadata(None, declared_properties),
            },
            "change": {"kind": "create"},
        },
        {
            "node": {
                "name": "fixture.populated",
                "node_type": "transform",
                "namespace": "fixture",
                "custom_metadata": custom_metadata(populated, declared_properties),
                "description": "A fully populated fixture.",
                "mode": "published",
                "owners": [
                    {"username": "one", "email": "one@example.com"},
                    {"username": "two", "email": "two@example.com"},
                ],
                "tags": [{"name": "fixture-tag", "tag_type": "fixture-tag-type"}],
                "columns": [
                    {"name": "first", "description": "The first column."},
                    {"name": "second", "description": "The second column."},
                ],
                "primary_key": ["first"],
                "dimension_links": [
                    {
                        "role": "fixture-role",
                        "join_type": "left",
                        "default_value": "fixture-default",
                        "dimension": {
                            "name": "fixture.dimension",
                            "custom_metadata": custom_metadata(
                                populated,
                                declared_properties,
                            ),
                        },
                    },
                ],
            },
            "dependencies": [
                {
                    "name": "fixture.parent",
                    "node_type": "source",
                    "custom_metadata": custom_metadata(
                        populated,
                        declared_properties,
                    ),
                },
            ],
            "previous": {
                "custom_metadata": custom_metadata(populated, declared_properties),
            },
            "change": {"kind": "remove"},
        },
    ]
