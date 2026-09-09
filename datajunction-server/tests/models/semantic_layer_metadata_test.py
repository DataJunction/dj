"""Tests for the built-in semantic-layer custom metadata schema."""

import jsonschema
import pytest

from datajunction_server.models.semantic_layer_metadata import (
    SEMANTIC_LAYER_METADATA_SCHEMA,
)


def test_semantic_layer_metadata_schema_is_valid_json_schema():
    jsonschema.Draft202012Validator.check_schema(SEMANTIC_LAYER_METADATA_SCHEMA)


@pytest.mark.parametrize(
    "metadata",
    [
        {
            "display_name": "Net revenue",
            "semantic_type": "currency",
            "unit": {"kind": "currency", "code": "USD"},
            "format": {"preset": "currency", "precision": 2},
            "filter": {
                "kind": "range",
                "operators": ["=", ">", ">="],
                "default_operator": ">=",
            },
            "extensions": {
                "superset": {"d3format": "$,.2f"},
                "google_sheets": {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "$#,##0.00",
                    },
                },
            },
        },
        {
            "columns": {
                "country": {
                    "semantic_type": "category",
                    "filter": {
                        "kind": "select",
                        "operators": ["IN", "NOT IN"],
                        "default_operator": "IN",
                        "multi": True,
                    },
                },
            },
        },
    ],
)
def test_semantic_layer_metadata_schema_accepts_supported_shapes(metadata):
    jsonschema.validate(metadata, SEMANTIC_LAYER_METADATA_SCHEMA)


@pytest.mark.parametrize(
    "metadata",
    [
        {"sematic_type": "currency"},
        {"format": {"preset": "money"}},
        {"extensions": {"superset": "not-an-object"}},
        {"columns": {"country": {"filter": {"kind": "dropdown"}}}},
    ],
)
def test_semantic_layer_metadata_schema_rejects_invalid_shapes(metadata):
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(metadata, SEMANTIC_LAYER_METADATA_SCHEMA)
