"""Tests for the built-in semantic-layer custom metadata schema."""

import jsonschema
import pytest

from datajunction_server.models.semantic_layer_metadata import (
    MAX_FORMAT_PRECISION,
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
                        "operators": ["=", "!="],
                        "default_operator": "=",
                    },
                },
            },
        },
        {"unit": {"kind": "currency"}},
        {"unit": {"kind": "time", "code": "s"}},
        {"unit": {"kind": "data_size", "code": "MiB"}},
        {"unit": {"kind": "count", "code": "requests"}},
        {"unit": {"kind": "percentage"}},
        {
            "unit": {
                "numerator": {"kind": "count", "code": "requests"},
                "denominator": {"kind": "time", "code": "s"},
            },
        },
        {"format": {"preset": "number", "precision": MAX_FORMAT_PRECISION}},
    ],
)
def test_semantic_layer_metadata_schema_accepts_supported_shapes(metadata):
    jsonschema.validate(metadata, SEMANTIC_LAYER_METADATA_SCHEMA)


@pytest.mark.parametrize(
    "metadata",
    [
        {"sematic_type": "currency"},
        {"format": {"preset": "money"}},
        {"filter": {"operators": ["IN"]}},
        {"filter": {"operators": ["NOT IN"]}},
        {"filter": {"operators": ["between"]}},
        {"filter": {"kind": "select", "multi": True}},
        {"format": {"preset": "number", "precision": MAX_FORMAT_PRECISION + 1}},
        {"unit": {"kind": "time"}},
        {"unit": {"kind": "time", "code": "seconds"}},
        {"unit": {"kind": "data_size"}},
        {"unit": {"kind": "data_size", "code": "BIT"}},
        {"unit": {"kind": "currency", "code": "usd"}},
        {"unit": {"kind": "currency", "code": "USDD"}},
        {"unit": {"kind": "percentage", "code": "%"}},
        {"unit": {"kind": "proportion", "code": "ratio"}},
        {"unit": {"kind": "unitless", "code": "x"}},
        {
            "unit": {
                "numerator": {"kind": "time"},
                "denominator": {"kind": "count"},
            },
        },
        {"extensions": {"superset": "not-an-object"}},
        {"columns": {"country": {"filter": {"kind": "dropdown"}}}},
    ],
)
def test_semantic_layer_metadata_schema_rejects_invalid_shapes(metadata):
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(metadata, SEMANTIC_LAYER_METADATA_SCHEMA)
