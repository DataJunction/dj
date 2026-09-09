"""Built-in JSON Schema for semantic-layer custom metadata."""

SEMANTIC_LAYER_METADATA_KEY = "semantic_layer"
SEMANTIC_LAYER_METADATA_DESCRIPTION = (
    "Portable semantic-layer column metadata and client-specific extensions."
)

_SEMANTIC_TYPES = [
    "currency",
    "percentage",
    "proportion",
    "count",
    "duration",
    "data_size",
    "date",
    "timestamp",
    "identifier",
    "category",
    "url",
    "boolean",
    "number",
    "string",
]

_FORMAT_PRESETS = [
    "smart_number",
    "number",
    "currency",
    "percentage",
    "duration",
    "data_size",
]

_FILTER_KINDS = [
    "text",
    "number",
    "range",
    "date",
    "datetime",
    "boolean",
    "select",
]

_FILTER_OPERATORS = [
    "=",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "IN",
    "NOT IN",
    "IS NULL",
    "IS NOT NULL",
    "between",
    "contains",
    "starts_with",
    "ends_with",
]

_COLUMN_METADATA_PROPERTIES = {
    "display_name": {"type": "string"},
    "semantic_type": {"type": "string", "enum": _SEMANTIC_TYPES},
    "unit": {"$ref": "#/$defs/Unit"},
    "attributes": {
        "type": "array",
        "items": {"type": "string"},
        "uniqueItems": True,
    },
    "format": {"$ref": "#/$defs/FormatMetadata"},
    "filter": {"$ref": "#/$defs/FilterMetadata"},
    "extensions": {"$ref": "#/$defs/Extensions"},
}

SEMANTIC_LAYER_METADATA_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "ColumnMetadata": {
            "type": "object",
            "additionalProperties": False,
            "properties": _COLUMN_METADATA_PROPERTIES,
        },
        "Unit": {
            "oneOf": [
                {"$ref": "#/$defs/AtomicUnit"},
                {"$ref": "#/$defs/CompoundUnit"},
            ],
        },
        "AtomicUnit": {
            "type": "object",
            "additionalProperties": False,
            "required": ["kind"],
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": [
                        "currency",
                        "time",
                        "data_size",
                        "percentage",
                        "proportion",
                        "count",
                        "unitless",
                    ],
                },
                "code": {"type": "string", "minLength": 1},
            },
        },
        "CompoundUnit": {
            "type": "object",
            "additionalProperties": False,
            "required": ["numerator", "denominator"],
            "properties": {
                "numerator": {"$ref": "#/$defs/AtomicUnit"},
                "denominator": {"$ref": "#/$defs/AtomicUnit"},
            },
        },
        "FormatMetadata": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "preset": {"type": "string", "enum": _FORMAT_PRESETS},
                "precision": {"type": "integer", "minimum": 0},
                "scale": {"type": "number"},
            },
        },
        "FilterMetadata": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "kind": {"type": "string", "enum": _FILTER_KINDS},
                "operators": {
                    "type": "array",
                    "items": {"type": "string", "enum": _FILTER_OPERATORS},
                    "uniqueItems": True,
                },
                "default_operator": {
                    "type": "string",
                    "enum": _FILTER_OPERATORS,
                },
                "multi": {"type": "boolean"},
            },
        },
        "Extensions": {
            "type": "object",
            "additionalProperties": {"type": "object"},
        },
    },
    "type": "object",
    "additionalProperties": False,
    "properties": {
        **_COLUMN_METADATA_PROPERTIES,
        "columns": {
            "type": "object",
            "additionalProperties": {"$ref": "#/$defs/ColumnMetadata"},
        },
    },
}
