"""Built-in JSON Schema for semantic-layer custom metadata."""

from datajunction_server.models.unit import DATA_SIZE_CODES, TIME_CODES, UnitKind

SEMANTIC_LAYER_METADATA_KEY = "semantic_layer"
SEMANTIC_LAYER_METADATA_DESCRIPTION = (
    "Portable semantic-layer column metadata and client-specific extensions."
)
MAX_FORMAT_PRECISION = 100

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
    "IS NULL",
    "IS NOT NULL",
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
                    "enum": [kind.value for kind in UnitKind],
                },
                "code": {"type": "string", "minLength": 1},
            },
            "allOf": [
                {
                    "if": {"properties": {"kind": {"const": "currency"}}},
                    "then": {"properties": {"code": {"pattern": "^[A-Z]{3}$"}}},
                },
                {
                    "if": {"properties": {"kind": {"const": "time"}}},
                    "then": {
                        "required": ["code"],
                        "properties": {"code": {"enum": sorted(TIME_CODES)}},
                    },
                },
                {
                    "if": {"properties": {"kind": {"const": "data_size"}}},
                    "then": {
                        "required": ["code"],
                        "properties": {"code": {"enum": sorted(DATA_SIZE_CODES)}},
                    },
                },
                {
                    "if": {
                        "properties": {
                            "kind": {
                                "enum": ["percentage", "proportion", "unitless"],
                            },
                        },
                    },
                    "then": {"not": {"required": ["code"]}},
                },
            ],
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
                "precision": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": MAX_FORMAT_PRECISION,
                },
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
                "multi": {"const": False},
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
