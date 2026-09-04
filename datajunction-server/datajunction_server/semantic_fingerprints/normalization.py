"""Semantic value normalization shared by comparison and fingerprinting."""

import json
import math
from collections.abc import Iterable
from decimal import Decimal
from enum import Enum
from typing import Any, get_args, get_origin

from pydantic import BaseModel

from datajunction_server.models.base import labelize
from datajunction_server.models.deployment import (
    ChangeTier,
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    LinkableNodeSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
)
from datajunction_server.models.node import MetricDirection
from datajunction_server.sql.parsing.backends.exceptions import DJParseException


def canonical_json(value: Any) -> str:
    """Serialize a fingerprint value deterministically."""
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def normalize_value(value: Any) -> Any:
    """Convert supported values to deterministic JSON-compatible values."""
    if isinstance(value, Enum):
        return normalize_value(value.value)
    if isinstance(value, BaseModel):
        return normalize_value(value.model_dump(mode="python"))
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("Semantic fingerprint mappings require string keys")
        return {key: normalize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Semantic fingerprint values must be finite")
        if value.is_integer():
            return int(value)
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Semantic fingerprint values must be finite")
        if value == value.to_integral_value():
            return int(value)
        return {"decimal": format(value.normalize(), "f")}
    if value is None or isinstance(value, (str, int)):
        return value
    raise TypeError(
        f"Unsupported semantic fingerprint value: {type(value).__name__}",
    )


def normalize_sequence(
    values: Iterable[Any],
    *,
    preserve_order: bool,
) -> list[Any]:
    """Normalize a sequence while removing duplicate semantic values."""
    unique: dict[str, Any] = {}
    for value in values:
        normalized = normalize_value(value)
        unique.setdefault(canonical_json(normalized), normalized)
    return (
        list(unique.values())
        if preserve_order
        else [unique[key] for key in sorted(unique)]
    )


def annotation_contains_list(annotation: Any) -> bool:
    """Return whether an annotation contains a list type."""
    return get_origin(annotation) is list or any(
        annotation_contains_list(argument) for argument in get_args(annotation)
    )


def normalize_column(
    column: ColumnSpec | None,
    name: str,
    fallback_type: str | None,
    compare_types: bool,
) -> ColumnSpec:
    """Fill equivalent column defaults before comparison or hashing."""
    normalized = (
        column.model_copy()
        if column
        else ColumnSpec(
            name=name,
            display_name=labelize(name),
            type=fallback_type or "",
            attributes=[],
        )
    )
    normalized.display_name = normalized.display_name or labelize(name)
    normalized.description = normalized.description or ""
    normalized.attributes = sorted(set(normalized.attributes) - {"primary_key"})
    if not compare_types:
        normalized.type = ""
    return normalized


def normalize_columns(
    columns: list[ColumnSpec] | None,
    *,
    compare_types: bool,
) -> list[Any]:
    """Normalize authored and resolved node columns."""
    column_map = {column.name: column for column in columns or []}
    normalized_columns = []
    for name in sorted(column_map):
        column = column_map[name]
        normalized = normalize_column(column, name, column.type, compare_types)
        if not compare_types:
            default = normalize_column(None, name, column.type, compare_types)
            if normalized == default:
                continue
        normalized_columns.append(normalize_value(normalized))
    return normalized_columns


def normalize_dimension_links(
    links: list[DimensionJoinLinkSpec | DimensionReferenceLinkSpec] | None,
    *,
    preserve_order: bool,
) -> list[Any]:
    """Normalize dimension links by their semantic comparison key."""
    return normalize_sequence(
        (link._comparison_key() for link in links or []),
        preserve_order=preserve_order,
    )


def normalize_cube_columns(columns: list[ColumnSpec] | None) -> dict[str, Any]:
    """Normalize the authored partition configuration of cube columns."""
    return {
        column.name: normalize_value(column.partition)
        for column in columns or []
        if column.partition
    }


def normalize_field(
    spec: NodeSpec,
    field: str,
    *,
    resolved_columns: list[ColumnSpec] | None = None,
    preserve_order: bool = False,
    structural_version: int | None = None,
) -> Any:
    """Return the normalized semantic value of one node field."""
    value = getattr(spec, field)
    if field == "query":
        from datajunction_server.sql.parsing.structural import serialize_ast

        return (
            serialize_ast(spec.query_ast, version=structural_version)
            if spec.query_ast is not None
            else spec.rendered_query
        )
    if field == "columns":
        if isinstance(spec, CubeSpec):
            return normalize_cube_columns(spec.matched_rendered_columns)
        return normalize_columns(
            resolved_columns
            if isinstance(spec, SourceSpec) and resolved_columns is not None
            else value,
            compare_types=isinstance(spec, SourceSpec),
        )
    if field == "dimension_links" and isinstance(spec, LinkableNodeSpec):
        return normalize_dimension_links(
            spec.dimension_links,
            preserve_order=preserve_order,
        )
    if field == "unit_enum" and isinstance(spec, MetricSpec):
        return normalize_value(spec._normalized_unit())
    if field == "direction" and isinstance(spec, MetricSpec):
        value = value or MetricDirection.NEUTRAL
    if field == "description":
        value = value or None
    if field == "custom_metadata":
        value = value or {}
    if value is None and annotation_contains_list(
        type(spec).model_fields[field].annotation,
    ):
        value = []

    normalized = normalize_value(value)
    return (
        normalize_sequence(normalized, preserve_order=preserve_order)
        if isinstance(normalized, list)
        else normalized
    )


def _normalize_comparison_field(
    spec: NodeSpec,
    field: str,
    *,
    resolved_columns: list[ColumnSpec] | None,
    preserve_order: bool = False,
) -> Any:
    """Normalize change detection without altering versioned fingerprints."""
    if field == "required_dimensions" and isinstance(spec, MetricSpec):
        try:
            value = spec.canonical_required_dimensions
        except DJParseException:
            value = spec.rendered_required_dimensions
        return normalize_sequence(value, preserve_order=preserve_order)
    return normalize_field(
        spec,
        field,
        resolved_columns=resolved_columns,
        preserve_order=preserve_order,
    )


def semantic_diff(
    one: NodeSpec,
    two: NodeSpec,
    *,
    resolved_columns: list[ColumnSpec] | None,
    other_resolved_columns: list[ColumnSpec] | None,
) -> tuple[list[str], list[str]]:
    """Compare two specs using their normalized semantic values."""
    if one.node_type != two.node_type:
        return ["node_type"], []

    rendered_one = one.rendered_spec()
    rendered_two = two.rendered_spec()
    changed_fields = []
    reordered_fields = []
    for field, field_info in type(rendered_two).model_fields.items():
        if field in {"name", "namespace", "node_type"}:
            continue
        if isinstance(rendered_two, MetricSpec) and field == "unit_structured":
            continue
        if field_info.exclude is True and field != "unit_enum":
            continue
        if type(rendered_two).field_change_tier(field) == ChangeTier.NONE:
            continue
        if field == "display_name" and getattr(rendered_two, field) is None:
            continue

        try:
            left = _normalize_comparison_field(
                rendered_one,
                field,
                resolved_columns=resolved_columns,
            )
            right = _normalize_comparison_field(
                rendered_two,
                field,
                resolved_columns=other_resolved_columns,
            )
        except DJParseException:
            if field != "query":  # pragma: no cover
                raise
            left = rendered_one.rendered_query
            right = rendered_two.rendered_query
        if left != right:
            changed_fields.append(field)
            continue

        if type(rendered_two).field_order_change_tier(field) == ChangeTier.NONE:
            continue
        left_ordered = _normalize_comparison_field(
            rendered_one,
            field,
            resolved_columns=resolved_columns,
            preserve_order=True,
        )
        right_ordered = _normalize_comparison_field(
            rendered_two,
            field,
            resolved_columns=other_resolved_columns,
            preserve_order=True,
        )
        if left_ordered != right_ordered:
            reordered_fields.append(field)

    return changed_fields, reordered_fields
