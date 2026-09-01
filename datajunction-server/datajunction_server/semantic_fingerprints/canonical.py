"""Canonical node values shared by comparison and fingerprinting."""

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


def canonical_json(value: Any) -> str:
    """Serialize a fingerprint value deterministically."""
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_value(value: Any) -> Any:
    """Convert supported values to deterministic JSON-compatible values."""
    if isinstance(value, Enum):
        return canonical_value(value.value)
    if isinstance(value, BaseModel):
        return canonical_value(value.model_dump(mode="python"))
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("Semantic fingerprint mappings require string keys")
        return {key: canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Semantic fingerprint values must be finite")
        if value == 0 or value.is_integer():
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


def canonical_sequence(
    values: Iterable[Any],
    *,
    preserve_order: bool,
) -> list[Any]:
    """Canonicalize a sequence while removing duplicate semantic values."""
    unique: dict[str, Any] = {}
    for value in values:
        canonical = canonical_value(value)
        unique.setdefault(canonical_json(canonical), canonical)
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


def normalized_column(
    column: ColumnSpec | None,
    name: str,
    fallback_type: str | None,
    compare_types: bool,
) -> ColumnSpec:
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


def canonical_columns(
    columns: list[ColumnSpec] | None,
    *,
    compare_types: bool,
) -> list[Any]:
    """Canonicalize authored and resolved node columns."""
    column_map = {column.name: column for column in columns or []}
    canonical = []
    for name in sorted(column_map):
        column = column_map[name]
        normalized = normalized_column(column, name, column.type, compare_types)
        if not compare_types:
            default = normalized_column(None, name, column.type, compare_types)
            if normalized == default:
                continue
        canonical.append(canonical_value(normalized))
    return canonical


def canonical_dimension_links(
    links: list[DimensionJoinLinkSpec | DimensionReferenceLinkSpec] | None,
    *,
    preserve_order: bool,
) -> list[Any]:
    """Canonicalize dimension links by their semantic comparison key."""
    return canonical_sequence(
        (link._comparison_key() for link in links or []),
        preserve_order=preserve_order,
    )


def canonical_cube_columns(columns: list[ColumnSpec] | None) -> dict[str, Any]:
    """Canonicalize the authored partition configuration of cube columns."""
    return {
        column.name: canonical_value(column.partition)
        for column in columns or []
        if column.partition
    }


def canonical_field_value(
    spec: NodeSpec,
    field: str,
    *,
    resolved_columns: list[ColumnSpec] | None = None,
    preserve_order: bool = False,
    structural_version: int | None = None,
) -> Any:
    """Return the canonical semantic value of one node field."""
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
            return canonical_cube_columns(spec.rendered_columns)
        return canonical_columns(
            resolved_columns
            if isinstance(spec, SourceSpec) and resolved_columns is not None
            else value,
            compare_types=isinstance(spec, SourceSpec),
        )
    if field == "dimension_links" and isinstance(spec, LinkableNodeSpec):
        return canonical_dimension_links(
            spec.dimension_links,
            preserve_order=preserve_order,
        )
    if field == "unit_enum" and isinstance(spec, MetricSpec):
        return canonical_value(spec._canonical_unit())
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

    canonical = canonical_value(value)
    return (
        canonical_sequence(canonical, preserve_order=preserve_order)
        if isinstance(canonical, list)
        else canonical
    )


def canonical_node_diff(
    one: NodeSpec,
    two: NodeSpec,
    *,
    resolved_columns: list[ColumnSpec] | None,
    other_resolved_columns: list[ColumnSpec] | None,
) -> tuple[list[str], list[str]]:
    """Compare two specs using their canonical semantic values."""
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

        left = canonical_field_value(
            rendered_one,
            field,
            resolved_columns=resolved_columns,
        )
        right = canonical_field_value(
            rendered_two,
            field,
            resolved_columns=other_resolved_columns,
        )
        if left != right:
            changed_fields.append(field)
            continue

        if type(rendered_two).field_order_change_tier(field) == ChangeTier.NONE:
            continue
        left_ordered = canonical_field_value(
            rendered_one,
            field,
            resolved_columns=resolved_columns,
            preserve_order=True,
        )
        right_ordered = canonical_field_value(
            rendered_two,
            field,
            resolved_columns=other_resolved_columns,
            preserve_order=True,
        )
        if left_ordered != right_ordered:
            reordered_fields.append(field)

    return changed_fields, reordered_fields
