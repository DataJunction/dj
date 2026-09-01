"""Frozen semantic fingerprint version 1."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import TYPE_CHECKING

from datajunction_server.models.deployment import (
    ChangeTier,
    CubeSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.normalization import (
    canonical_json,
    normalize_field,
    normalize_value,
)

if TYPE_CHECKING:
    from datajunction_server.models.deployment import ColumnSpec, NodeSpec


_FIELDS_BY_SPEC_TYPE: dict[type[NodeSpec], tuple[str, ...]] = {
    SourceSpec: (
        "columns",
        "dimension_links",
        "primary_key",
        "catalog",
        "schema_",
        "table",
    ),
    TransformSpec: (
        "columns",
        "dimension_links",
        "primary_key",
        "query",
    ),
    DimensionSpec: (
        "columns",
        "dimension_links",
        "primary_key",
        "query",
    ),
    MetricSpec: ("query", "required_dimensions"),
    CubeSpec: ("metrics", "dimensions", "filters", "columns"),
}


def semantic_fields(spec_type: type[NodeSpec]) -> tuple[str, ...]:
    """Return the frozen field projection for a concrete node type."""
    try:
        return _FIELDS_BY_SPEC_TYPE[spec_type]
    except KeyError as exc:
        raise TypeError(
            f"No semantic fingerprint fields for {spec_type.__name__}",
        ) from exc


def build_fingerprint(
    spec: NodeSpec,
    parent_fingerprints: Iterable[SemanticFingerprint],
    *,
    resolved_columns: list[ColumnSpec] | None,
) -> SemanticFingerprint:
    """Build a version 1 semantic fingerprint."""
    fingerprint_fields = semantic_fields(type(spec))
    for field in fingerprint_fields:
        canonical_json(normalize_value(getattr(spec, field)))

    rendered = spec.rendered_spec()
    fields = {
        field: normalize_field(
            rendered,
            field,
            resolved_columns=resolved_columns,
            preserve_order=(
                type(rendered).field_order_change_tier(field) == ChangeTier.MAJOR
            ),
            structural_version=1,
        )
        for field in fingerprint_fields
    }
    node_payload = {
        "domain": "datajunction/node-semantic",
        "node_type": normalize_value(rendered.node_type),
        "fields": fields,
    }
    node_digest = hashlib.sha256(
        canonical_json(node_payload).encode("utf-8"),
    ).hexdigest()
    parents = list(parent_fingerprints)
    if any(parent.version != 1 for parent in parents):
        raise ValueError("Parent fingerprint version does not match node version")
    payload = {
        "domain": "datajunction/node-semantic-merkle",
        "version": 1,
        "node": node_digest,
        "parents": sorted({parent.digest for parent in parents}),
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return SemanticFingerprint(version=1, digest=digest)
