"""Semantic fingerprint version 2: independent of the deploy namespace."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING

from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.normalization import (
    canonical_json,
    normalize_field,
    normalize_value,
)
from datajunction_server.semantic_fingerprints.v1 import semantic_fields

if TYPE_CHECKING:
    from datajunction_server.models.deployment import ColumnSpec, NodeSpec

NAMESPACE_MARKER = "__namespace__"


def namespace_free_spec(spec: NodeSpec) -> NodeSpec:
    """
    Renders a spec with its own namespace replaced by a fixed marker.

    Names in other namespaces are left as written.
    """
    rendered = spec.rendered_spec()
    if not spec.namespace:
        return rendered
    own_names = re.compile(rf"(?<![\w.]){re.escape(spec.namespace)}\.")
    text = own_names.sub(
        f"{NAMESPACE_MARKER}.",
        json.dumps(rendered.model_dump(mode="json")),
    )
    result = type(rendered).model_validate_json(text)
    result.namespace = None
    if spec._namespace_free_query_ast is None and result.rendered_query:
        spec._namespace_free_query_ast = result.query_ast
    result._query_ast = spec._namespace_free_query_ast
    return result


def build_fingerprint(
    spec: NodeSpec,
    parent_fingerprints: Iterable[SemanticFingerprint],
    *,
    resolved_columns: list[ColumnSpec] | None,
) -> SemanticFingerprint:
    """Build a version 2 semantic fingerprint."""
    rendered = namespace_free_spec(spec)
    fields = {
        field: normalize_field(
            rendered,
            field,
            resolved_columns=resolved_columns,
            structural_version=1,
        )
        for field in semantic_fields(type(spec))
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
    if any(parent.version != 2 for parent in parents):
        raise ValueError("Parent fingerprint version does not match node version")
    payload = {
        "domain": "datajunction/node-semantic-merkle",
        "version": 2,
        "node": node_digest,
        "parents": sorted({parent.digest for parent in parents}),
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return SemanticFingerprint(version=2, digest=digest)
