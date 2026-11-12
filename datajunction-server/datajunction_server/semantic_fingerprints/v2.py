"""Semantic fingerprint version 2: independent of the deploy namespace."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.normalization import (
    canonical_json,
    normalize_field,
    normalize_value,
)
from datajunction_server.semantic_fingerprints.v1 import semantic_fields
from datajunction_server.utils import SEPARATOR

if TYPE_CHECKING:
    from datajunction_server.models.deployment import ColumnSpec, NodeSpec

NAMESPACE_MARKER = "__namespace__"


@lru_cache(maxsize=4096)
def parse_query(query: str):
    """Parses a query once per distinct text."""
    from datajunction_server.sql.parsing.backends import antlr4

    return antlr4.parse(query)


# A single-quoted SQL string, where '' is an escaped quote.
_SQL_STRING = re.compile(r"('(?:[^']|'')*')")
# Fields holding SQL text, where quoted strings are values, not names.
EXPRESSION_KEYS = frozenset({"query", "join_on", "filters", "default"})
# Fields naming warehouse tables, which no deploy namespace changes.
PHYSICAL_KEYS = frozenset({"catalog", "schema_", "table"})


def namespace_free_name(text: str, namespace: str) -> str:
    """Replaces a leading namespace in node names with a fixed marker."""
    if not namespace:
        return text
    return re.sub(
        rf"(?<![\w.]){re.escape(namespace)}\.",
        f"{NAMESPACE_MARKER}.",
        text,
    )


def _outside_strings(text: str, namespace: str) -> str:
    parts = _SQL_STRING.split(text)
    # re.split puts the quoted strings at odd indexes.
    return "".join(
        part if index % 2 else namespace_free_name(part, namespace)
        for index, part in enumerate(parts)
    )


def _rewrite(value: Any, namespace: str, key: str | None = None) -> Any:
    if isinstance(value, str):
        if key in PHYSICAL_KEYS:
            return value
        if key in EXPRESSION_KEYS:
            return _outside_strings(value, namespace)
        return namespace_free_name(value, namespace)
    if isinstance(value, dict):
        return {
            namespace_free_name(name, namespace): _rewrite(item, namespace, name)
            for name, item in value.items()
        }
    if isinstance(value, list):
        return [_rewrite(item, namespace, key) for item in value]
    return value


def own_namespace(spec: NodeSpec) -> str:
    """
    The namespace a spec was deployed into.

    Specs loaded from the database have no namespace set, so use the one
    in the node's name.
    """
    return spec.namespace or spec.rendered_name.rpartition(SEPARATOR)[0]


def namespace_free_spec(spec: NodeSpec) -> NodeSpec:
    """
    Renders a spec with its own namespace replaced by a fixed marker.

    Only node names change. Quoted values and warehouse table names, and
    names in other namespaces, are left as written.
    """
    rendered = spec.rendered_spec()
    namespace = own_namespace(spec)
    if not namespace:
        return rendered
    values = rendered.model_dump(mode="json")
    for field in semantic_fields(type(spec)):
        values[field] = _rewrite(values[field], namespace, field)
    result = type(rendered).model_validate(values)
    result.namespace = None
    if result.rendered_query:
        result._query_ast = parse_query(result.rendered_query)
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
