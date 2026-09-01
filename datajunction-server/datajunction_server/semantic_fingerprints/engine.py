"""Version dispatch for semantic fingerprints."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from datajunction_server.models.semantic_fingerprint import (
    LATEST_SEMANTIC_FINGERPRINT_VERSION,
    SemanticFingerprint,
)
from datajunction_server.semantic_fingerprints.canonical import canonical_node_diff
from datajunction_server.semantic_fingerprints.v1 import build_fingerprint

if TYPE_CHECKING:
    from datajunction_server.models.deployment import ColumnSpec, NodeSpec


_BUILDERS: dict[int, Callable[..., SemanticFingerprint]] = {
    1: build_fingerprint,
}


def fingerprint_node(
    spec: NodeSpec,
    version: int = LATEST_SEMANTIC_FINGERPRINT_VERSION,
    *,
    parent_fingerprints: Iterable[SemanticFingerprint] = (),
    resolved_columns: list[ColumnSpec] | None = None,
) -> SemanticFingerprint:
    """Return a fingerprint of a node's semantic definition."""
    builder = _BUILDERS.get(version)
    if builder is None:
        raise ValueError(f"Unsupported semantic fingerprint version: {version}")
    return builder(
        spec,
        parent_fingerprints,
        resolved_columns=resolved_columns,
    )


def canonical_diff(
    one: NodeSpec,
    two: NodeSpec,
    *,
    resolved_columns: list[ColumnSpec] | None = None,
    other_resolved_columns: list[ColumnSpec] | None = None,
) -> tuple[list[str], list[str]]:
    """Compare two specs using the same canonical values as fingerprints."""
    return canonical_node_diff(
        one,
        two,
        resolved_columns=resolved_columns,
        other_resolved_columns=other_resolved_columns,
    )
