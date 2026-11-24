"""Inactive component based semantic fingerprints, version 2."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from functools import cached_property, lru_cache
from heapq import heapify, heappop, heappush
from types import MappingProxyType
from typing import Literal

from datajunction_server.models.deployment import (
    ColumnSpec,
    NodeSpec,
)
from datajunction_server.models.semantic_fingerprint import (
    SemanticFingerprint,
    SemanticFingerprintValue,
)
from datajunction_server.semantic_fingerprints.merkle import (
    strongly_connected_components,
)
from datajunction_server.semantic_fingerprints.normalization import canonical_json
from datajunction_server.sql.parsing.types import ColumnType

ComponentPrecision = Literal["precise", "conservative", "unknown"]
ComponentComparison = Literal["unchanged", "changed", "indeterminate"]


@dataclass(frozen=True, order=True)
class ComponentKey:
    """A stable address for one node output or row stage."""

    node: str
    kind: str
    field: str = ""

    @cached_property
    def token(self) -> str:
        return canonical_json([self.node, self.kind, self.field])


@dataclass(frozen=True)
class ComponentResult:
    """A component digest with its evidence quality and diagnostics."""

    fingerprint: SemanticFingerprintValue
    precision: ComponentPrecision
    reasons: tuple[str, ...] = ()

    @property
    def digest(self) -> str | None:
        return (
            self.fingerprint.digest
            if isinstance(self.fingerprint, SemanticFingerprint)
            else None
        )


@dataclass(frozen=True)
class NodeComponents:
    """Component fingerprints for one node in a frozen graph."""

    outputs: Mapping[str, ComponentResult]
    rows: ComponentResult
    full: ComponentResult
    precision: ComponentPrecision
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class V2FingerprintGraph:
    """The evaluated components and their labelled dependency bindings."""

    nodes: Mapping[str, NodeComponents]
    components: Mapping[ComponentKey, ComponentResult]
    bindings: Mapping[ComponentKey, tuple[tuple[str, ComponentKey], ...]]


@dataclass(frozen=True)
class SemanticGraphSnapshot:
    """An isolated copy of the definitions and resolved columns to evaluate."""

    _specs: Mapping[str, NodeSpec]
    _resolved_columns: Mapping[str, tuple[ColumnSpec, ...]]

    def __init__(
        self,
        specs: Mapping[str, NodeSpec],
        resolved_columns: Mapping[str, Iterable[ColumnSpec]] | None = None,
    ) -> None:
        captured_specs = {}
        for name, spec in sorted(specs.items()):
            captured = deepcopy(spec)
            if captured._query_ast is not None and (
                getattr(captured._query_ast, "_parsed_sql", None)
                != captured.rendered_query
            ):
                captured._query_ast = None
            captured_specs[name] = captured
        object.__setattr__(
            self,
            "_specs",
            MappingProxyType(captured_specs),
        )
        object.__setattr__(
            self,
            "_resolved_columns",
            MappingProxyType(
                {
                    name: tuple(deepcopy(tuple(columns)))
                    for name, columns in sorted((resolved_columns or {}).items())
                },
            ),
        )

    @property
    def specs(self) -> Mapping[str, NodeSpec]:
        return MappingProxyType(deepcopy(dict(self._specs)))

    @property
    def resolved_columns(self) -> Mapping[str, tuple[ColumnSpec, ...]]:
        return MappingProxyType(deepcopy(dict(self._resolved_columns)))

    def _with_internal_specs(
        self,
        specs: Mapping[str, NodeSpec],
    ) -> SemanticGraphSnapshot:
        snapshot = object.__new__(SemanticGraphSnapshot)
        object.__setattr__(
            snapshot,
            "_specs",
            MappingProxyType(dict(sorted(specs.items()))),
        )
        object.__setattr__(snapshot, "_resolved_columns", self._resolved_columns)
        return snapshot


@lru_cache(maxsize=256)
def _parsed_type(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        return str(ColumnType.validate(value))
    except Exception:
        return None


@dataclass(frozen=True)
class ComponentDefinition:
    """The local recipe and labelled provider components for one digest."""

    key: ComponentKey
    payload: object
    dependencies: tuple[tuple[str, ComponentKey], ...] = ()
    precision: ComponentPrecision = "precise"
    reasons: tuple[str, ...] = ()
    unavailable: bool = False
    semantic_field: str | None = None


@dataclass(frozen=True)
class UnorderedPayload:
    """Payload entries whose order does not affect query results."""

    values: tuple[object, ...]


def compare_components(
    before: ComponentResult,
    after: ComponentResult,
) -> ComponentComparison:
    """Compare component evidence without treating unknown as equality."""
    if before.digest is None or after.digest is None:
        return "indeterminate"
    return "unchanged" if before.digest == after.digest else "changed"


def _component_identity(key: ComponentKey, semantic_field: str | None) -> list[str]:
    return [key.kind, key.field if semantic_field is None else semantic_field]


def _component_payload(
    value: object,
    fingerprints: Mapping[ComponentKey, SemanticFingerprint],
) -> object:
    if isinstance(value, UnorderedPayload):
        return sorted(
            (_component_payload(item, fingerprints) for item in value.values),
            key=canonical_json,
        )
    if isinstance(value, ComponentKey):
        return {
            "component": value.kind,
            "fingerprint": fingerprints[value].model_dump(mode="json"),
        }
    if isinstance(value, dict):
        return {
            key: _component_payload(item, fingerprints) for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_component_payload(item, fingerprints) for item in value]
    return value


def _component_hash(
    key: ComponentKey,
    payload: object,
    dependencies: list[tuple[str, ComponentKey, SemanticFingerprint]],
    *,
    semantic_field: str | None = None,
) -> SemanticFingerprint:
    value = {
        "domain": "datajunction/component-semantic",
        "version": 2,
        "key": _component_identity(key, semantic_field),
        "payload": _component_payload(
            payload,
            {provider: fingerprint for _, provider, fingerprint in dependencies},
        ),
        "dependencies": sorted(
            [
                {
                    "binding": label,
                    "provider": provider.kind,
                    "fingerprint": fingerprint.model_dump(mode="json"),
                }
                for label, provider, fingerprint in dependencies
            ],
            key=canonical_json,
        ),
    }
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return SemanticFingerprint(version=2, digest=digest)


def _evaluate_definitions(
    definitions: Mapping[str, ComponentDefinition],
) -> dict[str, ComponentResult]:
    graph = {
        token: sorted(
            provider.token
            for _, provider in definition.dependencies
            if provider.token in definitions
        )
        for token, definition in definitions.items()
    }
    sccs = strongly_connected_components(graph)
    component_for = {
        token: index for index, members in enumerate(sccs) for token in members
    }
    cyclic = {
        index
        for index, members in enumerate(sccs)
        if len(members) > 1
        or any(
            provider.token == members[0]
            for _, provider in definitions[members[0]].dependencies
        )
    }
    component_dependencies: dict[int, set[int]] = {
        index: set() for index in range(len(sccs))
    }
    dependents: dict[int, set[int]] = {index: set() for index in range(len(sccs))}
    for token, definition in definitions.items():
        consumer = component_for[token]
        for _, provider in definition.dependencies:
            if provider.token not in definitions:
                continue
            parent = component_for[provider.token]
            if parent != consumer:
                component_dependencies[consumer].add(parent)
                dependents[parent].add(consumer)

    ready = [
        (members, index)
        for index, members in enumerate(sccs)
        if not component_dependencies[index]
    ]
    heapify(ready)
    results: dict[str, ComponentResult] = {}
    digest_by_token: dict[str, SemanticFingerprint] = {}
    evaluated = 0
    while ready:
        _, index = heappop(ready)
        evaluated += 1
        members = sccs[index]
        member_set = set(members)
        external_results = [
            results[provider.token]
            for token in members
            for _, provider in definitions[token].dependencies
            if provider.token not in member_set and provider.token in results
        ]
        unknown_precision = any(
            definitions[token].precision == "unknown" for token in members
        )
        unavailable = unknown_precision or any(
            definitions[token].unavailable
            or any(
                provider.token not in definitions
                or (
                    provider.token in results and results[provider.token].digest is None
                )
                for _, provider in definitions[token].dependencies
            )
            for token in members
        )
        reasons = tuple(
            sorted(
                {reason for token in members for reason in definitions[token].reasons}
                | {reason for result in external_results for reason in result.reasons}
                | ({"unknown_component_precision"} if unknown_precision else set())
                | ({"unresolved_component_dependency"} if unavailable else set()),
            ),
        )
        if unavailable:
            for token in members:
                results[token] = ComponentResult("unknown", "unknown", reasons)
        elif index in cyclic:
            cycle_reasons = tuple(sorted(set(reasons) | {"cyclic_component"}))
            for token in members:
                results[token] = ComponentResult(
                    "unknown",
                    "unknown",
                    cycle_reasons,
                )
        else:
            token = members[0]
            definition = definitions[token]
            dependencies = [
                (label, provider, digest_by_token[provider.token])
                for label, provider in definition.dependencies
            ]
            try:
                fingerprint = _component_hash(
                    definition.key,
                    definition.payload,
                    dependencies,
                    semantic_field=definition.semantic_field,
                )
            except KeyError:
                results[token] = ComponentResult(
                    "unknown",
                    "unknown",
                    tuple(sorted(set(reasons) | {"unresolved_payload_dependency"})),
                )
            else:
                digest_by_token[token] = fingerprint
                precision: ComponentPrecision = (
                    "conservative"
                    if definition.precision == "conservative"
                    or any(
                        result.precision == "conservative"
                        for result in external_results
                    )
                    else "precise"
                )
                results[token] = ComponentResult(fingerprint, precision, reasons)
        for child in sorted(dependents[index], key=lambda item: sccs[item]):
            component_dependencies[child].remove(index)
            if not component_dependencies[child]:
                heappush(ready, (sccs[child], child))

    if evaluated != len(sccs):
        raise RuntimeError("Component SCC condensation was not acyclic")
    return results


async def compute_v2_fingerprints(
    snapshot: SemanticGraphSnapshot,
    targets: Iterable[str] | None = None,
) -> V2FingerprintGraph:
    """Evaluate version 2 components over one explicit immutable graph snapshot."""
    from datajunction_server.semantic_fingerprints.v2_dependencies import (
        build_component_definitions,
        build_node_component_keys,
    )

    definitions, output_names, reasons_by_node = build_component_definitions(
        snapshot,
    )

    results = _evaluate_definitions(definitions)
    node_keys = build_node_component_keys(output_names)
    selected = set(targets) if targets is not None else set(snapshot._specs)
    node_results: dict[str, NodeComponents] = {}
    for name in sorted(selected & snapshot._specs.keys()):
        keys = node_keys[name]
        outputs = {
            output: results[key.token] for output, key in keys["outputs"].items()
        }
        rows = results[keys["rows"].token]
        full = results[keys["full"].token]
        node_reasons = tuple(
            sorted(
                set(reasons_by_node.get(name, ()))
                | set(full.reasons)
                | {reason for result in outputs.values() for reason in result.reasons}
                | set(rows.reasons),
            ),
        )
        node_results[name] = NodeComponents(
            outputs=MappingProxyType(outputs),
            rows=rows,
            full=full,
            precision=full.precision,
            reasons=node_reasons,
        )
    components = {
        definition.key: results[token] for token, definition in definitions.items()
    }
    bindings = {
        definition.key: tuple(definition.dependencies)
        for definition in definitions.values()
    }
    return V2FingerprintGraph(
        nodes=MappingProxyType(node_results),
        components=MappingProxyType(components),
        bindings=MappingProxyType(bindings),
    )
