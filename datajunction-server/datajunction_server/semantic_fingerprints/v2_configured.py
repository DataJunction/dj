"""Configured dimension route context for metric and cube fingerprints."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Mapping
from typing import cast

from datajunction_server.models.deployment import (
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    LinkableNodeSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.normalization import (
    canonical_json,
    normalize_value,
)
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentKey,
    ComponentPrecision,
    ComponentResult,
    SemanticFingerprint,
    _component_hash,
)
from datajunction_server.semantic_fingerprints.v2_dependencies import (
    _dimension_link_contract,
    _join_columns,
    _JoinColumnCache,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException

_PARSE_ERRORS = (DJParseException, TypeError, ValueError, AttributeError, KeyError)

_ContextLink = tuple[
    str,
    str,
    DimensionJoinLinkSpec | DimensionReferenceLinkSpec,
    str,
]

_ContextLinkIndex = Mapping[str, tuple[_ContextLink, ...]]

_AncestryCache = dict[str, frozenset[str] | None]

_ContextPaths = tuple[list[tuple[str, str, object]], bool]

_ContextPathCache = dict[
    tuple[frozenset[str], frozenset[tuple[str, str | None]]],
    _ContextPaths,
]


def _resolved_node(reference: str, specs: Mapping[str, NodeSpec]) -> str | None:
    reference = reference.split("[", 1)[0]
    if reference in specs:
        return reference
    parts = reference.split(".")
    for end in range(len(parts) - 1, 0, -1):
        candidate = ".".join(parts[:end])
        if candidate in specs:
            return candidate
    matches = [name for name in specs if name.rsplit(".", 1)[-1] == parts[0]]
    return matches[0] if len(matches) == 1 else None


def _dimension_ref(
    reference: str,
    specs: Mapping[str, NodeSpec],
) -> tuple[str, str, str] | None:
    reference, _, role = reference.partition("[")
    parts = reference.split(".")
    if len(parts) < 2:
        return None
    node_name = _resolved_node(".".join(parts[:-1]), specs)
    return (node_name, parts[-1], role.removesuffix("]")) if node_name else None


def _ancestors(
    parent: str,
    specs: Mapping[str, NodeSpec],
    cache: _AncestryCache,
) -> set[str] | None:
    active: set[str] = set()

    def visit(name: str) -> frozenset[str] | None:
        if name in cache:
            return cache[name]
        if name in active:
            return None
        spec = specs.get(name)
        if isinstance(spec, SourceSpec):
            cache[name] = frozenset({name})
            return cache[name]
        if not isinstance(spec, (TransformSpec, DimensionSpec)):
            cache[name] = None
            return None
        try:
            query = spec.query_ast
            tables = list(query.find_all(ast.Table)) if query is not None else []
        except _PARSE_ERRORS:
            cache[name] = None
            return None
        if query is None:
            cache[name] = None
            return None
        cte_names = {cte.alias_or_name.identifier() for cte in query.ctes}
        tables = [table for table in tables if table.name.identifier() not in cte_names]
        if not tables:
            cache[name] = None
            return None
        active.add(name)
        result: set[str] = {name}
        for table in tables:
            upstream = _resolved_node(table.identifier(quotes=False), specs)
            ancestors = visit(upstream) if upstream else None
            if ancestors is None:
                active.remove(name)
                cache[name] = None
                return None
            result.update(ancestors)
        active.remove(name)
        cache[name] = frozenset(result)
        return cache[name]

    result = visit(parent)
    return set(result) if result is not None else None


def _context_link_index(specs: Mapping[str, NodeSpec]) -> _ContextLinkIndex:
    outgoing: defaultdict[str, list[_ContextLink]] = defaultdict(list)
    for source, spec in specs.items():
        if not isinstance(spec, LinkableNodeSpec):
            continue
        for link in spec.dimension_links:
            if not isinstance(
                link,
                (DimensionJoinLinkSpec, DimensionReferenceLinkSpec),
            ):
                continue
            target = link.rendered_dimension_node
            edge = (
                source,
                target,
                link,
                canonical_json(normalize_value(_dimension_link_contract(link))),
            )
            outgoing[source].append(edge)
    return {source: tuple(items) for source, items in outgoing.items()}


def _context_links(
    roots: set[str],
    targets: set[tuple[str, str | None]],
    specs: Mapping[str, NodeSpec],
    link_index: _ContextLinkIndex,
) -> tuple[list[tuple[str, str, object]], bool]:
    outgoing = link_index
    relevant = {}
    for target, requested_role in sorted(
        targets,
        key=lambda item: (item[0], item[1] is not None, item[1] or ""),
    ):
        if target in roots and not requested_role:
            continue
        roles = tuple(requested_role.split("->")) if requested_role else ()
        reachable = {(root, 0) for root in roots}
        queue = deque(sorted(reachable))
        incoming: dict[tuple[str, int], list[tuple[tuple[str, int], _ContextLink]]] = (
            defaultdict(list)
        )
        incomplete = False
        while queue:
            source, consumed = queue.popleft()
            for edge in outgoing.get(source, ()):
                _, next_node, link, _ = edge
                link_roles = (
                    tuple(link.role.split("->"))
                    if link.role and requested_role is not None
                    else ()
                )
                end = consumed + len(link_roles)
                if roles[consumed:end] != link_roles:
                    continue
                if next_node not in specs:
                    incomplete = True
                    continue
                state = (next_node, end)
                incoming[state].append(((source, consumed), edge))
                if state not in reachable:
                    reachable.add(state)
                    queue.append(state)
        destination = (target, len(roles))
        if destination not in reachable or incomplete:
            return [], False
        can_reach = {destination}
        queue = deque([destination])
        while queue:
            state = queue.popleft()
            for previous, edge in incoming.get(state, ()):
                source, next_node, link, link_key = edge
                relevant[(source, link_key)] = (
                    source,
                    next_node,
                    link,
                )
                if previous not in can_reach:
                    can_reach.add(previous)
                    queue.append(previous)
    return [relevant[key] for key in sorted(relevant)], True


def _demand_context(
    metric: MetricSpec,
    parent: str,
    refs: set[str],
    filters: tuple[str, ...],
    specs: Mapping[str, NodeSpec],
    available: Mapping[ComponentKey, ComponentResult],
    key: str,
    ancestry_cache: _AncestryCache,
    link_index: _ContextLinkIndex,
    join_column_cache: _JoinColumnCache,
    path_cache: _ContextPathCache,
) -> tuple[ComponentResult, tuple[tuple[str, ComponentKey], ...]]:
    roots = _ancestors(parent, specs, ancestry_cache)
    if roots is None:
        return (
            ComponentResult("unknown", "unknown", ("unsupported_context_ancestry",)),
            (),
        )
    output_dependencies: dict[tuple[str, str], tuple[str, ComponentKey]] = {}
    target_nodes: set[tuple[str, str | None]] = set()
    unresolved = False
    references = [(reference, False) for reference in sorted(refs)]
    try:
        for column in metric.query_ast.find_all(ast.Column):
            reference = column.identifier(quotes=False)
            ref = _dimension_ref(reference, specs)
            if ref and isinstance(specs[ref[0]], DimensionSpec):
                references.append((reference, True))
    except _PARSE_ERRORS:
        unresolved = True
    for reference, any_role in references:
        ref_target = _dimension_ref(reference, specs)
        if ref_target is None:
            unresolved = True
            continue
        node, column, role = ref_target
        output_key = ComponentKey(
            node,
            "output",
            column,
        )
        output = available.get(output_key)
        if output is None or output.digest is None:
            unresolved = True
            continue
        if (
            isinstance(specs.get(node), (SourceSpec, TransformSpec, DimensionSpec))
            and node in roots
            and (not role or any_role)
        ):
            output_dependencies[(node, column)] = (
                f"context-column:{node}.{column}",
                ComponentKey(node, "output", column),
            )
        elif isinstance(specs.get(node), MetricSpec):
            output_dependencies[(node, column)] = (
                f"context-metric:{node}.{column}",
                ComponentKey(node, "output", column),
            )
        else:
            output_dependencies[(node, column)] = (
                f"context-column:{node}.{column}",
                ComponentKey(node, "output", column),
            )
            target_nodes.add((node, None if any_role else role))

    path_key = (frozenset(roots), frozenset(target_nodes))
    if path_key not in path_cache:
        path_cache[path_key] = _context_links(roots, target_nodes, specs, link_index)
    links, complete = path_cache[path_key]
    dependencies = list(output_dependencies.values())
    link_contracts = []
    for source, target, raw_link in links:
        link = cast(DimensionJoinLinkSpec | DimensionReferenceLinkSpec, raw_link)
        columns, parsed = _join_columns(source, target, link, join_column_cache)
        if not parsed:
            unresolved = True
            continue
        link_contracts.append(
            {"source": source, "link": normalize_value(_dimension_link_contract(link))},
        )
        dependencies.append(
            (f"link-source-rows:{source}", ComponentKey(source, "rows")),
        )
        dependencies.extend(
            (f"join-key:{node}.{column}", ComponentKey(node, "output", column))
            for node, column in sorted(columns)
        )
    context_nodes = {node for node, _ in target_nodes} | {
        target for _, target, _ in links if target not in roots
    }
    dependencies.extend(
        (f"dimension-rows:{node}", ComponentKey(node, "rows"))
        for node in sorted(context_nodes)
    )
    context_primary_keys = {
        node: sorted(cast(LinkableNodeSpec, specs[node]).primary_key or [])
        for node in sorted(context_nodes)
        if isinstance(specs.get(node), LinkableNodeSpec)
    }
    if not complete or unresolved:
        return (
            ComponentResult(
                "unknown",
                "unknown",
                ("unresolved_dimension_context",),
            ),
            (),
        )
    return (
        _combine(
            ComponentKey(metric.rendered_name, "demand_context", key),
            {
                "links": sorted(link_contracts, key=canonical_json),
                "filters": list(filters),
                "context_primary_keys": context_primary_keys,
                "root_grains": {
                    node: sorted(cast(LinkableNodeSpec, specs[node]).primary_key or [])
                    for node in sorted(roots)
                    if isinstance(specs.get(node), (TransformSpec, DimensionSpec))
                },
            },
            dependencies,
            available,
            reason="candidate_dimension_paths_over_approximated",
        ),
        tuple(dependencies),
    )


def _combine(
    key: ComponentKey,
    payload: object,
    dependencies: list[tuple[str, ComponentKey]],
    available: Mapping[ComponentKey, ComponentResult],
    *,
    reason: str | None = None,
) -> ComponentResult:
    reasons = {reason} if reason else set()
    inputs = []
    precision: ComponentPrecision = "conservative" if reason else "precise"
    for label, provider in dependencies:
        result = available.get(provider)
        if result is None or result.digest is None:
            if result:
                reasons.update(result.reasons)
            reasons.add("unresolved_component_dependency")
            return ComponentResult("unknown", "unknown", tuple(sorted(reasons)))
        reasons.update(result.reasons)
        if result.precision == "conservative":
            precision = "conservative"
        inputs.append((label, provider, cast(SemanticFingerprint, result.fingerprint)))
    return ComponentResult(
        _component_hash(key, payload, inputs),
        precision,
        tuple(sorted(reasons)),
    )
