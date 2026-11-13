"""Configured v2 fingerprints with conservative dimension-link context."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from hashlib import sha256
from types import MappingProxyType
from typing import cast

from datajunction_server.construction.build_v3.filter_requirements import (
    RequirementEntry,
    apply_requirements,
)
from datajunction_server.errors import DJException
from datajunction_server.models.deployment import (
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    LinkableNodeSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.normalization import (
    canonical_json,
    normalize_field,
    normalize_value,
)
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentKey,
    ComponentPrecision,
    ComponentResult,
    NodeComponents,
    SemanticGraphSnapshot,
    V2FingerprintGraph,
    _component_hash,
    compute_v2_fingerprints,
)
from datajunction_server.semantic_fingerprints.v2_dependencies import (
    _effective_recommended_filters,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException

_PARSE_ERRORS = (DJParseException, TypeError, ValueError, AttributeError, KeyError)


def _contract(spec: MetricSpec | CubeSpec) -> dict[str, object]:
    rendered = spec.rendered_spec()
    fields: tuple[str, ...]
    if isinstance(rendered, MetricSpec):
        rendered.recommended_filters = _effective_recommended_filters(
            rendered.recommended_filters,
        )
        fields = (
            "required_dimensions",
            "reaggregate",
            "fixed_grain",
            "required_filters",
            "recommended_filters",
        )
    else:
        fields = ("metrics", "dimensions", "filters", "columns")
    return {
        field: normalize_field(rendered, field, structural_version=1)
        for field in fields
    }


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
) -> tuple[str, str] | None:
    reference = reference.split("[", 1)[0]
    parts = reference.split(".")
    if len(parts) < 2:
        return None
    node_name = _resolved_node(".".join(parts[:-1]), specs)
    return (node_name, parts[-1]) if node_name else None


def _filter_columns(filters: Iterable[str]) -> tuple[set[str], bool]:
    references: set[str] = set()
    for text in filters:
        try:
            query = parse(f"SELECT 1 WHERE ({text})")
        except _PARSE_ERRORS:
            return references, False
        for column in query.find_all(ast.Column):
            names = [part.name for part in column.namespace or ()]
            names.append(column.name.name)
            reference = ".".join(names)
            if column.role:
                reference += f"[{column.role}]"
            references.add(reference)
    return references, True


def _ancestors(parent: str, specs: Mapping[str, NodeSpec]) -> set[str] | None:
    result: set[str] = set()
    active: set[str] = set()

    def visit(name: str) -> bool:
        if name in active:
            return False
        if name in result:
            return True
        spec = specs.get(name)
        if isinstance(spec, SourceSpec):
            result.add(name)
            return True
        if not isinstance(spec, (TransformSpec, DimensionSpec)):
            return False
        try:
            query = parse(spec.rendered_query or "")
            tables = list(query.find_all(ast.Table))
        except _PARSE_ERRORS:
            return False
        if len(tables) != 1:
            return False
        upstream = _resolved_node(tables[0].identifier(quotes=False), specs)
        if upstream is None:
            return False
        active.add(name)
        valid = visit(upstream)
        active.remove(name)
        if valid:
            result.add(name)
        return valid

    return result if visit(parent) else None


def _metric_parent(metric: MetricSpec, specs: Mapping[str, NodeSpec]) -> str | None:
    try:
        query = parse(metric.rendered_query or "")
        tables = list(query.find_all(ast.Table))
    except _PARSE_ERRORS:
        return None
    if len(tables) != 1:
        return None
    parent = _resolved_node(tables[0].identifier(quotes=False), specs)
    return parent if parent and not isinstance(specs[parent], MetricSpec) else None


def _context_links(
    roots: set[str],
    targets: set[str],
    specs: Mapping[str, NodeSpec],
) -> tuple[list[tuple[str, str, object]], bool]:
    edges = [
        (source, link.rendered_dimension_node, link)
        for source, spec in specs.items()
        if isinstance(spec, LinkableNodeSpec)
        for link in spec.dimension_links
        if isinstance(link, (DimensionJoinLinkSpec, DimensionReferenceLinkSpec))
    ]
    outgoing: dict[str, list[tuple[str, object]]] = defaultdict(list)
    incoming: dict[str, list[str]] = defaultdict(list)
    for source, target, link in edges:
        outgoing[source].append((target, link))
        incoming[target].append(source)

    reachable = set(roots)
    queue = deque(roots)
    incomplete = False
    while queue:
        source = queue.popleft()
        for target, _ in outgoing[source]:
            if target not in specs:
                incomplete = True
            elif target not in reachable:
                reachable.add(target)
                queue.append(target)

    relevant = {}
    for target in targets:
        if target in roots:
            continue
        if target not in reachable or incomplete:
            return [], False
        can_reach = {target}
        queue = deque([target])
        while queue:
            node = queue.popleft()
            for source in incoming[node]:
                if source not in can_reach:
                    can_reach.add(source)
                    queue.append(source)
        for source, next_node, link in edges:
            if source in reachable and next_node in can_reach:
                relevant[
                    (source, canonical_json(normalize_value(link._comparison_key())))
                ] = (
                    source,
                    next_node,
                    link,
                )
    return [relevant[key] for key in sorted(relevant)], True


def _join_columns(
    source: str,
    target: str,
    link: DimensionJoinLinkSpec | DimensionReferenceLinkSpec,
) -> tuple[set[tuple[str, str]], bool]:
    if isinstance(link, DimensionReferenceLinkSpec):
        return {(source, link.node_column), (target, link.dimension_attribute)}, True
    if not link.rendered_join_on:
        return set(), link.join_type.value == "cross"
    try:
        query = parse(f"SELECT 1 WHERE ({link.rendered_join_on})")
    except _PARSE_ERRORS:
        return set(), False
    columns = set()
    for column in query.find_all(ast.Column):
        namespace = ".".join(part.name for part in column.namespace or ())
        if namespace == source:
            columns.add((source, column.name.name))
        elif namespace == target:
            columns.add((target, column.name.name))
        else:
            return set(), False
    return columns, bool(columns)


def _demand_context(
    metric: MetricSpec,
    parent: str,
    refs: set[str],
    filters: tuple[str, ...],
    specs: Mapping[str, NodeSpec],
    available: Mapping[ComponentKey, ComponentResult],
    key: str,
) -> tuple[ComponentResult, tuple[tuple[str, ComponentKey], ...]]:
    roots = _ancestors(parent, specs)
    if roots is None:
        return (
            ComponentResult("unknown", "unknown", ("unsupported_context_ancestry",)),
            (),
        )
    output_dependencies: dict[tuple[str, str], tuple[str, ComponentKey]] = {}
    target_nodes: set[str] = set()
    unresolved = False
    for reference in refs:
        ref_target = _dimension_ref(reference, specs)
        if ref_target is None:
            unresolved = True
            continue
        node, column = ref_target
        output_key = ComponentKey(
            node,
            "formula:output" if isinstance(specs.get(node), MetricSpec) else "output",
            column,
        )
        output = available.get(output_key)
        if output is None or output.digest is None:
            unresolved = True
            continue
        if (
            isinstance(specs.get(node), (SourceSpec, TransformSpec, DimensionSpec))
            and node in roots
        ):
            output_dependencies[(node, column)] = (
                f"context-column:{node}.{column}",
                ComponentKey(node, "formula:output", column)
                if isinstance(specs.get(node), MetricSpec)
                else ComponentKey(node, "output", column),
            )
        elif isinstance(specs.get(node), MetricSpec):
            output_dependencies[(node, column)] = (
                f"context-metric:{node}.{column}",
                ComponentKey(node, "formula:output", column),
            )
        else:
            target_nodes.add(node)

    links, complete = _context_links(roots, target_nodes, specs)
    dependencies = list(output_dependencies.values())
    link_contracts = []
    for source, target, raw_link in links:
        link = cast(DimensionJoinLinkSpec | DimensionReferenceLinkSpec, raw_link)
        columns, parsed = _join_columns(source, target, link)
        if not parsed:
            unresolved = True
            continue
        link_contracts.append(
            {"source": source, "link": normalize_value(link._comparison_key())},
        )
        dependencies.append(
            (f"link-source-rows:{source}", ComponentKey(source, "rows")),
        )
        dependencies.extend(
            (f"join-key:{node}.{column}", ComponentKey(node, "output", column))
            for node, column in sorted(columns)
            if node == source
        )
    context_nodes = target_nodes | {
        target for _, target, _ in links if target not in roots
    }
    dependencies.extend(
        (f"dimension:{node}", ComponentKey(node, "full"))
        for node in sorted(context_nodes)
    )
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
                "root_grains": {
                    node: sorted(specs[node].primary_key or [])
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


async def compute_configured_v2_fingerprints(
    snapshot: SemanticGraphSnapshot,
) -> V2FingerprintGraph:
    """Fingerprint simple configured metrics with all-candidate join context."""
    specs = snapshot.specs
    formula_specs = {}
    unavailable: set[str] = set()
    for name, spec in specs.items():
        formula_specs[name] = (
            spec.model_copy(
                update={
                    "required_dimensions": [],
                    "fixed_grain": None,
                    "reaggregate": None,
                    "required_filters": [],
                    "recommended_filters": [],
                },
                deep=True,
            )
            if isinstance(spec, MetricSpec)
            else spec
        )
        if hasattr(formula_specs[name], "_query_ast"):
            formula_specs[name]._query_ast = None
    formula_graph = await compute_v2_fingerprints(
        SemanticGraphSnapshot(formula_specs, snapshot.resolved_columns),
        _component_namespace="formula:",
    )
    unsupported_metrics = {
        name
        for name, spec in specs.items()
        if isinstance(spec, MetricSpec)
        and formula_graph.nodes[name].precision != "precise"
    }
    intrinsic = await compute_v2_fingerprints(snapshot)
    available = {**intrinsic.components, **formula_graph.components}
    demands: dict[
        tuple[str, str],
        tuple[MetricSpec, str, set[str], tuple[str, ...], dict],
    ] = {}
    consumers: dict[str, list[tuple[str, str]]] = defaultdict(list)

    def add_demand(
        consumer: str,
        metric: MetricSpec,
        dimensions: tuple[str, ...] = (),
        filters: tuple[str, ...] = (),
    ) -> None:
        parent = _metric_parent(metric, specs)
        if parent is None:
            unavailable.add(consumer)
            return
        required = metric.rendered_required_filters or []
        recommended = _effective_recommended_filters(
            metric.rendered_recommended_filters,
        )
        parameterized = consumer == metric.rendered_name
        entries = [
            RequirementEntry(
                attributes=requirement.dimension,
                default=requirement.default,
                required=is_required and not parameterized,
                metrics=frozenset({metric.rendered_name}),
            )
            for is_required, requirements in ((True, required), (False, recommended))
            for requirement in requirements
        ]
        try:
            effective_filters, _ = apply_requirements(entries, list(filters))
        except (DJException, TypeError, ValueError):
            unavailable.add(consumer)
            return

        refs = {
            f"{parent}.{reference}" if "." not in reference else reference
            for reference in (*dimensions, *metric.rendered_required_dimensions)
        }
        refs.update(
            f"{parent}.{reference}" if "." not in reference else reference
            for reference in metric.rendered_fixed_grain or ()
        )
        refs.update(
            reference
            for requirement in [*required, *recommended]
            for reference in requirement.dimension
        )
        if metric.rendered_reaggregate:
            refs.update(rule.dimension for rule in metric.rendered_reaggregate.rules)
        filter_refs, parsed = _filter_columns(effective_filters)
        if not parsed:
            unavailable.add(consumer)
            return
        refs.update(
            f"{parent}.{reference}" if "." not in reference else reference
            for reference in filter_refs
        )
        if parameterized:
            refs.update(
                reference
                for requirement in required
                if not requirement.default
                for reference in requirement.dimension
            )

        contract = _contract(metric)
        demand_key = canonical_json(
            {
                "metric": metric.rendered_name,
                "parent": parent,
                "dimensions": sorted(dimensions),
                "filters": sorted(effective_filters),
                "contract": contract,
            },
        )
        compact_key = sha256(demand_key.encode("utf-8")).hexdigest()
        demands.setdefault(
            (metric.rendered_name, compact_key),
            (metric, parent, refs, tuple(sorted(effective_filters)), contract),
        )
        consumers[consumer].append((metric.rendered_name, compact_key))

    for name, spec in specs.items():
        if isinstance(spec, MetricSpec):
            add_demand(name, spec)
        elif isinstance(spec, CubeSpec):
            for metric_name in spec.rendered_metrics:
                metric = specs.get(metric_name)
                if not isinstance(metric, MetricSpec):
                    unavailable.add(name)
                    continue
                add_demand(
                    name,
                    metric,
                    tuple(spec.rendered_dimensions),
                    tuple(spec.rendered_filters),
                )

    nodes = dict(intrinsic.nodes)
    components = dict(available)
    bindings = dict(intrinsic.bindings)
    bindings.update(formula_graph.bindings)
    for key, (metric, parent, refs, filters, contract) in demands.items():
        metric_name, demand_id = key
        context_key = ComponentKey(metric_name, "demand_context", demand_id)
        context, context_dependencies = _demand_context(
            metric,
            parent,
            refs,
            filters,
            specs,
            available,
            demand_id,
        )
        components[context_key] = context
        bindings[context_key] = context_dependencies

    unknown = ComponentResult("unknown", "unknown", ("configured_demand_unavailable",))
    consumer_names = {
        name for name, spec in specs.items() if isinstance(spec, (MetricSpec, CubeSpec))
    }
    for consumer in sorted(consumer_names):
        spec = specs[consumer]
        references = consumers.get(consumer, [])
        row_dependencies = []
        value_dependencies = []
        outputs: dict[str, ComponentResult] = {}
        for index, ref in enumerate(references):
            metric_name, demand_id = ref
            demand_contract = demands[(metric_name, demand_id)][4]
            context_key = ComponentKey(metric_name, "demand_context", demand_id)
            if metric_name in unsupported_metrics:
                fallback_key = ComponentKey(metric_name, "full")
                rows_key = ComponentKey(metric_name, "demand_rows", demand_id)
                row = _combine(
                    rows_key,
                    {"metric": metric_name, "fallback": "intrinsic_full"},
                    [("intrinsic-full", fallback_key), ("context", context_key)],
                    components,
                    reason="unsupported_metric_formula_fallback",
                )
                components[rows_key] = row
                bindings[rows_key] = (
                    ("intrinsic-full", fallback_key),
                    ("context", context_key),
                )
                value_key = ComponentKey(metric_name, "demand_value", demand_id)
                value_deps = [
                    ("intrinsic-full", fallback_key),
                    ("context", context_key),
                ]
                value = _combine(
                    value_key,
                    demand_contract,
                    value_deps,
                    components,
                    reason="unsupported_metric_formula_fallback",
                )
                components[value_key] = value
                bindings[value_key] = tuple(value_deps)
                row_dependencies.append((f"metric:{index}:rows", rows_key))
                value_dependencies.append((f"metric:{index}:value", value_key))
                outputs.update(intrinsic.nodes[metric_name].outputs)
                continue
            formula = formula_graph.nodes.get(metric_name)
            rows_key = ComponentKey(metric_name, "demand_rows", demand_id)
            row = _combine(
                rows_key,
                {"metric": metric_name},
                [
                    ("formula-rows", ComponentKey(metric_name, "formula:rows")),
                    ("context", context_key),
                ],
                components,
            )
            components[rows_key] = row
            bindings[rows_key] = (
                ("formula-rows", ComponentKey(metric_name, "formula:rows")),
                ("context", context_key),
            )
            value_key = ComponentKey(metric_name, "demand_value", demand_id)
            value_deps = [
                (
                    f"metric-output:{output}",
                    ComponentKey(metric_name, "formula:output", output),
                )
                for output in (formula.outputs if formula else {})
            ]
            value_deps.extend((("rows", rows_key), ("context", context_key)))
            value = _combine(value_key, demand_contract, value_deps, components)
            components[value_key] = value
            bindings[value_key] = tuple(value_deps)
            row_dependencies.append((f"metric:{index}:rows", rows_key))
            value_dependencies.append((f"metric:{index}:value", value_key))
            if formula:
                outputs.update(formula.outputs)

        if consumer in unavailable or not value_dependencies:
            row_dependencies.append(
                ("unavailable", ComponentKey(consumer, "missing_configured_demand")),
            )
            value_dependencies.append(
                ("unavailable", ComponentKey(consumer, "missing_configured_demand")),
            )
            components[ComponentKey(consumer, "missing_configured_demand")] = unknown

        row_key = ComponentKey(consumer, "configured_rows")
        if isinstance(spec, CubeSpec):
            row_dependencies.extend(value_dependencies)
        rows = _combine(
            row_key,
            {"dimensions": list(spec.rendered_dimensions)}
            if isinstance(spec, CubeSpec)
            else {"metric": consumer},
            row_dependencies,
            components,
            reason="cube_blending_rows_conservative"
            if isinstance(spec, CubeSpec)
            else None,
        )
        components[row_key] = rows
        bindings[row_key] = tuple(row_dependencies)
        full_key = ComponentKey(consumer, "configured_full")
        full_dependencies = [("rows", row_key), *value_dependencies]
        full = _combine(full_key, _contract(spec), full_dependencies, components)
        components[full_key] = full
        bindings[full_key] = tuple(full_dependencies)
        nodes[consumer] = NodeComponents(
            outputs=MappingProxyType(outputs),
            rows=rows,
            full=full,
            precision=full.precision,
            reasons=full.reasons,
        )

    return V2FingerprintGraph(
        nodes=MappingProxyType(nodes),
        components=MappingProxyType(components),
        bindings=MappingProxyType(bindings),
    )
