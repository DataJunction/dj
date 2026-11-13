"""Configured v2 fingerprints with role-aware dimension-link context."""

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
from datajunction_server.naming import amenable_name
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
    _opaque_query_tokens,
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
_FilterColumnCache = dict[str, tuple[frozenset[str], bool]]
_JoinColumnCache = dict[
    tuple[str, str, str, str],
    tuple[frozenset[tuple[str, str]], bool],
]
_ContextPaths = tuple[list[tuple[str, str, object]], bool]
_ContextPathCache = dict[
    tuple[frozenset[str], frozenset[tuple[str, str | None]]],
    _ContextPaths,
]


def _contract(spec: MetricSpec | CubeSpec) -> dict[str, object]:
    rendered = spec.rendered_spec()
    if isinstance(spec, MetricSpec):
        rendered._query_ast = spec._query_ast
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
) -> tuple[str, str, str] | None:
    reference, _, role = reference.partition("[")
    parts = reference.split(".")
    if len(parts) < 2:
        return None
    node_name = _resolved_node(".".join(parts[:-1]), specs)
    return (node_name, parts[-1], role.removesuffix("]")) if node_name else None


def _filter_columns(
    filters: Iterable[str],
    cache: _FilterColumnCache,
) -> tuple[set[str], bool]:
    references: set[str] = set()
    for text in filters:
        parsed = cache.get(text)
        if parsed is None:
            try:
                query = parse(f"SELECT 1 WHERE ({text})")
            except _PARSE_ERRORS:
                parsed = (frozenset(), False)
            else:
                parsed = (
                    frozenset(
                        ".".join(
                            [
                                *(part.name for part in column.namespace or ()),
                                column.name.name,
                            ],
                        )
                        + (f"[{column.role}]" if column.role else "")
                        for column in query.find_all(ast.Column)
                    ),
                    True,
                )
            cache[text] = parsed
        references.update(parsed[0])
        if not parsed[1]:
            return references, False
    return references, True


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


def _metric_parent(metric: MetricSpec, specs: Mapping[str, NodeSpec]) -> str | None:
    try:
        query = metric.query_ast
        tables = list(query.find_all(ast.Table)) if query is not None else []
    except _PARSE_ERRORS:
        return None
    if len(tables) != 1:
        return None
    parent = _resolved_node(tables[0].identifier(quotes=False), specs)
    return parent if parent and not isinstance(specs[parent], MetricSpec) else None


def _derived_inputs(
    metric: MetricSpec,
    specs: Mapping[str, NodeSpec],
) -> tuple[MetricSpec, ...] | None:
    try:
        query = metric.query_ast
        if (
            query is None
            or query.ctes
            or query.select.from_ is not None
            or any(query.find_all(ast.Table))
            or any(query.find_all(ast.Wildcard))
        ):
            return None
        inputs = {}
        for column in query.find_all(ast.Column):
            name = _resolved_node(column.identifier(quotes=False), specs)
            spec = specs.get(name) if name else None
            if not isinstance(spec, MetricSpec) or column.role:
                return None
            inputs[spec.rendered_name] = spec
        return tuple(inputs[name] for name in sorted(inputs))
    except _PARSE_ERRORS:
        return None


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
                canonical_json(normalize_value(link._comparison_key())),
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


def _join_columns(
    source: str,
    target: str,
    link: DimensionJoinLinkSpec | DimensionReferenceLinkSpec,
    cache: _JoinColumnCache,
) -> tuple[set[tuple[str, str]], bool]:
    if isinstance(link, DimensionReferenceLinkSpec):
        return {(source, link.node_column), (target, link.dimension_attribute)}, True
    cache_key = (source, target, link.rendered_join_on or "", link.join_type.value)
    if cache_key in cache:
        cached_columns, parsed = cache[cache_key]
        return set(cached_columns), parsed
    if not link.rendered_join_on:
        parsed = link.join_type.value == "cross"
        cache[cache_key] = (frozenset(), parsed)
        return set(), parsed
    try:
        query = parse(f"SELECT 1 WHERE ({link.rendered_join_on})")
    except _PARSE_ERRORS:
        cache[cache_key] = (frozenset(), False)
        return set(), False
    columns: set[tuple[str, str]] = set()
    for column in query.find_all(ast.Column):
        namespace = ".".join(part.name for part in column.namespace or ())
        if namespace == source:
            columns.add((source, column.name.name))
        elif namespace == target:
            columns.add((target, column.name.name))
        else:
            cache[cache_key] = (frozenset(), False)
            return set(), False
    result = (frozenset(columns), bool(columns))
    cache[cache_key] = result
    return set(result[0]), result[1]


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
    context_nodes = {node for node, _ in target_nodes} | {
        target for _, target, _ in links if target not in roots
    }
    dependencies.extend(
        (f"dimension:{node}", ComponentKey(node, "calculation"))
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


async def compute_configured_v2_fingerprints(
    snapshot: SemanticGraphSnapshot,
) -> V2FingerprintGraph:
    """Fingerprint configured metric dependencies and matching join roles."""
    specs = snapshot._specs
    metric_relations = set()
    for spec in specs.values():
        if not isinstance(spec, (TransformSpec, DimensionSpec)):
            continue
        try:
            query = spec.query_ast
            for table in query.find_all(ast.Table) if query is not None else ():
                parent = _resolved_node(table.identifier(quotes=False), specs)
                if parent and isinstance(specs[parent], MetricSpec):
                    metric_relations.add(parent)
        except _PARSE_ERRORS:
            continue
    pending = deque(sorted(metric_relations))
    while pending:
        metric_name = pending.popleft()
        provider_metric = cast(MetricSpec, specs[metric_name])
        try:
            query = provider_metric.query_ast
            references = (
                [
                    item.identifier(quotes=False)
                    for kind in (ast.Table, ast.Column)
                    for item in query.find_all(kind)
                ]
                if query is not None
                else []
            )
        except _PARSE_ERRORS:
            continue
        for reference in references:
            parent = _resolved_node(reference, specs)
            if (
                parent
                and isinstance(specs[parent], MetricSpec)
                and parent not in metric_relations
            ):
                metric_relations.add(parent)
                pending.append(parent)
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
                deep=False,
            )
            if isinstance(spec, MetricSpec) and name not in metric_relations
            else spec
        )
    formula_graph = await compute_v2_fingerprints(
        snapshot._with_internal_specs(formula_specs),
    )
    unsupported_metrics = {
        name
        for name, spec in specs.items()
        if isinstance(spec, MetricSpec)
        and formula_graph.nodes[name].precision != "precise"
    }
    available = formula_graph.components
    link_index = _context_link_index(specs)
    ancestry_cache: _AncestryCache = {}
    filter_column_cache: _FilterColumnCache = {}
    join_column_cache: _JoinColumnCache = {}
    path_cache: _ContextPathCache = {}
    metric_parent_cache: dict[str, str | None] = {}
    contracts: dict[str, dict[str, object]] = {}
    requirements_cache: dict[str, tuple[str, ...] | None] = {}
    demands: dict[
        tuple[str, str],
        tuple[MetricSpec, str, set[str], tuple[str, ...], dict],
    ] = {}
    consumers: dict[str, set[tuple[str, str]]] = defaultdict(set)
    derived_formulas: dict[str, dict[str, object]] = defaultdict(dict)
    formula_contracts: dict[str, object] = {}
    derived_inputs: dict[str, tuple[MetricSpec, ...] | None] = {}

    def contract_for(spec: MetricSpec | CubeSpec) -> dict[str, object]:
        if spec.rendered_name not in contracts:
            contracts[spec.rendered_name] = _contract(spec)
        return contracts[spec.rendered_name]

    def add_demand(
        consumer: str,
        metric: MetricSpec,
        dimensions: tuple[str, ...] = (),
        filters: tuple[str, ...] = (),
        active: frozenset[str] = frozenset(),
    ) -> None:
        metric_name = metric.rendered_name
        if metric_name in active:
            unavailable.add(consumer)
            return
        if metric_name not in metric_parent_cache:
            metric_parent_cache[metric_name] = _metric_parent(metric, specs)
        parent = metric_parent_cache[metric_name]
        required = metric.rendered_required_filters or []
        recommended = _effective_recommended_filters(
            metric.rendered_recommended_filters,
        )
        # Definitions retain required filters as query-time parameters.
        entries = [
            RequirementEntry(
                attributes=requirement.dimension,
                default=requirement.default,
                required=False,
                metrics=frozenset({metric.rendered_name}),
            )
            for requirement in [*required, *recommended]
        ]
        requirements_key = canonical_json(
            [
                [
                    (entry.attributes, entry.default, entry.required)
                    for entry in entries
                ],
                filters,
            ],
        )
        if requirements_key not in requirements_cache:
            try:
                applied, _ = apply_requirements(entries, list(filters))
                requirements_cache[requirements_key] = tuple(applied)
            except (DJException, TypeError, ValueError):
                requirements_cache[requirements_key] = None
        effective_filters = requirements_cache[requirements_key]
        if effective_filters is None:
            unavailable.add(consumer)
            return

        refs = set(dimensions) | set(metric.rendered_required_dimensions)
        refs.update(metric.rendered_fixed_grain or ())
        refs.update(
            reference
            for requirement in [*required, *recommended]
            for reference in requirement.dimension
        )
        if metric.rendered_reaggregate:
            refs.update(rule.dimension for rule in metric.rendered_reaggregate.rules)
        filter_refs, parsed = _filter_columns(
            effective_filters,
            filter_column_cache,
        )
        if not parsed:
            unavailable.add(consumer)
            return
        refs.update(filter_refs)
        contract = contract_for(metric)
        if parent is None:
            if metric_name not in derived_inputs:
                derived_inputs[metric_name] = _derived_inputs(metric, specs)
            children = derived_inputs[metric_name]
            if children is None:
                unavailable.add(consumer)
                return
            if metric_name not in formula_contracts:
                try:
                    formula_contracts[metric_name] = {
                        "query": _opaque_query_tokens(metric.rendered_query or ""),
                        "contract": contract,
                    }
                except _PARSE_ERRORS:
                    unavailable.add(consumer)
                    return
            derived_formulas[consumer][metric_name] = formula_contracts[metric_name]
            for child in children:
                add_demand(
                    consumer,
                    child,
                    tuple(sorted(refs)),
                    effective_filters,
                    active | {metric_name},
                )
            return
        refs = {
            f"{parent}.{reference}" if "." not in reference else reference
            for reference in refs
        }
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
        consumers[consumer].add((metric.rendered_name, compact_key))

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

    nodes = dict(formula_graph.nodes)
    components = dict(available)
    bindings = dict(formula_graph.bindings)
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
            ancestry_cache,
            link_index,
            join_column_cache,
            path_cache,
        )
        components[context_key] = context
        bindings[context_key] = context_dependencies

    unknown = ComponentResult("unknown", "unknown", ("configured_demand_unavailable",))
    consumer_names = {
        name for name, spec in specs.items() if isinstance(spec, (MetricSpec, CubeSpec))
    }
    for consumer in sorted(consumer_names):
        spec = cast(MetricSpec | CubeSpec, specs[consumer])
        references = sorted(consumers.get(consumer, ()))
        row_dependencies = []
        value_dependencies = []
        outputs: dict[str, ComponentResult] = {}
        for index, ref in enumerate(references):
            metric_name, demand_id = ref
            demand_contract = demands[(metric_name, demand_id)][4]
            context_key = ComponentKey(metric_name, "demand_context", demand_id)
            if metric_name in unsupported_metrics:
                fallback_key = ComponentKey(metric_name, "calculation")
                rows_key = ComponentKey(metric_name, "demand_rows", demand_id)
                row = _combine(
                    rows_key,
                    {"metric": metric_name, "fallback": "formula_calculation"},
                    [("formula-calculation", fallback_key), ("context", context_key)],
                    components,
                    reason="unsupported_metric_formula_fallback",
                )
                components[rows_key] = row
                bindings[rows_key] = (
                    ("formula-calculation", fallback_key),
                    ("context", context_key),
                )
                value_key = ComponentKey(metric_name, "demand_value", demand_id)
                value_deps = [
                    ("formula-calculation", fallback_key),
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
                outputs.update(formula_graph.nodes[metric_name].outputs)
                continue
            formula = formula_graph.nodes.get(metric_name)
            rows_key = ComponentKey(metric_name, "demand_rows", demand_id)
            row = _combine(
                rows_key,
                {"metric": metric_name},
                [
                    ("formula-rows", ComponentKey(metric_name, "rows")),
                    ("context", context_key),
                ],
                components,
            )
            components[rows_key] = row
            bindings[rows_key] = (
                ("formula-rows", ComponentKey(metric_name, "rows")),
                ("context", context_key),
            )
            value_key = ComponentKey(metric_name, "demand_value", demand_id)
            value_deps = [
                (
                    f"metric-output:{output}",
                    ComponentKey(metric_name, "output", output),
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

        formulas = derived_formulas.get(consumer, {})
        if consumer in unavailable or (not value_dependencies and not formulas):
            row_dependencies.append(
                ("unavailable", ComponentKey(consumer, "missing_configured_demand")),
            )
            value_dependencies.append(
                ("unavailable", ComponentKey(consumer, "missing_configured_demand")),
            )
            components[ComponentKey(consumer, "missing_configured_demand")] = unknown

        row_key = ComponentKey(consumer, "configured_rows")
        if isinstance(spec, CubeSpec) or formulas:
            row_dependencies.extend(value_dependencies)
        rows = _combine(
            row_key,
            {
                "consumer": consumer,
                "dimensions": list(spec.rendered_dimensions)
                if isinstance(spec, CubeSpec)
                else None,
                "derived_formulas": formulas,
            },
            row_dependencies,
            components,
            reason="cube_blending_rows_conservative"
            if isinstance(spec, CubeSpec)
            else "derived_metric_dependencies_conservative"
            if formulas
            else None,
        )
        components[row_key] = rows
        bindings[row_key] = tuple(row_dependencies)
        full_key = ComponentKey(consumer, "configured_full")
        full_dependencies = [("rows", row_key), *value_dependencies]
        full = _combine(full_key, contract_for(spec), full_dependencies, components)
        components[full_key] = full
        bindings[full_key] = tuple(full_dependencies)
        if isinstance(spec, MetricSpec) and formulas:
            outputs = {amenable_name(consumer): full}
        nodes[consumer] = NodeComponents(
            outputs=MappingProxyType(outputs),
            rows=rows,
            full=full,
            precision=full.precision,
            reasons=full.reasons,
        )

    bindings = {
        key: tuple(sorted(value, key=lambda edge: (edge[0], edge[1])))
        for key, value in bindings.items()
    }
    return V2FingerprintGraph(
        nodes=MappingProxyType(nodes),
        components=MappingProxyType(components),
        bindings=MappingProxyType(bindings),
    )
