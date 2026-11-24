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
    normalize_cube_columns,
    normalize_fixed_grain,
    normalize_sequence,
    normalize_value,
)
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentKey,
    ComponentPrecision,
    ComponentResult,
    NodeComponents,
    SemanticGraphSnapshot,
    UnorderedPayload,
    V2FingerprintGraph,
    _component_hash,
    compute_v2_fingerprints,
)
from datajunction_server.semantic_fingerprints.v2_dependencies import (
    _canonical_dimension_join_on,
    _canonicalize_expression_ast,
    _column_reference_key,
    _dimension_link_contract,
    _effective_filter_requirements,
    _exact_numeric_literals,
    _has_lossy_sql_tokens,
    _join_columns,
    _JoinColumnCache,
    _opaque_query_tokens,
    _required_dimensions_contract,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import cached_parse as parse
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
_FilterColumnCache = dict[str, tuple[frozenset[str], bool, ast.Query | None]]
_FilterSemanticsCache = dict[
    tuple[tuple[str, ...], frozenset[str]],
    tuple[object, tuple[ComponentKey, ...]] | None,
]
_LinkContractCache = dict[tuple[str, str, str], object]
_ContextPaths = tuple[list[tuple[str, str, object]], bool]
_ContextPathCache = dict[
    tuple[frozenset[str], frozenset[tuple[str, str | None]]],
    _ContextPaths,
]


def _contract(spec: MetricSpec | CubeSpec) -> dict[str, object]:
    if isinstance(spec, MetricSpec):
        return {
            "required_dimensions": _required_dimensions_contract(spec),
            "reaggregate": normalize_value(spec.rendered_reaggregate),
            "fixed_grain": normalize_fixed_grain(spec.rendered_fixed_grain),
        }
    return {
        "metrics": normalize_sequence(spec.rendered_metrics, preserve_order=False),
        "dimensions": normalize_sequence(
            spec.rendered_dimensions,
            preserve_order=False,
        ),
        "filters": normalize_sequence(spec.rendered_filters, preserve_order=False),
        "columns": normalize_cube_columns(spec.matched_rendered_columns),
    }


def _semantic_reference(
    reference: str,
    specs: Mapping[str, NodeSpec],
) -> tuple[object, tuple[ComponentKey, ...]] | None:
    ref = _dimension_ref(reference, specs)
    if ref is None:
        if "." not in reference and "[" not in reference:
            return {"field": reference}, ()
        return None
    node, field, role = ref
    output = ComponentKey(node, "output", field)
    rows = ComponentKey(node, "rows")
    return {
        "output": output,
        "source_rows": rows,
        "role": role,
    }, (output, rows)


def _semantic_column_reference(
    reference: str,
    specs: Mapping[str, NodeSpec],
) -> tuple[object, tuple[ComponentKey, ...]] | None:
    node = _resolved_node(reference, specs)
    if node and isinstance(specs[node], MetricSpec):
        component = ComponentKey(node, "full")
        if _has_metric_context(specs[node]):
            return {
                "metric": reference,
                "configured_metric": component,
            }, (component,)
        return {"metric": component}, (component,)
    return _semantic_reference(reference, specs)


def _has_metric_context(metric: MetricSpec) -> bool:
    return bool(
        metric.rendered_required_dimensions
        or metric.rendered_fixed_grain is not None
        or metric.rendered_reaggregate is not None
        or _effective_filter_requirements(metric),
    )


def _derived_formula_contract(
    metric: MetricSpec,
    specs: Mapping[str, NodeSpec],
    own_contract: object,
    own_dependencies: Iterable[ComponentKey],
) -> tuple[object, tuple[ComponentKey, ...]]:
    raw_query = metric.rendered_query or ""
    try:
        query = metric.query_ast
        if (
            query is None
            or query.ctes
            or query.select.from_ is not None
            or len(query.select.projection) != 1
            or _has_lossy_sql_tokens(raw_query)
        ):
            raise ValueError("unsupported_derived_metric_expression")
        projection = query.select.projection[0]
        expression = (
            projection.child if isinstance(projection, ast.Alias) else projection
        )
        bound_columns = {}
        dependencies = set(own_dependencies)
        has_contextual_input = False
        for column in expression.find_all(ast.Column):
            name = _resolved_node(column.identifier(quotes=False), specs)
            if not name or not isinstance(specs.get(name), MetricSpec) or column.role:
                raise ValueError("unresolved_derived_metric_input")
            child = cast(MetricSpec, specs[name])
            has_contextual_input |= _has_metric_context(child)
            component = ComponentKey(name, "full")
            dependencies.add(component)
            bound_columns[_column_reference_key(column)] = (component, "value")
        if has_contextual_input:
            return {
                "raw_query": _opaque_query_tokens(raw_query),
                "metric_contract": own_contract,
            }, tuple(
                sorted(
                    dependencies,
                    key=lambda item: (item.kind, item.field, item.token),
                ),
            )
        ast_value = _canonicalize_expression_ast(
            expression,
            bound_columns,
            require_bound=True,
        )

        def replace_metric_names(value: object) -> object:
            if isinstance(value, list):
                return [replace_metric_names(item) for item in value]
            if not isinstance(value, dict):
                return value
            if value.get("type") == "Column":
                name = value["fields"]["name"]["fields"]
                name["name"] = "$metric"
                name["namespace"] = None
            return {key: replace_metric_names(item) for key, item in value.items()}

        return {
            "ast": replace_metric_names(ast_value),
            "numeric_literals": _exact_numeric_literals(
                expression,
                query,
                raw_query,
            ),
            "metric_contract": own_contract,
        }, tuple(
            sorted(dependencies, key=lambda item: (item.kind, item.field, item.token)),
        )
    except _PARSE_ERRORS:
        return {
            "raw_query": _opaque_query_tokens(raw_query),
            "metric_contract": own_contract,
        }, tuple(
            sorted(
                own_dependencies,
                key=lambda item: (item.kind, item.field, item.token),
            ),
        )


def _resolve_filter_column(
    column: ast.Column,
    specs: Mapping[str, NodeSpec],
    known_references: Iterable[str],
) -> tuple[str, str, str] | None:
    identifier = column.identifier(quotes=False)
    reference = identifier + (f"[{column.role}]" if column.role else "")
    resolved = _dimension_ref(reference, specs)
    if resolved is not None:
        return resolved
    if "." not in identifier:
        matches = {
            item
            for candidate in known_references
            if (item := _dimension_ref(candidate, specs)) is not None
            and item[1].casefold() == column.name.name.casefold()
            and item[2] == (column.role or "")
        }
        if len(matches) == 1:
            return next(iter(matches))
    return None


def _semantic_filters(
    filters: Iterable[str],
    specs: Mapping[str, NodeSpec],
    known_references: Iterable[str] = (),
    filter_column_cache: _FilterColumnCache | None = None,
) -> tuple[object, tuple[ComponentKey, ...]] | None:
    values: list[object] = []
    dependencies: set[ComponentKey] = set()
    for text in filters:
        sql = f"SELECT 1 WHERE ({text})"
        try:
            if _has_lossy_sql_tokens(sql):
                values.append(text)
                continue
            cached = (filter_column_cache or {}).get(text)
            if cached is not None and not cached[1]:
                values.append(text)
                continue
            query = (
                cached[2]
                if cached is not None and cached[2] is not None
                else parse(sql)
            )
            expression = query.select.where
            if expression is None:
                raise ValueError("filter_expression_missing")
            bound_columns: dict[str, tuple[object, str]] = {}
            filter_dependencies: set[ComponentKey] = set()
            for column in query.find_all(ast.Column):
                reference = _resolve_filter_column(column, specs, known_references)
                if reference is None:
                    raise ValueError("unresolved_filter_column")
                node, field, role = reference
                output = ComponentKey(node, "output", field)
                rows = ComponentKey(node, "rows")
                filter_dependencies.update((output, rows))
                bound_columns[_column_reference_key(column)] = (
                    (output, role),
                    field,
                )
            values.append(
                {
                    "ast": _canonicalize_expression_ast(
                        expression,
                        bound_columns,
                        require_bound=True,
                    ),
                    "numeric_literals": _exact_numeric_literals(
                        expression,
                        query,
                        sql,
                    ),
                },
            )
            dependencies.update(filter_dependencies)
        except _PARSE_ERRORS:
            values.append(text)
    return UnorderedPayload(tuple(values)), tuple(dependencies)


def _cached_semantic_filters(
    filters: tuple[str, ...],
    specs: Mapping[str, NodeSpec],
    known_references: Iterable[str],
    semantics_cache: _FilterSemanticsCache,
    column_cache: _FilterColumnCache,
) -> tuple[object, tuple[ComponentKey, ...]] | None:
    key = (filters, frozenset(known_references))
    if key not in semantics_cache:
        semantics_cache[key] = _semantic_filters(
            filters,
            specs,
            known_references,
            column_cache,
        )
    return semantics_cache[key]


def _semantic_link_contract(
    source: str,
    target: str,
    link: DimensionJoinLinkSpec | DimensionReferenceLinkSpec,
) -> object | None:
    contract = normalize_value(_dimension_link_contract(link))
    if isinstance(link, DimensionJoinLinkSpec) and link.rendered_join_on:
        try:
            contract["join_on"] = _canonical_dimension_join_on(
                link.rendered_join_on,
                source,
                target,
            )
        except _PARSE_ERRORS:
            return normalize_value(_dimension_link_contract(link))
        contract.pop("dimension_node", None)
    else:
        contract.pop("dimension_node", None)
    return contract


def _semantic_contract(
    spec: MetricSpec | CubeSpec,
    specs: Mapping[str, NodeSpec],
) -> tuple[dict[str, object], tuple[ComponentKey, ...]] | None:
    dependencies: set[ComponentKey] = set()
    if isinstance(spec, MetricSpec):
        required_dimensions = []
        for reference in _required_dimensions_contract(spec):
            semantic = _semantic_reference(reference, specs)
            if semantic is None:
                return None
            value, refs = semantic
            required_dimensions.append(value)
            dependencies.update(refs)
        fixed_grain = None
        normalized_fixed_grain = normalize_fixed_grain(spec.rendered_fixed_grain)
        if normalized_fixed_grain is not None:
            fixed_grain = []
            for reference in normalized_fixed_grain:
                semantic = _semantic_reference(reference, specs)
                if semantic is None:
                    return None
                value, refs = semantic
                fixed_grain.append(value)
                dependencies.update(refs)
        reaggregate = normalize_value(spec.rendered_reaggregate)
        if reaggregate is not None:
            for rule in reaggregate["rules"]:
                dimension = _semantic_reference(rule["dimension"], specs)
                if dimension is None:
                    return None
                rule["dimension"], refs = dimension
                dependencies.update(refs)
        return {
            "required_dimensions": UnorderedPayload(tuple(required_dimensions)),
            "fixed_grain": (
                None if fixed_grain is None else UnorderedPayload(tuple(fixed_grain))
            ),
            "reaggregate": reaggregate,
        }, tuple(dependencies)

    columns: list[dict[str, object]] = []
    for column in spec.matched_rendered_columns:
        if not column.partition:
            continue
        column_reference = _semantic_column_reference(column.name, specs)
        if column_reference is None:
            return None
        reference_value, reference_dependencies = column_reference
        dependencies.update(reference_dependencies)
        columns.append(
            {
                "reference": reference_value,
                "partition": normalize_value(column.partition),
            },
        )
    return {"columns": UnorderedPayload(tuple(columns))}, tuple(dependencies)


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
                parsed = (frozenset(), False, None)
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
                    query,
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
    filter_semantics_cache: _FilterSemanticsCache,
    filter_column_cache: _FilterColumnCache,
    link_contract_cache: _LinkContractCache,
) -> tuple[ComponentResult, tuple[tuple[str, ComponentKey], ...]]:
    roots = _ancestors(parent, specs, ancestry_cache)
    if roots is None:
        return (
            ComponentResult("unknown", "unknown", ("unsupported_context_ancestry",)),
            (),
        )
    output_dependencies: set[ComponentKey] = set()
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
            and node == parent
            and (not role or any_role)
        ):
            output_dependencies.add(ComponentKey(node, "output", column))
        elif node in roots and (not role or any_role):
            return (
                ComponentResult(
                    "unknown",
                    "unknown",
                    ("planner_dimension_binding_unavailable",),
                ),
                (),
            )
        elif isinstance(specs.get(node), MetricSpec):
            output_dependencies.add(ComponentKey(node, "output", column))
        else:
            output_dependencies.add(ComponentKey(node, "output", column))
            target_nodes.add((node, None if any_role else role))

    path_key = (frozenset(roots), frozenset(target_nodes))
    if path_key not in path_cache:
        path_cache[path_key] = _context_links(roots, target_nodes, specs, link_index)
    links, complete = path_cache[path_key]
    dependencies: list[tuple[str, ComponentKey]] = []
    output_keys = sorted(
        output_dependencies,
        key=lambda item: (item.kind, item.field, item.token),
    )
    dependencies.extend(
        (f"context-output:{output_key.kind}:{output_key.field}", output_key)
        for output_key in output_keys
    )
    link_contracts = []
    for source, target, raw_link in links:
        link = cast(DimensionJoinLinkSpec | DimensionReferenceLinkSpec, raw_link)
        columns, parsed = _join_columns(source, target, link, join_column_cache)
        if not parsed:
            unresolved = True
            continue
        link_key = (
            source,
            target,
            canonical_json(normalize_value(_dimension_link_contract(link))),
        )
        if link_key not in link_contract_cache:
            link_contract_cache[link_key] = _semantic_link_contract(
                source,
                target,
                link,
            )
        link_value = link_contract_cache[link_key]
        if link_value is None:
            unresolved = True
            continue
        join_keys = [ComponentKey(node, "output", column) for node, column in columns]
        link_record = {
            "source_rows": ComponentKey(source, "rows"),
            "target_rows": ComponentKey(target, "rows"),
            "link": link_value,
            "join_keys": UnorderedPayload(tuple(join_keys)),
        }
        link_contracts.append(link_record)
        dependencies.extend(
            [
                ("context-link:source-rows", ComponentKey(source, "rows")),
                ("context-link:target-rows", ComponentKey(target, "rows")),
            ],
        )
        dependencies.extend(
            (f"context-link:join-key:{join_key.field}", join_key)
            for join_key in join_keys
        )
    context_nodes = {node for node, _ in target_nodes} | {
        target for _, target, _ in links if target not in roots
    }
    context_primary_keys = []
    for node in context_nodes:
        row_key = ComponentKey(node, "rows")
        primary_key = (
            sorted(cast(LinkableNodeSpec, specs[node]).primary_key or [])
            if isinstance(specs.get(node), LinkableNodeSpec)
            else []
        )
        context_primary_keys.append({"rows": row_key, "primary_key": primary_key})
        dependencies.append(("context-row", row_key))
    root_grains = []
    for node in roots:
        if not isinstance(specs.get(node), (TransformSpec, DimensionSpec)):
            continue
        row_key = ComponentKey(node, "rows")
        primary_key = sorted(cast(LinkableNodeSpec, specs[node]).primary_key or [])
        root_grains.append({"rows": row_key, "primary_key": primary_key})
        dependencies.append(("root-row", row_key))
    cached_semantic_filters = _cached_semantic_filters(
        filters,
        specs,
        refs,
        filter_semantics_cache,
        filter_column_cache,
    )
    semantic_filter_payload: object
    filter_dependencies: tuple[ComponentKey, ...]
    if cached_semantic_filters is None:
        unresolved = True
        semantic_filter_payload = UnorderedPayload(())
        filter_dependencies = ()
    else:
        semantic_filter_payload, filter_dependencies = cached_semantic_filters
    dependencies.extend(("filter-input", key) for key in filter_dependencies)
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
                "links": UnorderedPayload(tuple(link_contracts)),
                "filters": semantic_filter_payload,
                "context_primary_keys": UnorderedPayload(tuple(context_primary_keys)),
                "root_grains": UnorderedPayload(tuple(root_grains)),
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
    try:
        fingerprint = _component_hash(
            key,
            payload,
            inputs,
            semantic_field="" if key.kind.startswith("demand_") else None,
        )
    except KeyError:
        return ComponentResult(
            "unknown",
            "unknown",
            ("unresolved_payload_dependency",),
        )
    return ComponentResult(fingerprint, precision, tuple(sorted(reasons)))


async def compute_configured_v2_fingerprints(
    snapshot: SemanticGraphSnapshot,
    targets: Iterable[str] | None = None,
) -> V2FingerprintGraph:
    """Fingerprint configured metric dependencies and matching join roles."""
    specs = snapshot._specs
    selected = set(targets) if targets is not None else set(specs)
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
    filter_semantics_cache: _FilterSemanticsCache = {}
    link_contract_cache: _LinkContractCache = {}
    metric_parent_cache: dict[str, str | None] = {}
    contracts: dict[
        str,
        tuple[dict[str, object], tuple[ComponentKey, ...]] | None,
    ] = {}
    requirements_cache: dict[str, tuple[str, ...] | None] = {}
    demands: dict[
        tuple[str, str],
        tuple[
            MetricSpec,
            str,
            set[str],
            tuple[str, ...],
            dict,
            tuple[ComponentKey, ...],
        ],
    ] = {}
    consumers: dict[str, set[tuple[str, str]]] = defaultdict(set)
    derived_formulas: dict[
        str,
        dict[str, tuple[object, tuple[ComponentKey, ...]]],
    ] = defaultdict(dict)
    formula_contracts: dict[str, tuple[object, tuple[ComponentKey, ...]]] = {}
    derived_inputs: dict[str, tuple[MetricSpec, ...] | None] = {}

    def contract_for(
        spec: MetricSpec | CubeSpec,
    ) -> tuple[dict[str, object], tuple[ComponentKey, ...]] | None:
        if spec.rendered_name not in contracts:
            contracts[spec.rendered_name] = _semantic_contract(spec, specs)
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
        requirements = _effective_filter_requirements(metric)
        entries = [
            RequirementEntry(
                attributes=requirement.dimension,
                default=requirement.default,
                required=False,
                metrics=frozenset({metric.rendered_name}),
            )
            for requirement in requirements
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
        semantic_contract = contract_for(metric)
        if semantic_contract is None:
            unavailable.add(consumer)
            return
        contract, contract_dependencies = semantic_contract
        if parent is None:
            if metric_name not in derived_inputs:
                derived_inputs[metric_name] = _derived_inputs(metric, specs)
            children = derived_inputs[metric_name]
            if children is None:
                unavailable.add(consumer)
                return
            if metric_name not in formula_contracts:
                formula_contracts[metric_name] = _derived_formula_contract(
                    metric,
                    specs,
                    contract,
                    contract_dependencies,
                )
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
                "contract": _contract(metric),
            },
        )
        compact_key = sha256(demand_key.encode("utf-8")).hexdigest()
        demands.setdefault(
            (metric.rendered_name, compact_key),
            (
                metric,
                parent,
                refs,
                tuple(sorted(effective_filters)),
                contract,
                contract_dependencies,
            ),
        )
        consumers[consumer].add((metric.rendered_name, compact_key))

    for name, spec in specs.items():
        if name not in selected:
            continue
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

    nodes = {
        name: node for name, node in formula_graph.nodes.items() if name in selected
    }
    components = dict(available)
    bindings = dict(formula_graph.bindings)
    for key, (metric, parent, refs, filters, contract, _) in demands.items():
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
            filter_semantics_cache,
            filter_column_cache,
            link_contract_cache,
        )
        components[context_key] = context
        bindings[context_key] = context_dependencies

    unknown = ComponentResult("unknown", "unknown", ("configured_demand_unavailable",))
    consumer_names = {
        name
        for name, spec in specs.items()
        if name in selected and isinstance(spec, (MetricSpec, CubeSpec))
    }
    for consumer in sorted(consumer_names):
        spec = cast(MetricSpec | CubeSpec, specs[consumer])
        references = sorted(consumers.get(consumer, ()), key=lambda ref: ref[1])
        row_dependencies = []
        value_dependencies = []
        outputs: dict[str, ComponentResult] = {}
        if contract_for(spec) is None:
            unavailable.add(consumer)
        for ref in references:
            metric_name, demand_id = ref
            demand_contract = demands[(metric_name, demand_id)][4]
            demand_contract_dependencies = demands[(metric_name, demand_id)][5]
            context_key = ComponentKey(metric_name, "demand_context", demand_id)
            if metric_name in unsupported_metrics:
                fallback_key = ComponentKey(metric_name, "calculation")
                rows_key = ComponentKey(metric_name, "demand_rows", demand_id)
                row = _combine(
                    rows_key,
                    {"fallback": "formula_calculation"},
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
                    *(
                        ("contract-reference", key)
                        for key in demand_contract_dependencies
                    ),
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
                row_dependencies.append(("metric:rows", rows_key))
                value_dependencies.append(("metric:value", value_key))
                outputs.update(formula_graph.nodes[metric_name].outputs)
                continue
            formula = formula_graph.nodes.get(metric_name)
            rows_key = ComponentKey(metric_name, "demand_rows", demand_id)
            row = _combine(
                rows_key,
                {},
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
                    "metric-output",
                    ComponentKey(metric_name, "output", output),
                )
                for output in (formula.outputs if formula else {})
            ]
            value_deps.extend((("rows", rows_key), ("context", context_key)))
            value_deps.extend(
                ("contract-reference", key) for key in demand_contract_dependencies
            )
            value = _combine(value_key, demand_contract, value_deps, components)
            components[value_key] = value
            bindings[value_key] = tuple(value_deps)
            row_dependencies.append(("metric:rows", rows_key))
            value_dependencies.append(("metric:value", value_key))
            if formula:
                outputs.update(formula.outputs)

        formulas = derived_formulas.get(consumer, {})
        formula_items = list(formulas.items())
        name_fallbacks = any(
            isinstance(formula, Mapping) and "raw_query" in formula
            for _, (formula, _) in formula_items
        )
        if name_fallbacks:
            value_dependencies = [
                (
                    f"metric:{key.node}:value" if key.kind == "demand_value" else label,
                    key,
                )
                for label, key in value_dependencies
            ]
        row_dependencies.extend(
            ("derived-formula-input", key)
            for _, (_, dependencies) in formula_items
            for key in dependencies
        )
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
        formula_payload = UnorderedPayload(
            tuple(
                {"metric": name, "formula": value}
                if name_fallbacks
                and isinstance(value, Mapping)
                and "raw_query" in value
                else value
                for name, (value, _) in formula_items
            ),
        )
        rows = _combine(
            row_key,
            {"derived_formulas": formula_payload},
            row_dependencies,
            components,
            reason=(
                "cube_blending_rows_conservative"
                if isinstance(spec, CubeSpec)
                else "derived_metric_dependencies_conservative"
                if formulas
                else None
            ),
        )
        components[row_key] = rows
        bindings[row_key] = tuple(row_dependencies)
        full_key = ComponentKey(consumer, "configured_full")
        semantic_contract = contract_for(spec)
        contract_dependencies = (
            [("contract-reference", key) for key in semantic_contract[1]]
            if semantic_contract is not None
            else []
        )
        full_dependencies = [
            ("rows", row_key),
            *value_dependencies,
            *contract_dependencies,
        ]
        full = (
            _combine(
                full_key,
                semantic_contract[0],
                full_dependencies,
                components,
            )
            if semantic_contract is not None and consumer not in unavailable
            else unknown
        )
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
