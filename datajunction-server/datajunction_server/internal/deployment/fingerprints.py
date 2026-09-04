import logging
from collections.abc import Callable, Iterable
from heapq import heappop, heappush

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.database import Node, NodeRevision
from datajunction_server.internal.deployment.utils import (
    extract_dimension_refs_from_filters,
    extract_node_graph,
)
from datajunction_server.models.deployment import (
    CubeSpec,
    DimensionSpec,
    LinkableNodeSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.node import NodeType
from datajunction_server.models.semantic_fingerprint import (
    LATEST_SEMANTIC_FINGERPRINT_VERSION,
    UNKNOWN_SEMANTIC_FINGERPRINT,
    SemanticFingerprint,
    SemanticFingerprintValue,
)
from datajunction_server.semantic_fingerprints.engine import (
    compose_node_fingerprint,
    local_node_fingerprint,
)
from datajunction_server.semantic_fingerprints.merkle import (
    cycle_component_fingerprint,
    strongly_connected_components,
)
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.utils import SEPARATOR

FingerprintMap = dict[str, SemanticFingerprintValue]
ParentOptions = tuple[str, ...]
ParentReferences = tuple[ParentOptions, ...]
ParentCandidates = tuple[frozenset[str], ParentReferences]
ParentCandidateCache = dict[int, ParentCandidates]
ParentResolver = Callable[[NodeSpec], ParentReferences]
logger = logging.getLogger(__name__)


def _exact_parent_references(names: Iterable[str]) -> ParentReferences:
    return tuple((name,) for name in sorted(set(names)))


def _dimension_parent_options(reference: str) -> ParentOptions:
    parts = reference.split(SEPARATOR)
    return tuple(SEPARATOR.join(parts[:end]) for end in range(len(parts) - 1, 1, -1))


def _dimension_parent_references(references: Iterable[str]) -> ParentReferences:
    resolved = []
    for reference in sorted(set(references)):
        options = _dimension_parent_options(reference)
        if options:
            resolved.append(options)
    return tuple(resolved)


def _linkable_parent_candidates(spec: NodeSpec) -> ParentReferences:
    linkable = spec
    if not isinstance(linkable, LinkableNodeSpec):  # pragma: no cover
        raise TypeError(f"Expected linkable spec, got {type(spec).__name__}")
    return _exact_parent_references(
        link.rendered_dimension_node for link in linkable.dimension_links
    )


def _metric_parent_candidates(spec: NodeSpec) -> ParentReferences:
    metric = spec
    if not isinstance(metric, MetricSpec):  # pragma: no cover
        raise TypeError(f"Expected metric spec, got {type(spec).__name__}")
    return _dimension_parent_references(metric.rendered_required_dimensions)


def _cube_parent_candidates(spec: NodeSpec) -> ParentReferences:
    cube = spec
    if not isinstance(cube, CubeSpec):  # pragma: no cover
        raise TypeError(f"Expected cube spec, got {type(spec).__name__}")
    filter_references = {
        f"{node_name}{SEPARATOR}{column_name}"
        for node_name, column_name in extract_dimension_refs_from_filters(
            cube.rendered_filters,
        )
    }
    return _dimension_parent_references(
        [*cube.rendered_dimensions, *filter_references],
    )


SEMANTIC_PARENT_RESOLVERS: dict[type[NodeSpec], ParentResolver] = {
    SourceSpec: _linkable_parent_candidates,
    TransformSpec: _linkable_parent_candidates,
    DimensionSpec: _linkable_parent_candidates,
    MetricSpec: _metric_parent_candidates,
    CubeSpec: _cube_parent_candidates,
}


def _candidate_parts(
    spec: NodeSpec,
    cache: ParentCandidateCache,
) -> ParentCandidates:
    key = id(spec)
    if key not in cache:
        resolver = SEMANTIC_PARENT_RESOLVERS.get(type(spec))
        if resolver is None:
            raise TypeError(
                f"No semantic parent resolver for {type(spec).__name__}",
            )
        query = (
            spec.rendered_metrics
            if isinstance(spec, CubeSpec)
            else extract_node_graph([spec]).get(spec.rendered_name, [])
        )
        cache[key] = (frozenset(query), resolver(spec))
    return cache[key]


def _parent_candidates(
    spec: NodeSpec,
    cache: ParentCandidateCache | None = None,
) -> set[str]:
    query, extra = _candidate_parts(spec, cache if cache is not None else {})
    return set(query) | {
        candidate for parent_options in extra for candidate in parent_options
    }


def _is_derived_metric(spec: NodeSpec) -> bool:
    return (
        isinstance(spec, MetricSpec)
        and spec.query_ast is not None
        and spec.query_ast.select.from_ is None
    )


def _direct_query_parents(
    spec: NodeSpec,
    specs: dict[str, NodeSpec],
    candidates: Iterable[str],
) -> set[str]:
    candidate_names = set(candidates)
    if _is_derived_metric(spec):
        return {
            name
            for name in candidate_names
            if name in specs and specs[name].node_type == NodeType.METRIC
        }
    return candidate_names & specs.keys()


def _resolve_parent_references(
    references: ParentReferences,
    specs: dict[str, NodeSpec],
) -> tuple[set[str], set[str]]:
    resolved = set()
    unresolved = set()
    for options in references:
        parent = next((candidate for candidate in options if candidate in specs), None)
        if parent is not None:
            resolved.add(parent)
        else:
            unresolved.add(options[0])
    return resolved, unresolved


def _resolved_parent_names(
    spec: NodeSpec,
    specs: dict[str, NodeSpec],
    cache: ParentCandidateCache,
) -> tuple[list[str], list[str]]:
    query, extra = _candidate_parts(spec, cache)
    query_candidates = set(query)
    query_parents = _direct_query_parents(spec, specs, query_candidates)
    unresolved_query = (
        set() if _is_derived_metric(spec) else query_candidates - specs.keys()
    )
    extra_parents, unresolved_extra = _resolve_parent_references(
        extra,
        specs,
    )
    resolved = query_parents | extra_parents
    unresolved = unresolved_query | unresolved_extra
    return sorted(resolved), sorted(unresolved)


def _spec_with_normalized_required_dimensions(
    spec: NodeSpec,
    specs: dict[str, NodeSpec],
    cache: ParentCandidateCache,
) -> NodeSpec:
    if not isinstance(spec, MetricSpec) or not spec.required_dimensions:
        return spec

    query_candidates, _ = _candidate_parts(spec, cache)
    direct_parents = _direct_query_parents(spec, specs, query_candidates)

    required_dimensions = []
    for dimension in spec.rendered_required_dimensions:
        direct_parent = next(
            (
                parent
                for parent in sorted(
                    direct_parents,
                    key=lambda name: (-len(name), name),
                )
                if dimension.startswith(f"{parent}{SEPARATOR}")
            ),
            None,
        )
        if direct_parent is not None:
            dimension = dimension[len(direct_parent) + 1 :]
        required_dimensions.append(dimension)
    return spec.model_copy(update={"required_dimensions": required_dimensions})


async def _load_external_specs(
    session: AsyncSession,
    seed_specs: Iterable[NodeSpec],
    ignored_parse_errors: set[str],
    parent_cache: ParentCandidateCache,
) -> dict[str, NodeSpec]:
    seeds = list(seed_specs)
    known_names = {spec.rendered_name for spec in seeds}
    external_specs: dict[str, NodeSpec] = {}
    pending = seeds
    while pending:
        candidates: set[str] = set()
        for spec in pending:
            try:
                candidates.update(_parent_candidates(spec, parent_cache))
            except (DJParseException, TypeError, ValueError) as exc:
                if spec.rendered_name not in ignored_parse_errors:
                    logger.warning(
                        "Semantic parent extraction failed for %s: %s",
                        spec.rendered_name,
                        exc,
                    )
        frontier = sorted(candidates - known_names)
        if not frontier:
            break
        known_names.update(frontier)
        nodes = await Node.get_by_names(
            session,
            frontier,
            options=[
                joinedload(Node.current).options(
                    *NodeRevision.export_load_options(),
                ),
                selectinload(Node.tags),
                selectinload(Node.owners),
            ],
        )
        pending = [await node.to_spec(session) for node in nodes]
        external_specs.update({spec.rendered_name: spec for spec in pending})
    return external_specs


def _resolved_proposed_specs(
    existing_specs: dict[str, NodeSpec],
    proposed_specs: Iterable[NodeSpec],
    deleted_names: set[str],
) -> dict[str, NodeSpec]:
    specs = {
        name: spec for name, spec in existing_specs.items() if name not in deleted_names
    }
    for proposed in proposed_specs:
        existing = existing_specs.get(proposed.rendered_name)
        if (
            isinstance(proposed, SourceSpec)
            and not proposed.columns
            and isinstance(existing, SourceSpec)
        ):
            proposed = proposed.model_copy(
                deep=True,
                update={"columns": existing.columns},
            )
        specs[proposed.rendered_name] = proposed
    return specs


def _compute_merkle_fingerprints(
    specs: dict[str, NodeSpec],
    ignored_parse_errors: set[str],
    parent_cache: ParentCandidateCache | None = None,
    *,
    version: int = LATEST_SEMANTIC_FINGERPRINT_VERSION,
) -> FingerprintMap:
    parent_cache = parent_cache if parent_cache is not None else {}
    graph: dict[str, list[str]] = {}
    failed_names: set[str] = set()
    fingerprint_specs: dict[str, NodeSpec] = {}
    for name in sorted(specs):
        spec = specs[name]
        try:
            graph[name], unresolved = _resolved_parent_names(
                spec,
                specs,
                parent_cache,
            )
            if unresolved:
                failed_names.add(name)
                if name not in ignored_parse_errors:
                    logger.warning(
                        "Fingerprint unavailable for %s; unresolved parents: %s",
                        name,
                        ", ".join(unresolved),
                    )
        except (DJParseException, TypeError, ValueError) as exc:
            graph[name] = []
            failed_names.add(name)
            if name not in ignored_parse_errors:
                logger.warning("Fingerprint unavailable for %s: %s", name, exc)
        try:
            fingerprint_specs[name] = _spec_with_normalized_required_dimensions(
                spec,
                specs,
                parent_cache,
            )
        except (DJParseException, TypeError, ValueError) as exc:
            failed_names.add(name)
            if name not in ignored_parse_errors:
                logger.warning("Fingerprint unavailable for %s: %s", name, exc)

    components = strongly_connected_components(graph)
    component_by_name = {
        name: component_index
        for component_index, members in enumerate(components)
        for name in members
    }
    component_results: dict[int, FingerprintMap] = {}
    component_parents: dict[int, set[int]] = {
        component_index: {
            component_by_name[parent]
            for member in members
            for parent in graph[member]
            if component_by_name[parent] != component_index
        }
        for component_index, members in enumerate(components)
    }
    component_children: dict[int, set[int]] = {
        component_index: set() for component_index in range(len(components))
    }
    for child, parents in component_parents.items():
        for parent_component in parents:
            component_children[parent_component].add(child)

    remaining_parents = {
        component_index: len(parents)
        for component_index, parents in component_parents.items()
    }
    ready: list[int] = []
    for component_index, remaining in remaining_parents.items():
        if remaining == 0:
            heappush(ready, component_index)

    processed = 0
    while ready:
        component_index = heappop(ready)
        processed += 1
        members = components[component_index]
        unavailable: FingerprintMap = {
            name: UNKNOWN_SEMANTIC_FINGERPRINT for name in members
        }
        if any(name in failed_names for name in members):
            component_results[component_index] = unavailable
        else:
            external_edges: list[tuple[str, str, SemanticFingerprint]] = []
            for member in members:
                for parent_name in graph[member]:
                    parent_component = component_by_name[parent_name]
                    if parent_component == component_index:
                        continue
                    parent_fingerprint = component_results[parent_component][
                        parent_name
                    ]
                    if parent_fingerprint == UNKNOWN_SEMANTIC_FINGERPRINT:
                        break
                    external_edges.append(
                        (member, parent_name, parent_fingerprint),
                    )
                else:
                    continue
                break
            else:
                is_cycle = len(members) > 1 or members[0] in graph[members[0]]
                try:
                    if not is_cycle:
                        component_results[component_index] = {
                            members[0]: compose_node_fingerprint(
                                fingerprint_specs[members[0]],
                                version,
                                parent_fingerprints=[
                                    edge[2] for edge in external_edges
                                ],
                            ),
                        }
                    else:
                        local_fingerprints = {
                            name: local_node_fingerprint(
                                fingerprint_specs[name],
                                version,
                            )
                            for name in members
                        }
                        internal_edges = [
                            (member, parent)
                            for member in members
                            for parent in graph[member]
                            if component_by_name[parent] == component_index
                        ]
                        component_fingerprint = cycle_component_fingerprint(
                            members,
                            local_fingerprints,
                            internal_edges,
                            external_edges,
                        )
                        component_results[component_index] = {
                            name: compose_node_fingerprint(
                                fingerprint_specs[name],
                                version,
                                parent_fingerprints=[component_fingerprint],
                            )
                            for name in members
                        }
                except (DJParseException, TypeError, ValueError) as exc:
                    component_results[component_index] = unavailable
                    if not all(name in ignored_parse_errors for name in members):
                        logger.warning(
                            "Fingerprint unavailable for component %s: %s",
                            members,
                            exc,
                        )
            if component_index not in component_results:
                component_results[component_index] = unavailable

        for child in component_children[component_index]:
            remaining_parents[child] -= 1
            if remaining_parents[child] == 0:
                heappush(ready, child)

    if processed != len(components):  # pragma: no cover
        raise RuntimeError("SCC condensation graph contains a cycle")

    return {name: component_results[component_by_name[name]][name] for name in specs}


class SemanticFingerprintGraph:
    """Semantic fingerprints evaluated within one graph snapshot."""

    def __init__(
        self,
        specs: dict[str, NodeSpec],
        *,
        ignored_parse_errors: set[str] | None = None,
        parent_cache: ParentCandidateCache | None = None,
        version: int = LATEST_SEMANTIC_FINGERPRINT_VERSION,
    ):
        self._specs = dict(specs)
        self._ignored_parse_errors = set(ignored_parse_errors or ())
        self._parent_cache = parent_cache if parent_cache is not None else {}
        self._version = version
        self._fingerprints: FingerprintMap | None = None

    def fingerprint(self, name: str) -> SemanticFingerprintValue:
        """Return one node's fingerprint in this graph snapshot."""
        return self._evaluate()[name]

    def fingerprints(
        self,
        names: Iterable[str] | None = None,
    ) -> FingerprintMap:
        """Return fingerprints for selected nodes in this graph snapshot."""
        target_names = list(self._specs if names is None else names)
        if not target_names:
            return {}
        fingerprints = self._evaluate()
        return {
            name: fingerprints[name] for name in target_names if name in fingerprints
        }

    def _evaluate(self) -> FingerprintMap:
        if self._fingerprints is None:
            self._fingerprints = _compute_merkle_fingerprints(
                self._specs,
                self._ignored_parse_errors,
                self._parent_cache,
                version=self._version,
            )
        return self._fingerprints


async def build_deployment_fingerprints(
    session: AsyncSession,
    existing_specs: dict[str, NodeSpec],
    proposed_specs: Iterable[NodeSpec],
    deleted_specs: Iterable[NodeSpec],
    *,
    additional_target_names: Iterable[str] = (),
    version: int = LATEST_SEMANTIC_FINGERPRINT_VERSION,
) -> tuple[FingerprintMap, FingerprintMap]:
    proposed_specs = list(proposed_specs)
    additional_target_names = set(additional_target_names)
    deleted_names = {spec.rendered_name for spec in deleted_specs}
    proposed = _resolved_proposed_specs(
        existing_specs,
        proposed_specs,
        deleted_names,
    )
    submitted_names = {spec.rendered_name for spec in proposed_specs}
    target_specs: dict[str, NodeSpec] = {}
    target_names_to_load = (
        additional_target_names - existing_specs.keys() - submitted_names
    )
    if target_names_to_load:
        target_nodes = await Node.get_by_names(
            session,
            sorted(target_names_to_load),
            options=[
                joinedload(Node.current).options(
                    *NodeRevision.export_load_options(),
                ),
                selectinload(Node.tags),
                selectinload(Node.owners),
            ],
        )
        target_specs = {
            spec.rendered_name: spec
            for spec in [await node.to_spec(session) for node in target_nodes]
        }

    parent_cache: ParentCandidateCache = {}
    external = await _load_external_specs(
        session,
        [*existing_specs.values(), *proposed.values(), *target_specs.values()],
        ignored_parse_errors=deleted_names,
        parent_cache=parent_cache,
    )
    external.update(target_specs)
    current_graph = SemanticFingerprintGraph(
        {**external, **existing_specs},
        ignored_parse_errors=deleted_names,
        parent_cache=parent_cache,
        version=version,
    )
    proposed_graph = SemanticFingerprintGraph(
        {**external, **proposed},
        parent_cache=parent_cache,
        version=version,
    )
    current = current_graph.fingerprints(
        deleted_names | additional_target_names,
    )
    proposed_hashes = proposed_graph.fingerprints(
        submitted_names | additional_target_names,
    )
    return current, proposed_hashes
