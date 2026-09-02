from typing import get_args
from unittest.mock import MagicMock

import pytest

from datajunction_server.internal.deployment.fingerprints import (
    SEMANTIC_PARENT_RESOLVERS,
    SemanticFingerprintGraph,
    _candidate_parts,
    _parent_candidates,
    _resolved_proposed_specs,
    build_deployment_fingerprints,
)
from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    NodeSpec,
    NodeUnion,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.semantic_fingerprint import (
    UNKNOWN_SEMANTIC_FINGERPRINT,
    SemanticFingerprint,
)
from datajunction_server.semantic_fingerprints.engine import local_node_fingerprint


def source_spec(
    name: str,
    *,
    namespace: str = "ns",
    table: str | None = None,
    columns: list[ColumnSpec] | None = None,
    dimension_links: list[DimensionJoinLinkSpec] | None = None,
) -> SourceSpec:
    return SourceSpec(
        namespace=namespace,
        name=name,
        catalog="catalog",
        schema_="schema",
        table=table or name,
        columns=columns,
        dimension_links=dimension_links or [],
    )


def transform_spec(
    name: str,
    query: str,
    *,
    namespace: str = "ns",
) -> TransformSpec:
    return TransformSpec(namespace=namespace, name=name, query=query)


def spec_map(*specs: NodeSpec) -> dict[str, NodeSpec]:
    return {spec.rendered_name: spec for spec in specs}


def linked_dimension(
    name: str,
    *targets: str,
    query: str = "SELECT 1",
) -> DimensionSpec:
    return DimensionSpec(
        namespace="cycle",
        name=name,
        query=query,
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node=f"cycle.{target}",
                join_type="cross",
            )
            for target in targets
        ],
    )


def test_parent_candidates_follow_oss_semantic_relationships():
    transform = TransformSpec(
        namespace="ns",
        name="transform",
        query="SELECT * FROM ${prefix}source",
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}dimension",
                join_on="${prefix}transform.id = ${prefix}dimension.id",
            ),
        ],
    )
    metric = MetricSpec(
        namespace="ns",
        name="metric",
        query="SELECT SUM(value) FROM ${prefix}transform",
        required_dimensions=["${prefix}dimension.id"],
    )
    cube = CubeSpec(
        namespace="ns",
        name="cube",
        metrics=["${prefix}metric"],
        dimensions=["${prefix}dimension.id"],
        filters=["${prefix}filter_dimension.id = 1"],
    )

    assert _parent_candidates(transform) == {"ns.source", "ns.dimension"}
    assert _parent_candidates(metric) == {"ns.transform", "ns.dimension"}
    assert _parent_candidates(cube) == {
        "ns.metric",
        "ns.dimension",
        "ns.filter_dimension",
    }
    node_union = get_args(NodeUnion)[0]
    assert set(SEMANTIC_PARENT_RESOLVERS) == set(get_args(node_union))

    cache = {}
    first = _candidate_parts(transform, cache)
    assert _candidate_parts(transform, cache) is first
    assert len(cache) == 1
    with pytest.raises(TypeError, match="No semantic parent resolver for NodeSpec"):
        _candidate_parts(NodeSpec(name="unknown", node_type="source"), {})


def test_merkle_fingerprints_propagate_changes_through_all_descendants():
    source = source_spec(
        "source",
        table="table",
        columns=[ColumnSpec(name="id", type="bigint")],
    )
    transform = transform_spec(
        "transform",
        "SELECT id FROM ${prefix}source",
    )
    metric = MetricSpec(
        namespace="ns",
        name="metric",
        query="SELECT COUNT(*) FROM ${prefix}transform",
    )
    specs = spec_map(source, transform, metric)
    original = SemanticFingerprintGraph(specs).fingerprints()

    changed_source = source.model_copy(update={"table": "changed"})
    changed_specs = {**specs, changed_source.rendered_name: changed_source}
    changed = SemanticFingerprintGraph(changed_specs).fingerprints()

    assert all(original[name] != changed[name] for name in specs)


def test_graph_fingerprint_uses_its_parent_snapshot():
    orders_v1 = source_spec("orders", table="orders_v1")
    orders_v2 = orders_v1.model_copy(update={"table": "orders_v2"})
    revenue = transform_spec(
        "revenue",
        "SELECT * FROM ${prefix}orders",
    )

    current = SemanticFingerprintGraph(spec_map(orders_v1, revenue))
    proposed = SemanticFingerprintGraph(spec_map(orders_v2, revenue))

    current_fingerprint = current.fingerprint(revenue.rendered_name)
    assert current.fingerprint(revenue.rendered_name) is current_fingerprint
    assert current_fingerprint != proposed.fingerprint(
        revenue.rendered_name,
    )
    assert (
        SemanticFingerprintGraph(spec_map(revenue)).fingerprint(revenue.rendered_name)
        == UNKNOWN_SEMANTIC_FINGERPRINT
    )


def test_merkle_fingerprints_support_deep_dependency_chains():
    specs = {}
    for index in range(1_100):
        links = (
            [
                DimensionJoinLinkSpec(
                    dimension_node=f"deep.node_{index - 1}",
                    join_type="cross",
                ),
            ]
            if index
            else []
        )
        spec = source_spec(
            f"node_{index}",
            namespace="deep",
            table=f"table_{index}",
            dimension_links=links,
        )
        specs[spec.rendered_name] = spec

    target = "deep.node_1099"
    assert (
        SemanticFingerprintGraph(specs).fingerprint(target)
        != UNKNOWN_SEMANTIC_FINGERPRINT
    )


@pytest.mark.parametrize(
    ("name", "query"),
    [
        ("unresolved", "SELECT * FROM missing.parent"),
        ("invalid", "SELECT ("),
    ],
)
def test_unavailable_fingerprint_propagates_to_dependents(name: str, query: str):
    unavailable = transform_spec(name, query)
    dependent = transform_spec(
        "dependent",
        f"SELECT * FROM ${{prefix}}{name}",
    )
    specs = spec_map(unavailable, dependent)

    assert SemanticFingerprintGraph(specs).fingerprints() == {
        unavailable.rendered_name: UNKNOWN_SEMANTIC_FINGERPRINT,
        dependent.rendered_name: UNKNOWN_SEMANTIC_FINGERPRINT,
    }


def test_direct_required_dimension_paths_use_persisted_identity():
    parent = source_spec("parent")
    authored = MetricSpec(
        namespace="ns",
        name="metric",
        query="SELECT COUNT(*) FROM ${prefix}parent",
        required_dimensions=["${prefix}parent.id"],
    )
    persisted = authored.model_copy(update={"required_dimensions": ["id"]})
    authored_specs = spec_map(parent, authored)
    persisted_specs = spec_map(parent, persisted)

    assert SemanticFingerprintGraph(authored_specs).fingerprint(
        authored.rendered_name,
    ) == SemanticFingerprintGraph(persisted_specs).fingerprint(
        persisted.rendered_name,
    )

    dimension = DimensionSpec(namespace="ns", name="dimension", query="SELECT 1")
    base_metric = MetricSpec(
        namespace="ns",
        name="base_metric",
        query="SELECT COUNT(*) FROM ${prefix}parent",
    )
    authored_derived = MetricSpec(
        namespace="ns",
        name="derived_metric",
        query="SELECT ${prefix}base_metric * 2",
        required_dimensions=[
            "${prefix}base_metric.id",
            "${prefix}dimension.id",
        ],
    )
    persisted_derived = authored_derived.model_copy(
        update={"required_dimensions": ["id", "ns.dimension.id"]},
    )
    authored_specs = spec_map(parent, dimension, base_metric, authored_derived)
    persisted_specs = spec_map(parent, dimension, base_metric, persisted_derived)
    assert SemanticFingerprintGraph(authored_specs).fingerprint(
        authored_derived.rendered_name,
    ) == SemanticFingerprintGraph(persisted_specs).fingerprint(
        persisted_derived.rendered_name,
    )


def test_cycle_hashing_is_stable_and_propagates_member_changes():
    first = linked_dimension("first", "second")
    second = linked_dimension("second", "third")
    third = linked_dimension("third", "first")
    downstream = transform_spec(
        "downstream",
        "SELECT * FROM ${prefix}first",
        namespace="cycle",
    )
    specs = spec_map(first, second, third, downstream)
    original = SemanticFingerprintGraph(specs).fingerprints()
    reversed_input = SemanticFingerprintGraph(
        dict(reversed(list(specs.items()))),
    ).fingerprints()
    assert original == reversed_input
    assert all(
        fingerprint != UNKNOWN_SEMANTIC_FINGERPRINT for fingerprint in original.values()
    )

    changed_second = second.model_copy(update={"query": "SELECT 2"})
    changed_specs = {
        **specs,
        changed_second.rendered_name: changed_second,
    }
    changed = SemanticFingerprintGraph(changed_specs).fingerprints()
    assert all(original[name] != changed[name] for name in specs)

    broken_third = linked_dimension("third")
    broken_specs = {**specs, broken_third.rendered_name: broken_third}
    broken = SemanticFingerprintGraph(broken_specs).fingerprints()
    assert all(original[name] != broken[name] for name in specs)


def test_self_link_and_external_parent_cycles_have_stable_hashes():
    self_link = linked_dimension("self", "self")
    assert (
        SemanticFingerprintGraph(
            {self_link.rendered_name: self_link},
        ).fingerprint(self_link.rendered_name)
        != UNKNOWN_SEMANTIC_FINGERPRINT
    )

    parent = source_spec(
        "parent",
        namespace="cycle",
        table="table",
    )
    first = linked_dimension(
        "first",
        "second",
        query="SELECT * FROM ${prefix}parent",
    )
    second = linked_dimension("second", "first")
    specs = spec_map(parent, first, second)
    original = SemanticFingerprintGraph(specs).fingerprints()
    changed_parent = parent.model_copy(update={"table": "changed"})
    changed_specs = {**specs, changed_parent.rendered_name: changed_parent}
    changed = SemanticFingerprintGraph(changed_specs).fingerprints()
    assert all(original[name] != changed[name] for name in specs)


@pytest.mark.parametrize("query", ["SELECT (", "SELECT * FROM missing.parent"])
def test_deleted_legacy_node_returns_unknown_without_failure(query: str):
    legacy = transform_spec("legacy", query)
    graph = SemanticFingerprintGraph(
        {legacy.rendered_name: legacy},
        ignored_parse_errors={legacy.rendered_name},
    )
    assert graph.fingerprint(legacy.rendered_name) == UNKNOWN_SEMANTIC_FINGERPRINT


def test_fingerprint_build_failures_are_unavailable():
    invalid_metric = MetricSpec(
        namespace="ns",
        name="invalid_metric",
        query="SELECT (",
        required_dimensions=["ns.dimension.id"],
    )
    invalid_source = source_spec(
        "invalid_source",
        columns=[ColumnSpec(name="id", type="bigint")],
    )
    invalid_source.columns[0].type = object()  # type: ignore[assignment]
    specs = spec_map(invalid_metric, invalid_source)

    graph = SemanticFingerprintGraph(specs)
    assert graph.fingerprints([]) == {}
    assert graph.fingerprints() == {
        invalid_metric.rendered_name: UNKNOWN_SEMANTIC_FINGERPRINT,
        invalid_source.rendered_name: UNKNOWN_SEMANTIC_FINGERPRINT,
    }
    assert SemanticFingerprintGraph(
        specs,
        ignored_parse_errors=set(specs),
    ).fingerprints() == {
        invalid_metric.rendered_name: UNKNOWN_SEMANTIC_FINGERPRINT,
        invalid_source.rendered_name: UNKNOWN_SEMANTIC_FINGERPRINT,
    }


def test_proposed_sources_reuse_resolved_columns_and_remove_deletes():
    source = source_spec(
        "source",
        table="table",
        columns=[ColumnSpec(name="id", type="bigint")],
    )
    deleted = source_spec(
        "deleted",
        table="deleted",
    )
    proposed = source.model_copy(update={"columns": None})
    resolved = _resolved_proposed_specs(
        {
            source.rendered_name: source,
            deleted.rendered_name: deleted,
        },
        [proposed],
        {deleted.rendered_name},
    )

    assert local_node_fingerprint(
        resolved[source.rendered_name],
    ) == local_node_fingerprint(source)
    assert deleted.rendered_name not in resolved


@pytest.mark.asyncio
async def test_build_deployment_fingerprints_without_external_parents():
    source = source_spec("source", table="table")
    transform = transform_spec(
        "transform",
        "SELECT * FROM ${prefix}source",
    )
    current, proposed = await build_deployment_fingerprints(
        MagicMock(),
        {},
        [source, transform],
        [],
    )

    assert current == {}
    graph = SemanticFingerprintGraph(spec_map(source, transform))
    source_fingerprint = proposed[source.rendered_name]
    assert isinstance(source_fingerprint, SemanticFingerprint)
    assert source_fingerprint == graph.fingerprint(source.rendered_name)
    assert proposed[transform.rendered_name] == graph.fingerprint(
        transform.rendered_name,
    )


@pytest.mark.asyncio
async def test_build_deployment_fingerprints_loads_external_ancestors(session):
    transform = transform_spec(
        "external_child",
        "SELECT * FROM default.hard_hat",
    )
    _, proposed = await build_deployment_fingerprints(
        session,
        {},
        [transform],
        [],
    )

    assert proposed[transform.rendered_name] != UNKNOWN_SEMANTIC_FINGERPRINT
    assert proposed[transform.rendered_name] != local_node_fingerprint(transform)

    unavailable = [
        transform_spec("missing_child", "SELECT * FROM missing.parent"),
        transform_spec("invalid_child", "SELECT ("),
    ]
    _, proposed = await build_deployment_fingerprints(
        session,
        {},
        unavailable,
        [],
    )
    assert proposed == {
        spec.rendered_name: UNKNOWN_SEMANTIC_FINGERPRINT for spec in unavailable
    }

    legacy = transform_spec("legacy", "SELECT (")
    current, _ = await build_deployment_fingerprints(
        session,
        {legacy.rendered_name: legacy},
        [],
        [legacy],
    )
    assert current == {legacy.rendered_name: UNKNOWN_SEMANTIC_FINGERPRINT}

    current, proposed = await build_deployment_fingerprints(
        session,
        {},
        [],
        [],
        additional_target_names=["default.hard_hat"],
    )
    assert current["default.hard_hat"] != UNKNOWN_SEMANTIC_FINGERPRINT
    assert proposed["default.hard_hat"] != UNKNOWN_SEMANTIC_FINGERPRINT
