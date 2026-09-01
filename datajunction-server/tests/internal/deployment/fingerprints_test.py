from unittest.mock import MagicMock

import pytest

from datajunction_server.internal.deployment.fingerprints import (
    SEMANTIC_PARENT_RESOLVERS,
    _candidate_parts,
    _compute_merkle_fingerprints,
    _parent_candidates,
    _resolved_proposed_specs,
    build_deployment_fingerprints,
    concrete_node_spec_types,
)
from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
    TransformSpec,
)


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
    assert set(SEMANTIC_PARENT_RESOLVERS) == concrete_node_spec_types()

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
    original = _compute_merkle_fingerprints(
        specs,
        specs,
        ignored_parse_errors=set(),
    )

    changed_source = source.model_copy(update={"table": "changed"})
    changed_specs = {**specs, changed_source.rendered_name: changed_source}
    changed = _compute_merkle_fingerprints(
        changed_specs,
        changed_specs,
        ignored_parse_errors=set(),
    )

    assert all(original[name] != changed[name] for name in specs)


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
    assert _compute_merkle_fingerprints(specs, [target], set())[target] is not None


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

    assert _compute_merkle_fingerprints(specs, specs, set()) == {
        unavailable.rendered_name: None,
        dependent.rendered_name: None,
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

    assert _compute_merkle_fingerprints(
        authored_specs,
        [authored.rendered_name],
        set(),
    ) == _compute_merkle_fingerprints(
        persisted_specs,
        [persisted.rendered_name],
        set(),
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
    assert _compute_merkle_fingerprints(
        authored_specs,
        [authored_derived.rendered_name],
        set(),
    ) == _compute_merkle_fingerprints(
        persisted_specs,
        [persisted_derived.rendered_name],
        set(),
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
    original = _compute_merkle_fingerprints(specs, specs, set())
    reversed_input = _compute_merkle_fingerprints(
        dict(reversed(list(specs.items()))),
        reversed(list(specs)),
        set(),
    )
    assert original == reversed_input
    assert all(fingerprint is not None for fingerprint in original.values())

    changed_second = second.model_copy(update={"query": "SELECT 2"})
    changed_specs = {
        **specs,
        changed_second.rendered_name: changed_second,
    }
    changed = _compute_merkle_fingerprints(changed_specs, changed_specs, set())
    assert all(original[name] != changed[name] for name in specs)

    broken_third = linked_dimension("third")
    broken_specs = {**specs, broken_third.rendered_name: broken_third}
    broken = _compute_merkle_fingerprints(broken_specs, broken_specs, set())
    assert all(original[name] != broken[name] for name in specs)


def test_self_link_and_external_parent_cycles_have_stable_hashes():
    self_link = linked_dimension("self", "self")
    assert (
        _compute_merkle_fingerprints(
            {self_link.rendered_name: self_link},
            [self_link.rendered_name],
            set(),
        )[self_link.rendered_name]
        is not None
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
    original = _compute_merkle_fingerprints(specs, specs, set())
    changed_parent = parent.model_copy(update={"table": "changed"})
    changed_specs = {**specs, changed_parent.rendered_name: changed_parent}
    changed = _compute_merkle_fingerprints(changed_specs, changed_specs, set())
    assert all(original[name] != changed[name] for name in specs)


@pytest.mark.parametrize("query", ["SELECT (", "SELECT * FROM missing.parent"])
def test_deleted_legacy_node_can_omit_unavailable_fingerprint(query: str):
    legacy = transform_spec("legacy", query)
    assert _compute_merkle_fingerprints(
        {legacy.rendered_name: legacy},
        [legacy.rendered_name],
        ignored_parse_errors={legacy.rendered_name},
    ) == {legacy.rendered_name: None}


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

    assert _compute_merkle_fingerprints(specs, specs, set()) == {
        invalid_metric.rendered_name: None,
        invalid_source.rendered_name: None,
    }
    assert _compute_merkle_fingerprints(specs, specs, set(specs)) == {
        invalid_metric.rendered_name: None,
        invalid_source.rendered_name: None,
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

    assert resolved[source.rendered_name].semantic_fingerprint() == (
        source.semantic_fingerprint()
    )
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
    assert proposed[source.rendered_name] == source.semantic_fingerprint()
    assert proposed[transform.rendered_name] == transform.semantic_fingerprint(
        parent_fingerprints=[proposed[source.rendered_name]],
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

    assert proposed[transform.rendered_name] is not None
    assert proposed[transform.rendered_name] != transform.semantic_fingerprint()

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
    assert proposed == {spec.rendered_name: None for spec in unavailable}

    legacy = transform_spec("legacy", "SELECT (")
    current, _ = await build_deployment_fingerprints(
        session,
        {legacy.rendered_name: legacy},
        [],
        [legacy],
    )
    assert current == {legacy.rendered_name: None}

    current, proposed = await build_deployment_fingerprints(
        session,
        {},
        [],
        [],
        additional_target_names=["default.hard_hat"],
    )
    assert current["default.hard_hat"] is not None
    assert proposed["default.hard_hat"] is not None
