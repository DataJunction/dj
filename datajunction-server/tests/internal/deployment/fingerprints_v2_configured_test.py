"""Configured hashes retain output pruning and coarse context dependencies."""

import asyncio
import copy

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentKey,
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)
from datajunction_server.semantic_fingerprints.v2_configured import (
    compute_configured_v2_fingerprints,
)


def specs():
    fact = SourceSpec(
        name="ns.fact",
        catalog="warehouse",
        schema_="sales",
        table="fact",
        columns=[
            ColumnSpec(name="amount", type="double"),
            ColumnSpec(name="debug", type="bigint"),
            ColumnSpec(name="country_id", type="bigint"),
            ColumnSpec(name="region_id", type="bigint"),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.country",
                join_on="ns.fact.country_id = ns.country.id",
            ),
        ],
    )
    lookup = SourceSpec(
        name="ns.lookup",
        catalog="warehouse",
        schema_="sales",
        table="lookup",
        columns=[
            ColumnSpec(name="id", type="bigint"),
            ColumnSpec(name="name", type="string"),
        ],
    )
    country = DimensionSpec(
        name="ns.country",
        query="SELECT id, name FROM ns.lookup",
        columns=lookup.columns,
        primary_key=["id"],
    )
    region = country.model_copy(update={"name": "ns.region"})
    metric = MetricSpec(
        name="ns.total",
        query="SELECT SUM(amount) FROM ns.fact",
    )
    cube = CubeSpec(
        name="ns.by_country",
        metrics=["ns.total"],
        dimensions=["ns.country.name"],
    )
    return {
        item.rendered_name: item
        for item in (fact, lookup, country, region, metric, cube)
    }


def evaluate(nodes):
    return asyncio.run(
        compute_configured_v2_fingerprints(SemanticGraphSnapshot(nodes)),
    )


def compare(before, after, name):
    return compare_components(before.nodes[name].full, after.nodes[name].full)


def _contextual(nodes):
    nodes["ns.total"].required_dimensions = ["ns.country.name"]
    return nodes


def test_output_edit_stays_local_with_configured_dimension_context():
    before_specs = _contextual(specs())
    before = evaluate(before_specs)
    after_specs = copy.deepcopy(before_specs)
    after_specs["ns.fact"].columns[1].type = "string"
    after = evaluate(after_specs)

    assert compare(before, after, "ns.fact") == "changed"
    assert compare(before, after, "ns.total") == "unchanged"
    assert compare(before, after, "ns.by_country") == "unchanged"


def test_new_route_to_requested_dimension_changes_coarse_context_consumers():
    before_specs = _contextual(specs())
    before_specs["ns.fact"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.region",
            join_on="ns.fact.region_id = ns.region.id",
        ),
    ]
    before_specs["ns.region"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.country",
            join_on="ns.region.id = ns.country.id",
        ),
    ]
    before = evaluate(before_specs)
    after_specs = copy.deepcopy(before_specs)
    after_specs["ns.fact"].dimension_links.append(
        DimensionJoinLinkSpec(
            dimension_node="ns.country",
            join_on="ns.fact.country_id = ns.country.id",
        ),
    )
    after = evaluate(after_specs)

    assert compare(before, after, "ns.total") == "changed"
    assert compare(before, after, "ns.by_country") == "changed"


def test_intermediate_dimension_key_changes_context_consumers():
    before_specs = _contextual(specs())
    before_specs["ns.fact"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.region",
            join_on="ns.fact.region_id = ns.region.id",
        ),
    ]
    before_specs["ns.region"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.country",
            join_on="ns.region.id = ns.country.id",
        ),
    ]
    before = evaluate(before_specs)

    after_specs = copy.deepcopy(before_specs)
    after_specs["ns.region"].primary_key = ["name"]
    after = evaluate(after_specs)

    assert compare(before, after, "ns.total") == "changed"
    assert compare(before, after, "ns.by_country") == "changed"


def test_root_transform_grain_changes_configured_context_consumers():
    nodes = specs()
    nodes["ns.root"] = TransformSpec(
        name="ns.root",
        query="SELECT amount, country_id FROM ns.fact",
        primary_key=["country_id"],
    )
    nodes["ns.total"].query = "SELECT SUM(amount) FROM ns.root"
    nodes["ns.total"].required_dimensions = ["ns.country.name"]
    before = evaluate(nodes)

    changed = copy.deepcopy(nodes)
    changed["ns.root"].primary_key = ["amount"]
    after = evaluate(changed)

    assert compare(before, after, "ns.total") == "changed"
    assert compare(before, after, "ns.by_country") == "changed"


def test_missing_requested_dimension_output_is_unknown():
    nodes = _contextual(specs())
    nodes["ns.lookup"].columns = [
        column for column in nodes["ns.lookup"].columns if column.name != "name"
    ]

    result = evaluate(nodes)

    assert result.nodes["ns.total"].full.fingerprint == "unknown"
    assert result.nodes["ns.by_country"].full.fingerprint == "unknown"


def test_context_precision_is_conservative_and_bindings_are_recorded():
    result = evaluate(_contextual(specs()))
    context_keys = [key for key in result.components if key.kind == "demand_context"]

    assert context_keys
    assert all(
        result.components[key].precision == "conservative" for key in context_keys
    )
    assert all(result.bindings[key] for key in context_keys)
    assert any(
        provider == ComponentKey("ns.country", "full")
        for key in context_keys
        for _, provider in result.bindings[key]
    )


def test_cube_uses_intrinsic_fallback_for_unsupported_metric():
    nodes = _contextual(specs())
    nodes["ns.derived"] = MetricSpec(
        name="ns.derived",
        query="SELECT unsupported_fn(amount) AS value FROM ns.fact",
        required_dimensions=["ns.country.name"],
    )
    nodes["ns.derived_cube"] = CubeSpec(
        name="ns.derived_cube",
        metrics=["ns.derived"],
        dimensions=["ns.country.name"],
    )
    before = evaluate(nodes)
    changed = copy.deepcopy(nodes)
    changed["ns.fact"].columns[0].type = "bigint"
    after = evaluate(changed)

    assert before.nodes["ns.derived"].precision in {"conservative", "unknown"}
    assert compare(before, after, "ns.derived") == "changed"
    assert compare(before, after, "ns.derived_cube") == "changed"
    assert after.nodes["ns.derived_cube"].precision == "conservative"


def test_unsupported_metric_cube_tracks_requested_dimension_routes():
    nodes = _contextual(specs())
    nodes["ns.derived"] = MetricSpec(
        name="ns.derived",
        query="SELECT unsupported_fn(amount) AS value FROM ns.fact",
        required_dimensions=["ns.country.name"],
    )
    nodes["ns.derived_cube"] = CubeSpec(
        name="ns.derived_cube",
        metrics=["ns.derived"],
        dimensions=["ns.country.name"],
    )
    before = evaluate(nodes)

    changed_route = copy.deepcopy(nodes)
    changed_route["ns.fact"].dimension_links[0] = DimensionJoinLinkSpec(
        dimension_node="ns.country",
        join_on="ns.fact.region_id = ns.country.id",
    )
    routed = evaluate(changed_route)

    missing_route = copy.deepcopy(nodes)
    missing_route["ns.fact"].dimension_links = []
    unresolved = evaluate(missing_route)

    assert compare(before, routed, "ns.derived") == "changed"
    assert compare(before, routed, "ns.derived_cube") == "changed"
    assert unresolved.nodes["ns.derived"].full.fingerprint == "unknown"
    assert unresolved.nodes["ns.derived_cube"].full.fingerprint == "unknown"


@pytest.mark.parametrize(
    "update",
    [
        {"join_type": "inner"},
        {"join_cardinality": "one_to_many"},
        {"default_value": True},
        {"default_value": 1},
        {"default_value": 1.0},
        {"join_on": "ns.fact.region_id = ns.country.id"},
    ],
)
def test_join_contract_changes_configured_metric_and_cube(update):
    before_specs = _contextual(specs())
    before = evaluate(before_specs)
    after_specs = copy.deepcopy(before_specs)
    original = after_specs["ns.fact"].dimension_links[0]
    after_specs["ns.fact"].dimension_links[0] = DimensionJoinLinkSpec.model_validate(
        {**original.model_dump(), **update},
    )
    after = evaluate(after_specs)

    assert compare(before, after, "ns.total") == "changed"
    assert compare(before, after, "ns.by_country") == "changed"


def test_target_definition_and_required_filter_changes_are_preserved():
    before_specs = _contextual(specs())
    before_specs["ns.total"].required_filters = [
        FilterRequirement(
            dimension=["ns.country.name"],
            default="ns.country.name IS NOT NULL",
        ),
    ]
    before = evaluate(before_specs)

    after_specs = copy.deepcopy(before_specs)
    after_specs["ns.country"].query = "SELECT id, name FROM ns.lookup WHERE id > 0"
    after = evaluate(after_specs)
    assert compare(before, after, "ns.total") == "changed"
    assert compare(before, after, "ns.by_country") == "changed"


def test_missing_context_target_is_unknown():
    nodes = _contextual(specs())
    del nodes["ns.country"]
    result = evaluate(nodes)
    assert result.nodes["ns.total"].full.fingerprint == "unknown"
    assert result.nodes["ns.by_country"].full.fingerprint == "unknown"
    assert compare(result, result, "ns.by_country") == "indeterminate"


def test_unsupported_formula_retains_intrinsic_output_fallback():
    nodes = _contextual(specs())
    nodes["ns.derived"] = MetricSpec(
        name="ns.derived",
        query="SELECT unsupported_fn(amount) FROM ns.fact",
    )
    snapshot = SemanticGraphSnapshot(nodes)
    intrinsic = asyncio.run(compute_v2_fingerprints(snapshot))
    configured = asyncio.run(compute_configured_v2_fingerprints(snapshot))
    key = ComponentKey("ns.derived", "full")
    assert (
        configured.nodes["ns.derived"].outputs == intrinsic.nodes["ns.derived"].outputs
    )
    assert configured.components[key].precision == "conservative"
    assert "unsupported_function" in configured.components[key].reasons
    assert configured.components[key].fingerprint != "unknown"
