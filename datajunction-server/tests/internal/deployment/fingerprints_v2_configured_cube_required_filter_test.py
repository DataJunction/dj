"""Configured fingerprints for cubes with query-time metric requirements."""

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
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.semantic_fingerprints.v2 import (
    SemanticGraphSnapshot,
    compare_components,
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
    metric = MetricSpec(
        name="ns.total",
        query="SELECT SUM(amount) FROM ns.fact",
        required_filters=[FilterRequirement(dimension=["ns.country.name"])],
    )
    cube = CubeSpec(
        name="ns.by_country",
        metrics=["ns.total"],
        dimensions=["ns.country.name"],
    )
    return {item.rendered_name: item for item in (fact, lookup, country, metric, cube)}


def evaluate(nodes):
    return asyncio.run(
        compute_configured_v2_fingerprints(SemanticGraphSnapshot(nodes)),
    )


@pytest.mark.parametrize("formula", ["SUM(amount)", "unsupported_udf(amount)"])
def test_cube_ignores_validation_only_filter_requirements(formula):
    nodes = specs()
    nodes["ns.total"].query = f"SELECT {formula} FROM ns.fact"
    result = evaluate(nodes)

    assert result.nodes["ns.total"].full.digest
    assert result.nodes["ns.by_country"].full.digest
    assert "configured_demand_unavailable" not in result.nodes["ns.by_country"].reasons

    for requirements in ([], [FilterRequirement(dimension=["ns.missing.name"])]):
        changed = copy.deepcopy(nodes)
        changed["ns.total"].required_filters = requirements
        after = evaluate(changed)
        for name in ("ns.total", "ns.by_country"):
            assert (
                compare_components(result.nodes[name].full, after.nodes[name].full)
                == "unchanged"
            )


def test_default_requirement_class_does_not_change_cube_data():
    nodes = specs()
    nodes["ns.total"].required_filters[0] = FilterRequirement(
        dimension=["ns.country.name"],
        default="ns.country.name IS NOT NULL",
    )
    before = evaluate(nodes)
    changed = copy.deepcopy(nodes)
    changed["ns.total"].recommended_filters = changed["ns.total"].required_filters
    changed["ns.total"].required_filters = []
    after = evaluate(changed)
    for name in ("ns.total", "ns.by_country"):
        assert (
            compare_components(before.nodes[name].full, after.nodes[name].full)
            == "unchanged"
        )


def test_explicit_cube_filter_suppresses_default_and_requirement_attributes():
    nodes = specs()
    nodes["ns.total"].required_filters = [
        FilterRequirement(
            dimension=["ns.country.name", "ns.unused.name"],
            default="ns.country.name = 'US'",
        ),
    ]
    nodes["ns.by_country"].filters = ["ns.country.name = 'CA'"]
    before = evaluate(nodes)

    changed = copy.deepcopy(nodes)
    changed["ns.total"].required_filters = [
        FilterRequirement(
            dimension=["ns.country.name", "ns.other_unused.name"],
            default="ns.country.name = 'GB'",
        ),
    ]
    after = evaluate(changed)

    assert (
        compare_components(
            before.nodes["ns.by_country"].full,
            after.nodes["ns.by_country"].full,
        )
        == "unchanged"
    )
    assert (
        compare_components(
            before.nodes["ns.total"].full,
            after.nodes["ns.total"].full,
        )
        == "changed"
    )


def test_derived_cube_ignores_validation_only_filter_requirements():
    nodes = specs()
    nodes["ns.derived"] = MetricSpec(name="ns.derived", query="SELECT ns.total * 2")
    nodes["ns.by_country"].metrics = ["ns.derived"]
    before = evaluate(nodes)
    changed = copy.deepcopy(nodes)
    changed["ns.total"].required_filters = [
        FilterRequirement(dimension=["ns.missing.name"])
    ]
    after = evaluate(changed)
    for name in ("ns.total", "ns.derived", "ns.by_country"):
        assert (
            compare_components(before.nodes[name].full, after.nodes[name].full)
            == "unchanged"
        )


def test_reordering_cube_selections_preserves_data_fingerprint():
    nodes = specs()
    nodes["ns.count"] = MetricSpec(
        name="ns.count", query="SELECT COUNT(*) FROM ns.fact"
    )
    cube = nodes["ns.by_country"]
    cube.metrics.append("ns.count")
    cube.dimensions.append("ns.country.id")
    before = evaluate(nodes)
    changed = copy.deepcopy(nodes)
    changed["ns.by_country"].metrics.reverse()
    changed["ns.by_country"].dimensions.reverse()
    after = evaluate(changed)
    assert (
        compare_components(
            before.nodes["ns.by_country"].full, after.nodes["ns.by_country"].full
        )
        == "unchanged"
    )


def test_parameterized_cube_retains_formula_filter_and_dimension_changes():
    original = specs()
    before = evaluate(original)

    changed_formula = copy.deepcopy(original)
    changed_formula["ns.total"].query = "SELECT SUM(amount) * 2 FROM ns.fact"
    assert (
        compare_components(
            before.nodes["ns.by_country"].full,
            evaluate(changed_formula).nodes["ns.by_country"].full,
        )
        == "changed"
    )

    changed_requirement = copy.deepcopy(original)
    changed_requirement["ns.total"].required_filters = [
        FilterRequirement(
            dimension=["ns.country.name"],
            default="ns.country.name IS NOT NULL",
        ),
    ]
    assert (
        compare_components(
            before.nodes["ns.by_country"].full,
            evaluate(changed_requirement).nodes["ns.by_country"].full,
        )
        == "changed"
    )

    changed_cube_filter = copy.deepcopy(original)
    changed_cube_filter["ns.by_country"].filters = ["ns.country.name = 'CA'"]
    assert (
        compare_components(
            before.nodes["ns.by_country"].full,
            evaluate(changed_cube_filter).nodes["ns.by_country"].full,
        )
        == "changed"
    )

    changed_dimension = copy.deepcopy(original)
    changed_dimension[
        "ns.country"
    ].query = "SELECT id, name FROM ns.lookup WHERE id > 0"
    assert (
        compare_components(
            before.nodes["ns.by_country"].full,
            evaluate(changed_dimension).nodes["ns.by_country"].full,
        )
        == "changed"
    )

    changed_route = copy.deepcopy(original)
    changed_route["ns.fact"].dimension_links[
        0
    ].join_on = "ns.fact.region_id = ns.country.id"
    assert (
        compare_components(
            before.nodes["ns.by_country"].full,
            evaluate(changed_route).nodes["ns.by_country"].full,
        )
        == "changed"
    )


def test_parameterized_cube_stays_unknown_without_required_dimension_route():
    nodes = specs()
    nodes["ns.fact"].dimension_links = []

    result = evaluate(nodes)

    assert result.nodes["ns.by_country"].full.fingerprint == "unknown"


def test_parameterized_cube_stays_unknown_for_invalid_explicit_filter():
    nodes = specs()
    nodes["ns.by_country"].filters = ["ns.country.name >"]

    result = evaluate(nodes)

    assert result.nodes["ns.by_country"].full.fingerprint == "unknown"


def test_parameterized_cube_stays_unknown_for_conflicting_defaults():
    nodes = specs()
    nodes["ns.total"].required_filters = [
        FilterRequirement(
            dimension=["ns.country.name"],
            default=f"ns.country.name = '{country}'",
        )
        for country in ("US", "CA")
    ]

    result = evaluate(nodes)

    assert result.nodes["ns.by_country"].full.fingerprint == "unknown"
