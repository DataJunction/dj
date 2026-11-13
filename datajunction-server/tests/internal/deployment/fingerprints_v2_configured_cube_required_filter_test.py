"""Configured fingerprints for cubes with query-time metric requirements."""

import asyncio
import copy

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


def test_cube_with_unspecified_required_filter_is_query_parameterized():
    result = evaluate(specs())

    assert result.nodes["ns.total"].full.digest
    assert result.nodes["ns.by_country"].full.digest
    assert "configured_demand_unavailable" not in result.nodes["ns.by_country"].reasons


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
