"""Configured metric and cube hashes ignore logical node names."""

import asyncio
import copy

from datajunction_server.models.deployment import (
    CubeSpec,
    DimensionJoinLinkSpec,
    MetricSpec,
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.semantic_fingerprints.v2 import (
    SemanticGraphSnapshot,
    compare_components,
)
from datajunction_server.semantic_fingerprints.v2_configured import (
    compute_configured_v2_fingerprints,
)
from tests.internal.deployment.configured_fingerprint_specs import (
    configured_graph_specs,
)


def _evaluate(specs):
    return asyncio.run(
        compute_configured_v2_fingerprints(SemanticGraphSnapshot(specs)),
    )


def test_renamed_dimension_keeps_configured_metric_and_cube_fingerprints():
    before_specs = configured_graph_specs()
    before = _evaluate(before_specs)

    after_specs = copy.deepcopy(before_specs)
    country = after_specs.pop("ns.country")
    after_specs["ns.nation"] = country.model_copy(
        update={
            "name": "ns.nation",
            "query": "SELECT id, name FROM ns.lookup",
        },
    )
    fact = after_specs["ns.fact"]
    fact.dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.nation",
            join_on="ns.fact.country_id = ns.nation.id",
        ),
    ]
    cube = after_specs["ns.by_country"]
    assert isinstance(cube, CubeSpec)
    cube.dimensions = ["ns.nation.name"]

    after = _evaluate(after_specs)

    for name in ("ns.total", "ns.by_country"):
        assert (
            compare_components(before.nodes[name].full, after.nodes[name].full)
            == "unchanged"
        )


def test_configured_dimension_provider_change_still_changes_the_cube():
    before_specs = configured_graph_specs()
    before_specs["ns.region"] = before_specs["ns.region"].model_copy(
        update={
            "query": "SELECT id, name FROM ns.region_rows",
            "columns": before_specs["ns.lookup"].columns,
        },
    )
    before_specs["ns.region_rows"] = before_specs["ns.lookup"].model_copy(
        update={"name": "ns.region_rows", "table": "region_lookup"},
    )
    before = _evaluate(before_specs)

    after_specs = copy.deepcopy(before_specs)
    cube = after_specs["ns.by_country"]
    cube.dimensions = ["ns.region.name"]
    after_specs["ns.fact"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.region",
            join_on="ns.fact.region_id = ns.region.id",
        ),
    ]
    after = _evaluate(after_specs)

    assert (
        compare_components(
            before.nodes["ns.by_country"].full,
            after.nodes["ns.by_country"].full,
        )
        == "changed"
    )


def test_renamed_derived_metric_and_its_input_keep_configured_fingerprint():
    before_specs = configured_graph_specs()
    before_specs["ns.base"] = MetricSpec(
        name="ns.base",
        query="SELECT SUM(amount) FROM ns.fact",
    )
    before_specs["ns.ratio"] = MetricSpec(
        name="ns.ratio",
        query="SELECT ns.base * 100 AS ratio",
    )
    before_specs["ns.by_country"].metrics = ["ns.ratio"]
    before = _evaluate(before_specs)

    after_specs = copy.deepcopy(before_specs)
    base = after_specs.pop("ns.base")
    ratio = after_specs.pop("ns.ratio")
    after_specs["ns.renamed_base"] = base.model_copy(
        update={"name": "ns.renamed_base"},
    )
    after_specs["ns.renamed_ratio"] = ratio.model_copy(
        update={
            "name": "ns.renamed_ratio",
            "query": "SELECT ns.renamed_base * 100 AS ratio",
        },
    )
    after_specs["ns.by_country"].metrics = ["ns.renamed_ratio"]
    after = _evaluate(after_specs)

    for name in ("ns.ratio", "ns.by_country"):
        renamed = "ns.renamed_ratio" if name == "ns.ratio" else name
        assert (
            compare_components(before.nodes[name].full, after.nodes[renamed].full)
            == "unchanged"
        )

    changed_specs = copy.deepcopy(after_specs)
    changed_specs["ns.renamed_ratio"].query = "SELECT ns.renamed_base * 200 AS ratio"
    changed = _evaluate(changed_specs)
    assert (
        compare_components(
            after.nodes["ns.renamed_ratio"].full,
            changed.nodes["ns.renamed_ratio"].full,
        )
        == "changed"
    )


def test_swapping_contextual_derived_inputs_changes_metric_and_cube():
    before_specs = configured_graph_specs()
    for name, default in (
        ("ns.first", "ns.country.name = 'US'"),
        ("ns.second", "ns.country.name = 'CA'"),
    ):
        before_specs[name] = MetricSpec(
            name=name,
            query="SELECT SUM(amount) FROM ns.fact",
            required_filters=[
                FilterRequirement(
                    dimension=["ns.country.name"],
                    default=default,
                ),
            ],
        )
    before_specs["ns.delta"] = MetricSpec(
        name="ns.delta",
        query="SELECT ns.first - ns.second AS delta",
    )
    before_specs["ns.by_country"].metrics = ["ns.delta"]
    before = _evaluate(before_specs)

    after_specs = copy.deepcopy(before_specs)
    after_specs["ns.delta"].query = "SELECT ns.second - ns.first AS delta"
    after = _evaluate(after_specs)

    for name in ("ns.delta", "ns.by_country"):
        assert (
            compare_components(before.nodes[name].full, after.nodes[name].full)
            == "changed"
        )


def test_swapping_contextual_input_defaults_changes_metric_and_cube():
    before_specs = configured_graph_specs()
    for name, default in (
        ("ns.first", "ns.country.name = 'US'"),
        ("ns.second", "ns.country.name = 'CA'"),
    ):
        before_specs[name] = MetricSpec(
            name=name,
            query="SELECT SUM(amount) FROM ns.fact",
            required_filters=[
                FilterRequirement(
                    dimension=["ns.country.name"],
                    default=default,
                ),
            ],
        )
    before_specs["ns.delta"] = MetricSpec(
        name="ns.delta",
        query="SELECT ns.first - ns.second AS delta",
    )
    before_specs["ns.by_country"].metrics = ["ns.delta"]
    before = _evaluate(before_specs)

    after_specs = copy.deepcopy(before_specs)
    after_specs["ns.first"].required_filters = [
        FilterRequirement(
            dimension=["ns.country.name"],
            default="ns.country.name = 'CA'",
        ),
    ]
    after_specs["ns.second"].required_filters = [
        FilterRequirement(
            dimension=["ns.country.name"],
            default="ns.country.name = 'US'",
        ),
    ]
    after = _evaluate(after_specs)

    for name in ("ns.delta", "ns.by_country"):
        assert (
            compare_components(before.nodes[name].full, after.nodes[name].full)
            == "changed"
        )


def test_swapping_contextual_intermediate_formulas_changes_cube():
    before_specs = configured_graph_specs()
    for name, default in (
        ("ns.first", "ns.country.name = 'US'"),
        ("ns.second", "ns.country.name = 'CA'"),
    ):
        before_specs[name] = MetricSpec(
            name=name,
            query="SELECT SUM(amount) FROM ns.fact",
            required_filters=[
                FilterRequirement(
                    dimension=["ns.country.name"],
                    default=default,
                ),
            ],
        )
    before_specs["ns.diff_a"] = MetricSpec(
        name="ns.diff_a",
        query="SELECT ns.first - ns.second AS value",
    )
    before_specs["ns.diff_b"] = MetricSpec(
        name="ns.diff_b",
        query="SELECT ns.first + ns.second AS value",
    )
    before_specs["ns.final"] = MetricSpec(
        name="ns.final",
        query="SELECT ns.diff_a - ns.diff_b AS value",
    )
    before_specs["ns.by_country"].metrics = ["ns.final"]
    before = _evaluate(before_specs)

    after_specs = copy.deepcopy(before_specs)
    after_specs["ns.diff_a"].query = "SELECT ns.first + ns.second AS value"
    after_specs["ns.diff_b"].query = "SELECT ns.first - ns.second AS value"
    after = _evaluate(after_specs)

    for name in ("ns.final", "ns.by_country"):
        assert (
            compare_components(before.nodes[name].full, after.nodes[name].full)
            == "changed"
        )
