"""Configured dimension route context binds only demanded values and rows."""

import asyncio

from datajunction_server.models.deployment import (
    ColumnSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentKey,
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)
from datajunction_server.semantic_fingerprints.v2_configured import (
    _AncestryCache,
    _ContextPathCache,
    _context_link_index,
    _context_links,
    _demand_context,
)
from datajunction_server.semantic_fingerprints.v2_dependencies import (
    _JoinColumnCache,
)


def _specs(*, route=True):
    fact = SourceSpec(
        name="ns.fact",
        catalog="warehouse",
        schema_="sales",
        table="fact",
        columns=[
            ColumnSpec(name="amount", type="double"),
            ColumnSpec(name="country_id", type="bigint"),
            ColumnSpec(name="debug", type="bigint"),
        ],
        dimension_links=(
            [
                DimensionJoinLinkSpec(
                    dimension_node="ns.country",
                    join_on="ns.fact.country_id = ns.country.id",
                ),
            ]
            if route
            else []
        ),
    )
    country_rows = SourceSpec(
        name="ns.country_rows",
        catalog="warehouse",
        schema_="sales",
        table="country",
        columns=[
            ColumnSpec(name="id", type="bigint"),
            ColumnSpec(name="name", type="string"),
        ],
    )
    country = DimensionSpec(
        name="ns.country",
        query="SELECT id, name FROM ns.country_rows",
        columns=country_rows.columns,
        primary_key=["id"],
    )
    metric = MetricSpec(
        name="ns.total",
        query="SELECT SUM(amount) FROM ns.fact",
    )
    return {spec.rendered_name: spec for spec in (fact, country_rows, country, metric)}


def _demand(nodes, *, refs=("ns.country.name",), filters=()):
    return _demand_result(nodes, refs=refs, filters=filters)[0]


def _demand_result(nodes, *, refs=("ns.country.name",), filters=()):
    graph = asyncio.run(compute_v2_fingerprints(SemanticGraphSnapshot(nodes)))
    metric = nodes["ns.total"]
    return _demand_context(
        metric,
        "ns.fact",
        set(refs),
        tuple(filters),
        nodes,
        graph.components,
        "test-demand",
        _AncestryCache(),
        _context_link_index(nodes),
        _JoinColumnCache(),
        _ContextPathCache(),
    )


def test_context_binds_selected_value_join_key_and_rows_but_not_unused_outputs():
    before_nodes = _specs()
    before, dependencies = _demand_result(before_nodes)
    before_providers = {provider for _, provider in dependencies}

    expanded = _specs()
    rows = expanded["ns.country_rows"]
    rows.columns.append(ColumnSpec(name="code", type="string"))
    expanded["ns.country"].query = "SELECT id, name, code FROM ns.country_rows"
    expanded["ns.country"].columns.append(ColumnSpec(name="code", type="string"))
    after = _demand(expanded)

    assert compare_components(before, after) == "unchanged"
    assert ComponentKey("ns.country", "output", "name") in before_providers
    assert ComponentKey("ns.country", "output", "id") in before_providers
    assert ComponentKey("ns.fact", "output", "country_id") in before_providers
    assert ComponentKey("ns.fact", "rows") in before_providers
    assert ComponentKey("ns.country", "rows") in before_providers
    assert ComponentKey("ns.country", "output", "code") not in before_providers
    assert ComponentKey("ns.country", "calculation") not in before_providers


def test_requested_role_sequence_selects_only_matching_multihop_routes():
    nodes = _specs(route=False)
    nodes["ns.fact"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.region",
            join_on="ns.fact.country_id = ns.region.id",
            role="customer",
        ),
    ]
    nodes["ns.region"] = DimensionSpec(
        name="ns.region",
        query="SELECT id, country_id FROM ns.country_rows",
        columns=[
            ColumnSpec(name="id", type="bigint"),
            ColumnSpec(name="country_id", type="bigint"),
        ],
        primary_key=["id"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.country",
                join_on="ns.region.country_id = ns.country.id",
                role="active",
            ),
        ],
    )
    index = _context_link_index(nodes)

    links, complete = _context_links(
        {"ns.fact"}, {("ns.country", "customer->active")}, nodes, index
    )
    wrong_role, wrong_role_complete = _context_links(
        {"ns.fact"}, {("ns.country", "customer->former")}, nodes, index
    )

    assert complete
    assert [(source, target) for source, target, _ in links] == [
        ("ns.fact", "ns.region"),
        ("ns.region", "ns.country"),
    ]
    assert not wrong_role_complete
    assert wrong_role == []


def test_missing_output_or_incomplete_route_returns_unknown():
    nodes = _specs()
    nodes["ns.country"].query = "SELECT id FROM ns.country_rows"
    nodes["ns.country"].columns = [
        column for column in nodes["ns.country"].columns if column.name != "name"
    ]
    missing_output = _demand(nodes)
    assert missing_output.fingerprint == "unknown"
    assert missing_output.reasons == ("unresolved_dimension_context",)

    missing_route = _demand(_specs(route=False))
    assert missing_route.fingerprint == "unknown"
    assert missing_route.reasons == ("unresolved_dimension_context",)


def test_root_grain_change_changes_demand_context():
    nodes = _specs(route=False)
    nodes["ns.root"] = TransformSpec(
        name="ns.root",
        query="SELECT amount, country_id FROM ns.fact",
        primary_key=["country_id"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.country",
                join_on="ns.root.country_id = ns.country.id",
            ),
        ],
    )
    nodes["ns.total"].query = "SELECT SUM(amount) FROM ns.root"
    before = _demand_context_for_parent(nodes, "ns.root")

    nodes["ns.root"].primary_key = ["amount"]
    after = _demand_context_for_parent(nodes, "ns.root")

    assert compare_components(before, after) == "changed"


def _demand_context_for_parent(nodes, parent):
    graph = asyncio.run(compute_v2_fingerprints(SemanticGraphSnapshot(nodes)))
    return _demand_context(
        nodes["ns.total"],
        parent,
        {"ns.country.name"},
        (),
        nodes,
        graph.components,
        "test-demand",
        _AncestryCache(),
        _context_link_index(nodes),
        _JoinColumnCache(),
        _ContextPathCache(),
    )[0]
