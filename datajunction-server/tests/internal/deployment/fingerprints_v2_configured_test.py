"""Configured hashes retain output pruning and dimension context dependencies."""

import asyncio
import copy

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    MetricSpec,
    TransformSpec,
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.semantic_fingerprints import v2_configured, v2_dependencies
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentKey,
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)
from datajunction_server.semantic_fingerprints.v2_configured import (
    compute_configured_v2_fingerprints,
)
from datajunction_server.sql.parsing import ast
from tests.internal.deployment.configured_fingerprint_specs import (
    configured_graph_specs as specs,
)


def evaluate(nodes):
    return asyncio.run(
        compute_configured_v2_fingerprints(SemanticGraphSnapshot(nodes)),
    )


def compare(before, after, name):
    return compare_components(before.nodes[name].full, after.nodes[name].full)


def test_configured_metric_contract_preserves_ambiguous_required_dimension_providers():
    query = (
        "SELECT SUM(o.amount) FROM ns.orders o "
        "JOIN ns.customers c ON o.customer_id = c.id"
    )
    orders = MetricSpec(
        name="ns.total",
        query=query,
        required_dimensions=["ns.orders.id"],
    )
    customers = orders.model_copy(
        update={"required_dimensions": ["ns.customers.id"]},
    )

    assert v2_configured._contract(orders) != v2_configured._contract(customers)


def _contextual(nodes):
    nodes["ns.total"].required_dimensions = ["ns.country.name"]
    return nodes


def _indirect_country_route():
    nodes = _contextual(specs())
    nodes["ns.fact"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.region",
            join_on="ns.fact.region_id = ns.region.id",
        ),
    ]
    nodes["ns.region"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.country",
            join_on="ns.region.id = ns.country.id",
        ),
    ]
    return nodes


def _add_unused_country_code(nodes):
    nodes["ns.lookup"].columns.append(ColumnSpec(name="code", type="string"))
    nodes["ns.country"].query = "SELECT code, name, id FROM ns.lookup"
    nodes["ns.country"].columns = [
        ColumnSpec(name="code", type="string"),
        ColumnSpec(name="name", type="string"),
        ColumnSpec(name="id", type="bigint"),
    ]
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


@pytest.mark.parametrize("try_cast", [False, True])
@pytest.mark.parametrize(
    "edit,expected",
    [
        ("unused_fact", "unchanged"),
        ("unused_dimension", "unchanged"),
        ("join_key", "changed"),
        ("dimension_value", "changed"),
        ("dimension_rows", "changed"),
        ("link", "changed"),
        ("cast", "changed"),
        ("grain", "changed"),
    ],
)
def test_implicit_metric_columns_retain_rows_keys_and_used_values(
    edit,
    expected,
    try_cast,
):
    nodes = specs()
    fact = nodes["ns.fact"]
    nodes["ns.fact_rows"] = fact.model_copy(
        update={"name": "ns.fact_rows", "dimension_links": []},
    )
    nodes["ns.fact"] = TransformSpec(
        name="ns.fact",
        query="SELECT amount, debug, country_id, region_id FROM ns.fact_rows",
        columns=fact.columns,
        dimension_links=fact.dimension_links,
    )
    nodes["ns.total"].query = (
        "SELECT SUM(CASE WHEN ns.country.name = 'US' THEN amount ELSE 0 END) "
        "FROM ns.fact"
    )
    if try_cast:
        nodes["ns.fact"].dimension_links[
            0
        ].join_on = "TRY_CAST(ns.fact.country_id AS INT) = ns.country.id"
    before = evaluate(nodes)
    changed = copy.deepcopy(nodes)
    queries = {
        "unused_fact": (
            "ns.fact",
            "SELECT amount, debug + 1 AS debug, country_id, region_id FROM ns.fact_rows",
        ),
        "join_key": ("ns.country", "SELECT id + 1 AS id, name FROM ns.lookup"),
        "dimension_value": (
            "ns.country",
            "SELECT id, UPPER(name) AS name FROM ns.lookup",
        ),
        "dimension_rows": ("ns.country", "SELECT id, name FROM ns.lookup WHERE id > 0"),
    }
    if edit in queries:
        node, query = queries[edit]
        changed[node].query = query
    elif edit == "unused_dimension":
        _add_unused_country_code(changed)
    elif edit == "link":
        changed["ns.fact"].dimension_links[
            0
        ].join_on = "ns.fact.region_id = ns.country.id"
    elif edit == "cast":
        cast = "CAST" if try_cast else "TRY_CAST"
        changed["ns.fact"].dimension_links[
            0
        ].join_on = f"{cast}(ns.fact.country_id AS INT) = ns.country.id"
    elif edit == "grain":
        changed["ns.country"].primary_key = ["id", "name"]
    after = evaluate(changed)
    assert compare(before, after, "ns.total") == expected
    assert compare(before, after, "ns.by_country") == expected


@pytest.mark.parametrize("opaque_fact", [False, True])
def test_unrequested_dimension_outputs_and_projection_order_do_not_change_cube(
    opaque_fact,
):
    before_specs = _contextual(specs(opaque_fact=opaque_fact))
    before = evaluate(before_specs)

    after_specs = _add_unused_country_code(copy.deepcopy(before_specs))
    after = evaluate(after_specs)

    assert compare(before, after, "ns.country") == "changed"
    assert compare(before, after, "ns.by_country") == "unchanged"

    changed_type = copy.deepcopy(after_specs)
    changed_type["ns.country"].columns[0].type = "bigint"
    changed = evaluate(changed_type)

    assert compare(after, changed, "ns.country") == "changed"
    assert compare(after, changed, "ns.by_country") == "unchanged"


@pytest.mark.parametrize("opaque_fact", [False, True])
def test_selected_dimension_value_and_target_join_key_change_cube(opaque_fact):
    before_specs = _contextual(specs(opaque_fact=opaque_fact))
    before = evaluate(before_specs)

    changed_value = copy.deepcopy(before_specs)
    changed_value["ns.country"].query = "SELECT id, UPPER(name) AS name FROM ns.lookup"
    changed_value_result = evaluate(changed_value)
    assert compare(before, changed_value_result, "ns.by_country") == "changed"

    changed_join_key = copy.deepcopy(before_specs)
    changed_join_key["ns.country"].query = "SELECT id + 1 AS id, name FROM ns.lookup"
    changed_join_key_result = evaluate(changed_join_key)
    assert compare(before, changed_join_key_result, "ns.by_country") == "changed"


@pytest.mark.parametrize("opaque_fact", [False, True])
def test_cross_dimension_context_tracks_target_rows(opaque_fact):
    before_specs = _contextual(specs(opaque_fact=opaque_fact))
    before_specs["ns.fact"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.country",
            join_type="cross",
        ),
    ]
    before_specs["ns.country"].query = "SELECT id, name FROM ns.lookup WHERE id > 0"
    before = evaluate(before_specs)

    changed_specs = copy.deepcopy(before_specs)
    changed_specs["ns.country"].query = "SELECT id, name FROM ns.lookup WHERE id > 1"
    changed = evaluate(changed_specs)

    assert compare(before, changed, "ns.by_country") == "changed"
    context_bindings = [
        edges for key, edges in before.bindings.items() if key.kind == "demand_context"
    ]
    assert any(
        ComponentKey("ns.country", "rows") in {provider for _, provider in edges}
        for edges in context_bindings
    )


def test_selected_dimension_output_type_change_updates_cube():
    before_specs = _contextual(specs())
    before = evaluate(before_specs)

    changed_specs = copy.deepcopy(before_specs)
    changed_specs["ns.country"].columns[1].type = "bigint"
    changed = evaluate(changed_specs)

    assert compare(before, changed, "ns.by_country") == "changed"


def test_multihop_context_tracks_target_key_and_ignores_intermediate_unused_output():
    before_specs = _indirect_country_route()
    before = evaluate(before_specs)

    changed_output = _add_unused_country_code(copy.deepcopy(before_specs))
    changed_output["ns.region"].query = "SELECT code, name, id FROM ns.lookup"
    changed_output["ns.region"].columns = [
        ColumnSpec(name="code", type="string"),
        ColumnSpec(name="name", type="string"),
        ColumnSpec(name="id", type="bigint"),
    ]
    changed_output_result = evaluate(changed_output)
    assert compare(before, changed_output_result, "ns.by_country") == "unchanged"

    changed_target_key = copy.deepcopy(before_specs)
    changed_target_key["ns.country"].primary_key = ["name"]
    changed_target_key_result = evaluate(changed_target_key)
    assert compare(before, changed_target_key_result, "ns.by_country") == "changed"


def test_configured_targets_match_full_evaluation_without_other_demands():
    nodes = specs()
    nodes["ns.by_region"] = CubeSpec(
        name="ns.by_region",
        metrics=["ns.total"],
        dimensions=["ns.region.name"],
    )
    snapshot = SemanticGraphSnapshot(nodes)
    full = asyncio.run(compute_configured_v2_fingerprints(snapshot))
    targeted = asyncio.run(
        compute_configured_v2_fingerprints(snapshot, targets={"ns.by_country"}),
    )

    assert set(targeted.nodes) == {"ns.by_country"}
    assert (
        compare_components(
            full.nodes["ns.by_country"].full,
            targeted.nodes["ns.by_country"].full,
        )
        == "unchanged"
    )
    assert sum(key.kind == "demand_context" for key in full.components) > sum(
        key.kind == "demand_context" for key in targeted.components
    )


def test_new_route_to_requested_dimension_changes_coarse_context_consumers():
    before_specs = _indirect_country_route()
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
    before_specs = _indirect_country_route()
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


@pytest.mark.parametrize("opaque_fact", [False, True])
def test_missing_requested_dimension_output_is_unknown(opaque_fact):
    nodes = _contextual(specs(opaque_fact=opaque_fact))
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
    providers = {
        provider for key in context_keys for _, provider in result.bindings[key]
    }
    assert ComponentKey("ns.country", "rows") in providers
    assert ComponentKey("ns.country", "output", "name") in providers
    assert ComponentKey("ns.country", "output", "id") in providers
    assert ComponentKey("ns.fact", "output", "country_id") in providers
    assert ComponentKey("ns.country", "calculation") not in providers
    assert all(
        edges == tuple(sorted(edges, key=lambda edge: (edge[0], edge[1])))
        for edges in result.bindings.values()
    )


def test_intermediate_context_rows_and_both_join_endpoints_are_bound():
    result = evaluate(_indirect_country_route())
    providers = {
        provider
        for key, edges in result.bindings.items()
        if key.kind == "demand_context"
        for _, provider in edges
    }

    assert ComponentKey("ns.fact", "output", "region_id") in providers
    assert ComponentKey("ns.region", "output", "id") in providers
    assert ComponentKey("ns.region", "rows") in providers
    assert ComponentKey("ns.country", "output", "id") in providers
    assert ComponentKey("ns.country", "rows") in providers


def test_dimension_context_ignores_presentation_metadata():
    before_specs = _contextual(specs())
    before = evaluate(before_specs)

    changed_specs = copy.deepcopy(before_specs)
    changed_specs["ns.country"].description = "Updated description"
    changed_specs["ns.country"].tags = ["updated"]
    changed_specs["ns.country"].columns[0].description = "Updated column description"
    changed_specs["ns.country"].columns[0].display_name = "Updated label"
    changed = evaluate(changed_specs)

    assert compare(before, changed, "ns.country") == "unchanged"
    assert compare(before, changed, "ns.by_country") == "unchanged"


def test_ancestry_walk_is_reused_within_one_evaluation(monkeypatch):
    nodes = specs()
    nodes["ns.root"] = TransformSpec(
        name="ns.root",
        query="SELECT amount, country_id FROM ns.fact",
    )
    query = nodes["ns.root"].query_ast
    original_find_all = ast.Query.find_all
    table_walks = 0

    def count_table_walks(self, node_type):
        nonlocal table_walks
        if self is query and node_type is ast.Table:
            table_walks += 1
        return original_find_all(self, node_type)

    monkeypatch.setattr(ast.Query, "find_all", count_table_walks)
    first_evaluation = {}
    assert v2_configured._ancestors("ns.root", nodes, first_evaluation) == {
        "ns.root",
        "ns.fact",
    }
    assert v2_configured._ancestors("ns.root", nodes, first_evaluation) == {
        "ns.root",
        "ns.fact",
    }
    assert table_walks == 1

    second_evaluation = {}
    assert v2_configured._ancestors("ns.root", nodes, second_evaluation) == {
        "ns.root",
        "ns.fact",
    }
    assert table_walks == 2


def test_filter_and_join_predicates_parse_once_per_evaluation(monkeypatch):
    nodes = specs()
    filter_text = "ns.country.name IS NOT NULL"
    nodes["ns.total"].required_dimensions = ["ns.country.name"]
    nodes["ns.total"].required_filters = [
        FilterRequirement(
            dimension=["ns.country.name"],
            default=filter_text,
        ),
    ]
    snapshot = SemanticGraphSnapshot(nodes)
    parse_calls = []
    original_parse = v2_configured.parse

    def count_parse(query):
        parse_calls.append(query)
        return original_parse(query)

    monkeypatch.setattr(v2_configured, "parse", count_parse)
    monkeypatch.setattr(v2_dependencies, "parse", count_parse)
    for _ in range(2):
        asyncio.run(compute_configured_v2_fingerprints(snapshot))

    filter_sql = f"SELECT 1 WHERE ({filter_text})"
    link = nodes["ns.fact"].dimension_links[0]
    join_sql = f"SELECT 1 WHERE ({link.rendered_join_on})"
    assert parse_calls.count(filter_sql) == 2
    assert parse_calls.count(join_sql) == 2


def test_nonmetric_definitions_are_shared_only_within_one_evaluation(monkeypatch):
    nodes = specs()
    nodes["ns.project"] = TransformSpec(
        name="ns.project",
        query="SELECT amount, country_id FROM ns.fact",
    )
    nodes["ns.total"].query = "SELECT SUM(amount) FROM ns.project"
    nodes["ns.total"].required_dimensions = ["ns.country.name"]
    counts = {"source": 0, "project": 0}
    original_source_definitions = v2_dependencies._source_definitions
    original_query_definitions = v2_dependencies._query_definitions

    def count_source_definitions(name, *args, **kwargs):
        if name == "ns.fact":
            counts["source"] += 1
        return original_source_definitions(name, *args, **kwargs)

    def count_query_definitions(name, *args, **kwargs):
        if name == "ns.project":
            counts["project"] += 1
        return original_query_definitions(name, *args, **kwargs)

    monkeypatch.setattr(
        v2_dependencies,
        "_source_definitions",
        count_source_definitions,
    )
    monkeypatch.setattr(
        v2_dependencies,
        "_query_definitions",
        count_query_definitions,
    )

    before = evaluate(nodes)
    assert counts == {"source": 1, "project": 1}

    changed = copy.deepcopy(nodes)
    changed["ns.fact"].columns[0].type = "bigint"
    after = evaluate(changed)

    assert counts == {"source": 2, "project": 2}
    assert compare(before, after, "ns.total") == "changed"


def test_unsupported_metric_cube_tracks_material_value_and_route_changes():
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
        {"default_value": True},
        {"default_value": 1},
        {"default_value": 1.0},
        {"join_on": "ns.fact.region_id = ns.country.id"},
    ],
)
@pytest.mark.parametrize("opaque_fact", [False, True])
def test_join_contract_changes_configured_metric_and_cube(update, opaque_fact):
    before_specs = _contextual(specs(opaque_fact=opaque_fact))
    before = evaluate(before_specs)
    after_specs = copy.deepcopy(before_specs)
    original = after_specs["ns.fact"].dimension_links[0]
    after_specs["ns.fact"].dimension_links[0] = DimensionJoinLinkSpec.model_validate(
        {**original.model_dump(), **update},
    )
    after = evaluate(after_specs)

    assert compare(before, after, "ns.total") == "changed"
    assert compare(before, after, "ns.by_country") == "changed"


@pytest.mark.parametrize("opaque_fact", [False, True])
def test_join_cardinality_does_not_change_metric_or_cube_data(opaque_fact):
    before_specs = _contextual(specs(opaque_fact=opaque_fact))
    before = evaluate(before_specs)
    after_specs = copy.deepcopy(before_specs)
    original = after_specs["ns.fact"].dimension_links[0]
    changed = DimensionJoinLinkSpec.model_validate(
        {**original.model_dump(), "join_cardinality": "one_to_many"},
    )
    after_specs["ns.fact"].dimension_links[0] = changed
    after = evaluate(after_specs)

    assert original._comparison_key() != changed._comparison_key()
    for name in ("ns.fact", "ns.total", "ns.by_country"):
        assert compare(before, after, name) == "unchanged"


def test_dimension_definition_change_reaches_required_metric_and_cube():
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


def test_unsupported_formula_retains_known_output_fallback():
    nodes = _contextual(specs())
    nodes["ns.derived"] = MetricSpec(
        name="ns.derived",
        query="SELECT unsupported_fn(amount) FROM ns.fact",
    )
    snapshot = SemanticGraphSnapshot(nodes)
    intrinsic = asyncio.run(compute_v2_fingerprints(snapshot))
    configured = asyncio.run(compute_configured_v2_fingerprints(snapshot))
    key = ComponentKey("ns.derived", "full")
    assert configured.nodes["ns.derived"].outputs.keys() == (
        intrinsic.nodes["ns.derived"].outputs.keys()
    )
    assert all(
        output.digest and "unsupported_function" in output.reasons
        for output in configured.nodes["ns.derived"].outputs.values()
    )
    assert configured.components[key].precision == "conservative"
    assert "unsupported_function" in configured.components[key].reasons
    assert configured.components[key].fingerprint != "unknown"


def _derived_specs():
    nodes = _contextual(specs())
    nodes["ns.count"] = MetricSpec(
        name="ns.count",
        query="SELECT COUNT(*) FROM ns.fact",
    )
    nodes["ns.average"] = MetricSpec(
        name="ns.average",
        query="SELECT ns.total / NULLIF(ns.count, 0)",
    )
    nodes["ns.per_minute"] = MetricSpec(
        name="ns.per_minute",
        query="SELECT ns.average / 60.0",
    )
    nodes["ns.average_cube"] = CubeSpec(
        name="ns.average_cube",
        metrics=["ns.per_minute"],
        dimensions=["ns.country.name"],
    )
    return nodes


def test_derived_metrics_and_cubes_have_known_configured_dependencies():
    nodes = _derived_specs()
    before = evaluate(nodes)
    consumers = ["ns.average", "ns.per_minute", "ns.average_cube"]
    assert all(before.nodes[name].full.digest for name in consumers)

    nodes["ns.country"].dimension_links.append(
        DimensionJoinLinkSpec(
            dimension_node="ns.region",
            join_on="ns.country.id = ns.region.id",
        ),
    )
    after = evaluate(nodes)
    assert all(compare(before, after, name) == "unchanged" for name in consumers)


@pytest.mark.parametrize(
    "mutation",
    ["base_formula", "derived_formula", "used_type", "route"],
)
def test_derived_metrics_retain_material_dependency_changes(mutation):
    nodes = _derived_specs()
    before = evaluate(nodes)
    if mutation == "base_formula":
        nodes["ns.total"].query = "SELECT SUM(amount) * 2 FROM ns.fact"
    elif mutation == "derived_formula":
        nodes["ns.average"].query = "SELECT ns.total / NULLIF(ns.count, 0) * 2"
    elif mutation == "used_type":
        nodes["ns.fact"].columns[0].type = "bigint"
    else:
        nodes["ns.fact"].dimension_links[
            0
        ].join_on = "ns.fact.region_id = ns.country.id"
    after = evaluate(nodes)
    for name in ("ns.average", "ns.per_minute", "ns.average_cube"):
        assert compare(before, after, name) == "changed"


def test_derived_missing_inputs_and_cycles_remain_unknown():
    for query in ("SELECT ns.missing / ns.total", "SELECT ns.per_minute / 2"):
        nodes = _derived_specs()
        nodes["ns.average"].query = query
        result = evaluate(nodes)
        for name in ("ns.average", "ns.per_minute", "ns.average_cube"):
            assert result.nodes[name].full.digest is None


def test_role_specific_alternate_route_does_not_change_unroled_demand():
    nodes = _contextual(specs())
    before = evaluate(nodes)
    nodes["ns.fact"].dimension_links.append(
        DimensionJoinLinkSpec(
            dimension_node="ns.region",
            join_on="ns.fact.region_id = ns.region.id",
        ),
    )
    nodes["ns.region"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.country",
            join_on="ns.region.id = ns.country.id",
            role="launch",
        ),
    ]
    after = evaluate(nodes)
    for name in ("ns.total", "ns.by_country"):
        assert compare(before, after, name) == "unchanged"

    nodes["ns.total"].required_dimensions = ["ns.country.name[launch]"]
    role_before = evaluate(nodes)
    assert role_before.nodes["ns.total"].full.digest
    nodes["ns.region"].dimension_links[0].join_on = "ns.region.id + 1 = ns.country.id"
    role_after = evaluate(nodes)
    assert compare(role_before, role_after, "ns.total") == "changed"


def test_composed_roles_require_the_requested_role_sequence():
    nodes = specs()
    nodes["ns.fact"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.region",
            join_on="ns.fact.region_id = ns.region.id",
            role="home",
        ),
    ]
    nodes["ns.region"].dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node="ns.country",
            join_on="ns.region.id = ns.country.id",
            role="launch",
        ),
    ]
    nodes["ns.total"].required_dimensions = ["ns.country.name[home->launch]"]
    known = evaluate(nodes)
    assert known.nodes["ns.total"].full.digest
    for role in ("", "launch", "launch->home"):
        nodes["ns.total"].required_dimensions = [
            f"ns.country.name[{role}]" if role else "ns.country.name",
        ]
        assert evaluate(nodes).nodes["ns.total"].full.digest is None


def test_metric_expression_dimension_uses_planner_any_role_lookup():
    nodes = specs()
    nodes["ns.fact"].dimension_links[0].role = "home"
    nodes["ns.total"].query = "SELECT COUNT(ns.country.name) FROM ns.fact"
    before = evaluate(nodes)
    assert before.nodes["ns.total"].full.digest
    nodes["ns.fact"].dimension_links[0].join_on = "ns.fact.region_id = ns.country.id"
    after = evaluate(nodes)
    assert compare(before, after, "ns.total") == "changed"


def test_transform_reading_metric_retains_its_metric_contract():
    nodes = _contextual(specs())
    nodes["ns.metric_rows"] = TransformSpec(
        name="ns.metric_rows",
        query="SELECT ns_DOT_total AS amount FROM ns.total",
        columns=[ColumnSpec(name="amount", type="double")],
    )
    before = evaluate(nodes)
    assert before.nodes["ns.metric_rows"].full.digest
    nodes["ns.total"].required_dimensions = ["ns.country.id"]
    after = evaluate(nodes)
    assert compare(before, after, "ns.metric_rows") == "changed"


def test_opaque_node_link_order_does_not_change_fingerprints():
    nodes = specs()
    nodes["ns.project"] = TransformSpec(
        name="ns.project",
        query="SELECT unsupported_fn(country_id) AS id FROM ns.fact",
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node=dimension,
                join_on=f"ns.project.id = {dimension}.id",
            )
            for dimension in ("ns.country", "ns.region")
        ],
    )
    before = evaluate(nodes)
    assert before.nodes["ns.project"].full.digest
    nodes["ns.project"].dimension_links.reverse()
    after = evaluate(nodes)
    assert before.nodes == after.nodes
    assert before.components == after.components
