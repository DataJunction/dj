"""Reuse of DJ's parsed query ASTs during semantic fingerprinting."""

import asyncio

import pytest

from datajunction_server.internal.deployment.utils import extract_node_graph
from datajunction_server.models.deployment import (
    ColumnSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints import v2_dependencies
from datajunction_server.semantic_fingerprints.engine import local_node_fingerprint
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentKey,
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)
from datajunction_server.semantic_fingerprints.v2_dependencies import (
    _opaque_relation_references,
    _opaque_sql_references,
)
from datajunction_server.sql.parsing import structural
from datajunction_server.sql.parsing.backends import antlr4


def source(
    name="ns.source",
    table="orders",
    columns=(("amount", "double"),),
    dimension_links=(),
):
    return SourceSpec(
        name=name,
        catalog="warehouse",
        schema_="sales",
        table=table,
        columns=[ColumnSpec(name=column, type=kind) for column, kind in columns],
        dimension_links=list(dimension_links),
    )


def project(query="SELECT amount FROM ns.source"):
    return TransformSpec(name="ns.project", query=query)


def metric(
    query="SELECT SUM(amount) FROM ns.project",
    required_dimensions=None,
):
    return MetricSpec(
        name="ns.total",
        query=query,
        required_dimensions=required_dimensions,
    )


def evaluate(*specs):
    return asyncio.run(
        compute_v2_fingerprints(
            SemanticGraphSnapshot({spec.rendered_name: spec for spec in specs}),
        ),
    )


def test_fingerprinting_reuses_cached_asts_without_mutating_them(monkeypatch):
    specs = [source(), project(), metric()]
    fresh_result = evaluate(*specs)
    query_specs = [spec for spec in specs if spec.rendered_query]
    caller_asts = {
        spec.rendered_name: structural.serialize_ast(spec.query_ast, version=1)
        for spec in query_specs
    }
    snapshot = SemanticGraphSnapshot(
        {spec.rendered_name: spec for spec in specs},
    )
    snapshot_asts = {
        name: structural.serialize_ast(spec.query_ast, version=1)
        for name, spec in snapshot._specs.items()
        if spec.rendered_query
    }

    parse_calls = []
    original_parse = antlr4.parse

    def count_parse(query):
        parse_calls.append(query)
        return original_parse(query)

    monkeypatch.setattr(antlr4, "parse", count_parse)
    monkeypatch.setattr(v2_dependencies, "parse", count_parse)
    result = asyncio.run(compute_v2_fingerprints(snapshot))

    assert result.nodes == fresh_result.nodes
    assert parse_calls == []
    assert result.nodes["ns.project"].precision == "precise"
    assert result.nodes["ns.total"].precision == "precise"
    assert result.nodes["ns.total"].full.digest == (
        "fc625ea9bb4d4508a5fb1d7e7321f6b63fe2793771f3d2352c483966693f6f1e"
    )
    assert {
        spec.rendered_name: structural.serialize_ast(spec.query_ast, version=1)
        for spec in query_specs
    } == caller_asts
    assert {
        name: structural.serialize_ast(spec.query_ast, version=1)
        for name, spec in snapshot._specs.items()
        if spec.rendered_query
    } == snapshot_asts


@pytest.mark.parametrize(
    ("query", "comparison"),
    [
        ("SELECT amount - 1 AS value FROM ns.source", "changed"),
        ("SELECT (", "indeterminate"),
    ],
)
def test_copied_query_invalidates_stale_ast_without_mutating_caller(query, comparison):
    original = project("SELECT amount + 1 AS value FROM ns.source")
    caller_ast = original.query_ast
    before = evaluate(source(), original)
    changed = original.model_copy(update={"query": query})

    after = evaluate(source(), changed)

    assert after.nodes == evaluate(source(), project(query)).nodes
    assert (
        compare_components(
            before.nodes["ns.project"].full,
            after.nodes["ns.project"].full,
        )
        == comparison
    )
    assert changed._query_ast is original._query_ast is caller_ast


def test_copied_namespace_invalidates_stale_ast_without_mutating_caller():
    original = TransformSpec(
        name="project",
        namespace="team_a",
        query="SELECT amount + 1 AS value FROM ${prefix}source",
    )
    caller_ast = original.query_ast
    changed = original.model_copy(update={"namespace": "team_b"})
    fresh = TransformSpec.model_validate(
        {**changed.model_dump(), "namespace": changed.namespace},
    )

    after = evaluate(source("team_b.source"), changed)

    assert after.nodes["team_b.project"].precision == "precise"
    assert after.nodes == evaluate(source("team_b.source"), fresh).nodes
    assert changed._query_ast is original._query_ast is caller_ast


@pytest.mark.parametrize(
    ("required_dimensions", "expected_precision"),
    [([], "precise"), (["ns.project.amount"], "conservative")],
)
def test_authored_metric_alias_is_stable_after_v1_parse_cache(
    required_dimensions,
    expected_precision,
):
    specs = [
        source(),
        project(),
        metric(
            "SELECT SUM(amount) AS authored_alias FROM ns.project",
            required_dimensions,
        ),
    ]
    fresh = evaluate(*specs)
    for spec in specs:
        local_node_fingerprint(spec)
    extract_node_graph(specs)
    after_v1_cache = evaluate(*specs)

    assert fresh.nodes == after_v1_cache.nodes
    assert after_v1_cache.nodes["ns.total"].precision == expected_precision
    assert after_v1_cache.nodes["ns.total"].full.digest is not None


def evaluate_projection(query):
    return evaluate(source(), project(query))


def evaluate_metric(query, required_dimensions):
    return evaluate(source(), project(), metric(query, required_dimensions))


def assert_changed(before, after):
    assert compare_components(before, after) == "changed"


def assert_unchanged(before, after):
    assert compare_components(before, after) == "unchanged"


def assert_conservative(result, node):
    assert result.nodes[node].precision == "conservative"
    assert result.nodes[node].full.digest is not None


def evaluate_opaque_relation(query, hidden_table):
    hidden = source(
        "ns.hidden",
        hidden_table,
        (("cutoff", "double"), ("trim_char", "string")),
    )
    return evaluate(
        source(),
        hidden,
        TransformSpec(
            name="ns.project",
            query=query,
            columns=[ColumnSpec(name="amount", type="double")],
        ),
    )


def test_opaque_query_tokens_ignore_comments_and_whitespace():
    before = evaluate_metric(
        "SELECT SUM(amount) FROM ns.project",
        ["ns.project.amount"],
    )
    after = evaluate_metric(
        "SELECT /* explanation */ SUM ( amount ) FROM ns.project",
        ["ns.project.amount"],
    )

    assert_conservative(before, "ns.total")
    assert_conservative(after, "ns.total")
    assert_unchanged(before.nodes["ns.total"].full, after.nodes["ns.total"].full)


def test_tablesample_erased_from_ast_does_not_produce_equal_fingerprint():
    before = evaluate_projection(
        "SELECT amount FROM ns.source TABLESAMPLE (10 PERCENT)",
    )
    after = evaluate_projection(
        "SELECT amount FROM ns.source TABLESAMPLE (20 PERCENT)",
    )

    assert_changed(before.nodes["ns.project"].full, after.nodes["ns.project"].full)


def test_numeric_literal_lexemes_do_not_collapse_to_the_same_fingerprint():
    cases = [
        ("SELECT amount + 1 FROM ns.source", "SELECT amount + 1.0 FROM ns.source"),
        (
            "SELECT amount + 0.30000000000000001 FROM ns.source",
            "SELECT amount + 0.3 FROM ns.source",
        ),
    ]
    for before_query, after_query in cases:
        before = evaluate_projection(before_query)
        after = evaluate_projection(after_query)

        assert compare_components(
            before.nodes["ns.project"].full,
            after.nodes["ns.project"].full,
        ) in {"changed", "indeterminate"}


@pytest.mark.parametrize(
    ("supported_query", "unsupported_query"),
    [
        (
            "SELECT CAST(amount AS INT) FROM ns.source",
            "SELECT TRY_CAST(amount AS INT) FROM ns.source",
        ),
        (
            "SELECT SUM(amount) FROM ns.source",
            "SELECT SUM(amount) FILTER (WHERE amount IS NULL) FROM ns.source",
        ),
        (
            "SELECT ANY_VALUE(amount) FROM ns.source",
            "SELECT ANY_VALUE(amount) IGNORE NULLS FROM ns.source",
        ),
    ],
)
def test_dj_ast_lossy_syntax_is_not_reported_as_equal(
    supported_query,
    unsupported_query,
):
    supported = evaluate_projection(supported_query)
    unsupported = evaluate_projection(unsupported_query)

    assert supported.nodes["ns.project"].precision == "precise"
    assert unsupported.nodes["ns.project"].precision != "precise"
    assert compare_components(
        supported.nodes["ns.project"].full,
        unsupported.nodes["ns.project"].full,
    ) in {"changed", "indeterminate"}


@pytest.mark.parametrize(
    "query",
    [
        (
            "SELECT amount FROM ns.source "
            "/* TRY_CAST FILTER IGNORE NULLS RESPECT NULLS */"
        ),
        (
            "SELECT 'TRY_CAST FILTER IGNORE NULLS RESPECT NULLS' AS keywords "
            "FROM ns.source"
        ),
    ],
)
def test_lossy_syntax_keywords_in_comments_and_strings_remain_supported(query):
    result = evaluate_projection(query)

    assert result.nodes["ns.project"].precision == "precise"


def test_nondefault_trim_characters_are_indeterminate():
    trim_x = evaluate_projection(
        "SELECT TRIM('x' FROM amount) FROM ns.source",
    )
    trim_y = evaluate_projection(
        "SELECT TRIM('y' FROM amount) FROM ns.source",
    )

    assert_conservative(trim_x, "ns.project")
    assert_conservative(trim_y, "ns.project")
    assert_changed(trim_x.nodes["ns.project"].full, trim_y.nodes["ns.project"].full)


@pytest.mark.parametrize(
    "query",
    [
        (
            "SELECT SUM(amount) FILTER (WHERE amount > "
            "(SELECT cutoff FROM ns.hidden)) AS amount FROM ns.source"
        ),
        (
            "SELECT TRIM((SELECT trim_char FROM ns.hidden) FROM amount) AS amount "
            "FROM ns.source"
        ),
        (
            "SELECT amount FROM ns.source TABLESAMPLE (10 PERCENT) "
            "WHERE amount > (SELECT cutoff FROM ns.hidden)"
        ),
    ],
)
def test_nested_relations_in_opaque_sql_modifiers_keep_parent_dependencies(query):
    before = evaluate_opaque_relation(query, "hidden_before")
    after = evaluate_opaque_relation(query, "hidden_after")

    assert_conservative(before, "ns.project")
    assert_changed(before.nodes["ns.project"].full, after.nodes["ns.project"].full)


def test_inner_cte_names_do_not_shadow_physical_relations_outside_their_scope():
    query = (
        "SELECT amount FROM orders WHERE EXISTS ("
        "WITH orders AS ("
        "SELECT amount FROM ns.source TABLESAMPLE (10 PERCENT)"
        ") SELECT 1 FROM orders)"
    )
    physical_orders = source("ns.orders", "orders_before")
    specs = {
        "ns.source": source(),
        "ns.orders": physical_orders,
        "ns.project": TransformSpec(name="ns.project", query=query),
    }
    relations, _, unresolved = _opaque_sql_references(specs["ns.project"], specs)
    assert "orders" in relations
    assert not unresolved

    before = evaluate(*specs.values())
    specs["ns.orders"] = physical_orders.model_copy(
        update={"table": "orders_after"},
    )
    after = evaluate(*specs.values())

    assert_changed(before.nodes["ns.project"].full, after.nodes["ns.project"].full)


def test_abbreviated_dimension_attribute_in_filter_keeps_node_and_role_route():
    fact = source(
        "ns.fact",
        "fact",
        (("amount", "double"), ("country_id", "bigint")),
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.country",
                join_on="ns.fact.country_id = ns.country.id",
                role="launch",
            ),
        ],
    )
    country_rows = source(
        "ns.country_rows",
        "countries",
        (("id", "bigint"), ("name", "string")),
    )
    country = DimensionSpec(
        name="ns.country",
        query="SELECT id, name FROM ns.country_rows",
        columns=country_rows.columns,
    )
    query = (
        "SELECT SUM(amount) FILTER "
        "(WHERE country.name[launch] = 'US') AS total FROM ns.fact"
    )
    transform = TransformSpec(
        name="ns.total",
        query=query,
        columns=[ColumnSpec(name="total", type="double")],
    )
    specs = {
        spec.rendered_name: spec for spec in (fact, country_rows, country, transform)
    }
    before = evaluate(*specs.values())
    changed_country = country.model_copy(
        update={"query": "SELECT id, UPPER(name) AS name FROM ns.country_rows"},
    )
    after_specs = {**specs, "ns.country": changed_country}
    after = evaluate(*after_specs.values())
    assert_changed(before.nodes["ns.total"].full, after.nodes["ns.total"].full)

    changed_route = fact.model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.country",
                    join_on="ns.fact.country_id + 1 = ns.country.id",
                    role="launch",
                ),
            ],
        },
        deep=True,
    )
    route_before = evaluate(*specs.values())
    route_after = evaluate(
        changed_route,
        country_rows,
        country,
        transform,
    )
    assert_changed(
        route_before.nodes["ns.total"].full,
        route_after.nodes["ns.total"].full,
    )
    assert (
        "opaque_dimension_output:ns.country:name",
        ComponentKey("ns.country", "output", "name"),
    ) in before.bindings[ComponentKey("ns.total", "opaque")]


def test_dimension_alias_overlap_uses_the_longest_matching_name():
    source_node = source()
    dimension = DimensionSpec(
        name="ns.ns",
        query="SELECT amount AS attr FROM ns.source",
        columns=[ColumnSpec(name="attr", type="double")],
    )
    transform = TransformSpec(
        name="ns.total",
        query=(
            "SELECT SUM(amount) FILTER (WHERE ns.ns.attr = 'x') AS total FROM ns.source"
        ),
    )
    specs = {
        "ns.source": source_node,
        "ns.ns": dimension,
        "ns.total": transform,
    }

    transform.query_ast
    references = _opaque_sql_references(transform, specs)

    assert references == ({"ns.source"}, {"ns.ns.attr"}, False)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT COUNT(amount) FILTER (WHERE ns.missing.country = 'x') AS n "
        "FROM ns.source",
        "SELECT COUNT(amount) FILTER (WHERE ns.country.missing = 'x') AS n "
        "FROM ns.source",
    ],
)
def test_unbound_or_missing_hidden_dimension_attributes_are_unknown(query):
    source_node = source()
    dimension = DimensionSpec(
        name="ns.country",
        query="SELECT amount AS id FROM ns.source",
        columns=[ColumnSpec(name="id", type="string")],
    )
    transform = TransformSpec(name="ns.total", query=query)
    result = evaluate(source_node, dimension, transform)

    assert result.nodes["ns.total"].precision == "unknown"
    assert result.nodes["ns.total"].full.digest is None


@pytest.mark.parametrize(
    "query",
    [
        "SELECT COUNT(amount) FILTER (WHERE s.amount > 0) AS n FROM ns.source AS s",
        "SELECT COUNT(amount) FILTER (WHERE ns.source.amount > 0) AS n FROM ns.source",
        (
            "SELECT COUNT(amount) FILTER "
            "(WHERE s.profile.country = 'US') AS n FROM ns.source AS s"
        ),
        (
            "WITH ns_source AS (SELECT amount, profile FROM ns.source) "
            "SELECT COUNT(amount) FILTER "
            "(WHERE ns_source.profile.country = 'US') AS n FROM ns_source"
        ),
    ],
)
def test_opaque_dotted_input_refs_bind_to_alias_schema_or_cte(query):
    source_node = source(
        columns=(("amount", "double"), ("profile", "struct<country:string>")),
    )
    transform = TransformSpec(name="ns.total", query=query)
    result = evaluate(source_node, transform)

    assert result.nodes["ns.total"].precision == "conservative"
    assert result.nodes["ns.total"].full.digest is not None


def test_lambda_local_dereference_uses_its_lexical_binding():
    source_node = source(
        columns=(("amount", "double"), ("items", "array<struct<foo:int>>")),
    )
    transform = TransformSpec(
        name="ns.total",
        query=(
            "SELECT COUNT(amount) FILTER "
            "(WHERE ARRAY_FILTER(items, x -> x.foo > 0) IS NOT NULL) AS n "
            "FROM ns.source"
        ),
    )
    refs = _opaque_sql_references(
        transform,
        {"ns.source": source_node, "ns.total": transform},
    )

    assert refs == ({"ns.source"}, set(), False)
    result = evaluate(source_node, transform)
    assert_conservative(result, "ns.total")


def test_later_sibling_cte_reference_is_unknown():
    transform = TransformSpec(
        name="ns.total",
        query=(
            "WITH first AS (SELECT amount FROM later), "
            "later AS (SELECT amount FROM ns.source) "
            "SELECT amount FROM first"
        ),
    )

    _, unresolved = _opaque_relation_references(transform)

    assert unresolved


@pytest.mark.parametrize(
    "query",
    [
        (
            "SELECT COUNT(amount) FILTER "
            "(WHERE u.profile.country = 'US') AS n "
            "FROM (SELECT amount, profile FROM ns.source) AS u"
        ),
        (
            "SELECT COUNT(amount) FILTER (WHERE observed.profile_id > 0) AS n "
            "FROM ns.source LATERAL VIEW EXPLODE(items) observed AS profile_id"
        ),
    ],
)
def test_opaque_dotted_refs_bind_to_derived_and_lateral_outputs(query):
    source_node = source(
        columns=(
            ("amount", "double"),
            ("profile", "struct<country:string>"),
            ("items", "array<bigint>"),
        ),
    )
    transform = TransformSpec(name="ns.total", query=query)
    result = evaluate(source_node, transform)

    assert_conservative(result, "ns.total")


def test_unqualified_struct_root_from_derived_projection_is_bound():
    source_node = source(
        columns=(
            ("amount", "double"),
            ("alloc_windows", "array<struct<alloc_cell:string>>"),
        ),
    )
    transform = TransformSpec(
        name="ns.total",
        query=(
            "SELECT COUNT(amount) FILTER "
            "(WHERE last_window.alloc_cell IS NOT NULL) AS n "
            "FROM (SELECT amount, ARRAY_MAX(alloc_windows) AS last_window "
            "FROM ns.source) windows"
        ),
    )
    result = evaluate(source_node, transform)

    assert_conservative(result, "ns.total")
