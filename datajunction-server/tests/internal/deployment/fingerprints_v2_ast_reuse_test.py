"""Reuse of DJ's parsed query ASTs during semantic fingerprinting."""

import asyncio

import pytest

from datajunction_server.internal.deployment.utils import extract_node_graph
from datajunction_server.models.deployment import (
    ColumnSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.engine import local_node_fingerprint
from datajunction_server.semantic_fingerprints.v2 import (
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)
from datajunction_server.sql.parsing import structural
from datajunction_server.sql.parsing.backends import antlr4


def source():
    return SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="sales",
        table="orders",
        columns=[ColumnSpec(name="amount", type="double")],
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
    ("required_dimensions", "expected_precision"),
    [([], "precise"), (["ns.project.amount"], "unknown")],
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
    if expected_precision == "unknown":
        assert after_v1_cache.nodes["ns.total"].full.fingerprint == "unknown"
    else:
        assert after_v1_cache.nodes["ns.total"].full.digest is not None


def evaluate_projection(query):
    return evaluate(source(), project(query))


def test_tablesample_erased_from_ast_does_not_produce_equal_fingerprint():
    before = evaluate_projection(
        "SELECT amount FROM ns.source TABLESAMPLE (10 PERCENT)",
    )
    after = evaluate_projection(
        "SELECT amount FROM ns.source TABLESAMPLE (20 PERCENT)",
    )

    assert (
        compare_components(
            before.nodes["ns.project"].full,
            after.nodes["ns.project"].full,
        )
        == "indeterminate"
    )


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

    assert trim_x.nodes["ns.project"].precision != "precise"
    assert trim_y.nodes["ns.project"].precision != "precise"
    assert (
        compare_components(
            trim_x.nodes["ns.project"].full,
            trim_y.nodes["ns.project"].full,
        )
        == "indeterminate"
    )
