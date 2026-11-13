"""Flat projection and row fingerprint behavior."""

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


def source(*, extra=False, amount_type="double", table="orders"):
    columns = [
        ColumnSpec(name="amount", type=amount_type),
        ColumnSpec(name="debug", type="bigint"),
        ColumnSpec(name="active", type="boolean"),
    ]
    if extra:
        columns.append(ColumnSpec(name="unused", type="string"))
    return SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="sales",
        table=table,
        columns=columns,
    )


def project(query="SELECT amount, debug FROM ns.source"):
    return TransformSpec(name="ns.project", query=query)


def metric(name="ns.total", query="SELECT SUM(amount) FROM ns.project"):
    return MetricSpec(name=name, query=query)


def evaluate(*specs, targets=None, resolved_columns=None):
    snapshot = SemanticGraphSnapshot(
        {spec.rendered_name: spec for spec in specs},
        resolved_columns=resolved_columns,
    )
    return asyncio.run(compute_v2_fingerprints(snapshot, targets=targets))


def compare(before, after, name):
    return compare_components(before.nodes[name].full, after.nodes[name].full)


def test_unused_source_output_does_not_change_consumers():
    before = evaluate(source(), project(), metric())
    after = evaluate(source(extra=True), project(), metric())
    assert compare(before, after, "ns.source") == "changed"
    assert compare(before, after, "ns.total") == "unchanged"


def test_editing_one_projected_value_only_changes_its_consumers():
    count = metric("ns.count", "SELECT COUNT(*) FROM ns.project")
    total = metric()
    debug = metric("ns.debug", "SELECT SUM(debug) FROM ns.project")
    before = evaluate(source(), project(), total, count, debug)
    after = evaluate(
        source(),
        project("SELECT amount, debug + 1 AS debug FROM ns.source"),
        total,
        count,
        debug,
    )
    assert compare(before, after, "ns.project") == "changed"
    assert compare(before, after, "ns.total") == "unchanged"
    assert compare(before, after, "ns.count") == "unchanged"
    assert compare(before, after, "ns.debug") == "changed"


def test_where_edit_changes_rows_and_aggregate_consumers():
    count = metric("ns.count", "SELECT COUNT(*) FROM ns.project")
    total = metric()
    before = evaluate(source(), project(), total, count)
    after = evaluate(
        source(),
        project("SELECT amount, debug FROM ns.source WHERE active = true"),
        total,
        count,
    )
    assert before.nodes["ns.project"].rows != after.nodes["ns.project"].rows
    assert compare(before, after, "ns.total") == "changed"
    assert compare(before, after, "ns.count") == "changed"


def test_used_source_type_and_source_grain_remain_semantic():
    total = metric()
    before = evaluate(source(), project(), total)
    typed = evaluate(source(amount_type="bigint"), project(), total)
    keyed_source = source().model_copy(update={"primary_key": ["amount"]})
    keyed = evaluate(keyed_source, project(), total)
    assert compare(before, typed, "ns.total") == "changed"
    assert compare(before, keyed, "ns.total") == "changed"


def test_captured_external_type_overrides_an_empty_authored_schema():
    external = source().model_copy(update={"columns": None})
    projection = project()
    total = metric()
    before = evaluate(
        external,
        projection,
        total,
        resolved_columns={
            "ns.source": [
                ColumnSpec(name="amount", type="double"),
                ColumnSpec(name="debug", type="bigint"),
                ColumnSpec(name="active", type="boolean"),
            ],
        },
    )
    after = evaluate(
        external,
        projection,
        total,
        resolved_columns={
            "ns.source": [
                ColumnSpec(name="amount", type="bigint"),
                ColumnSpec(name="debug", type="bigint"),
                ColumnSpec(name="active", type="boolean"),
            ],
        },
    )
    assert compare(before, after, "ns.total") == "changed"


def test_ast_canonicalizes_aliases_and_comments():
    before = evaluate(source(), project("SELECT a.amount, a.debug FROM ns.source a"))
    after = evaluate(
        source(),
        project("select b.amount, /* prose */ b.debug from ns.source b"),
    )
    assert compare(before, after, "ns.project") == "unchanged"


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM ns.source",
        "SELECT amount FROM ns.source JOIN ns.source other ON ns.source.amount = other.amount",
        "WITH rows AS (SELECT amount FROM ns.source) SELECT amount FROM rows",
        "SELECT amount FROM ns.source GROUP BY 1",
    ],
)
def test_unsupported_sql_uses_conservative_fallback(query):
    result = evaluate(
        source(),
        project(query),
        metric("ns.count", "SELECT COUNT(*) FROM ns.project"),
    )
    assert result.nodes["ns.project"].precision in {"conservative", "unknown"}
    assert result.nodes["ns.project"].reasons


def test_context_metric_uses_conservative_fallback_in_standalone_v2():
    contextual = metric().model_copy(update={"required_dimensions": ["ns.region.name"]})
    result = evaluate(source(), project(), contextual)
    assert result.nodes["ns.total"].precision in {"conservative", "unknown"}


def test_missing_source_and_unknown_comparison_are_indeterminate():
    missing = evaluate(project()).nodes["ns.project"].full
    known = evaluate(source(), project()).nodes["ns.project"].full
    assert compare_components(missing, missing) == "indeterminate"
    assert compare_components(missing, known) == "indeterminate"
    assert compare_components(known, known) == "unchanged"


def test_snapshot_ignores_later_input_mutation():
    original = source()
    snapshot = SemanticGraphSnapshot({"ns.source": original, "ns.project": project()})
    before = asyncio.run(compute_v2_fingerprints(snapshot))
    original.table = "changed"
    after = asyncio.run(compute_v2_fingerprints(snapshot))
    assert before.nodes == after.nodes


def test_v1_fast_parse_cache_does_not_change_v2_results():
    nodes = [source(), project(), metric()]
    fresh = evaluate(*nodes)
    for node in nodes:
        local_node_fingerprint(node)
    extract_node_graph(nodes)
    after_v1_parse = evaluate(*nodes)
    assert fresh.nodes == after_v1_parse.nodes
