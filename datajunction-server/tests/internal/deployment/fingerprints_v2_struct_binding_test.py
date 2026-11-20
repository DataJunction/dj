"""One-level struct references bind to a typed input output."""

import asyncio

import pytest
from datajunction_server.models.deployment import (
    ColumnSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.v2 import (
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)


def _evaluate(query, details_type="struct<audience_cnt bigint,unused_cnt bigint>"):
    specs = (
        SourceSpec(
            name="ns.input",
            catalog="warehouse",
            schema_="sales",
            table="input",
            columns=[
                ColumnSpec(name="details", type=details_type),
                ColumnSpec(name="unrelated", type="bigint"),
            ],
        ),
        TransformSpec(name="ns.projected", query=query),
        MetricSpec(
            name="ns.audience_total",
            query="SELECT SUM(audience_cnt) FROM ns.projected",
        ),
        MetricSpec(
            name="ns.unused_total",
            query="SELECT SUM(unused_cnt) FROM ns.projected",
        ),
    )
    return asyncio.run(
        compute_v2_fingerprints(
            SemanticGraphSnapshot({spec.rendered_name: spec for spec in specs}),
        ),
    )


def _changed(before, after, node, component="full", field=""):
    before_component = getattr(before.nodes[node], component)
    after_component = getattr(after.nodes[node], component)
    if component == "outputs":
        before_component = before_component[field]
        after_component = after_component[field]
    return compare_components(before_component, after_component)


def test_struct_field_change_isolated_to_the_selected_output():
    before = _evaluate(
        "SELECT details.audience_cnt AS audience_cnt, "
        "details.unused_cnt AS unused_cnt, unrelated FROM ns.input",
    )
    after = _evaluate(
        "SELECT details.unused_cnt AS audience_cnt, "
        "details.unused_cnt AS unused_cnt, unrelated FROM ns.input",
    )

    assert before.nodes["ns.projected"].precision == "precise"
    assert _changed(before, after, "ns.projected", "rows") == "unchanged"
    assert (
        _changed(before, after, "ns.projected", "outputs", "audience_cnt") == "changed"
    )
    assert (
        _changed(before, after, "ns.projected", "outputs", "unused_cnt") == "unchanged"
    )
    assert (
        _changed(before, after, "ns.projected", "outputs", "unrelated") == "unchanged"
    )
    assert _changed(before, after, "ns.audience_total") == "changed"
    assert _changed(before, after, "ns.unused_total") == "unchanged"


def test_struct_field_names_are_case_insensitive_and_use_declared_names():
    lower = _evaluate("SELECT details.audience_cnt AS audience_cnt FROM ns.input")
    upper = _evaluate("SELECT DETAILS.AUDIENCE_CNT AS audience_cnt FROM ns.input")

    assert lower.nodes["ns.projected"].precision == "precise"
    assert _changed(lower, upper, "ns.projected") == "unchanged"
    assert _changed(lower, upper, "ns.audience_total") == "unchanged"


def test_struct_root_type_change_reaches_downstream_metrics():
    query = "SELECT details.audience_cnt AS audience_cnt FROM ns.input"
    before = _evaluate(query)
    after = _evaluate(
        query,
        details_type="struct<audience_cnt double,unused_cnt bigint>",
    )

    assert before.nodes["ns.projected"].precision == "precise"
    assert (
        _changed(before, after, "ns.projected", "outputs", "audience_cnt") == "changed"
    )
    assert _changed(before, after, "ns.audience_total") == "changed"


@pytest.mark.parametrize(
    "details_type",
    [
        None,
        "string",
        "struct<audience_cnt bigint,AUDIENCE_CNT double>",
    ],
    ids=["missing-type", "non-struct-root", "ambiguous-field"],
)
def test_unknown_or_ambiguous_struct_schemas_remain_conservative(details_type):
    result = _evaluate(
        "SELECT details.audience_cnt AS audience_cnt FROM ns.input",
        details_type=details_type,
    )

    assert result.nodes["ns.projected"].precision in {"conservative", "unknown"}
    assert result.nodes["ns.projected"].reasons


def test_table_alias_takes_precedence_over_a_struct_root_name():
    specs = (
        SourceSpec(
            name="ns.input",
            catalog="warehouse",
            schema_="sales",
            table="input",
            columns=[
                ColumnSpec(name="details", type="struct<audience_cnt bigint>"),
                ColumnSpec(name="audience_cnt", type="bigint"),
                ColumnSpec(name="unrelated", type="bigint"),
            ],
        ),
        TransformSpec(
            name="ns.projected",
            query=(
                "SELECT details.audience_cnt AS audience_cnt FROM ns.input AS details"
            ),
        ),
    )
    result = asyncio.run(
        compute_v2_fingerprints(
            SemanticGraphSnapshot({spec.rendered_name: spec for spec in specs}),
        ),
    )

    assert result.nodes["ns.projected"].precision == "precise"
