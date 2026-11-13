"""Joined SQL fingerprints track calculations while keeping optional links separate."""

import asyncio

import pytest

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


def _source(name, table, columns, *, dimension_links=None):
    return SourceSpec(
        name=name,
        catalog="warehouse",
        schema_="sales",
        table=table,
        columns=[ColumnSpec(name=column, type=kind) for column, kind in columns],
        dimension_links=dimension_links or [],
    )


def _evaluate(*specs):
    snapshot = SemanticGraphSnapshot(
        {spec.rendered_name: spec for spec in specs},
    )
    return asyncio.run(compute_v2_fingerprints(snapshot))


def _component(result, node, kind, field=""):
    return result.components[ComponentKey(node, kind, field)]


def _comparison(before, after, node, kind, field=""):
    return compare_components(
        _component(before, node, kind, field),
        _component(after, node, kind, field),
    )


def _region():
    rows = _source("ns.region_rows", "regions", [("id", "bigint"), ("name", "string")])
    region = DimensionSpec(
        name="ns.region",
        query="SELECT id, name FROM ns.region_rows",
        columns=rows.columns,
        primary_key=["id"],
    )
    return rows, region


def _mobile_join_specs(*, query=None):
    sessions = _source(
        "ns.profile_sessions",
        "profile_sessions",
        [
            ("session_id", "string"),
            ("account_id", "bigint"),
            ("source", "string"),
            ("device_type_id", "bigint"),
            ("device_model", "string"),
            ("alternate_model", "string"),
            ("amount", "double"),
        ],
    )
    device_rows = _source(
        "ns.device_rows",
        "device_rows",
        [
            ("device_type_id", "bigint"),
            ("device_model", "string"),
            ("hw_major_category", "string"),
            ("cl_source", "string"),
        ],
    )
    columns = [
        ColumnSpec(name="device_type_id", type="bigint"),
        ColumnSpec(name="device_model", type="string"),
        ColumnSpec(name="hw_major_category", type="string"),
    ]
    device = DimensionSpec(
        name="ns.device",
        query="SELECT device_type_id, device_model, hw_major_category FROM ns.device_rows",
        columns=columns,
        primary_key=["device_type_id", "device_model"],
    )
    joined = TransformSpec(
        name="ns.mobile_sessions",
        query=query
        or """
            SELECT s.session_id, s.amount AS amount, d.hw_major_category
            FROM ns.profile_sessions AS s
            JOIN ns.device AS d
              ON s.device_type_id = d.device_type_id
             AND s.device_model = d.device_model
            WHERE s.source IN ('android', 'iOS')
              AND d.hw_major_category IN ('Mobile', 'Tablet')
        """,
    )
    count = MetricSpec(
        name="ns.mobile_session_count",
        query="SELECT COUNT(*) FROM ns.mobile_sessions",
    )
    amount = MetricSpec(
        name="ns.mobile_session_amount",
        query="SELECT SUM(amount) FROM ns.mobile_sessions",
    )
    return sessions, device_rows, device, joined, count, amount


def test_joined_fallback_tracks_all_input_calculations():
    left = _source(
        "ns.orders",
        "orders",
        [("id", "bigint"), ("amount", "double")],
    )
    right = _source(
        "ns.adjustments",
        "adjustments",
        [("order_id", "bigint"), ("offset", "double"), ("active", "boolean")],
    )
    query = (
        "SELECT SUM(o.amount + a.offset) AS value FROM ns.orders o "
        "JOIN ns.adjustments a ON o.id = a.order_id WHERE a.active = true"
    )
    joined = TransformSpec(name="ns.joined", query=query)
    before = _evaluate(left, right, joined)

    changed_right = right.model_copy(
        update={
            "columns": [
                ColumnSpec(name="order_id", type="bigint"),
                ColumnSpec(name="offset", type="bigint"),
                ColumnSpec(name="active", type="boolean"),
            ],
        },
    )
    after = _evaluate(left, changed_right, joined)

    assert before.nodes["ns.joined"].precision == "conservative"
    assert before.nodes["ns.joined"].full.digest is not None
    assert (
        compare_components(
            before.nodes["ns.joined"].outputs["value"],
            after.nodes["ns.joined"].outputs["value"],
        )
        == "changed"
    )


def test_unused_joined_dimension_output_does_not_change_existing_metrics():
    specs = _mobile_join_specs()
    before = _evaluate(*specs)
    sessions, device_rows, _, joined, count, amount = specs
    expanded_device = DimensionSpec(
        name="ns.device",
        query=(
            "SELECT device_type_id, device_model, hw_major_category, cl_source "
            "FROM ns.device_rows"
        ),
        columns=[
            ColumnSpec(name="device_type_id", type="bigint"),
            ColumnSpec(name="device_model", type="string"),
            ColumnSpec(name="hw_major_category", type="string"),
            ColumnSpec(name="cl_source", type="string"),
        ],
        primary_key=["device_type_id", "device_model"],
    )
    after = _evaluate(sessions, device_rows, expanded_device, joined, count, amount)

    assert _comparison(before, after, "ns.device", "full") == "changed"
    assert before.nodes["ns.mobile_sessions"].precision == "precise"
    assert all(
        _comparison(before, after, node, kind, field) == "unchanged"
        for node, kind, field in (
            ("ns.mobile_sessions", "rows", ""),
            ("ns.mobile_sessions", "full", ""),
            ("ns.mobile_sessions", "output", "amount"),
            ("ns.mobile_session_count", "full", ""),
            ("ns.mobile_session_amount", "full", ""),
        )
    )


@pytest.mark.parametrize(
    "change",
    [
        "join_key",
        "dimension_filter",
        "input_filter",
    ],
)
def test_join_and_filter_changes_reach_metric_rows(change):
    specs = _mobile_join_specs()
    before = _evaluate(*specs)
    sessions, device_rows, device, joined, count, amount = specs
    query = joined.query
    if change == "join_key":
        query = query.replace(
            "s.device_model = d.device_model",
            "s.alternate_model = d.device_model",
        )
    elif change == "dimension_filter":
        query = query.replace("('Mobile', 'Tablet')", "('Mobile')")
    else:
        query = query.replace("('android', 'iOS')", "('android')")
    changed = joined.model_copy(update={"query": query})
    after = _evaluate(sessions, device_rows, device, changed, count, amount)

    assert before.nodes["ns.mobile_sessions"].precision == "precise"
    assert _comparison(before, after, "ns.mobile_sessions", "rows") == "changed"
    for metric_name in ("ns.mobile_session_count", "ns.mobile_session_amount"):
        assert _comparison(before, after, metric_name, "full") == "changed"


def test_referenced_join_value_changes_only_its_metric():
    specs = _mobile_join_specs()
    before = _evaluate(*specs)
    sessions, device_rows, device, joined, count, amount = specs
    changed = joined.model_copy(
        update={"query": joined.query.replace("s.amount", "s.amount + 1")},
    )
    after = _evaluate(sessions, device_rows, device, changed, count, amount)

    assert (
        _comparison(
            before,
            after,
            "ns.mobile_session_count",
            "full",
        )
        == "unchanged"
    )
    assert (
        _comparison(
            before,
            after,
            "ns.mobile_session_amount",
            "full",
        )
        == "changed"
    )


def test_same_named_and_nested_join_columns_keep_provider_identity():
    left = SourceSpec(
        name="ns.left_rows",
        catalog="warehouse",
        schema_="sales",
        table="left_rows",
        columns=[
            ColumnSpec(name="id", type="bigint"),
            ColumnSpec(name="amount", type="double"),
            ColumnSpec(name="payload_a", type="struct<id bigint>"),
            ColumnSpec(name="payload_b", type="struct<id bigint>"),
        ],
    )
    right = SourceSpec(
        name="ns.right_rows",
        catalog="warehouse",
        schema_="sales",
        table="right_rows",
        columns=[
            ColumnSpec(name="id", type="bigint"),
            ColumnSpec(name="amount", type="double"),
        ],
        primary_key=["id"],
    )
    query = (
        "SELECT l.amount - r.amount AS difference, l.payload_a.id AS nested_value "
        "FROM ns.left_rows l JOIN ns.right_rows r ON l.id = r.id"
    )
    joined = TransformSpec(name="ns.bound_join", query=query)
    before = _evaluate(left, right, joined)
    swapped = joined.model_copy(
        update={"query": query.replace("l.amount - r.amount", "r.amount - l.amount")},
    )
    nested = joined.model_copy(
        update={"query": query.replace("l.payload_a.id", "l.payload_b.id")},
    )
    swapped_result = _evaluate(left, right, swapped)
    nested_result = _evaluate(left, right, nested)

    assert before.nodes["ns.bound_join"].precision == "precise"
    assert (
        _comparison(
            before,
            swapped_result,
            "ns.bound_join",
            "output",
            "difference",
        )
        == "changed"
    )
    assert (
        _comparison(
            before,
            nested_result,
            "ns.bound_join",
            "output",
            "nested_value",
        )
        == "changed"
    )


@pytest.mark.parametrize(
    "query",
    [
        "SELECT s.session_id FROM ns.profile_sessions s LEFT JOIN ns.device d ON s.device_type_id = d.device_type_id AND s.device_model = d.device_model",
        "SELECT s.session_id FROM ns.profile_sessions s JOIN ns.device d ON s.device_type_id = d.device_type_id AND s.device_model = d.device_model JOIN ns.device d2 ON s.device_type_id = d2.device_type_id AND s.device_model = d2.device_model",
        "SELECT s.session_id FROM ns.profile_sessions s JOIN ns.profile_sessions s2 ON s.session_id = s2.session_id",
        'SELECT s.session_id FROM ns.profile_sessions AS "s" JOIN ns.device d ON s.device_type_id = d.device_type_id AND s.device_model = d.device_model',
        "SELECT s.device_type_id.missing FROM ns.profile_sessions s JOIN ns.device d ON s.device_type_id = d.device_type_id AND s.device_model = d.device_model",
        "SELECT * FROM ns.profile_sessions s JOIN ns.device d ON s.device_type_id = d.device_type_id AND s.device_model = d.device_model",
    ],
)
def test_outer_and_other_join_shapes_remain_conservative(query):
    sessions, device_rows, device, _, _, _ = _mobile_join_specs()
    result = _evaluate(
        sessions,
        device_rows,
        device,
        TransformSpec(name="ns.unsupported_join", query=query),
    )

    assert result.nodes["ns.unsupported_join"].precision == "conservative"
    assert result.nodes["ns.unsupported_join"].reasons


def test_optional_links_do_not_change_unrelated_join_calculation_components():
    dimension_source, region = _region()
    fact = _source(
        "ns.fact",
        "fact",
        [("id", "bigint"), ("region_id", "bigint"), ("amount", "double")],
    )
    linked_fact = fact.model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.region",
                    join_on="ns.fact.region_id = ns.region.id",
                ),
            ],
        },
        deep=True,
    )
    joined = TransformSpec(
        name="ns.joined",
        query="SELECT SUM(f.amount) AS value FROM ns.fact f",
    )

    before = _evaluate(fact, dimension_source, region, joined)
    after = _evaluate(linked_fact, dimension_source, region, joined)

    assert (
        _component(before, "ns.fact", "calculation").digest
        == _component(
            after,
            "ns.fact",
            "calculation",
        ).digest
    )
    assert before.nodes["ns.fact"].outputs == after.nodes["ns.fact"].outputs
    assert before.nodes["ns.fact"].rows == after.nodes["ns.fact"].rows
    assert (
        compare_components(before.nodes["ns.fact"].full, after.nodes["ns.fact"].full)
        == "changed"
    )
    assert (
        compare_components(
            before.nodes["ns.joined"].full,
            after.nodes["ns.joined"].full,
        )
        == "unchanged"
    )


@pytest.mark.parametrize("through_ancestor", [False, True])
def test_implicit_dimension_reference_tracks_the_link_contract(through_ancestor):
    dimension_source, region = _region()
    fact = _source(
        "ns.fact",
        "fact",
        [("region_id", "bigint"), ("amount", "double")],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.region",
                join_on="ns.fact.region_id = ns.region.id",
            ),
        ],
    )
    parent = TransformSpec(
        name="ns.input",
        query="SELECT amount, region_id FROM ns.fact",
    )
    metric = MetricSpec(
        name="ns.region_name_count",
        query=(
            "SELECT COUNT(amount) FILTER (WHERE ns.region.name = 'US') FROM ns.input"
            if through_ancestor
            else "SELECT COUNT(ns.region.name) FROM ns.fact"
        ),
    )
    before = _evaluate(dimension_source, region, fact, parent, metric)

    changed_fact = fact.model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.region",
                    join_on="ns.fact.region_id + 1 = ns.region.id",
                ),
            ],
        },
        deep=True,
    )
    after = _evaluate(dimension_source, region, changed_fact, parent, metric)

    assert before.nodes["ns.region_name_count"].precision == "conservative"
    assert (
        compare_components(
            before.nodes["ns.region_name_count"].full,
            after.nodes["ns.region_name_count"].full,
        )
        == "changed"
    )
    missing_link = fact.model_copy(update={"dimension_links": []})
    unresolved = _evaluate(dimension_source, region, missing_link, parent, metric)
    assert unresolved.nodes["ns.region_name_count"].full.digest is None


def test_required_dimension_context_tracks_the_link_contract():
    dimension_source, region = _region()
    fact = _source(
        "ns.fact",
        "fact",
        [("region_id", "bigint"), ("amount", "double")],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.region",
                join_on="ns.fact.region_id = ns.region.id",
            ),
        ],
    )
    metric = MetricSpec(
        name="ns.required_region_metric",
        query="SELECT SUM(amount) FROM ns.fact",
        required_dimensions=["ns.region.name"],
    )
    before = _evaluate(dimension_source, region, fact, metric)

    changed_fact = fact.model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.region",
                    join_on="ns.fact.region_id + 1 = ns.region.id",
                ),
            ],
        },
        deep=True,
    )
    after = _evaluate(dimension_source, region, changed_fact, metric)

    assert before.nodes["ns.required_region_metric"].precision == "conservative"
    assert (
        compare_components(
            before.nodes["ns.required_region_metric"].full,
            after.nodes["ns.required_region_metric"].full,
        )
        == "changed"
    )


def test_optional_link_cycles_do_not_make_calculations_unknown():
    left = _source(
        "ns.left",
        "left_rows",
        [("id", "bigint"), ("right_id", "bigint")],
    )
    right = _source(
        "ns.right",
        "right_rows",
        [("id", "bigint"), ("left_id", "bigint")],
    )
    linked_left = left.model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.right",
                    join_on="ns.left.right_id = ns.right.id",
                ),
            ],
        },
        deep=True,
    )
    linked_right = right.model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.left",
                    join_on="ns.right.left_id = ns.left.id",
                ),
            ],
        },
        deep=True,
    )

    result = _evaluate(linked_left, linked_right)

    assert result.nodes["ns.left"].full.digest is not None
    assert result.nodes["ns.right"].full.digest is not None
    assert _component(result, "ns.left", "calculation").digest is not None
    assert _component(result, "ns.right", "calculation").digest is not None


def test_joined_fallback_is_independent_of_input_mapping_order():
    left = _source("ns.left", "left_rows", [("id", "bigint"), ("value", "double")])
    right = _source("ns.right", "right_rows", [("id", "bigint"), ("other", "double")])
    joined = TransformSpec(
        name="ns.joined",
        query=(
            "SELECT SUM(a.value + b.other) AS total FROM ns.left a "
            "JOIN ns.right b ON a.id = b.id"
        ),
    )

    forward = _evaluate(left, right, joined)
    reverse = _evaluate(joined, right, left)

    assert forward.nodes == reverse.nodes
    assert forward.components == reverse.components


def test_cte_fallback_resolves_the_underlying_source_not_the_cte_alias():
    source = _source("ns.source", "orders", [("amount", "double")])
    transform = TransformSpec(
        name="ns.cte",
        query=("WITH rows AS (SELECT amount FROM ns.source) SELECT amount FROM rows"),
    )
    before = _evaluate(source, transform)
    after = _evaluate(
        source.model_copy(update={"table": "replacement_orders"}),
        transform,
    )

    assert before.nodes["ns.cte"].precision == "conservative"
    assert (
        compare_components(before.nodes["ns.cte"].full, after.nodes["ns.cte"].full)
        == "changed"
    )
