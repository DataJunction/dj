"""Joined SQL fallback tracks calculations while keeping optional links separate."""

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


def _region():
    rows = _source("ns.region_rows", "regions", [("id", "bigint"), ("name", "string")])
    region = DimensionSpec(
        name="ns.region",
        query="SELECT id, name FROM ns.region_rows",
        columns=rows.columns,
        primary_key=["id"],
    )
    return rows, region


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
