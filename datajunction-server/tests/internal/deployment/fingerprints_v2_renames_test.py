"""Logical node renames preserve SQL fingerprints when data stays fixed."""

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
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)


def _source(name="ns.source", *, table="orders", amount_type="double"):
    return SourceSpec(
        name=name,
        catalog="warehouse",
        schema_="sales",
        table=table,
        columns=[
            ColumnSpec(name="amount", type=amount_type),
            ColumnSpec(name="id", type="bigint"),
        ],
    )


def _graph(
    source_name="ns.source",
    project_name="ns.project",
    *,
    table="orders",
    amount_type="double",
    calculation="amount",
    literal=None,
    metric_name="ns.total",
):
    where = f" WHERE '{literal}' = 'keep'" if literal is not None else ""
    return (
        _source(source_name, table=table, amount_type=amount_type),
        TransformSpec(
            name=project_name,
            query=f"SELECT {calculation} AS amount FROM {source_name}{where}",
        ),
        MetricSpec(name=metric_name, query=f"SELECT SUM(amount) FROM {project_name}"),
    )


def _evaluate(*specs):
    snapshot = SemanticGraphSnapshot({spec.rendered_name: spec for spec in specs})
    return asyncio.run(compute_v2_fingerprints(snapshot))


def _assert_comparison(
    before,
    after,
    node,
    expected,
    *,
    component="full",
    field=None,
    after_node=None,
):
    before_value = getattr(before.nodes[node], component)
    after_value = getattr(after.nodes[after_node or node], component)
    if field:
        before_value = before_value[field]
        after_value = after_value[field]
    assert compare_components(before_value, after_value) == expected


def _linked_specs(dimension_name="ns.region", join_on=None):
    orders = _source("ns.orders")
    orders.columns.append(ColumnSpec(name="region_id", type="bigint"))
    orders.dimension_links = [
        DimensionJoinLinkSpec(
            dimension_node=dimension_name,
            join_on=join_on or f"ns.orders.region_id = {dimension_name}.id",
        ),
    ]
    region_rows = _source("ns.region_rows", table="regions")
    region = DimensionSpec(
        name=dimension_name,
        query="SELECT id FROM ns.region_rows",
        columns=region_rows.columns,
        primary_key=["id"],
    )
    return orders, region_rows, region


def test_renaming_source_and_transform_preserves_downstream_digest():
    before = _evaluate(*_graph())
    after = _evaluate(
        *_graph(source_name="ns.order_events", project_name="ns.order_amounts"),
    )

    for original, renamed in (
        ("ns.source", "ns.order_events"),
        ("ns.project", "ns.order_amounts"),
    ):
        _assert_comparison(
            before,
            after,
            original,
            "unchanged",
            component="outputs",
            field="amount",
            after_node=renamed,
        )
    _assert_comparison(before, after, "ns.total", "unchanged")


def test_renaming_a_metric_preserves_its_value_fingerprint():
    before = _evaluate(*_graph())
    after = _evaluate(*_graph(metric_name="ns.renamed_total"))
    _assert_comparison(
        before,
        after,
        "ns.total",
        "unchanged",
        after_node="ns.renamed_total",
    )


@pytest.mark.parametrize("modifier", ["LIMIT 5", "ORDER BY amount"])
def test_opaque_nodes_do_not_hash_their_own_lookup_address(modifier):
    def evaluate(name):
        return _evaluate(
            _source(),
            TransformSpec(name=name, query=f"SELECT amount FROM ns.source {modifier}"),
            MetricSpec(name="ns.total", query=f"SELECT SUM(amount) FROM {name}"),
        )

    _assert_comparison(
        evaluate("ns.old"),
        evaluate("ns.renamed"),
        "ns.total",
        "unchanged",
    )


def test_physical_table_type_and_used_calculation_changes_still_propagate():
    before = _evaluate(*_graph())
    table = _evaluate(*_graph(table="order_history"))
    column_type = _evaluate(*_graph(amount_type="bigint"))
    calculation = _evaluate(*_graph(calculation="amount * 2"))

    for result in (table, column_type, calculation):
        _assert_comparison(before, result, "ns.total", "changed")
    _assert_comparison(
        before,
        table,
        "ns.source",
        "changed",
        component="outputs",
        field="amount",
    )


def test_renaming_never_rewrites_matching_string_literals():
    before = _evaluate(*_graph(literal="ns.source"))
    after = _evaluate(*_graph(literal="ns.order_events"))
    _assert_comparison(before, after, "ns.total", "changed")


def test_joined_expression_keeps_which_relation_supplies_each_field():
    left = _source("ns.left_input", table="left_rows")
    right = _source("ns.right_input", table="right_rows")
    before_query = (
        "SELECT l.amount - r.amount AS delta FROM ns.left_input l "
        "JOIN ns.right_input r ON l.id = r.id"
    )
    after_query = before_query.replace("l.amount - r.amount", "r.amount - l.amount")
    before = _evaluate(left, right, TransformSpec(name="ns.delta", query=before_query))
    after = _evaluate(left, right, TransformSpec(name="ns.delta", query=after_query))
    _assert_comparison(
        before,
        after,
        "ns.delta",
        "changed",
        component="outputs",
        field="delta",
    )


def test_joined_node_renames_ignore_lexical_name_order():
    alpha = _source("ns.alpha", table="alpha_rows")
    zeta = _source("ns.zeta", table="zeta_rows")
    query = (
        "SELECT a.amount + z.amount AS total FROM ns.alpha a "
        "JOIN ns.zeta z ON a.id = z.id AND a.amount > 0 "
        "WHERE z.amount > 1.000000000000000001"
    )
    before = _evaluate(alpha, zeta, TransformSpec(name="ns.joined", query=query))
    renamed = query.replace("ns.alpha", "ns.zeta").replace("ns.zeta z", "ns.alpha z")
    after = _evaluate(
        _source("ns.zeta", table="alpha_rows"),
        _source("ns.alpha", table="zeta_rows"),
        TransformSpec(name="ns.joined", query=renamed),
    )
    _assert_comparison(before, after, "ns.joined", "unchanged", component="rows")
    _assert_comparison(
        before,
        after,
        "ns.joined",
        "unchanged",
        component="outputs",
        field="total",
    )


def test_high_precision_query_filter_literal_change_changes_rows():
    source = _source()
    query = "SELECT amount FROM ns.source WHERE amount > {}"
    before = _evaluate(
        source,
        TransformSpec(
            name="ns.project",
            query=query.format("0.100000000000000001"),
        ),
    )
    after = _evaluate(
        source,
        TransformSpec(
            name="ns.project",
            query=query.format("0.100000000000000002"),
        ),
    )
    _assert_comparison(before, after, "ns.project", "changed", component="rows")


def test_renaming_a_dimension_link_target_preserves_the_link_fingerprint():
    before = _evaluate(*_linked_specs())
    after = _evaluate(*_linked_specs("ns.sales_region"))
    _assert_comparison(before, after, "ns.orders", "unchanged")


def test_dimension_join_preserves_exact_numeric_literal_values():
    low = _linked_specs(
        join_on="ns.orders.region_id + 0.100000000000000001 = ns.region.id",
    )
    high = _linked_specs(
        join_on="ns.orders.region_id + 0.100000000000000002 = ns.region.id",
    )
    _assert_comparison(_evaluate(*low), _evaluate(*high), "ns.orders", "changed")
