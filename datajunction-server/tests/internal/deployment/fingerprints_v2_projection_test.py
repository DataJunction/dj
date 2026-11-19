"""Flat joined projections isolate output edits from row changes."""

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


def _source(name, table, columns):
    return SourceSpec(
        name=name,
        catalog="warehouse",
        schema_="sales",
        table=table,
        columns=[ColumnSpec(name=column, type=kind) for column, kind in columns],
    )


def _query(
    *,
    join_type="LEFT",
    join_on="o.adjustment_id = r.id",
    where="o.status = 'active'",
    rate="r.rate",
    unused="r.unused_amount",
):
    join = (
        "CROSS JOIN ns.adjustments AS r"
        if join_type == "CROSS"
        else f"{join_type} JOIN ns.adjustments AS r ON {join_on}"
    )
    return f"""
        SELECT
          o.id AS id,
          o.amount + {rate} AS billed_amount,
          {unused} AS unused_amount
        FROM ns.orders AS o
        {join}
        WHERE {where}
    """


def _evaluate(query):
    specs = (
        _source(
            "ns.orders",
            "orders",
            [
                ("id", "bigint"),
                ("adjustment_id", "bigint"),
                ("alternate_adjustment_id", "bigint"),
                ("amount", "double"),
                ("status", "string"),
                ("properties", "map<string,string>"),
            ],
        ),
        _source(
            "ns.adjustments",
            "adjustments",
            [
                ("id", "bigint"),
                ("rate", "double"),
                ("unused_amount", "double"),
                ("rates", "array<double>"),
            ],
        ),
        TransformSpec(name="ns.joined", query=query),
        MetricSpec(
            name="ns.billed_total",
            query="SELECT SUM(billed_amount) FROM ns.joined",
        ),
        MetricSpec(name="ns.row_count", query="SELECT COUNT(*) FROM ns.joined"),
        MetricSpec(
            name="ns.unused_total",
            query="SELECT SUM(unused_amount) FROM ns.joined",
        ),
    )
    snapshot = SemanticGraphSnapshot({spec.rendered_name: spec for spec in specs})
    return asyncio.run(compute_v2_fingerprints(snapshot))


def _changed(before, after, node, component="full", field=""):
    before_component = getattr(before.nodes[node], component)
    after_component = getattr(after.nodes[node], component)
    if component == "outputs":
        before_component = before_component[field]
        after_component = after_component[field]
    return compare_components(before_component, after_component)


@pytest.mark.parametrize("join_type", ["LEFT", "CROSS"])
def test_unused_joined_projection_does_not_change_rows_or_other_consumers(join_type):
    before = _evaluate(_query(join_type=join_type))
    after = _evaluate(_query(join_type=join_type, unused="r.unused_amount + 1"))

    assert _changed(before, after, "ns.joined", "rows") == "unchanged"
    assert (
        _changed(before, after, "ns.joined", "outputs", "billed_amount") == "unchanged"
    )
    assert _changed(before, after, "ns.joined", "outputs", "unused_amount") == "changed"
    assert _changed(before, after, "ns.billed_total") == "unchanged"
    assert _changed(before, after, "ns.row_count") == "unchanged"
    assert _changed(before, after, "ns.unused_total") == "changed"
    assert _changed(before, after, "ns.joined") == "changed"


def test_unique_unqualified_join_column_change_reaches_its_aggregate():
    before = _evaluate(_query(rate="rate"))
    after = _evaluate(_query(rate="rate + 1"))

    assert _changed(before, after, "ns.joined", "rows") == "unchanged"
    assert _changed(before, after, "ns.joined", "outputs", "billed_amount") == "changed"
    assert _changed(before, after, "ns.billed_total") == "changed"
    assert _changed(before, after, "ns.row_count") == "unchanged"
    assert _changed(before, after, "ns.unused_total") == "unchanged"


def test_unsupported_sibling_projection_retains_conservative_rows_without_coupling_outputs():
    where = "o.properties['status'] = 'active'"
    before = _evaluate(_query(unused="unknown_udf(r.unused_amount)", where=where))
    after = _evaluate(
        _query(unused="unknown_udf(r.unused_amount)", rate="r.rate + 1", where=where)
    )
    assert _changed(before, after, "ns.joined", "rows") == "unchanged"
    assert _changed(before, after, "ns.row_count") == "unchanged"
    assert _changed(before, after, "ns.billed_total") == "changed"
    assert _changed(before, after, "ns.unused_total") == "unchanged"

    changed_udf = _evaluate(
        _query(unused="unknown_udf(r.unused_amount + 1)", where=where)
    )
    assert _changed(before, changed_udf, "ns.joined", "rows") == "changed"
    assert _changed(before, changed_udf, "ns.row_count") == "changed"


def test_map_filter_key_change_invalidates_rows():
    before = _evaluate(_query(where="o.properties['status'] = 'active'"))
    after = _evaluate(_query(where="o.properties['phase'] = 'active'"))
    assert _changed(before, after, "ns.joined", "rows") == "changed"
    assert _changed(before, after, "ns.row_count") == "changed"


@pytest.mark.parametrize(
    ("join_type", "join_on", "where"),
    [
        ("LEFT", "o.alternate_adjustment_id = r.id", "o.status = 'active'"),
        ("LEFT", "o.adjustment_id = r.id", "o.status = 'pending'"),
        ("CROSS", "o.adjustment_id = r.id", "o.status = 'pending'"),
    ],
    ids=["left-join-key", "left-filter", "cross-filter"],
)
def test_join_and_filter_edits_change_rows_and_aggregate_consumers(
    join_type,
    join_on,
    where,
):
    before = _evaluate(_query(join_type=join_type))
    after = _evaluate(_query(join_type=join_type, join_on=join_on, where=where))

    assert _changed(before, after, "ns.joined", "rows") == "changed"
    assert _changed(before, after, "ns.billed_total") == "changed"
    assert _changed(before, after, "ns.row_count") == "changed"


@pytest.mark.parametrize(
    "query",
    [
        "SELECT DISTINCT o.id AS id, r.rate AS rate FROM ns.orders o LEFT JOIN ns.adjustments r ON o.adjustment_id = r.id",
        "SELECT o.id AS id, SUM(r.rate) AS rate FROM ns.orders o LEFT JOIN ns.adjustments r ON o.adjustment_id = r.id GROUP BY o.id",
        "SELECT o.id AS id, r.rate AS rate FROM ns.orders o LEFT JOIN ns.adjustments r ON o.adjustment_id = r.id ORDER BY r.rate LIMIT 1",
        "SELECT o.id AS id, ROW_NUMBER() OVER (ORDER BY r.rate) AS row_number FROM ns.orders o LEFT JOIN ns.adjustments r ON o.adjustment_id = r.id",
        "SELECT o.id AS id, item FROM ns.orders o LATERAL VIEW EXPLODE(ARRAY(1, 2)) generated AS item",
        "SELECT EXPLODE(r.rates) AS rate FROM ns.orders o LEFT JOIN ns.adjustments r ON o.adjustment_id = r.id",
    ],
    ids=[
        "distinct",
        "group-by",
        "order-limit",
        "window",
        "lateral-generator",
        "select-generator",
    ],
)
def test_row_shaping_selects_remain_conservative(query):
    result = _evaluate(query)

    assert result.nodes["ns.joined"].precision in {"conservative", "unknown"}
    assert result.nodes["ns.joined"].reasons


@pytest.mark.parametrize(
    ("rate", "reason"),
    [
        ("id", "unresolved_or_ambiguous_column"),
        ("UNSUPPORTED_UDF(r.rate)", "unsupported_function"),
    ],
    ids=["ambiguous-unqualified-column", "unknown-function"],
)
def test_ambiguous_columns_and_unknown_functions_remain_conservative(rate, reason):
    result = _evaluate(_query(rate=rate))

    assert result.nodes["ns.joined"].precision in {"conservative", "unknown"}
    assert reason in result.nodes["ns.joined"].reasons
