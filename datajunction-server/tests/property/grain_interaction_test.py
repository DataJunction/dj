"""Grain broadcasts and snapshot collapse have different aggregation orders."""

from dataclasses import replace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tests.property.budget import examples
from tests.property.graph import (
    assert_same_results,
    build_graph,
    by_group,
    create_metric,
    reference,
)
from tests.property.interaction_graph import materialize, query
from tests.property.interaction_scenarios import grouped_scenarios


@pytest.mark.asyncio
@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("routed", [False, True])
@settings(max_examples=examples(8))
@given(scenario=grouped_scenarios())
async def test_fixed_grain_share_broadcasts_at_declared_grain(
    module__client_with_roads,
    duckdb_conn,
    partitioned,
    routed,
    scenario,
):
    client, conn = module__client_with_roads, duckdb_conn
    graph = await build_graph(client, conn, scenario)
    total = await create_metric(
        client,
        graph,
        "total",
        f"SELECT SUM(x) FROM {graph.fact}",
    )
    fixed = await create_metric(
        client,
        graph,
        "fixed",
        f"SELECT SUM(x) FROM {graph.fact}",
        fixed_grain=[graph.dimension("label")] if partitioned else [],
    )
    share = await create_metric(client, graph, "share", f"SELECT {total} / {fixed}")
    dimensions = [graph.dimension(d) for d in scenario.dimensions]
    tables = (
        await materialize(client, conn, graph, [total, fixed], dimensions)
        if routed
        else []
    )
    _, numerators = reference(conn, graph, scenario, "SUM(f.x)")
    ref_sql, denominators = reference(
        conn,
        graph,
        replace(scenario, dimensions=("label",) if partitioned else ()),
        "SUM(f.x)",
    )
    expected = {}
    for group, (numerator,) in numerators.items():
        denominator = denominators[group[:1] if partitioned else ()][0]
        value = (
            None
            if numerator is None or denominator in (None, 0)
            else numerator / denominator
        )
        expected[group] = (numerator, denominator, value)
    for use_materialized in [False, True] if routed else [False]:
        sql, actual = await query(
            client,
            conn,
            [total, fixed, share],
            dimensions,
            use_materialized=use_materialized,
        )
        assert all((table in sql) == use_materialized for table in tables), sql
        assert_same_results(actual, expected, f"{scenario!r}\n{ref_sql}\n{sql}")


@pytest.mark.asyncio
@pytest.mark.parametrize("collapse", ["first_value", "last_value", "min", "max"])
@settings(max_examples=examples(10))
@given(scenario=grouped_scenarios())
async def test_snapshot_ratio_collapses_operands_before_dividing(
    module__client_with_roads,
    duckdb_conn,
    collapse,
    scenario,
):
    client, conn = module__client_with_roads, duckdb_conn
    graph = await build_graph(client, conn, scenario)
    declaration = {"rules": [{"dimension": graph.dimension("bucket"), "fn": collapse}]}
    metrics = [
        await create_metric(
            client,
            graph,
            f"snapshot_{column}",
            f"SELECT SUM({column}) FROM {graph.fact}",
            reaggregate=declaration,
        )
        for column in ("x", "y")
    ]
    ratio = await create_metric(
        client,
        graph,
        "ratio",
        f"SELECT {metrics[0]} / {metrics[1]}",
    )
    aggregate = {
        "first_value": "ARG_MIN({v}, bucket)",
        "last_value": "ARG_MAX({v}, bucket)",
        "min": "MIN({v})",
        "max": "MAX({v})",
    }[collapse]
    # Multiple raw rows can share a snapshot. Aggregate them first, then
    # collapse each operand, and only then compute the ratio.
    ref_sql = (
        "WITH snapshots AS (SELECT d.label, d.bucket, SUM(f.x) AS x, SUM(f.y) AS y "
        f'FROM "default".pbt.{graph.fact_table} f JOIN "default".pbt.{graph.dim_table} d '
        "ON f.k = d.k GROUP BY d.label, d.bucket), collapsed AS (SELECT label, "
        f"{aggregate.format(v='x')} AS x, {aggregate.format(v='y')} AS y "
        "FROM snapshots GROUP BY label) SELECT label, x, y, x / NULLIF(y, 0) FROM collapsed"
    )
    expected = by_group(conn.sql(ref_sql).fetchall(), 1)
    sql, actual = await query(
        client,
        conn,
        [*metrics, ratio],
        [graph.dimension("label")],
    )
    assert_same_results(actual, expected, f"{scenario!r}\n{ref_sql}\n{sql}")


@pytest.mark.asyncio
@pytest.mark.parametrize("aggregate", ["AVG", "COUNT DISTINCT"])
@settings(max_examples=examples(4))
@given(scenario=grouped_scenarios(), partitioned=st.booleans())
async def test_fixed_grain_rejects_nonadditive_components(
    module__client_with_roads,
    duckdb_conn,
    aggregate,
    scenario,
    partitioned,
):
    client = module__client_with_roads
    graph = await build_graph(client, duckdb_conn, scenario)
    expression = "AVG(x)" if aggregate == "AVG" else "COUNT(DISTINCT x)"
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": graph.prefix + "unsupported",
            "mode": "published",
            "query": f"SELECT {expression} FROM {graph.fact}",
            "fixed_grain": [graph.dimension("label")] if partitioned else [],
        },
    )
    assert response.status_code == 422, response.json()
    assert "fixed_grain" in str(response.json()), response.json()
