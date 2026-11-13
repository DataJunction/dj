"""Fanout is observable in results and must be advertised for sensitive metrics."""

import pytest
from hypothesis import given, settings

from tests.conftest import transpile_to_duckdb
from tests.property.budget import examples
from tests.property.graph import (
    assert_same_results,
    build_graph,
    by_group,
    create_metric,
)
from tests.property.interaction_graph import link
from tests.property.interaction_scenarios import grouped_scenarios


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "aggregation, warns",
    [("SUM(x)", True), ("AVG(x)", True), ("COUNT(DISTINCT x)", False)],
)
@settings(max_examples=examples(6))
@given(scenario=grouped_scenarios())
async def test_fanout_warning_follows_aggregation_sensitivity(
    module__client_with_roads,
    duckdb_conn,
    aggregation,
    warns,
    scenario,
):
    client, conn = module__client_with_roads, duckdb_conn
    graph = await build_graph(client, conn, scenario)
    await link(
        client,
        graph.fact,
        graph.dim,
        f"{graph.fact}.k = {graph.dim}.bucket",
        join_type="left",
        join_cardinality="many_to_many",
    )
    metric = await create_metric(
        client,
        graph,
        "base",
        f"SELECT {aggregation} FROM {graph.fact}",
    )
    derived = await create_metric(client, graph, "derived", f"SELECT {metric} * 2")
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": [derived],
            "dimensions": [graph.dimension("label")],
            "dialect": "spark",
            "use_materialized": False,
        },
    )
    assert response.status_code == 200, response.json()
    body = response.json()
    assert any("fan-out" in w["message"] for w in body["warnings"]) == warns, body
    expression = {
        "SUM(x)": "SUM(f.x)",
        "AVG(x)": "AVG(f.x)",
        "COUNT(DISTINCT x)": "COUNT(DISTINCT f.x)",
    }[aggregation]
    ref_sql = (
        f'SELECT d.label, {expression} * 2 FROM "default".pbt.{graph.fact_table} f '
        f'LEFT JOIN "default".pbt.{graph.dim_table} d ON f.k = d.bucket GROUP BY d.label'
    )
    actual = by_group(conn.sql(transpile_to_duckdb(body["sql"])).fetchall(), 1)
    assert_same_results(
        actual,
        by_group(conn.sql(ref_sql).fetchall(), 1),
        f"{scenario!r}\n{ref_sql}\n{body['sql']}",
    )
