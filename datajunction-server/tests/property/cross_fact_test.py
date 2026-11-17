"""Aggregate facts independently: a raw join would hide or introduce fanout."""

from dataclasses import replace
from copy import copy

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tests.property.budget import examples
from tests.property.graph import (
    assert_same_results,
    build_graph,
    create_metric,
    reference,
)
from tests.property.interaction_graph import link, materialize, query, source
from tests.property.interaction_scenarios import grouped_scenarios
from tests.property.scenario import METRIC_BY_DEFINITION, values


@pytest.mark.asyncio
@pytest.mark.parametrize("aggregation", ["SUM(x)", "AVG(x)", "COUNT(DISTINCT x)"])
@pytest.mark.parametrize("routing", ["live", "partial", "incompatible"])
@settings(max_examples=examples(6))
@given(scenario=grouped_scenarios(), data=st.data())
async def test_cross_fact_ratio_joins_aggregates_without_fanout(
    module__client_with_roads,
    duckdb_conn,
    aggregation,
    routing,
    scenario,
    data,
):
    client, conn = module__client_with_roads, duckdb_conn
    graph = await build_graph(client, conn, scenario)
    other_table, other_fact = graph.fact_table + "_other", graph.prefix + "other"
    other_rows = tuple(
        data.draw(
            st.lists(
                st.tuples(
                    st.integers(0, len(scenario.dimension_rows) - 1),
                    values,
                    values,
                ),
                max_size=10,
            ),
            label="second fact",
        ),
    )
    conn.execute(f'CREATE TABLE "default".pbt.{other_table} (k INT, x INT, y INT)')
    if other_rows:
        conn.executemany(
            f'INSERT INTO "default".pbt.{other_table} VALUES (?, ?, ?)',
            other_rows,
        )
    await source(
        client,
        other_fact,
        other_table,
        [("k", "int"), ("x", "int"), ("y", "int")],
    )
    await link(
        client,
        other_fact,
        graph.dim,
        f"{other_fact}.k = {graph.dim}.k",
        join_type="left",
    )
    first = await create_metric(
        client,
        graph,
        "first",
        f"SELECT {aggregation} FROM {graph.fact}",
    )
    second = await create_metric(
        client,
        graph,
        "second",
        f"SELECT SUM(x) FROM {other_fact}",
    )
    ratio = await create_metric(client, graph, "ratio", f"SELECT {first} / {second}")
    tables = []
    if routing != "live":
        # Only the second fact is materialized. A coarser grain cannot answer
        # the requested label+bucket grain and must fall back to raw rows.
        grain = [graph.dimension("label")]
        if routing == "partial":
            grain.append(graph.dimension("bucket"))
        tables = await materialize(client, conn, graph, [second], grain)
    ref_sql, left = reference(
        conn,
        graph,
        scenario,
        METRIC_BY_DEFINITION[aggregation].reference,
    )
    # A distinct physical table, aggregated separately at the same requested
    # grain. Never join raw fact rows in this oracle.
    other_graph = copy(graph)
    other_graph.fact_table, other_graph.dim_table = other_table, graph.dim_table
    other_sql, right = reference(
        conn,
        other_graph,
        replace(scenario, fact_rows=other_rows),
        "SUM(f.x)",
    )
    expected = {}
    for group in left.keys() | right.keys():
        a, b = left.get(group, (None,))[0], right.get(group, (None,))[0]
        expected[group] = (a, b, None if a is None or b in (None, 0) else a / b)
    dimensions = [graph.dimension(d) for d in scenario.dimensions]
    for routed in [False] if routing == "live" else [False, True]:
        sql, actual = await query(
            client,
            conn,
            [first, second, ratio],
            dimensions,
            use_materialized=routed,
        )
        assert all(
            (table in sql) == (routed and routing == "partial") for table in tables
        ), sql
        assert_same_results(
            actual,
            expected,
            f"{scenario!r}\nother={other_rows!r}\n{ref_sql}\n{other_sql}\n{sql}",
        )
