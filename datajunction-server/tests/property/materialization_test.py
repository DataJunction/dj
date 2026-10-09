"""
Property: serving a request from a pre-aggregation gives the same answer as
computing it from the raw tables.

Each example plans a pre-aggregation at a grain covering the request's
dimensions and filters, materializes DJ's planned SQL on DuckDB, reports it
available, then asks for SQL with and without `use_materialized`.

Both answers come from DJ, so a bug that affects both paths the same way is
invisible here; `random_graph_test` compares against a reference query instead.
"""

import duckdb
import pytest
from httpx import AsyncClient
from hypothesis import assume, event, given, settings

from tests.conftest import transpile_to_duckdb
from tests.property.budget import examples
from tests.property.graph import (
    assert_same_results,
    build_graph,
    create_metric,
    describe,
    has_orphan_facts,
    metrics_sql,
    post,
)
from tests.property.known_issues import (
    EMPTY_PREAGG_COUNT_IS_NULL,
    PREAGG_INNER_JOIN_DROPS_ORPHANS,
)
from tests.property.scenario import (
    METRIC_BY_DEFINITION,
    Scenario,
    materialization_scenarios,
)


async def check_preaggregated(
    client,
    conn,
    scenario: Scenario,
    *,
    skip_known: bool,
    discard_if_not_served: bool = False,
):
    preagg_grain = scenario.preagg_grain
    if preagg_grain is None:
        raise ValueError("a materialization scenario needs a pre-aggregation grain")
    graph = await build_graph(client, conn, scenario)
    metrics = [
        await create_metric(
            client,
            graph,
            f"m{i}",
            f"SELECT {query.definition} FROM {graph.fact}",
        )
        for i, query in enumerate(scenario.metrics)
    ]

    plan = await post(
        client,
        "/preaggs/plan/",
        {
            "metrics": metrics,
            "dimensions": [graph.dimension(d) for d in preagg_grain],
        },
    )
    tables = []
    for i, preagg in enumerate(plan["preaggs"]):
        table = f"{graph.fact_table}_preagg_{i}"
        conn.execute(
            f'CREATE TABLE "default".pbt.{table} AS '
            f"{transpile_to_duckdb(preagg['sql'])}",
        )
        await post(
            client,
            f"/preaggs/{preagg['id']}/availability/",
            {
                "catalog": "default",
                "schema_": "pbt",
                "table": table,
                "valid_through_ts": 20990101,
            },
        )
        tables.append(table)

    preagg_sql, preaggregated = await metrics_sql(
        client,
        conn,
        graph,
        scenario,
        metrics,
    )
    raw_sql, raw = await metrics_sql(
        client,
        conn,
        graph,
        scenario,
        metrics,
        use_materialized=False,
    )
    served_from_preagg = any(table in preagg_sql for table in tables)
    if discard_if_not_served:
        # Some otherwise compatible requests still cannot use the planned
        # pre-aggregation; retain only examples that exercise substitution.
        event(f"served from pre-aggregation: {served_from_preagg}")
        assume(served_from_preagg)
    else:
        assert served_from_preagg, "the planned pre-aggregation was not used"

    if skip_known and () in preaggregated and () in raw:
        assume(
            not any(p is None and r == 0 for p, r in zip(preaggregated[()], raw[()])),
        )
    assert_same_results(
        preaggregated,
        raw,
        describe(
            scenario,
            f"pre-aggregation grain: {preagg_grain}  used: {served_from_preagg}",
            "planned SQL:\n" + "\n---\n".join(p["sql"] for p in plan["preaggs"]),
            f"SQL using the pre-aggregation:\n{preagg_sql}",
            f"SQL from raw tables:\n{raw_sql}",
        ),
    )


@pytest.mark.asyncio
@settings(max_examples=examples(15))
@given(scenario=materialization_scenarios())
async def test_preaggregated_matches_raw(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
    scenario: Scenario,
):
    assume(
        not (
            scenario.join_type == "inner"
            and scenario.preagg_grain
            and not scenario.dimensions
            and not scenario.filters
            and has_orphan_facts(scenario)
        ),
    )  # known_issues.PREAGG_INNER_JOIN_DROPS_ORPHANS
    await check_preaggregated(
        module__client_with_roads,
        duckdb_conn,
        scenario,
        skip_known=True,
        discard_if_not_served=True,
    )


@pytest.mark.asyncio
async def test_eligible_preaggregation_is_used_and_matches_raw(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    """A compatible rollup must route through the available pre-aggregation."""
    await check_preaggregated(
        module__client_with_roads,
        duckdb_conn,
        Scenario(
            dimension_rows=((0, "a", 1), (1, "b", 2)),
            fact_rows=((0, 1, 2), (0, 2, 3), (1, 3, 4)),
            metrics=(
                METRIC_BY_DEFINITION["SUM(x)"],
                METRIC_BY_DEFINITION["COUNT(x)"],
            ),
            op="+",
            join_type="left",
            dimensions=(),
            filters=(),
            preagg_grain=("label",),
        ),
        skip_known=False,
    )


@pytest.mark.asyncio
@EMPTY_PREAGG_COUNT_IS_NULL.xfail()
async def test_known_issue_count_from_empty_preaggregation(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    await check_preaggregated(
        module__client_with_roads,
        duckdb_conn,
        Scenario(
            dimension_rows=((0, None, None),),
            fact_rows=(),
            metrics=(
                METRIC_BY_DEFINITION["SUM(x)"],
                METRIC_BY_DEFINITION["COUNT(x)"],
            ),
            op="+",
            join_type="left",
            dimensions=(),
            filters=(),
            preagg_grain=("label",),
        ),
        skip_known=False,
    )


@pytest.mark.asyncio
@PREAGG_INNER_JOIN_DROPS_ORPHANS.xfail()
async def test_known_issue_inner_linked_preaggregation_drops_orphan_facts(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    await check_preaggregated(
        module__client_with_roads,
        duckdb_conn,
        Scenario(
            dimension_rows=((0, "a", 1),),
            fact_rows=((0, 1, 0), (None, 5, 0)),
            metrics=(METRIC_BY_DEFINITION["SUM(x)"],) * 2,
            op="+",
            join_type="inner",
            dimensions=(),
            filters=(),
            preagg_grain=("label",),
        ),
        skip_known=False,
    )
