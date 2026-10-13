"""
Property: for a randomly generated fact + dimension graph, the SQL DJ builds for
a metric request returns the same rows as a plain hand-written query over the
raw tables.
"""

from dataclasses import replace

import duckdb
import pytest
from httpx import AsyncClient
from hypothesis import assume, given, settings

from tests.property.budget import examples
from tests.property.graph import (
    accepts_missing_dimension,
    assert_same_results,
    build_graph,
    create_metric,
    describe,
    metrics_sql,
    reference,
)
from tests.property.known_issues import (
    FILTER_PUSHED_PAST_LEFT_JOIN,
    PRINTER_DROPS_PARENTHESES,
)
from tests.property.scenario import (
    METRIC_BY_DEFINITION,
    SIMPLE_METRICS,
    FilterAtom,
    OrFilter,
    Scenario,
    scenarios,
)


def skip_known_filter_pushdown(conn, scenario: Scenario):
    """known_issues.FILTER_PUSHED_PAST_LEFT_JOIN"""
    assume(
        not (
            scenario.join_type == "left"
            and scenario.filters
            and accepts_missing_dimension(conn, scenario.filters)
        ),
    )


async def check_base_metric(client, conn, scenario, *, parenthesize_filters=True):
    graph = await build_graph(client, conn, scenario)
    query = scenario.metrics[0]
    metric = await create_metric(
        client,
        graph,
        "metric",
        f"SELECT {query.definition} FROM {graph.fact}",
    )

    dj_sql, dj = await metrics_sql(
        client,
        conn,
        graph,
        scenario,
        [metric],
        parenthesize_filters=parenthesize_filters,
    )
    reference_sql, expected = reference(conn, graph, scenario, query.reference)
    assert_same_results(dj, expected, describe(scenario, reference_sql, dj_sql))


async def check_derived_metric(client, conn, scenario):
    graph = await build_graph(client, conn, scenario)
    first, second = scenario.metrics
    op = scenario.op
    m1 = await create_metric(
        client,
        graph,
        "m1",
        f"SELECT {first.definition} FROM {graph.fact}",
    )
    m2 = await create_metric(
        client,
        graph,
        "m2",
        f"SELECT {second.definition} FROM {graph.fact}",
    )
    derived = await create_metric(client, graph, "derived", f"SELECT {m1} {op} {m2}")

    dj_sql, dj = await metrics_sql(client, conn, graph, scenario, [derived])
    # DJ defines metric division as NULL on a zero denominator. Keep that
    # contract in the separately written reference query, and compare NULL
    # distinctly from infinity or NaN in assert_same_results.
    right = f"NULLIF({second.reference}, 0)" if op == "/" else second.reference
    reference_sql, expected = reference(
        conn,
        graph,
        scenario,
        f"({first.reference}) {op} ({right})",
    )
    assert_same_results(
        dj,
        expected,
        describe(scenario, f"derived: m1 {op} m2", reference_sql, dj_sql),
    )
    return dj


@pytest.mark.asyncio
@settings(max_examples=examples(20))
@given(scenario=scenarios())
async def test_base_metric_matches_reference_query(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
    scenario,
):
    skip_known_filter_pushdown(duckdb_conn, scenario)
    await check_base_metric(module__client_with_roads, duckdb_conn, scenario)


@pytest.mark.asyncio
@settings(max_examples=examples(20))
@given(scenario=scenarios(metric_pool=SIMPLE_METRICS))
async def test_derived_metric_matches_reference_query(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
    scenario,
):
    skip_known_filter_pushdown(duckdb_conn, scenario)
    await check_derived_metric(module__client_with_roads, duckdb_conn, scenario)


def fixed(**overrides) -> Scenario:
    scenario = Scenario(
        dimension_rows=((0, "a", 0), (1, "b", 1)),
        fact_rows=((0, 1, 0), (1, 2, 0)),
        metrics=(METRIC_BY_DEFINITION["SUM(x)"],) * 2,
        op="+",
        join_type="left",
        dimensions=("label",),
        filters=(),
    )
    return replace(scenario, **overrides)


@pytest.mark.asyncio
async def test_derived_metric_division_by_zero_returns_null(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    result = await check_derived_metric(
        module__client_with_roads,
        duckdb_conn,
        fixed(
            fact_rows=((0, 1, 0),),
            metrics=(
                METRIC_BY_DEFINITION["COUNT(*)"],
                METRIC_BY_DEFINITION["STDDEV_POP(x)"],
            ),
            op="/",
            dimensions=(),
        ),
    )
    assert result[()] == (None,)


@pytest.mark.asyncio
@PRINTER_DROPS_PARENTHESES.xfail()
async def test_known_issue_or_filter_combined_with_another_filter(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    await check_base_metric(
        module__client_with_roads,
        duckdb_conn,
        fixed(
            filters=(
                OrFilter(FilterAtom("label", "=", "a"), FilterAtom("label", "=", "b")),
                FilterAtom("label", "=", "b"),
            ),
        ),
        parenthesize_filters=False,
    )


@pytest.mark.asyncio
@PRINTER_DROPS_PARENTHESES.xfail()
async def test_known_issue_derived_metric_over_compound_metric(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    await check_derived_metric(
        module__client_with_roads,
        duckdb_conn,
        fixed(
            fact_rows=((None, -4, 0),),
            metrics=(
                METRIC_BY_DEFINITION["MAX(x) - MIN(y)"],
                METRIC_BY_DEFINITION["SUM(x)"],
            ),
            op="/",
            dimensions=(),
        ),
    )


@pytest.mark.asyncio
@FILTER_PUSHED_PAST_LEFT_JOIN.xfail()
async def test_known_issue_null_accepting_filter_on_left_joined_dimension(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    await check_base_metric(
        module__client_with_roads,
        duckdb_conn,
        fixed(
            dimension_rows=((0, "b", None), (1, None, None)),
            fact_rows=((0, 3, None), (1, 5, None)),
            dimensions=("label",),
            filters=(FilterAtom("label", "IS NULL"),),
        ),
    )
