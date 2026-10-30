"""Ranking selects tuples; Others must regroup components before ratios/distinct."""

from dataclasses import replace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datajunction_server.api.semantic_layer import _limit_to_ranked_groups
from datajunction_server.construction.build_v3.builder import build_metrics_sql
from datajunction_server.construction.build_v3.group_others import GroupOthers
from datajunction_server.models.dialect import Dialect
from tests.conftest import transpile_to_duckdb
from tests.property.budget import examples
from tests.property.graph import (
    assert_same_results,
    build_graph,
    by_group,
    create_metric,
)
from tests.property.random_graph_test import fixed
from tests.property.scenario import values


@pytest.mark.asyncio
@pytest.mark.parametrize("others", [False, True])
@settings(max_examples=examples(10))
@given(
    extra=st.lists(st.tuples(st.integers(0, 5), values, values), max_size=10),
    amounts=st.lists(values, min_size=6, max_size=6),
    top=st.integers(1, 2),
    descending=st.booleans(),
)
async def test_ranked_groups_match_independent_ranking_and_regrouping(
    module__client_with_roads,
    module__session,
    duckdb_conn,
    others,
    extra,
    amounts,
    top,
    descending,
):
    client, conn = module__client_with_roads, duckdb_conn
    # Include NULL ranking keys and guarantee more groups than the requested N.
    dims = tuple((i, ("a", "b", None)[i // 2], i % 2) for i in range(6))
    rows = tuple((i, amounts[i], i % 3) for i in range(6)) + tuple(extra)
    scenario = replace(fixed(), dimension_rows=dims, fact_rows=rows)
    graph = await build_graph(client, conn, scenario)
    total = await create_metric(
        client,
        graph,
        "total",
        f"SELECT SUM(x) FROM {graph.fact}",
    )
    count = await create_metric(
        client,
        graph,
        "distinct",
        f"SELECT COUNT(DISTINCT x) FROM {graph.fact}",
    )
    denominator = await create_metric(
        client,
        graph,
        "denominator",
        f"SELECT SUM(y) FROM {graph.fact}",
    )
    ratio = await create_metric(
        client,
        graph,
        "ratio",
        f"SELECT {total} / {denominator}",
    )
    label, day = graph.dimension("label"), graph.dimension("bucket")
    # SQL ranking comes from DJ, but the winning tuples below are computed in
    # Python. A shared ranking bug therefore cannot make both sides pass.
    ranked = await build_metrics_sql(
        module__session,
        [total],
        [label],
        orderby=[f"{total} {'DESC' if descending else 'ASC'}", f"{label} ASC"],
        limit=top,
        dialect=Dialect.SPARK,
        use_materialized=False,
    )
    grouped = {}
    for k, x, _ in rows:
        grouped.setdefault(dims[k][1], []).extend([] if x is None else [x])
    totals = {key: sum(v) if v else None for key, v in grouped.items()}

    # Spark ASC puts NULL first, DESC last. Secondary label ASC breaks ties.
    def ranking_key(key):
        value = totals[key]
        return (
            (value is None) if descending else (value is not None),
            0 if value is None else (-value if descending else value),
            key is not None,
            key or "",
        )

    winners = set(sorted(totals, key=ranking_key)[:top])
    expected_groups = {}
    for k, x, y in rows:
        original, day_value = dims[k][1:]
        if not others and original not in winners:
            continue
        key = (original if original in winners else "Others", day_value)
        xs, ys = expected_groups.setdefault(key, ([], []))
        if x is not None:
            xs.append(x)
        if y is not None:
            ys.append(y)
    expected = {}
    for key, (xs, ys) in expected_groups.items():
        a, b = sum(xs) if xs else None, sum(ys) if ys else None
        expected[key] = (
            a,
            len(set(xs)),
            None if a is None or b in (None, 0) else a / b,
        )
    main = await build_metrics_sql(
        module__session,
        [total, count, ratio],
        [label, day],
        dialect=Dialect.SPARK,
        use_materialized=False,
        group_others=GroupOthers((label,), ranked) if others else None,
    )
    result = (
        main if others else _limit_to_ranked_groups(main, ranked, [label], [], 1000)
    )
    actual = by_group(conn.sql(transpile_to_duckdb(result.sql)).fetchall(), 2)
    assert_same_results(
        actual,
        expected,
        f"rows={rows!r}\ntop={top}, descending={descending}\n{ranked.sql}\n{result.sql}",
    )
