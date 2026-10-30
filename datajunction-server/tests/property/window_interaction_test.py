"""Window answers come from short Python series, never DJ's window SQL."""

from dataclasses import replace

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from tests.property.budget import examples
from tests.property.graph import assert_same_results, build_graph, create_metric, post
from tests.property.interaction_graph import link, query, source
from tests.property.interaction_scenarios import time_scenarios
from tests.property.random_graph_test import fixed


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["lag", "lead", "sum", "avg", "share"])
@pytest.mark.parametrize("filtered", [False, True])
@settings(max_examples=examples(6))
@example(case=(3, ((0, 2, 0), (1, 3, 2))), extent=1, threshold=0)
@given(case=time_scenarios(), extent=st.integers(1, 3), threshold=st.integers(-3, 3))
async def test_partitioned_windows_and_scalar_descendants(
    module__client_with_roads,
    duckdb_conn,
    kind,
    filtered,
    case,
    extent,
    threshold,
):
    client, conn = module__client_with_roads, duckdb_conn
    size, rows = case
    scenario = replace(fixed(), fact_rows=rows)
    graph = await build_graph(client, conn, scenario)
    table, time_src, time_dim = (
        graph.fact_table + "_dates",
        graph.prefix + "dates_src",
        graph.prefix + "dates",
    )
    conn.execute(f'CREATE TABLE "default".pbt.{table} (day INT)')
    conn.executemany(
        f'INSERT INTO "default".pbt.{table} VALUES (?)',
        [(day,) for day in range(size)],
    )
    await source(client, time_src, table, [("day", "int")])
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": time_dim,
            "query": f"SELECT day FROM {time_src}",
            "primary_key": ["day"],
            "mode": "published",
        },
    )
    await link(
        client,
        graph.fact,
        time_dim,
        f"{graph.fact}.y = {time_dim}.day",
        join_type="left",
        role="time",
    )
    day, partition = f"{time_dim}.day[time]", graph.dimension("label")
    total = await create_metric(
        client,
        graph,
        "total",
        f"SELECT SUM(x) FROM {graph.fact}",
    )
    over = f"PARTITION BY {partition} ORDER BY {day}"
    if kind in ("lag", "lead"):
        expression = f"{kind.upper()}({total}, {extent}) OVER ({over})"
    elif kind == "share":
        expression = f"{total} / SUM({total}) OVER (PARTITION BY {partition})"
    else:
        expression = f"{kind.upper()}({total}) OVER ({over} ROWS BETWEEN {extent} PRECEDING AND CURRENT ROW)"
    window = await create_metric(
        client,
        graph,
        "window",
        f"SELECT {expression}",
        required_dimensions=[day, partition],
    )
    ratio = await create_metric(client, graph, "ratio", f"SELECT {window} / {total}")
    nested = await create_metric(client, graph, "nested", f"SELECT ({ratio} + 1) * 2")
    filters = [f"{nested} > {threshold}"] if filtered else []
    # Rolling requests ask for a narrow output range. DJ must read earlier
    # dates and densify gaps before evaluating the window.
    rolling = kind in ("sum", "avg")
    low = size - 2 if rolling else 0
    if rolling:
        filters += [f"{day} >= {low}", f"{day} <= {size - 1}"]
    expected = {}
    for group in (0, 1):
        observed = sorted({d for k, _, d in rows if k == group})
        dates = list(range(max(0, low - extent), size)) if rolling else observed
        totals = []
        for d in dates:
            nonnull = [
                x for k, x, date in rows if k == group and date == d and x is not None
            ]
            totals.append(sum(nonnull) if nonnull else (0 if rolling else None))
        for index, (d, value) in enumerate(zip(dates, totals)):
            if kind in ("lag", "lead"):
                offset = index + (extent if kind == "lead" else -extent)
                result = totals[offset] if 0 <= offset < len(totals) else None
            elif kind == "share":
                denominator = sum(v for v in totals if v is not None)
                result = (
                    None if value is None or denominator == 0 else value / denominator
                )
            else:
                frame = totals[max(0, index - extent) : index + 1]
                result = sum(frame) / (len(frame) if kind == "avg" else 1)
            scalar = None if result is None or value in (None, 0) else result / value
            final = None if scalar is None else (scalar + 1) * 2
            if d >= low and (not filtered or (final is not None and final > threshold)):
                expected[(chr(97 + group), d)] = (value, result, scalar, final)
    sql, actual = await query(
        client,
        conn,
        [total, window, ratio, nested],
        [partition, day],
        filters,
    )
    assert_same_results(
        actual,
        expected,
        f"{case!r}\nkind={kind}, extent={extent}, filtered={filtered}\n{sql}",
    )
