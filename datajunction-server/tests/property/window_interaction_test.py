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
from tests.property.scenario import values


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind, window_role, filter_role",
    [(kind, "[time]", "[time]") for kind in ("lag", "lead", "sum", "avg", "share")]
    + [
        (kind, window_role, filter_role)
        for kind in ("sum", "avg")
        for window_role, filter_role in (("", ""),)
    ],
)
@pytest.mark.parametrize("filtered", [False, True])
@settings(max_examples=examples(6))
@example(case=(3, ((0, 2, 0), (1, 3, 2))), extent=1, threshold=0)
@given(case=time_scenarios(), extent=st.integers(1, 3), threshold=st.integers(-3, 3))
async def test_partitioned_windows_and_scalar_descendants(
    module__client_with_roads,
    duckdb_conn,
    kind,
    window_role,
    filter_role,
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
        role="time" if window_role else None,
    )
    day, partition = f"{time_dim}.day{window_role}", graph.dimension("label")
    filter_day = f"{time_dim}.day{filter_role}"
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
        filters += [f"{filter_day} >= {low}", f"{filter_day} <= {size - 1}"]
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


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sum", "avg"])
@pytest.mark.parametrize("role", ["order", "ship", None])
@settings(max_examples=examples(6))
@example(rows=[(1, 2, 0), (2, 3, 5), (3, 7, None)], extent=1)
@given(
    rows=st.lists(
        st.tuples(
            st.integers(0, 5),
            values,
            st.one_of(st.none(), st.integers(0, 6)),
        ),
        min_size=1,
        max_size=12,
    ),
    extent=st.integers(1, 3),
)
async def test_rolling_window_expands_only_its_resolved_date_role(
    module__client_with_roads,
    duckdb_conn,
    kind,
    role,
    rows,
    extent,
):
    client, conn = module__client_with_roads, duckdb_conn
    # The unrelated date can be outside the scan bounds or NULL. Neither
    # changes which orders contribute to the window on the selected date.
    fact_rows = tuple(
        (other, value, day) if role == "ship" else (day, value, other)
        for day, value, other in rows
    )
    graph = await build_graph(client, conn, replace(fixed(), fact_rows=fact_rows))
    fact_source = graph.fact
    graph.fact = graph.prefix + "orders"
    # Exercise scan-filter injection into a transform CTE, as in a modeled
    # fact table, rather than reading the physical source directly.
    await post(
        client,
        "/nodes/transform/",
        {
            "name": graph.fact,
            "query": f"SELECT k, x, y FROM {fact_source}",
            "mode": "published",
        },
    )
    table, time_src, time_dim = (
        graph.fact_table + "_dates",
        graph.prefix + "dates_src",
        graph.prefix + "dates",
    )
    conn.execute(f'CREATE TABLE "default".pbt.{table} (day INT)')
    conn.executemany(
        f'INSERT INTO "default".pbt.{table} VALUES (?)',
        [(d,) for d in range(7)],
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
    date_links = [("k", "order"), ("y", "ship")]
    if role is None:
        date_links.append(("k", None))
    for column, link_role in date_links:
        await link(
            client,
            graph.fact,
            time_dim,
            f"{graph.fact}.{column} = {time_dim}.day",
            join_type="left",
            role=link_role,
        )
    day = f"{time_dim}.day" + (f"[{role}]" if role else "")
    filter_day = day
    total = await create_metric(
        client,
        graph,
        "total",
        f"SELECT SUM(x) FROM {graph.fact}",
    )
    window = await create_metric(
        client,
        graph,
        "window",
        f"SELECT {kind.upper()}({total}) OVER "
        f"(ORDER BY {day} ROWS BETWEEN {extent} PRECEDING AND CURRENT ROW)",
        required_dimensions=[day],
    )
    totals = [
        sum(v for d, v, _ in rows if d == date and v is not None) for date in range(7)
    ]
    expected = {}
    for date in (2, 3):
        frame = totals[max(0, date - extent) : date + 1]
        expected[(date,)] = (
            totals[date],
            sum(frame) / (len(frame) if kind == "avg" else 1),
        )
    sql, actual = await query(
        client,
        conn,
        [total, window],
        [day],
        [f"{filter_day} >= 2", f"{filter_day} <= 3"],
    )
    assert_same_results(
        actual,
        expected,
        f"{rows!r}\nrole={role}, kind={kind}, extent={extent}\n{sql}",
    )
