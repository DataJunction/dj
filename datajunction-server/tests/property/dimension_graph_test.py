"""Join paths, roles, composite keys, and defaults compared with raw joins."""

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
    post,
    reference,
)
from tests.property.interaction_graph import link, query
from tests.property.interaction_scenarios import grouped_scenarios
from tests.property.known_issues import SELF_JOIN_ROLE_FALLBACK
from tests.property.random_graph_test import fixed
from tests.property.scenario import values


@pytest.mark.asyncio
@pytest.mark.parametrize("defaults", [False, True])
@settings(max_examples=examples(8))
@given(
    rows=st.lists(
        st.tuples(
            st.one_of(st.none(), st.integers(0, 3)),
            values,
            st.one_of(st.none(), st.integers(0, 3)),
        ),
        min_size=1,
        max_size=12,
    ),
)
async def test_two_roles_of_one_dimension_do_not_share_joins(
    module__client_with_roads,
    duckdb_conn,
    defaults,
    rows,
):
    client, conn = module__client_with_roads, duckdb_conn
    scenario = replace(fixed(), fact_rows=tuple(rows))
    graph = await build_graph(client, conn, scenario)
    for column, role in (("k", "home"), ("y", "work")):
        await link(
            client,
            graph.fact,
            graph.dim,
            f"{graph.fact}.{column} = {graph.dim}.k",
            role=role,
            join_type="left",
            default_value="missing" if defaults else None,
        )
    metric = await create_metric(
        client,
        graph,
        "sum",
        f"SELECT SUM(x) FROM {graph.fact}",
    )
    projections = [
        f"COALESCE({a}.label, 'missing')" if defaults else f"{a}.label"
        for a in ("h", "w")
    ]
    ref_sql = (
        f"SELECT {', '.join(projections)}, SUM(f.x) "
        f'FROM "default".pbt.{graph.fact_table} f '
        f'LEFT JOIN "default".pbt.{graph.dim_table} h ON f.k = h.k '
        f'LEFT JOIN "default".pbt.{graph.dim_table} w ON f.y = w.k GROUP BY 1, 2'
    )
    sql, actual = await query(
        client,
        conn,
        [metric],
        [graph.dimension("label") + f"[{r}]" for r in ("home", "work")],
    )
    assert_same_results(
        actual,
        by_group(conn.sql(ref_sql).fetchall(), 2),
        f"{rows!r}\n{ref_sql}\n{sql}",
    )


async def check_dimension_path(client, conn, path, scenario):
    graph = await build_graph(client, conn, scenario)
    target = graph.prefix + "target" if path != "self" else graph.dim
    if path != "self":
        await post(
            client,
            "/nodes/dimension/",
            {
                "name": target,
                "mode": "published",
                "query": (
                    f"SELECT CAST(FLOOR(k / 2.0) AS INT) AS code, k % 2 AS locale, label FROM {graph.dim_src}"
                    if path == "composite"
                    else f"SELECT k, label FROM {graph.dim_src}"
                ),
                "primary_key": ["code", "locale"] if path == "composite" else ["k"],
            },
        )
    if path == "composite":
        await link(
            client,
            graph.fact,
            target,
            f"{graph.fact}.k = {target}.code AND {graph.fact}.y = {target}.locale",
            join_type="left",
        )
        join_sql = f'LEFT JOIN "default".pbt.{graph.dim_table} t ON f.k = FLOOR(t.k / 2.0) AND f.y = t.k % 2'
        dimension = f"{target}.label"
    else:
        await link(
            client,
            graph.dim,
            target,
            f"{graph.dim}.bucket = {target}.k",
            role="parent",
            join_type="left",
        )
        join_sql = (
            f'LEFT JOIN "default".pbt.{graph.dim_table} d ON f.k = d.k '
            f'LEFT JOIN "default".pbt.{graph.dim_table} t ON d.bucket = t.k'
        )
        dimension = f"{target}.label[parent]"
    metric = await create_metric(
        client,
        graph,
        "sum",
        f"SELECT SUM(x) FROM {graph.fact}",
    )
    ref_sql = f'SELECT t.label, SUM(f.x) FROM "default".pbt.{graph.fact_table} f {join_sql} GROUP BY t.label'
    sql, actual = await query(client, conn, [metric], [dimension])
    assert_same_results(
        actual,
        by_group(conn.sql(ref_sql).fetchall(), 1),
        f"{scenario!r}\n{ref_sql}\n{sql}",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["composite", "multi_hop"])
@settings(max_examples=examples(8))
@given(scenario=grouped_scenarios())
async def test_dimension_paths_match_explicit_joins(
    module__client_with_roads,
    duckdb_conn,
    path,
    scenario,
):
    await check_dimension_path(module__client_with_roads, duckdb_conn, path, scenario)


@pytest.mark.asyncio
@SELF_JOIN_ROLE_FALLBACK.xfail()
async def test_known_issue_self_join_uses_parent_role(
    module__client_with_roads,
    duckdb_conn,
):
    await check_dimension_path(
        module__client_with_roads,
        duckdb_conn,
        "self",
        replace(
            fixed(),
            dimension_rows=((0, "a", 0), (1, "b", 0)),
            fact_rows=((1, 1, 0),),
        ),
    )


@pytest.mark.asyncio
@settings(max_examples=examples(8))
@given(scenario=grouped_scenarios())
async def test_required_dimension_is_preserved_in_output_grain(
    module__client_with_roads,
    duckdb_conn,
    scenario,
):
    client, conn = module__client_with_roads, duckdb_conn
    graph = await build_graph(client, conn, scenario)
    metric = await create_metric(
        client,
        graph,
        "required",
        f"SELECT SUM(x) FROM {graph.fact}",
        required_dimensions=[graph.dimension("bucket")],
    )
    # Required dimensions are appended even when the caller asks for just label.
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": [metric],
            "dimensions": [graph.dimension("label")],
            "dialect": "spark",
            "use_materialized": False,
        },
    )
    assert response.status_code == 200, response.json()
    assert [c["semantic_entity"] for c in response.json()["columns"][:2]] == [
        graph.dimension(d) for d in scenario.dimensions
    ]
    from tests.conftest import transpile_to_duckdb

    sql = response.json()["sql"]
    ref_sql, expected = reference(conn, graph, scenario, "SUM(f.x)")
    assert_same_results(
        by_group(conn.sql(transpile_to_duckdb(sql)).fetchall(), 2),
        expected,
        f"{ref_sql}\n{sql}",
    )
