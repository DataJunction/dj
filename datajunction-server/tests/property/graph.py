"""
Random fact + dimension graphs, created in DJ through its API and mirrored as
tables in DuckDB, for properties that compare DJ's SQL against other answers.
"""

import itertools

from httpx import AsyncClient

from tests.conftest import transpile_to_duckdb
from tests.property.comparison import PropertyMismatch, same_value
from tests.property.scenario import (
    DimensionColumn,
    FilterSpec,
    Scenario,
    render_filter,
)

counter = itertools.count()


def accepts_missing_dimension(conn, filters: tuple[FilterSpec, ...]) -> bool:
    """Whether all filters accept a fact with no matching dimension row."""
    if not filters:
        return True

    def missing_column(column: DimensionColumn) -> str:
        return "CAST(NULL AS VARCHAR)" if column == "label" else "CAST(NULL AS INTEGER)"

    where = " AND ".join(
        f"({render_filter(filter_, missing_column)})" for filter_ in filters
    )
    return bool(conn.sql(f"SELECT COALESCE(({where}), FALSE)").fetchone()[0])


def has_orphan_facts(scenario: Scenario) -> bool:
    keys = {key for key, _, _ in scenario.dimension_rows}
    return any(key not in keys for key, _, _ in scenario.fact_rows)


def by_group(rows, width: int) -> dict:
    """Rows keyed by their first `width` columns (the requested dimensions)."""
    grouped = {}
    for row in rows:
        key = row[:width]
        assert key not in grouped, f"duplicate result group: {key}"
        grouped[key] = row[width:]
    return grouped


def assert_same_results(actual: dict, expected: dict, context: str) -> None:
    wrong = {
        key: (actual.get(key, "<missing>"), expected.get(key, "<missing>"))
        for key in actual.keys() | expected.keys()
        if key not in actual
        or key not in expected
        or len(actual[key]) != len(expected[key])
        or not all(same_value(a, e) for a, e in zip(actual[key], expected[key]))
    }
    if wrong:
        raise PropertyMismatch(f"\n{context}\n(actual, expected) by group: {wrong}")


class Graph:
    def __init__(self, n: int):
        self.fact_table, self.dim_table = f"fact_{n}", f"dim_{n}"
        self.fact = f"default.pbt{n}_fact"
        self.dim_src = f"default.pbt{n}_dim_src"
        self.dim = f"default.pbt{n}_dim"
        self.prefix = f"default.pbt{n}_"

    def dimension(self, column: str) -> str:
        return f"{self.dim}.{column}"


async def post(client: AsyncClient, path: str, payload: dict):
    response = await client.post(path, json=payload)
    assert response.status_code in (200, 201), (path, response.json())
    return response.json()


async def build_graph(client, conn, scenario: Scenario) -> Graph:
    graph = Graph(next(counter))
    conn.execute('CREATE SCHEMA IF NOT EXISTS "default".pbt')
    conn.execute(
        f'CREATE TABLE "default".pbt.{graph.dim_table} '
        "(k INTEGER, label VARCHAR, bucket INTEGER)",
    )
    conn.execute(
        f'CREATE TABLE "default".pbt.{graph.fact_table} '
        "(k INTEGER, x INTEGER, y INTEGER)",
    )
    conn.executemany(
        f'INSERT INTO "default".pbt.{graph.dim_table} VALUES (?, ?, ?)',
        scenario.dimension_rows,
    )
    if scenario.fact_rows:
        conn.executemany(
            f'INSERT INTO "default".pbt.{graph.fact_table} VALUES (?, ?, ?)',
            scenario.fact_rows,
        )

    def source(name, table, columns):
        return {
            "name": name,
            "catalog": "default",
            "schema_": "pbt",
            "table": table,
            "mode": "published",
            "columns": [{"name": c, "type": t} for c, t in columns],
        }

    await post(
        client,
        "/nodes/source/",
        source(
            graph.fact,
            graph.fact_table,
            [("k", "int"), ("x", "int"), ("y", "int")],
        ),
    )
    await post(
        client,
        "/nodes/source/",
        source(
            graph.dim_src,
            graph.dim_table,
            [("k", "int"), ("label", "string"), ("bucket", "int")],
        ),
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": graph.dim,
            "query": f"SELECT k, label, bucket FROM {graph.dim_src}",
            "primary_key": ["k"],
            "mode": "published",
        },
    )
    await post(
        client,
        f"/nodes/{graph.fact}/link",
        {
            "dimension_node": graph.dim,
            "join_type": scenario.join_type,
            "join_on": f"{graph.fact}.k = {graph.dim}.k",
        },
    )
    return graph


async def create_metric(client, graph: Graph, name: str, query: str) -> str:
    metric = graph.prefix + name
    await post(
        client,
        "/nodes/metric/",
        {"name": metric, "query": query, "mode": "published"},
    )
    return metric


async def metrics_sql(
    client,
    conn,
    graph: Graph,
    scenario: Scenario,
    metrics: list[str],
    *,
    use_materialized: bool = True,
    parenthesize_filters: bool = True,
) -> tuple[str, dict]:
    """DJ's SQL for the scenario's request, and its result keyed by dimensions."""
    filters = [
        render_filter(filter_, lambda column: graph.dimension(column))
        for filter_ in scenario.filters
    ]
    if parenthesize_filters:
        # Combining an unparenthesized OR with another filter is
        # known_issues.PRINTER_DROPS_PARENTHESES.
        filters = [f"({f})" for f in filters]
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": metrics,
            "dimensions": [graph.dimension(d) for d in scenario.dimensions],
            "filters": filters,
            "use_materialized": use_materialized,
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    rows = conn.sql(transpile_to_duckdb(sql)).fetchall()
    return sql, by_group(rows, len(scenario.dimensions))


def reference(
    conn,
    graph: Graph,
    scenario: Scenario,
    value_expression: str,
) -> tuple[str, dict]:
    """A plain join-filter-group query over the raw tables."""
    group_cols = [f"d.{column}" for column in scenario.dimensions]
    join = (
        f' {scenario.join_type.upper()} JOIN "default".pbt.{graph.dim_table} d '
        "ON f.k = d.k"
        if scenario.dimensions or scenario.filters
        else ""
    )
    where = (
        " WHERE "
        + " AND ".join(
            f"({render_filter(filter_, lambda column: f'd.{column}')})"
            for filter_ in scenario.filters
        )
        if scenario.filters
        else ""
    )
    group_by = f" GROUP BY {', '.join(group_cols)}" if group_cols else ""
    sql = (
        f"SELECT {', '.join([*group_cols, value_expression])} "
        f'FROM "default".pbt.{graph.fact_table} f{join}{where}{group_by}'
    )
    return sql, by_group(conn.sql(sql).fetchall(), len(group_cols))


def describe(scenario: Scenario, *sql: str) -> str:
    return "\n".join(
        [
            f"metrics: {[m.definition for m in scenario.metrics]}  join: {scenario.join_type}",
            f"dimensions: {scenario.dimensions}  "
            f"filters: {[render_filter(f, str) for f in scenario.filters]}",
            *sql,
        ],
    )
