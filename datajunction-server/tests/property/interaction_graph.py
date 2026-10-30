"""API and execution helpers shared by the feature interaction properties."""

from tests.conftest import transpile_to_duckdb
from tests.property.graph import by_group, post


async def source(client, name, table, columns):
    await post(
        client,
        "/nodes/source/",
        {
            "name": name,
            "catalog": "default",
            "schema_": "pbt",
            "table": table,
            "mode": "published",
            "columns": [{"name": c, "type": t} for c, t in columns],
        },
    )


async def link(client, fact, dimension, condition, **options):
    await post(
        client,
        f"/nodes/{fact}/link",
        {"dimension_node": dimension, "join_on": condition, **options},
    )


async def query(client, conn, metrics, dimensions=(), filters=(), **options):
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": list(metrics),
            "dimensions": list(dimensions),
            "filters": list(filters),
            "dialect": "spark",
            "use_materialized": False,
            **options,
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    return sql, by_group(conn.sql(transpile_to_duckdb(sql)).fetchall(), len(dimensions))


async def materialize(client, conn, graph, metrics, dimensions):
    """Install real planned tables and return their names for routing assertions."""
    plan = await post(
        client,
        "/preaggs/plan/",
        {"metrics": metrics, "dimensions": list(dimensions)},
    )
    tables = []
    for i, preagg in enumerate(plan["preaggs"]):
        table = f"{graph.fact_table}_routed_{i}"
        conn.execute(
            f'CREATE TABLE "default".pbt.{table} AS '
            + transpile_to_duckdb(preagg["sql"]),
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
    assert tables, plan
    return tables
