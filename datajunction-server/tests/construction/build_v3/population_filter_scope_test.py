"""
Spike: a filter applies only to the metrics that can reach its attribute.

Model: ``exp.allocated_accounts`` counts every allocated account (the
population). ``exp.total_secs`` sums play time per play event. A device
filter is reachable from the plays but not from the allocation, so
``exp.secs_per_account`` filtered by device should divide the filtered play
time by the whole population, not by the accounts that played on that device.
"""

import pytest
import pytest_asyncio

from tests.conftest import transpile_to_duckdb

ALLOCATION = [(1, "A"), (2, "A"), (3, "B"), (4, "B")]
PLAYS = [
    (1, "tv", 100.0),
    (1, "phone", 50.0),
    (2, "phone", 30.0),
    (3, "tv", 60.0),
]


async def _post(client, path, payload):
    response = await client.post(path, json=payload)
    assert response.status_code in (200, 201, 409), response.json()


@pytest_asyncio.fixture(scope="module")
async def exp_model(module__client_with_build_v3):
    client = module__client_with_build_v3
    await _post(client, "/namespaces/exp/", {})
    tables = {
        "src_allocation": [("account_id", "int"), ("cell", "string")],
        "src_play": [
            ("account_id", "int"),
            ("device_type", "string"),
            ("secs", "float"),
        ],
        "src_device": [("device_type", "string")],
    }
    for name, columns in tables.items():
        await _post(
            client,
            "/nodes/source/",
            {
                "name": f"exp.{name}",
                "description": name,
                "columns": [{"name": n, "type": t} for n, t in columns],
                "mode": "published",
                "catalog": "default",
                "schema_": "exp",
                "table": name.removeprefix("src_"),
            },
        )
    await _post(
        client,
        "/nodes/dimension/",
        {
            "name": "exp.allocation",
            "description": "Allocated accounts",
            "query": "SELECT account_id, cell FROM exp.src_allocation",
            "primary_key": ["account_id"],
            "mode": "published",
        },
    )
    await _post(
        client,
        "/nodes/dimension/",
        {
            "name": "exp.device_d",
            "description": "Devices",
            "query": "SELECT device_type FROM exp.src_device",
            "primary_key": ["device_type"],
            "mode": "published",
        },
    )
    await _post(
        client,
        "/nodes/transform/",
        {
            "name": "exp.play",
            "description": "Play events",
            "query": "SELECT account_id, device_type, secs FROM exp.src_play",
            "mode": "published",
        },
    )
    for dimension, join_on in [
        ("exp.allocation", "exp.play.account_id = exp.allocation.account_id"),
        ("exp.device_d", "exp.play.device_type = exp.device_d.device_type"),
    ]:
        await _post(
            client,
            "/nodes/exp.play/link",
            {
                "dimension_node": dimension,
                "join_type": "left",
                "join_on": join_on,
                "join_cardinality": "many_to_one",
            },
        )
    for name, query in [
        (
            "exp.allocated_accounts",
            "SELECT COUNT(DISTINCT account_id) FROM exp.allocation",
        ),
        ("exp.total_secs", "SELECT SUM(secs) FROM exp.play"),
        (
            "exp.secs_per_account",
            "SELECT exp.total_secs / NULLIF(exp.allocated_accounts, 0)",
        ),
    ]:
        await _post(
            client,
            "/nodes/metric/",
            {
                "name": name,
                "description": name,
                "query": query,
                "mode": "published",
            },
        )
    return client


@pytest.fixture(scope="module")
def exp_tables(duckdb_conn):
    duckdb_conn.execute('CREATE SCHEMA IF NOT EXISTS "default".exp')
    duckdb_conn.execute(
        'CREATE OR REPLACE TABLE "default".exp.allocation(account_id INT, cell VARCHAR)',
    )
    duckdb_conn.execute(
        'CREATE OR REPLACE TABLE "default".exp.play'
        "(account_id INT, device_type VARCHAR, secs DOUBLE)",
    )
    duckdb_conn.execute(
        'CREATE OR REPLACE TABLE "default".exp.device(device_type VARCHAR)',
    )
    duckdb_conn.executemany(
        'INSERT INTO "default".exp.allocation VALUES (?, ?)',
        ALLOCATION,
    )
    duckdb_conn.executemany('INSERT INTO "default".exp.play VALUES (?, ?, ?)', PLAYS)
    duckdb_conn.executemany(
        'INSERT INTO "default".exp.device VALUES (?)',
        [("tv",), ("phone",)],
    )
    return duckdb_conn


async def _query(client, filters=None):
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["exp.secs_per_account"],
            "dimensions": ["exp.allocation.cell"],
            "filters": filters or [],
        },
    )
    return response


def _rows(conn, sql):
    return sorted(conn.execute(transpile_to_duckdb(sql)).fetchall())


@pytest.mark.asyncio
async def test_without_filter_divides_by_whole_population(exp_model, exp_tables):
    response = await _query(exp_model)
    assert response.status_code == 200, response.text
    body = response.json()
    assert _rows(exp_tables, body["sql"]) == [("A", 90.0), ("B", 30.0)]
    assert body["warnings"] == []


@pytest.mark.asyncio
async def test_device_filter_keeps_population_unfiltered(exp_model, exp_tables):
    response = await _query(exp_model, ["exp.device_d.device_type = 'tv'"])
    assert response.status_code == 200, response.text
    body = response.json()
    # Cell A: 100 tv secs / 2 allocated accounts. Cell B: 60 / 2.
    # Account 4 never played and account 2 never used a tv; both stay counted.
    assert _rows(exp_tables, body["sql"]) == [("A", 50.0), ("B", 30.0)]


@pytest.mark.asyncio
async def test_device_filter_reports_where_it_applied(exp_model):
    response = await _query(exp_model, ["exp.device_d.device_type = 'tv'"])
    assert response.status_code == 200, response.text
    assert [(w["code"], w["message"]) for w in response.json()["warnings"]] == [
        (
            "FILTER_SCOPED",
            "Filter exp.device_d.device_type = 'tv' applied to exp.total_secs "
            "only; exp.allocated_accounts cannot reach it",
        ),
    ]


@pytest.mark.asyncio
async def test_filter_both_sides_reach_applies_to_both(exp_model, exp_tables):
    response = await _query(exp_model, ["exp.allocation.cell = 'A'"])
    assert response.status_code == 200, response.text
    body = response.json()
    assert _rows(exp_tables, body["sql"]) == [("A", 90.0)]
    assert body["warnings"] == []


@pytest.mark.asyncio
async def test_filter_no_metric_reaches_still_fails(exp_model):
    response = await _query(exp_model, ["exp.nowhere.col = 1"])
    assert response.status_code in (404, 422), response.text
