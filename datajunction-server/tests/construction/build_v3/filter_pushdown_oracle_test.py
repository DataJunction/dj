"""
Generated SQL returns the same rows as a plain SQL answer written by hand.

The fact and the dimension both have a `status` column with different values,
and the dimension may sit on a transform. Wherever a filter lands in the
generated SQL, the rows must match.
"""

import duckdb
import pytest

from tests.construction.build_v3 import get_first_grain_group

FACT_FILTER = "v3.fo_facts.status = 'f1'"
DIM_FILTER = "v3.fo_account.status = 'd1'"
SQL_FILTERS = {FACT_FILTER: "status = 'f1'", DIM_FILTER: "d.status = 'd1'"}


async def post(client, path, body):
    response = await client.post(path, json=body)
    assert response.status_code in (200, 201), response.text


async def create_source(client, name, table, columns):
    await post(
        client,
        "/nodes/source/",
        {
            "name": name,
            "catalog": "default",
            "schema_": "v3",
            "table": table,
            "mode": "published",
            "columns": [{"name": n, "type": t} for n, t in columns],
        },
    )


async def build_nodes(client, join_type, through_transform):
    await create_source(
        client,
        "v3.fo_src_facts",
        "fo_facts",
        [("account_id", "int"), ("amount", "double"), ("status", "string")],
    )
    await create_source(
        client,
        "v3.fo_src_accounts",
        "fo_accounts",
        [("account_id", "int"), ("status", "string")],
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.fo_facts",
            "query": "SELECT account_id, amount, status FROM v3.fo_src_facts",
            "mode": "published",
            "primary_key": ["account_id", "amount"],
        },
    )
    account_source = "v3.fo_src_accounts"
    if through_transform:
        await post(
            client,
            "/nodes/transform/",
            {
                "name": "v3.fo_account_base",
                "query": "SELECT account_id, status FROM v3.fo_src_accounts",
                "mode": "published",
                "primary_key": ["account_id"],
            },
        )
        account_source = "v3.fo_account_base"
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.fo_account",
            "query": f"SELECT account_id, status FROM {account_source}",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/v3.fo_facts/link",
        {
            "dimension_node": "v3.fo_account",
            "join_type": join_type,
            "join_cardinality": "many_to_one",
            "join_on": "v3.fo_facts.account_id = v3.fo_account.account_id",
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.fo_amount",
            "query": "SELECT SUM(amount) FROM v3.fo_facts",
            "mode": "published",
        },
    )


def run_sql(sql):
    conn = duckdb.connect(":memory:")
    conn.execute("""ATTACH ':memory:' AS "default" """)
    conn.execute("""USE "default" """)
    conn.execute("CREATE SCHEMA v3")
    conn.execute(
        """
        CREATE TABLE v3.fo_facts (account_id INT, amount DOUBLE, status TEXT);
        INSERT INTO v3.fo_facts VALUES
          (1, 5, 'f1'), (2, 7, 'f2'), (3, 11, 'f1'), (1, 13, 'f2'), (9, 17, 'f1');
        CREATE TABLE v3.fo_accounts (account_id INT, status TEXT);
        INSERT INTO v3.fo_accounts VALUES
          (1, 'd1'), (2, 'd2'), (3, 'd1'), (4, 'd2');
        """,
    )
    rows = conn.execute(sql.replace("default.v3.", '"default".v3.')).fetchall()
    return sorted(rows, key=lambda row: (row[0] is None, row))


def expected_sql(join_type, filters):
    fact_where = " AND ".join(SQL_FILTERS[f] for f in filters if f == FACT_FILTER)
    dim_where = " AND ".join(SQL_FILTERS[f] for f in filters if f == DIM_FILTER)
    return f"""
        SELECT d.status, SUM(f.amount)
        FROM (
          SELECT * FROM v3.fo_facts {"WHERE " + fact_where if fact_where else ""}
        ) f
        {join_type.upper()} JOIN v3.fo_accounts d ON f.account_id = d.account_id
        {"WHERE " + dim_where if dim_where else ""}
        GROUP BY d.status
    """


@pytest.mark.asyncio
@pytest.mark.parametrize("join_type", ["left", "right"])
@pytest.mark.parametrize("through_transform", [False, True])
@pytest.mark.parametrize(
    "filters",
    [[FACT_FILTER], [DIM_FILTER], [FACT_FILTER, DIM_FILTER]],
    ids=["fact", "dimension", "both"],
)
async def test_rows_match_plain_sql(
    client_with_build_v3,
    join_type,
    through_transform,
    filters,
):
    await build_nodes(client_with_build_v3, join_type, through_transform)
    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.fo_amount"],
            "dimensions": ["v3.fo_account.status"],
            "filters": filters,
        },
    )
    assert response.status_code == 200, response.text
    generated = run_sql(get_first_grain_group(response.json())["sql"])
    assert generated == run_sql(expected_sql(join_type, filters))
