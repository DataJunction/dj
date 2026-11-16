"""
Generated SQL returns the same rows as a plain SQL answer written by hand.

The facts and the dimension all have a `status` column with different values.
The dimension may sit on a chain of transforms that also carry a decoy
`status`, take its `status` from a renamed column, or be linked with a role.
Wherever a filter lands in the generated SQL, the rows must match.
"""

import duckdb
import pytest

from tests.construction.build_v3 import get_first_grain_group

FACT = "fact"
DIM = "dimension"
SHAPES = {
    "direct": {},
    "one_transform": {"depth": 1},
    "three_transforms": {"depth": 3},
    "renamed": {"depth": 2, "rename": True},
    "role": {"role": "owner"},
    "role_deep_renamed": {"depth": 3, "rename": True, "role": "owner"},
}


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


async def build_nodes(client, join_type, depth=0, rename=False, role=None):
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
        [("account_id", "int"), ("status", "string"), ("real_status", "string")],
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
    previous = "v3.fo_src_accounts"
    for level in range(depth):
        name = f"v3.fo_account_base{level}"
        await post(
            client,
            "/nodes/transform/",
            {
                "name": name,
                "query": f"SELECT account_id, status, real_status FROM {previous}",
                "mode": "published",
                "primary_key": ["account_id"],
            },
        )
        previous = name
    column = "real_status AS status" if rename else "status"
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.fo_account",
            "query": f"SELECT account_id, {column} FROM {previous}",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await link_facts(client, "v3.fo_facts", join_type, role)
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.fo_amount",
            "query": "SELECT SUM(amount) FROM v3.fo_facts",
            "mode": "published",
        },
    )


async def link_facts(client, fact, join_type, role):
    await post(
        client,
        f"/nodes/{fact}/link",
        {
            "dimension_node": "v3.fo_account",
            "join_type": join_type,
            "join_cardinality": "many_to_one",
            "join_on": f"{fact}.account_id = v3.fo_account.account_id",
            **({"role": role} if role else {}),
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
        CREATE TABLE v3.fo_facts2 (account_id INT, amount2 DOUBLE, status TEXT);
        INSERT INTO v3.fo_facts2 VALUES
          (1, 3, 'f1'), (2, 4, 'f2'), (4, 6, 'f1'), (9, 8, 'f2');
        CREATE TABLE v3.fo_accounts (
          account_id INT, status TEXT, real_status TEXT
        );
        INSERT INTO v3.fo_accounts VALUES
          (1, 'd1', 'd2'), (2, 'd2', 'd1'), (3, 'd1', 'd2'), (4, 'd2', 'd1');
        """,
    )
    rows = conn.execute(sql.replace("default.v3.", '"default".v3.')).fetchall()
    return sorted(rows, key=lambda row: (row[0] is None, row))


def expected_sql(join_type, filters, rename, fact="fo_facts", amount="amount"):
    label = "real_status" if rename else "status"
    fact_where = "WHERE status = 'f1'" if FACT in filters else ""
    dim_where = f"WHERE d.{label} = 'd1'" if DIM in filters else ""
    return f"""
        SELECT d.{label}, SUM(f.{amount})
        FROM (SELECT * FROM v3.{fact} {fact_where}) f
        {join_type.upper()} JOIN v3.fo_accounts d ON f.account_id = d.account_id
        {dim_where}
        GROUP BY d.{label}
    """


def request_refs(filters, role):
    ref = f"v3.fo_account.status[{role}]" if role else "v3.fo_account.status"
    refs = {FACT: "v3.fo_facts.status = 'f1'", DIM: f"{ref} = 'd1'"}
    return ref, [refs[kind] for kind in filters]


@pytest.mark.asyncio
@pytest.mark.parametrize("join_type", ["left", "right"])
@pytest.mark.parametrize("shape", SHAPES)
@pytest.mark.parametrize(
    "filters",
    [[FACT], [DIM], [FACT, DIM]],
    ids=["fact", "dimension", "both"],
)
async def test_rows_match_plain_sql(client_with_build_v3, join_type, shape, filters):
    spec = SHAPES[shape]
    await build_nodes(client_with_build_v3, join_type, **spec)
    dimension, filter_strings = request_refs(filters, spec.get("role"))
    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.fo_amount"],
            "dimensions": [dimension],
            "filters": filter_strings,
        },
    )
    assert response.status_code == 200, response.text
    generated = run_sql(get_first_grain_group(response.json())["sql"])
    assert generated == run_sql(
        expected_sql(join_type, filters, spec.get("rename", False)),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("join_type", ["left", "right"])
async def test_two_facts_match_plain_sql(client_with_build_v3, join_type):
    client = client_with_build_v3
    await build_nodes(client, join_type, depth=2, rename=True)
    await create_source(
        client,
        "v3.fo_src_facts2",
        "fo_facts2",
        [("account_id", "int"), ("amount2", "double"), ("status", "string")],
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.fo_facts2",
            "query": "SELECT account_id, amount2, status FROM v3.fo_src_facts2",
            "mode": "published",
            "primary_key": ["account_id", "amount2"],
        },
    )
    await link_facts(client, "v3.fo_facts2", join_type, None)
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.fo_amount2",
            "query": "SELECT SUM(amount2) FROM v3.fo_facts2",
            "mode": "published",
        },
    )
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.fo_amount", "v3.fo_amount2"],
            "dimensions": ["v3.fo_account.status"],
            "filters": ["v3.fo_account.status = 'd1'"],
        },
    )
    assert response.status_code == 200, response.text
    generated = sorted(
        (run_sql(group["sql"]) for group in response.json()["grain_groups"]),
        key=repr,
    )
    expected = sorted(
        [
            run_sql(expected_sql(join_type, [DIM], True)),
            run_sql(expected_sql(join_type, [DIM], True, "fo_facts2", "amount2")),
        ],
        key=repr,
    )
    assert generated == expected
