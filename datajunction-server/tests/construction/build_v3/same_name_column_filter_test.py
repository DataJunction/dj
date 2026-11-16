"""
A filter on a dimension attribute is never bound to a same-named column of
another node. Events and the account dimension both have a `status` column.
"""

import duckdb
import pytest
import pytest_asyncio

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group

ACCOUNT_STATUS = "v3.sn_account.status = 'acct-vip'"
ACCOUNT_STATUS_ROLED = "v3.sn_account.status[owner] = 'acct-vip'"
EVENT_STATUS = "v3.sn_events.status = 'evt-open'"


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


@pytest_asyncio.fixture
async def status_client(client_with_build_v3):
    client = client_with_build_v3
    await create_source(
        client,
        "v3.sn_src_events",
        "sn_events",
        [("account_id", "int"), ("amount", "double"), ("status", "string")],
    )
    await create_source(
        client,
        "v3.sn_src_accounts",
        "sn_accounts",
        [("account_id", "int"), ("status", "string")],
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.sn_events",
            "query": "SELECT account_id, amount, status FROM v3.sn_src_events",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.sn_account",
            "query": "SELECT account_id, status FROM v3.sn_src_accounts",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    for role in (None, "owner"):
        await post(
            client,
            "/nodes/v3.sn_events/link",
            {
                "dimension_node": "v3.sn_account",
                "join_type": "left",
                "join_on": "v3.sn_events.account_id = v3.sn_account.account_id",
                **({"role": role} if role else {}),
            },
        )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.sn_amount",
            "query": "SELECT SUM(amount) FROM v3.sn_events",
            "mode": "published",
        },
    )
    return client


async def measures_sql(client, dimensions, filters):
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.sn_amount"],
            "dimensions": dimensions,
            "filters": filters,
        },
    )
    assert response.status_code == 200, response.text
    return get_first_grain_group(response.json())["sql"]


def run_in_duckdb(sql: str) -> list[tuple]:
    conn = duckdb.connect(":memory:")
    conn.execute("""ATTACH ':memory:' AS "default" """)
    conn.execute("""USE "default" """)
    conn.execute("CREATE SCHEMA v3")
    conn.execute(
        """
        CREATE TABLE v3.sn_accounts (account_id INT, status TEXT);
        INSERT INTO v3.sn_accounts VALUES
          (1, 'acct-vip'), (2, 'acct-new'), (3, 'acct-vip');
        CREATE TABLE v3.sn_events (account_id INT, amount DOUBLE, status TEXT);
        INSERT INTO v3.sn_events VALUES
          (1, 5, 'evt-open'), (2, 7, 'evt-open'),
          (3, 11, 'evt-closed'), (1, 13, 'evt-closed');
        """,
    )
    return conn.execute(sql.replace("default.v3.", '"default".v3.')).fetchall()


@pytest.mark.asyncio
async def test_account_filter_is_not_bound_to_event_status(status_client):
    sql = await measures_sql(status_client, ["v3.sn_account.status"], [ACCOUNT_STATUS])
    assert_sql_equal(
        sql,
        """
        WITH v3_sn_account AS (
          SELECT account_id, status
          FROM default.v3.sn_accounts
          WHERE status = 'acct-vip'
        ),
        v3_sn_events AS (
          SELECT account_id, amount
          FROM default.v3.sn_events
        )
        SELECT t2.status, SUM(t1.amount) amount_sum_HASH
        FROM v3_sn_events t1
        LEFT OUTER JOIN v3_sn_account t2 ON t1.account_id = t2.account_id
        WHERE t2.status = 'acct-vip'
        GROUP BY t2.status
        """,
        normalize_aliases=True,
    )
    assert run_in_duckdb(sql) == [("acct-vip", 29.0)]


@pytest.mark.asyncio
async def test_roled_account_filter_is_not_bound_to_event_status(status_client):
    sql = await measures_sql(
        status_client,
        ["v3.sn_account.status[owner]"],
        [ACCOUNT_STATUS_ROLED],
    )
    assert_sql_equal(
        sql,
        """
        WITH v3_sn_account AS (
          SELECT account_id, status
          FROM default.v3.sn_accounts
          WHERE status = 'acct-vip'
        ),
        v3_sn_events AS (
          SELECT account_id, amount
          FROM default.v3.sn_events
        )
        SELECT t2.status status_owner, SUM(t1.amount) amount_sum_HASH
        FROM v3_sn_events t1
        LEFT OUTER JOIN v3_sn_account t2 ON t1.account_id = t2.account_id
        WHERE t2.status = 'acct-vip'
        GROUP BY t2.status
        """,
        normalize_aliases=True,
    )
    assert run_in_duckdb(sql) == [("acct-vip", 29.0)]


@pytest.mark.asyncio
async def test_event_filter_is_not_bound_to_account_status(status_client):
    sql = await measures_sql(status_client, ["v3.sn_account.status"], [EVENT_STATUS])
    assert_sql_equal(
        sql,
        """
        WITH v3_sn_account AS (
          SELECT account_id, status
          FROM default.v3.sn_accounts
        ),
        v3_sn_events AS (
          SELECT account_id, amount
          FROM default.v3.sn_events
          WHERE status = 'evt-open'
        )
        SELECT t2.status, SUM(t1.amount) amount_sum_HASH
        FROM v3_sn_events t1
        LEFT OUTER JOIN v3_sn_account t2 ON t1.account_id = t2.account_id
        GROUP BY t2.status
        """,
        normalize_aliases=True,
    )
    assert sorted(run_in_duckdb(sql)) == [("acct-new", 7.0), ("acct-vip", 5.0)]


@pytest.mark.asyncio
async def test_both_filters_land_on_their_own_nodes(status_client):
    sql = await measures_sql(
        status_client,
        ["v3.sn_account.status"],
        [ACCOUNT_STATUS, EVENT_STATUS],
    )
    assert_sql_equal(
        sql,
        """
        WITH v3_sn_account AS (
          SELECT account_id, status
          FROM default.v3.sn_accounts
          WHERE status = 'acct-vip'
        ),
        v3_sn_events AS (
          SELECT account_id, amount
          FROM default.v3.sn_events
          WHERE status = 'evt-open'
        )
        SELECT t2.status, SUM(t1.amount) amount_sum_HASH
        FROM v3_sn_events t1
        LEFT OUTER JOIN v3_sn_account t2 ON t1.account_id = t2.account_id
        WHERE t2.status = 'acct-vip'
        GROUP BY t2.status
        """,
        normalize_aliases=True,
    )
    assert run_in_duckdb(sql) == [("acct-vip", 5.0)]


@pytest.mark.asyncio
async def test_account_filter_without_its_dimension_requested(status_client):
    sql = await measures_sql(
        status_client,
        ["v3.sn_account.account_id"],
        [ACCOUNT_STATUS],
    )
    assert_sql_equal(
        sql,
        """
        WITH v3_sn_account AS (
          SELECT account_id, status
          FROM default.v3.sn_accounts
          WHERE status = 'acct-vip'
        ),
        v3_sn_events AS (
          SELECT account_id, amount
          FROM default.v3.sn_events
        )
        SELECT COALESCE(t1.account_id, t2.account_id) account_id,
          SUM(t1.amount) amount_sum_HASH
        FROM v3_sn_events t1
        LEFT OUTER JOIN v3_sn_account t2 ON t1.account_id = t2.account_id
        WHERE t2.status = 'acct-vip'
        GROUP BY COALESCE(t1.account_id, t2.account_id)
        """,
        normalize_aliases=True,
    )
    assert sorted(run_in_duckdb(sql)) == [(1, 18.0), (3, 11.0)]


@pytest.mark.asyncio
async def test_event_filter_with_roled_account_requested(status_client):
    sql = await measures_sql(
        status_client,
        ["v3.sn_account.status[owner]"],
        [EVENT_STATUS],
    )
    assert_sql_equal(
        sql,
        """
        WITH v3_sn_account AS (
          SELECT account_id, status
          FROM default.v3.sn_accounts
        ),
        v3_sn_events AS (
          SELECT account_id, amount
          FROM default.v3.sn_events
          WHERE status = 'evt-open'
        )
        SELECT t2.status status_owner, SUM(t1.amount) amount_sum_HASH
        FROM v3_sn_events t1
        LEFT OUTER JOIN v3_sn_account t2 ON t1.account_id = t2.account_id
        GROUP BY t2.status
        """,
        normalize_aliases=True,
    )
    assert sorted(run_in_duckdb(sql)) == [("acct-new", 7.0), ("acct-vip", 5.0)]


@pytest_asyncio.fixture
async def snapshot_client(client_with_build_v3):
    client = client_with_build_v3
    await create_source(
        client,
        "v3.sn_src_facts",
        "sn_facts",
        [
            ("test_id", "int"),
            ("account_id", "int"),
            ("amount", "double"),
            ("snapshot_utc_date", "int"),
        ],
    )
    await create_source(
        client,
        "v3.sn_src_alloc",
        "sn_alloc",
        [
            ("test_id", "int"),
            ("account_id", "int"),
            ("snapshot_utc_date", "int"),
            ("country", "string"),
        ],
    )
    await create_source(
        client,
        "v3.sn_src_dates",
        "sn_dates",
        [("dateint", "int"), ("week", "int")],
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.sn_facts",
            "query": (
                "SELECT test_id, account_id, amount, snapshot_utc_date "
                "FROM v3.sn_src_facts"
            ),
            "mode": "published",
            "primary_key": ["test_id", "account_id", "snapshot_utc_date"],
        },
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.sn_alloc",
            "query": (
                "SELECT test_id, account_id, snapshot_utc_date, country "
                "FROM v3.sn_src_alloc"
            ),
            "mode": "published",
            "primary_key": ["test_id", "account_id", "snapshot_utc_date"],
        },
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.sn_dates",
            "query": "SELECT dateint, week FROM v3.sn_src_dates",
            "mode": "published",
            "primary_key": ["dateint"],
        },
    )
    await post(
        client,
        "/nodes/v3.sn_facts/link",
        {
            "dimension_node": "v3.sn_alloc",
            "join_type": "right",
            "join_cardinality": "one_to_many",
            "join_on": (
                "v3.sn_facts.test_id = v3.sn_alloc.test_id "
                "AND v3.sn_facts.account_id = v3.sn_alloc.account_id"
            ),
        },
    )
    await post(
        client,
        "/nodes/v3.sn_alloc/link",
        {
            "dimension_node": "v3.sn_dates",
            "join_type": "left",
            "join_cardinality": "many_to_one",
            "join_on": "v3.sn_alloc.snapshot_utc_date = v3.sn_dates.dateint",
            "role": "allocation_snapshot",
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.sn_snapshot_amount",
            "query": "SELECT SUM(amount) FROM v3.sn_facts",
            "mode": "published",
        },
    )
    return client


@pytest.mark.asyncio
async def test_snapshot_role_filter_is_not_bound_to_fact_snapshot_column(
    snapshot_client,
):
    """
    The fact and the allocation both have `snapshot_utc_date`. A filter on the
    allocation's snapshot role belongs on the allocation only.
    """
    response = await snapshot_client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.sn_snapshot_amount"],
            "dimensions": ["v3.sn_alloc.country"],
            "filters": ["v3.sn_dates.dateint[allocation_snapshot] = 20260101"],
        },
    )
    assert response.status_code == 200, response.text
    sql = get_first_grain_group(response.json())["sql"]
    assert_sql_equal(
        sql,
        """
        WITH v3_sn_alloc AS (
          SELECT test_id, account_id, snapshot_utc_date, country
          FROM default.v3.sn_alloc
          WHERE snapshot_utc_date = 20260101
        ),
        v3_sn_facts AS (
          SELECT test_id, account_id, amount
          FROM default.v3.sn_facts
        )
        SELECT t2.country, SUM(t1.amount) amount_sum_HASH
        FROM v3_sn_facts t1
        RIGHT OUTER JOIN v3_sn_alloc t2
          ON t1.test_id = t2.test_id AND t1.account_id = t2.account_id
        WHERE t2.snapshot_utc_date = 20260101
        GROUP BY t2.country
        """,
        normalize_aliases=True,
    )
    conn = duckdb.connect(":memory:")
    conn.execute("""ATTACH ':memory:' AS "default" """)
    conn.execute("""USE "default" """)
    conn.execute("CREATE SCHEMA v3")
    conn.execute(
        """
        CREATE TABLE v3.sn_facts (
          test_id INT, account_id INT, amount DOUBLE, snapshot_utc_date INT
        );
        INSERT INTO v3.sn_facts VALUES (1, 1, 5, 20260102), (1, 2, 7, 20260101);
        CREATE TABLE v3.sn_alloc (
          test_id INT, account_id INT, snapshot_utc_date INT, country TEXT
        );
        INSERT INTO v3.sn_alloc VALUES
          (1, 1, 20260101, 'US'), (1, 2, 20260102, 'CA');
        """,
    )
    rows = conn.execute(sql.replace("default.v3.", '"default".v3.')).fetchall()
    assert rows == [("US", 5.0)]


@pytest_asyncio.fixture
async def based_status_client(client_with_build_v3):
    """Like `status_client`, with a transform between the account dimension and its source."""
    client = client_with_build_v3
    await create_source(
        client,
        "v3.sb_src_events",
        "sn_events",
        [("account_id", "int"), ("amount", "double"), ("status", "string")],
    )
    await create_source(
        client,
        "v3.sb_src_accounts",
        "sn_accounts",
        [("account_id", "int"), ("status", "string")],
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.sb_events",
            "query": "SELECT account_id, amount, status FROM v3.sb_src_events",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.sb_account_base",
            "query": "SELECT account_id, status FROM v3.sb_src_accounts",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.sb_account",
            "query": "SELECT account_id, status FROM v3.sb_account_base",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/v3.sb_events/link",
        {
            "dimension_node": "v3.sb_account",
            "join_type": "left",
            "join_on": "v3.sb_events.account_id = v3.sb_account.account_id",
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.sb_amount",
            "query": "SELECT SUM(amount) FROM v3.sb_events",
            "mode": "published",
        },
    )
    return client


@pytest.mark.asyncio
async def test_event_filter_is_not_bound_to_transform_under_account(
    based_status_client,
):
    response = await based_status_client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.sb_amount"],
            "dimensions": ["v3.sb_account.status"],
            "filters": ["v3.sb_events.status = 'evt-open'"],
        },
    )
    assert response.status_code == 200, response.text
    sql = get_first_grain_group(response.json())["sql"]
    assert_sql_equal(
        sql,
        """
        WITH v3_sb_account_base AS (
          SELECT account_id, status
          FROM default.v3.sn_accounts
        ),
        v3_sb_events AS (
          SELECT account_id, amount
          FROM default.v3.sn_events
          WHERE status = 'evt-open'
        ),
        v3_sb_account AS (
          SELECT account_id, status
          FROM v3_sb_account_base
        )
        SELECT t2.status, SUM(t1.amount) amount_sum_HASH
        FROM v3_sb_events t1
        LEFT OUTER JOIN v3_sb_account t2 ON t1.account_id = t2.account_id
        GROUP BY t2.status
        """,
        normalize_aliases=True,
    )
    assert sorted(run_in_duckdb(sql)) == [("acct-new", 7.0), ("acct-vip", 5.0)]


@pytest_asyncio.fixture
async def renamed_status_client(client_with_build_v3):
    """`rn_fact.status` is `rn_d.other_status` renamed twice, not `rn_d.status`."""
    client = client_with_build_v3
    await create_source(
        client,
        "v3.rn_src",
        "rn_rows",
        [("status", "string"), ("other_status", "string"), ("amount", "double")],
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.rn_d",
            "query": "SELECT status, other_status, amount FROM v3.rn_src",
            "mode": "published",
            "primary_key": ["status"],
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.rn_mid",
            "query": "SELECT other_status AS old_status, amount FROM v3.rn_d",
            "mode": "published",
            "primary_key": ["old_status"],
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.rn_fact",
            "query": "SELECT old_status AS status, amount FROM v3.rn_mid",
            "mode": "published",
            "primary_key": ["status"],
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.rn_amount",
            "query": "SELECT SUM(amount) FROM v3.rn_fact",
            "mode": "published",
        },
    )
    return client


@pytest.mark.asyncio
async def test_filter_is_not_bound_through_renamed_columns(renamed_status_client):
    response = await renamed_status_client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.rn_amount"],
            "dimensions": [],
            "filters": ["v3.rn_fact.status = 'open'"],
        },
    )
    assert response.status_code == 200, response.text
    sql = get_first_grain_group(response.json())["sql"]
    assert_sql_equal(
        sql,
        """
        WITH v3_rn_d AS (
          SELECT other_status, amount
          FROM default.v3.rn_rows
        ),
        v3_rn_mid AS (
          SELECT other_status AS old_status, amount
          FROM v3_rn_d
        ),
        v3_rn_fact AS (
          SELECT amount
          FROM v3_rn_mid
          WHERE old_status = 'open'
        )
        SELECT SUM(t1.amount) amount_sum_HASH
        FROM v3_rn_fact t1
        """,
        normalize_aliases=True,
    )
    conn = duckdb.connect(":memory:")
    conn.execute("""ATTACH ':memory:' AS "default" """)
    conn.execute("""USE "default" """)
    conn.execute("CREATE SCHEMA v3")
    conn.execute(
        """
        CREATE TABLE v3.rn_rows (status TEXT, other_status TEXT, amount DOUBLE);
        INSERT INTO v3.rn_rows VALUES ('closed', 'open', 5), ('open', 'closed', 7);
        """,
    )
    rows = conn.execute(sql.replace("default.v3.", '"default".v3.')).fetchall()
    assert rows == [(5.0,)]


async def build_window_nodes(client, own_column):
    """`meta` reads the `window` dimension. With `own_column`, `meta` also
    has a computed column of the same name as the dimension's attribute."""
    await create_source(
        client,
        "v3.mr_window_src",
        "mr_window",
        [("observation_window", "string"), ("window_end", "int")],
    )
    await create_source(
        client,
        "v3.mr_plans_src",
        "mr_plans",
        [("test_id", "int"), ("end_date", "int")],
    )
    await create_source(
        client,
        "v3.mr_facts_src",
        "mr_facts",
        [("test_id", "int"), ("amount", "double")],
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.mr_window",
            "query": "SELECT observation_window, window_end FROM v3.mr_window_src",
            "mode": "published",
            "primary_key": ["observation_window"],
        },
    )
    extra = ", MAX(w.window_end) AS observation_window" if own_column else ""
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.mr_meta",
            "query": (
                "SELECT p.test_id, MAX(p.end_date + w.window_end) AS end_dt"
                f"{extra} FROM v3.mr_plans_src p CROSS JOIN "
                "(SELECT observation_window AS obs_window, window_end "
                "FROM v3.mr_window) w GROUP BY p.test_id"
            ),
            "mode": "published",
            "primary_key": ["test_id"],
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.mr_facts",
            "query": (
                "SELECT f.test_id, f.amount, w.observation_window "
                "FROM v3.mr_facts_src f CROSS JOIN "
                "(SELECT observation_window FROM v3.mr_window) w "
                "INNER JOIN v3.mr_meta m ON f.test_id = m.test_id"
            ),
            "mode": "published",
            "primary_key": ["test_id", "observation_window"],
        },
    )
    await post(
        client,
        "/nodes/v3.mr_facts/link",
        {
            "dimension_node": "v3.mr_window",
            "join_type": "left",
            "join_cardinality": "many_to_one",
            "join_on": "v3.mr_facts.observation_window = v3.mr_window.observation_window",
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.mr_amount",
            "query": "SELECT SUM(amount) FROM v3.mr_facts",
            "mode": "published",
        },
    )
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.mr_amount"],
            "dimensions": ["v3.mr_window.observation_window"],
            "filters": ["v3.mr_window.observation_window IN ('1-35')"],
        },
    )
    assert response.status_code == 200, response.text
    return get_first_grain_group(response.json())["sql"]


def window_rows(sql):
    conn = duckdb.connect(":memory:")
    conn.execute("""ATTACH ':memory:' AS "default" """)
    conn.execute("""USE "default" """)
    conn.execute("CREATE SCHEMA v3")
    conn.execute(
        """
        CREATE TABLE v3.mr_window (observation_window TEXT, window_end INT);
        INSERT INTO v3.mr_window VALUES ('1-35', 35), ('1-70', 70);
        CREATE TABLE v3.mr_plans (test_id INT, end_date INT);
        INSERT INTO v3.mr_plans VALUES (1, 100);
        CREATE TABLE v3.mr_facts (test_id INT, amount DOUBLE);
        INSERT INTO v3.mr_facts VALUES (1, 5);
        """,
    )
    return conn.execute(sql.replace("default.v3.", '"default".v3.')).fetchall()


@pytest.mark.asyncio
async def test_dimension_reading_the_filtered_dimension_is_filtered(
    client_with_build_v3,
):
    sql = await build_window_nodes(client_with_build_v3, own_column=False)
    assert_sql_equal(
        sql,
        """
        WITH v3_mr_window AS (
          SELECT observation_window, window_end
          FROM default.v3.mr_window
          WHERE observation_window IN ('1-35')
        ),
        v3_mr_meta AS (
          SELECT p.test_id
          FROM default.v3.mr_plans p
          CROSS JOIN (
            SELECT observation_window AS obs_window, window_end
            FROM v3_mr_window
            WHERE v3_mr_window.observation_window IN ('1-35')
          ) w
          GROUP BY p.test_id
        ),
        v3_mr_facts AS (
          SELECT f.amount, w.observation_window
          FROM default.v3.mr_facts f
          CROSS JOIN (
            SELECT observation_window
            FROM v3_mr_window
            WHERE v3_mr_window.observation_window IN ('1-35')
          ) w
          INNER JOIN v3_mr_meta m ON f.test_id = m.test_id
          WHERE w.observation_window IN ('1-35')
        )
        SELECT t1.observation_window, SUM(t1.amount) amount_sum_HASH
        FROM v3_mr_facts t1
        GROUP BY t1.observation_window
        """,
        normalize_aliases=True,
    )
    assert window_rows(sql) == [("1-35", 5.0)]


@pytest.mark.asyncio
async def test_dimension_with_its_own_column_of_that_name_is_not_filtered(
    client_with_build_v3,
):
    sql = await build_window_nodes(client_with_build_v3, own_column=True)
    assert_sql_equal(
        sql,
        """
        WITH v3_mr_window AS (
          SELECT observation_window, window_end
          FROM default.v3.mr_window
          WHERE observation_window IN ('1-35')
        ),
        v3_mr_meta AS (
          SELECT p.test_id, MAX(w.window_end) AS observation_window
          FROM default.v3.mr_plans p
          CROSS JOIN (
            SELECT observation_window AS obs_window, window_end
            FROM v3_mr_window
          ) w
          GROUP BY p.test_id
        ),
        v3_mr_facts AS (
          SELECT f.amount, w.observation_window
          FROM default.v3.mr_facts f
          CROSS JOIN (
            SELECT observation_window
            FROM v3_mr_window
            WHERE v3_mr_window.observation_window IN ('1-35')
          ) w
          INNER JOIN v3_mr_meta m ON f.test_id = m.test_id
          WHERE w.observation_window IN ('1-35')
        )
        SELECT t1.observation_window, SUM(t1.amount) amount_sum_HASH
        FROM v3_mr_facts t1
        GROUP BY t1.observation_window
        """,
        normalize_aliases=True,
    )
    assert window_rows(sql) == [("1-35", 5.0)]
