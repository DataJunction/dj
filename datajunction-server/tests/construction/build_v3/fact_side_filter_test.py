"""
Filters on dimensions reached only through the fact apply before a RIGHT
OUTER JOIN to a spine dimension, so spine rows with no fact rows survive.
"""

import duckdb
import pytest
import pytest_asyncio

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group

ACCOUNT_FILTER = "v3.fs_country.country_name[account->home] = 'United States'"
ORIGIN_FILTER = "v3.fs_country.country_name[show->origin] = 'Canada'"
ALLOCATION_FILTER = "v3.fs_country.country_name[allocation->home] = 'United States'"
MEASURE_FILTER = "v3.date.date_id[measure] BETWEEN 20260101 AND 20260131"


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


async def create_dimension(client, name, query, primary_key):
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": name,
            "query": query,
            "mode": "published",
            "primary_key": primary_key,
        },
    )


async def link(client, node, dimension, join_on, role=None, join_type="left"):
    body = {
        "dimension_node": dimension,
        "join_type": join_type,
        "join_on": join_on,
    }
    if role:
        body["role"] = role
    await post(client, f"/nodes/{node}/link", body)


@pytest_asyncio.fixture
async def spine_client(client_with_build_v3):
    """Events RIGHT JOINed to an allocation spine; one country dimension is
    reached through the account, the show and the spine's account."""
    client = client_with_build_v3
    await create_source(
        client,
        "v3.fs_src_events",
        "fs_events",
        [
            ("account_id", "int"),
            ("show_id", "int"),
            ("amount", "double"),
            ("event_date", "int"),
        ],
    )
    await create_source(
        client,
        "v3.fs_src_accounts",
        "fs_accounts",
        [("account_id", "int"), ("country_code", "string")],
    )
    await create_source(
        client,
        "v3.fs_src_shows",
        "fs_shows",
        [("show_id", "int"), ("origin_code", "string")],
    )
    await create_source(
        client,
        "v3.fs_src_countries",
        "fs_countries",
        [("country_code", "string"), ("country_name", "string")],
    )
    await create_source(
        client,
        "v3.fs_src_alloc_days",
        "fs_alloc_days",
        [("account_id", "int"), ("snapshot_date", "int")],
    )
    await create_source(
        client,
        "v3.fs_src_survivals",
        "fs_survivals",
        [("snapshot_dateint", "int"), ("cohort", "string")],
    )
    await create_source(
        client,
        "v3.fs_src_budgets",
        "fs_budgets",
        [("account_id", "int"), ("cap", "double")],
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.fs_events",
            "query": (
                "SELECT account_id, show_id, amount, event_date FROM v3.fs_src_events"
            ),
            "mode": "published",
            "primary_key": ["account_id", "show_id", "event_date"],
        },
    )
    await create_dimension(
        client,
        "v3.fs_account",
        "SELECT account_id, country_code FROM v3.fs_src_accounts",
        ["account_id"],
    )
    await create_dimension(
        client,
        "v3.fs_show",
        "SELECT show_id, origin_code FROM v3.fs_src_shows",
        ["show_id"],
    )
    await create_dimension(
        client,
        "v3.fs_country",
        "SELECT country_code, country_name FROM v3.fs_src_countries",
        ["country_code"],
    )
    await create_dimension(
        client,
        "v3.fs_alloc_day",
        "SELECT account_id, snapshot_date FROM v3.fs_src_alloc_days",
        ["account_id", "snapshot_date"],
    )
    await create_dimension(
        client,
        "v3.fs_survival",
        "SELECT snapshot_dateint, cohort FROM v3.fs_src_survivals",
        ["snapshot_dateint"],
    )
    await link(
        client,
        "v3.fs_alloc_day",
        "v3.fs_survival",
        "v3.fs_alloc_day.snapshot_date = v3.fs_survival.snapshot_dateint",
    )
    await link(
        client,
        "v3.fs_survival",
        "v3.date",
        "v3.fs_survival.snapshot_dateint = v3.date.date_id",
        role="survival",
    )
    await create_dimension(
        client,
        "v3.fs_budget",
        "SELECT account_id, cap FROM v3.fs_src_budgets",
        ["account_id"],
    )
    await link(
        client,
        "v3.fs_events",
        "v3.fs_budget",
        "v3.fs_events.account_id = v3.fs_budget.account_id",
        join_type="right",
    )
    await link(
        client,
        "v3.fs_budget",
        "v3.fs_account",
        "v3.fs_budget.account_id = v3.fs_account.account_id",
        role="budget",
    )
    await link(
        client,
        "v3.fs_events",
        "v3.fs_account",
        "v3.fs_events.account_id = v3.fs_account.account_id",
        role="account",
    )
    await link(
        client,
        "v3.fs_events",
        "v3.fs_show",
        "v3.fs_events.show_id = v3.fs_show.show_id",
        role="show",
    )
    await link(
        client,
        "v3.fs_events",
        "v3.date",
        "v3.fs_events.event_date = v3.date.date_id",
        role="measure",
    )
    await link(
        client,
        "v3.fs_account",
        "v3.fs_country",
        "v3.fs_account.country_code = v3.fs_country.country_code",
        role="home",
    )
    await link(
        client,
        "v3.fs_show",
        "v3.fs_country",
        "v3.fs_show.origin_code = v3.fs_country.country_code",
        role="origin",
    )
    await link(
        client,
        "v3.fs_events",
        "v3.fs_alloc_day",
        "v3.fs_events.account_id = v3.fs_alloc_day.account_id",
        join_type="right",
    )
    await link(
        client,
        "v3.fs_alloc_day",
        "v3.fs_account",
        "v3.fs_alloc_day.account_id = v3.fs_account.account_id",
        role="allocation",
    )
    await link(
        client,
        "v3.fs_alloc_day",
        "v3.date",
        "v3.fs_alloc_day.snapshot_date = v3.date.date_id",
        role="snapshot",
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.fs_amount",
            "query": "SELECT SUM(amount) FROM v3.fs_events",
            "mode": "published",
        },
    )
    return client


async def measures_sql(client, filters, dimensions=None):
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.fs_amount"],
            "dimensions": dimensions or [],
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
        CREATE TABLE v3.fs_countries (country_code TEXT, country_name TEXT);
        INSERT INTO v3.fs_countries VALUES
          ('US', 'United States'), ('CA', 'Canada'), ('GB', 'Britain');
        CREATE TABLE v3.fs_accounts (account_id INT, country_code TEXT);
        INSERT INTO v3.fs_accounts VALUES (1, 'US'), (2, 'US'), (3, 'GB'), (4, 'GB');
        CREATE TABLE v3.fs_shows (show_id INT, origin_code TEXT);
        INSERT INTO v3.fs_shows VALUES (10, 'CA'), (20, 'GB');
        CREATE TABLE v3.fs_events
          (account_id INT, show_id INT, amount DOUBLE, event_date INT);
        INSERT INTO v3.fs_events VALUES
          (1, 10, 5, 20260105),
          (1, 20, 7, 20260106),
          (3, 10, 11, 20260107),
          (1, 10, 13, 20260301);
        CREATE TABLE v3.fs_alloc_days (account_id INT, snapshot_date INT);
        INSERT INTO v3.fs_alloc_days VALUES
          (1, 20260110), (2, 20260111), (3, 20260112);
        """,
    )
    return conn.execute(sql.replace("default.v3.", '"default".v3.')).fetchall()


FACT_SIDE_FILTERS = [ACCOUNT_FILTER, ORIGIN_FILTER, ALLOCATION_FILTER, MEASURE_FILTER]

FACT_SIDE_FILTER_SQL = """
WITH v3_fs_account AS (
  SELECT account_id, country_code
  FROM default.v3.fs_accounts
),
v3_fs_alloc_day AS (
  SELECT account_id, snapshot_date
  FROM default.v3.fs_alloc_days
),
v3_fs_country AS (
  SELECT country_code, country_name
  FROM default.v3.fs_countries
),
v3_fs_events AS (
  SELECT account_id, show_id, amount
  FROM default.v3.fs_events
  WHERE event_date BETWEEN 20260101 AND 20260131
),
v3_fs_show AS (
  SELECT show_id, origin_code
  FROM default.v3.fs_shows
),
v3_fs_events_filtered AS (
  SELECT t1.*
  FROM v3_fs_events t1
  LEFT OUTER JOIN v3_fs_account t2 ON t1.account_id = t2.account_id
  LEFT OUTER JOIN v3_fs_country t3 ON t2.country_code = t3.country_code
  LEFT OUTER JOIN v3_fs_show t4 ON t1.show_id = t4.show_id
  LEFT OUTER JOIN v3_fs_country t5 ON t4.origin_code = t5.country_code
  WHERE t3.country_name = 'United States' AND t5.country_name = 'Canada'
)
SELECT t6.snapshot_date,
  SUM(t1.amount) amount_sum_18c89f4b
FROM v3_fs_events_filtered t1
RIGHT OUTER JOIN v3_fs_alloc_day t6 ON t1.account_id = t6.account_id
LEFT OUTER JOIN v3_fs_account t7 ON t6.account_id = t7.account_id
LEFT OUTER JOIN v3_fs_country t8 ON t7.country_code = t8.country_code
WHERE t8.country_name = 'United States'
GROUP BY t6.snapshot_date
"""


@pytest.mark.asyncio
async def test_fact_side_filters_apply_before_the_right_join(spine_client):
    """Account and origin country filters move into a fact-side CTE; the
    allocation country filter stays on the spine side."""
    sql = await measures_sql(
        spine_client,
        FACT_SIDE_FILTERS,
        ["v3.fs_alloc_day.snapshot_date"],
    )
    assert_sql_equal(sql, FACT_SIDE_FILTER_SQL)
    # Spine day 20260110 matches a fact row; 20260111 has none but survives;
    # 20260112 belongs to a GB account and the allocation filter drops it.
    assert sorted(run_in_duckdb(sql)) == [(20260110, 5.0), (20260111, None)]


@pytest.mark.asyncio
async def test_request_order_does_not_change_the_sql(spine_client):
    forward = await measures_sql(
        spine_client,
        FACT_SIDE_FILTERS,
        ["v3.fs_alloc_day.snapshot_date"],
    )
    backward = await measures_sql(
        spine_client,
        FACT_SIDE_FILTERS[::-1],
        ["v3.fs_alloc_day.snapshot_date"],
    )
    assert backward == forward
    assert_sql_equal(backward, FACT_SIDE_FILTER_SQL)


@pytest.mark.asyncio
async def test_fact_side_dimensions_sharing_a_column_name_stay_distinct(
    spine_client,
):
    """Both country names are grouped on, so each gets its own column."""
    sql = await measures_sql(
        spine_client,
        [ACCOUNT_FILTER, ORIGIN_FILTER],
        [
            "v3.fs_alloc_day.snapshot_date",
            "v3.fs_country.country_name[account->home]",
            "v3.fs_country.country_name[show->origin]",
        ],
    )
    assert_sql_equal(
        sql,
        """
        WITH v3_fs_account AS (
          SELECT account_id, country_code
          FROM default.v3.fs_accounts
        ),
        v3_fs_alloc_day AS (
          SELECT account_id, snapshot_date
          FROM default.v3.fs_alloc_days
        ),
        v3_fs_country AS (
          SELECT country_code, country_name
          FROM default.v3.fs_countries
        ),
        v3_fs_events AS (
          SELECT account_id, show_id, amount
          FROM default.v3.fs_events
        ),
        v3_fs_show AS (
          SELECT show_id, origin_code
          FROM default.v3.fs_shows
        ),
        v3_fs_events_filtered AS (
          SELECT t1.*,
            t3.country_name country_name_t3,
            t5.country_name country_name_t5
          FROM v3_fs_events t1
          LEFT OUTER JOIN v3_fs_account t2 ON t1.account_id = t2.account_id
          LEFT OUTER JOIN v3_fs_country t3 ON t2.country_code = t3.country_code
          LEFT OUTER JOIN v3_fs_show t4 ON t1.show_id = t4.show_id
          LEFT OUTER JOIN v3_fs_country t5 ON t4.origin_code = t5.country_code
          WHERE t3.country_name = 'United States' AND t5.country_name = 'Canada'
        )
        SELECT t6.snapshot_date,
          t1.country_name_t3 country_name_home,
          t1.country_name_t5 country_name_origin,
          SUM(t1.amount) amount_sum_18c89f4b
        FROM v3_fs_events_filtered t1
        RIGHT OUTER JOIN v3_fs_alloc_day t6 ON t1.account_id = t6.account_id
        GROUP BY t6.snapshot_date, t1.country_name_t3, t1.country_name_t5
        """,
    )
    assert sorted(run_in_duckdb(sql), key=str) == [
        (20260110, "United States", "Canada", 18.0),
        (20260111, None, None, None),
        (20260112, None, None, None),
    ]


@pytest.mark.asyncio
async def test_fact_side_filters_apply_before_two_right_joins(spine_client):
    """Each side before the last RIGHT join filters in its own query; the
    last side keeps its filters in the final WHERE."""
    sql = await measures_sql(
        spine_client,
        [
            ACCOUNT_FILTER,
            ALLOCATION_FILTER,
            "v3.fs_country.country_name[budget->home] = 'Britain'",
            "v3.fs_budget.cap > 1",
        ],
        ["v3.fs_alloc_day.snapshot_date"],
    )
    assert_sql_equal(
        sql,
        """
        WITH v3_fs_account AS (
          SELECT account_id, country_code
          FROM default.v3.fs_accounts
        ),
        v3_fs_alloc_day AS (
          SELECT account_id, snapshot_date
          FROM default.v3.fs_alloc_days
        ),
        v3_fs_budget AS (
          SELECT account_id, cap
          FROM default.v3.fs_budgets
          WHERE cap > 1
        ),
        v3_fs_country AS (
          SELECT country_code, country_name
          FROM default.v3.fs_countries
        ),
        v3_fs_events AS (
          SELECT account_id, amount
          FROM default.v3.fs_events
        ),
        v3_fs_events_filtered AS (
          SELECT t1.*
          FROM v3_fs_events t1
          LEFT OUTER JOIN v3_fs_account t2 ON t1.account_id = t2.account_id
          LEFT OUTER JOIN v3_fs_country t3 ON t2.country_code = t3.country_code
          WHERE t3.country_name = 'United States'
        ),
        v3_fs_alloc_day_filtered AS (
          SELECT t4.*
          FROM v3_fs_alloc_day t4
          LEFT OUTER JOIN v3_fs_account t6 ON t4.account_id = t6.account_id
          LEFT OUTER JOIN v3_fs_country t7 ON t6.country_code = t7.country_code
          WHERE t7.country_name = 'United States'
        )
        SELECT t4.snapshot_date,
          SUM(t1.amount) amount_sum_18c89f4b
        FROM v3_fs_events_filtered t1
        RIGHT OUTER JOIN v3_fs_alloc_day_filtered t4 ON t1.account_id = t4.account_id
        RIGHT OUTER JOIN v3_fs_budget t5 ON t1.account_id = t5.account_id
        LEFT OUTER JOIN v3_fs_account t8 ON t5.account_id = t8.account_id
        LEFT OUTER JOIN v3_fs_country t9 ON t8.country_code = t9.country_code
        WHERE t5.cap > 1 AND t9.country_name = 'Britain'
        GROUP BY t4.snapshot_date
        """,
    )


@pytest.mark.asyncio
async def test_filters_without_a_spine_are_unchanged(spine_client):
    assert_sql_equal(
        await measures_sql(
            spine_client,
            [ACCOUNT_FILTER, ORIGIN_FILTER, MEASURE_FILTER],
        ),
        """
        WITH v3_fs_account AS (
          SELECT account_id, country_code
          FROM default.v3.fs_accounts
        ),
        v3_fs_country AS (
          SELECT country_code, country_name
          FROM default.v3.fs_countries
        ),
        v3_fs_events AS (
          SELECT account_id, show_id, amount
          FROM default.v3.fs_events
          WHERE event_date BETWEEN 20260101 AND 20260131
        ),
        v3_fs_show AS (
          SELECT show_id, origin_code
          FROM default.v3.fs_shows
        )
        SELECT SUM(t1.amount) amount_sum_18c89f4b
        FROM v3_fs_events t1
        LEFT OUTER JOIN v3_fs_account t2 ON t1.account_id = t2.account_id
        LEFT OUTER JOIN v3_fs_country t3 ON t2.country_code = t3.country_code
        LEFT OUTER JOIN v3_fs_show t4 ON t1.show_id = t4.show_id
        LEFT OUTER JOIN v3_fs_country t5 ON t4.origin_code = t5.country_code
        WHERE t3.country_name = 'United States' AND t5.country_name = 'Canada'
        """,
    )


@pytest.mark.asyncio
async def test_measure_date_filter_skips_a_spine_node_with_another_role(
    spine_client,
):
    """The survival node links the date as `survival`, so the `measure` range
    applies to the events only."""
    assert_sql_equal(
        await measures_sql(
            spine_client,
            [MEASURE_FILTER, "v3.fs_survival.cohort = 'a'", ACCOUNT_FILTER],
            ["v3.fs_alloc_day.snapshot_date"],
        ),
        """
        WITH v3_fs_account AS (
          SELECT account_id, country_code
          FROM default.v3.fs_accounts
        ),
        v3_fs_alloc_day AS (
          SELECT account_id, snapshot_date
          FROM default.v3.fs_alloc_days
        ),
        v3_fs_country AS (
          SELECT country_code, country_name
          FROM default.v3.fs_countries
          WHERE country_name = 'United States'
        ),
        v3_fs_events AS (
          SELECT account_id, amount
          FROM default.v3.fs_events
          WHERE event_date BETWEEN 20260101 AND 20260131
        ),
        v3_fs_survival AS (
          SELECT snapshot_dateint, cohort
          FROM default.v3.fs_survivals
          WHERE cohort = 'a'
        ),
        v3_fs_events_filtered AS (
          SELECT t1.*
          FROM v3_fs_events t1
          LEFT OUTER JOIN v3_fs_account t2 ON t1.account_id = t2.account_id
          LEFT OUTER JOIN v3_fs_country t3 ON t2.country_code = t3.country_code
          WHERE t3.country_name = 'United States'
        )
        SELECT t4.snapshot_date,
          SUM(t1.amount) amount_sum_18c89f4b
        FROM v3_fs_events_filtered t1
        RIGHT OUTER JOIN v3_fs_alloc_day t4 ON t1.account_id = t4.account_id
        LEFT OUTER JOIN v3_fs_survival t5 ON t4.snapshot_date = t5.snapshot_dateint
        WHERE t5.cohort = 'a'
        GROUP BY t4.snapshot_date
        """,
    )
