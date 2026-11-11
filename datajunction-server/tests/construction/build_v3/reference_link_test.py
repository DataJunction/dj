"""
A column reference to a dimension attribute wins over a dimension link to the
same dimension: the parent's own column is used and the join is skipped.

A reference column holds the attribute value directly, so a row whose key has
no match in the dimension keeps its raw value (the join would give NULL). The
v2 builder also read the local column and never joined in this case.
"""

import duckdb
import pytest
import pytest_asyncio

from tests.construction.build_v3 import assert_sql_equal

PLAN_LABEL = "v3.rl_plan.plan_label"
PLAN_TIER = "v3.rl_plan.plan_tier"
ALT_LABEL = "v3.rl_plan.plan_label[alt]"


async def post(client, path, body=None, params=None):
    response = await client.post(path, json=body, params=params)
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
async def ref_client(client_with_build_v3):
    """Events carry plan_label as a reference to the plan dimension and also
    link to it on plan_key, once with no role and once with role alt."""
    client = client_with_build_v3
    await create_source(
        client,
        "v3.rl_src_events",
        "rl_events",
        [
            ("event_id", "int"),
            ("plan_key", "string"),
            ("plan_label", "string"),
            ("amount", "double"),
        ],
    )
    await create_source(
        client,
        "v3.rl_src_plans",
        "rl_plans",
        [("plan_key", "string"), ("plan_label", "string"), ("plan_tier", "string")],
    )
    await create_source(
        client,
        "v3.rl_src_spine",
        "rl_spine",
        [("plan_key", "string"), ("day", "int")],
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.rl_events",
            "query": (
                "SELECT event_id, plan_key, plan_label, amount FROM v3.rl_src_events"
            ),
            "mode": "published",
            "primary_key": ["event_id"],
        },
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.rl_plan",
            "query": ("SELECT plan_key, plan_label, plan_tier FROM v3.rl_src_plans"),
            "mode": "published",
            "primary_key": ["plan_key"],
        },
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.rl_spine",
            "query": "SELECT plan_key, day FROM v3.rl_src_spine",
            "mode": "published",
            "primary_key": ["plan_key", "day"],
        },
    )
    await post(
        client,
        "/nodes/v3.rl_events/columns/plan_label/link",
        params={"dimension_node": "v3.rl_plan", "dimension_column": "plan_label"},
    )
    for role in (None, "alt"):
        body = {
            "dimension_node": "v3.rl_plan",
            "join_type": "left",
            "join_on": "v3.rl_events.plan_key = v3.rl_plan.plan_key",
        }
        if role:
            body["role"] = role
        await post(client, "/nodes/v3.rl_events/link", body)
    await post(
        client,
        "/nodes/v3.rl_events/link",
        {
            "dimension_node": "v3.rl_spine",
            "join_type": "right",
            "join_on": "v3.rl_events.plan_key = v3.rl_spine.plan_key",
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.rl_amount",
            "query": "SELECT SUM(amount) FROM v3.rl_events",
            "mode": "published",
        },
    )
    for name, attribute, value in (
        ("v3.rl_pro_amount", "plan_label", "Pro"),
        ("v3.rl_tier_amount", "plan_tier", "tier-b"),
    ):
        await post(
            client,
            "/nodes/metric/",
            {
                "name": name,
                "query": (
                    f"SELECT SUM(CASE WHEN v3.rl_plan.{attribute} = '{value}' "
                    "THEN amount ELSE 0 END) FROM v3.rl_events"
                ),
                "mode": "published",
            },
        )
    return client


async def metrics_sql(client, dimensions=None, filters=None, metric="v3.rl_amount"):
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": [metric],
            "dimensions": dimensions or [],
            "filters": filters or [],
            "dialect": "trino",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["sql"]


def run_in_duckdb(sql: str) -> list[tuple]:
    conn = duckdb.connect(":memory:")
    conn.execute("""ATTACH ':memory:' AS "default" """)
    conn.execute("""USE "default" """)
    conn.execute("CREATE SCHEMA v3")
    conn.execute(
        """
        CREATE TABLE v3.rl_plans (plan_key TEXT, plan_label TEXT, plan_tier TEXT);
        INSERT INTO v3.rl_plans VALUES
          ('p1', 'Basic', 'tier-a'), ('p2', 'Pro', 'tier-b');
        CREATE TABLE v3.rl_events
          (event_id INT, plan_key TEXT, plan_label TEXT, amount DOUBLE);
        INSERT INTO v3.rl_events VALUES
          (1, 'p1', 'Basic', 10), (2, 'p2', 'Pro', 20),
          (3, 'p2', 'Pro', 30), (4, 'p9', 'Legacy', 40);
        CREATE TABLE v3.rl_spine (plan_key TEXT, day INT);
        INSERT INTO v3.rl_spine VALUES ('p1', 1), ('p2', 2), ('p3', 3);
        """,
    )
    return conn.execute(sql.replace("default.v3.", '"default".v3.')).fetchall()


GROUP_BY_REFERENCE_SQL = """
WITH v3_rl_events AS (
  SELECT plan_label, amount
  FROM default.v3.rl_events
),
rl_events_0 AS (
  SELECT t1.plan_label, SUM(t1.amount) amount_sum_e21a929c
  FROM v3_rl_events t1
  GROUP BY t1.plan_label
)
SELECT rl_events_0.plan_label AS plan_label,
  SUM(rl_events_0.amount_sum_e21a929c) AS rl_amount
FROM rl_events_0
GROUP BY rl_events_0.plan_label
"""


@pytest.mark.asyncio
async def test_reference_column_skips_the_join(ref_client):
    """Grouping by the reference attribute reads the events column and does
    not join the plan dimension. An event whose plan key has no plan row
    keeps its own label, as it did in v2."""
    sql = await metrics_sql(ref_client, [PLAN_LABEL])
    assert_sql_equal(sql, GROUP_BY_REFERENCE_SQL)
    assert sorted(run_in_duckdb(sql)) == [
        ("Basic", 10.0),
        ("Legacy", 40.0),
        ("Pro", 50.0),
    ]


@pytest.mark.asyncio
async def test_role_qualified_request_still_joins(ref_client):
    """A role-qualified request goes through the dimension link. The event
    whose plan key has no plan row gets a NULL label, the one result that
    differs from the reference column."""
    sql = await metrics_sql(ref_client, [ALT_LABEL])
    assert_sql_equal(
        sql,
        """
        WITH v3_rl_events AS (
          SELECT plan_key, amount
          FROM default.v3.rl_events
        ),
        v3_rl_plan AS (
          SELECT plan_key, plan_label
          FROM default.v3.rl_plans
        ),
        rl_events_0 AS (
          SELECT t2.plan_label plan_label_alt, SUM(t1.amount) amount_sum_e21a929c
          FROM v3_rl_events t1
          LEFT OUTER JOIN v3_rl_plan t2 ON t1.plan_key = t2.plan_key
          GROUP BY t2.plan_label
        )
        SELECT rl_events_0.plan_label_alt AS plan_label_alt,
          SUM(rl_events_0.amount_sum_e21a929c) AS rl_amount
        FROM rl_events_0
        GROUP BY rl_events_0.plan_label_alt
        """,
    )
    assert sorted(run_in_duckdb(sql), key=str) == [
        ("Basic", 10.0),
        ("Pro", 50.0),
        (None, 40.0),
    ]


@pytest.mark.asyncio
async def test_attribute_without_reference_still_joins(ref_client):
    """An attribute no events column references is still read through the
    dimension link."""
    sql = await metrics_sql(ref_client, [PLAN_TIER])
    assert_sql_equal(
        sql,
        """
        WITH v3_rl_events AS (
          SELECT plan_key, amount
          FROM default.v3.rl_events
        ),
        v3_rl_plan AS (
          SELECT plan_key, plan_tier
          FROM default.v3.rl_plans
        ),
        rl_events_0 AS (
          SELECT t2.plan_tier, SUM(t1.amount) amount_sum_e21a929c
          FROM v3_rl_events t1
          LEFT OUTER JOIN v3_rl_plan t2 ON t1.plan_key = t2.plan_key
          GROUP BY t2.plan_tier
        )
        SELECT rl_events_0.plan_tier AS plan_tier,
          SUM(rl_events_0.amount_sum_e21a929c) AS rl_amount
        FROM rl_events_0
        GROUP BY rl_events_0.plan_tier
        """,
    )
    assert sorted(run_in_duckdb(sql), key=str) == [
        ("tier-a", 10.0),
        ("tier-b", 50.0),
        (None, 40.0),
    ]


@pytest.mark.asyncio
async def test_filter_on_reference_attribute(ref_client):
    """Filtering on the reference attribute alone filters the events column
    and does not join the plan dimension. Plan label Pro still sums to 50."""
    sql = await metrics_sql(ref_client, filters=[f"{PLAN_LABEL} = 'Pro'"])
    assert_sql_equal(
        sql,
        """
        WITH v3_rl_events AS (
          SELECT amount
          FROM default.v3.rl_events
          WHERE plan_label = 'Pro'
        ),
        rl_events_0 AS (
          SELECT SUM(t1.amount) amount_sum_e21a929c
          FROM v3_rl_events t1
        )
        SELECT SUM(rl_events_0.amount_sum_e21a929c) AS rl_amount
        FROM rl_events_0
        """,
    )
    assert run_in_duckdb(sql) == [(50.0,)]
    joined = await metrics_sql(ref_client, filters=[f"{ALT_LABEL} = 'Pro'"])
    assert run_in_duckdb(joined) == [(50.0,)]


@pytest.mark.asyncio
async def test_filter_on_label_without_a_plan_row(ref_client):
    """The reference column holds the raw label, so an event whose plan key
    has no plan row matches a filter on its own label (40). The joined
    version finds no plan row and sums nothing."""
    sql = await metrics_sql(ref_client, filters=[f"{PLAN_LABEL} = 'Legacy'"])
    assert_sql_equal(
        sql,
        """
        WITH v3_rl_events AS (
          SELECT amount
          FROM default.v3.rl_events
          WHERE plan_label = 'Legacy'
        ),
        rl_events_0 AS (
          SELECT SUM(t1.amount) amount_sum_e21a929c
          FROM v3_rl_events t1
        )
        SELECT SUM(rl_events_0.amount_sum_e21a929c) AS rl_amount
        FROM rl_events_0
        """,
    )
    assert run_in_duckdb(sql) == [(40.0,)]
    joined = await metrics_sql(ref_client, filters=[f"{ALT_LABEL} = 'Legacy'"])
    assert run_in_duckdb(joined) == [(None,)]


@pytest.mark.asyncio
async def test_filter_and_group_by_the_same_reference(ref_client):
    """The filter and the grouping both read the events column, so only the
    Pro group comes back, with no join."""
    sql = await metrics_sql(
        ref_client,
        [PLAN_LABEL],
        [f"{PLAN_LABEL} = 'Pro'"],
    )
    assert_sql_equal(
        sql,
        """
        WITH v3_rl_events AS (
          SELECT plan_label, amount
          FROM default.v3.rl_events
          WHERE plan_label = 'Pro'
        ),
        rl_events_0 AS (
          SELECT t1.plan_label, SUM(t1.amount) amount_sum_e21a929c
          FROM v3_rl_events t1
          GROUP BY t1.plan_label
        )
        SELECT rl_events_0.plan_label AS plan_label,
          SUM(rl_events_0.amount_sum_e21a929c) AS rl_amount
        FROM rl_events_0
        WHERE rl_events_0.plan_label = 'Pro'
        GROUP BY rl_events_0.plan_label
        """,
    )
    assert run_in_duckdb(sql) == [("Pro", 50.0)]


@pytest.mark.asyncio
async def test_filter_with_a_role_still_joins(ref_client):
    """A role-qualified filter goes through the link, filtering the plan
    rows as well as the events. Pro still sums to 50."""
    sql = await metrics_sql(ref_client, filters=[f"{ALT_LABEL} = 'Pro'"])
    assert_sql_equal(
        sql,
        """
        WITH v3_rl_events AS (
          SELECT plan_key, amount
          FROM default.v3.rl_events
          WHERE plan_label = 'Pro'
        ),
        v3_rl_plan AS (
          SELECT plan_key, plan_label
          FROM default.v3.rl_plans
          WHERE plan_label = 'Pro'
        ),
        rl_events_0 AS (
          SELECT SUM(t1.amount) amount_sum_e21a929c
          FROM v3_rl_events t1
          LEFT OUTER JOIN v3_rl_plan t2 ON t1.plan_key = t2.plan_key
          WHERE t2.plan_label = 'Pro'
        )
        SELECT SUM(rl_events_0.amount_sum_e21a929c) AS rl_amount
        FROM rl_events_0
        """,
    )
    assert run_in_duckdb(sql) == [(50.0,)]


@pytest.mark.asyncio
async def test_filter_on_attribute_without_reference_still_joins(ref_client):
    """A filter on an attribute with no reference joins the plan dimension
    even when the grouping uses the reference column. Tier-b is plan Pro."""
    sql = await metrics_sql(
        ref_client,
        [PLAN_LABEL],
        [f"{PLAN_TIER} = 'tier-b'"],
    )
    assert_sql_equal(
        sql,
        """
        WITH v3_rl_events AS (
          SELECT plan_key, plan_label, amount
          FROM default.v3.rl_events
        ),
        v3_rl_plan AS (
          SELECT plan_key, plan_tier
          FROM default.v3.rl_plans
          WHERE plan_tier = 'tier-b'
        ),
        rl_events_0 AS (
          SELECT t1.plan_label, SUM(t1.amount) amount_sum_e21a929c
          FROM v3_rl_events t1
          LEFT OUTER JOIN v3_rl_plan t2 ON t1.plan_key = t2.plan_key
          WHERE t2.plan_tier = 'tier-b'
          GROUP BY t1.plan_label
        )
        SELECT rl_events_0.plan_label AS plan_label,
          SUM(rl_events_0.amount_sum_e21a929c) AS rl_amount
        FROM rl_events_0
        GROUP BY rl_events_0.plan_label
        """,
    )
    assert run_in_duckdb(sql) == [("Pro", 50.0)]


@pytest.mark.asyncio
async def test_reference_filter_with_a_right_join_spine(ref_client):
    """The reference filter applies to the events before the RIGHT join, so
    spine days with no matching events survive with a NULL total. The joined
    version returns the same rows."""
    sql = await metrics_sql(
        ref_client,
        ["v3.rl_spine.day"],
        [f"{PLAN_LABEL} = 'Pro'"],
    )
    assert_sql_equal(
        sql,
        """
        WITH v3_rl_events AS (
          SELECT plan_key, amount
          FROM default.v3.rl_events
          WHERE plan_label = 'Pro'
        ),
        v3_rl_spine AS (
          SELECT plan_key, day
          FROM default.v3.rl_spine
        ),
        rl_events_0 AS (
          SELECT t2.day, SUM(t1.amount) amount_sum_e21a929c
          FROM v3_rl_events t1
          RIGHT OUTER JOIN v3_rl_spine t2 ON t1.plan_key = t2.plan_key
          GROUP BY t2.day
        )
        SELECT rl_events_0.day AS day,
          SUM(rl_events_0.amount_sum_e21a929c) AS rl_amount
        FROM rl_events_0
        GROUP BY rl_events_0.day
        """,
    )
    expected = [(1, None), (2, 50.0), (3, None)]
    assert sorted(run_in_duckdb(sql), key=str) == expected
    joined = await metrics_sql(
        ref_client,
        ["v3.rl_spine.day"],
        [f"{ALT_LABEL} = 'Pro'"],
    )
    assert sorted(run_in_duckdb(joined), key=str) == expected


PRO_COLUMN = "v3_DOT_rl_plan_DOT_plan_label_amount_sum_25018869"
TIER_COLUMN = "v3_DOT_rl_plan_DOT_plan_tier_amount_sum_273f5b64"


@pytest.mark.asyncio
async def test_metric_on_reference_attribute_grouped_by_it(ref_client):
    """The metric tests the same label the request groups by. The label
    stays in the SELECT and GROUP BY once, read from the events column, and
    the label groups are not merged."""
    sql = await metrics_sql(ref_client, [PLAN_LABEL], metric="v3.rl_pro_amount")
    assert_sql_equal(
        sql,
        f"""
        WITH v3_rl_events AS (
          SELECT plan_label, amount
          FROM default.v3.rl_events
        ),
        rl_events_0 AS (
          SELECT t1.plan_label,
            SUM(CASE WHEN t1.plan_label = 'Pro' THEN t1.amount ELSE 0 END)
              {PRO_COLUMN}
          FROM v3_rl_events t1
          GROUP BY t1.plan_label
        )
        SELECT rl_events_0.plan_label AS plan_label,
          SUM(rl_events_0.{PRO_COLUMN}) AS rl_pro_amount
        FROM rl_events_0
        GROUP BY rl_events_0.plan_label
        """,
    )
    assert sorted(run_in_duckdb(sql)) == [
        ("Basic", 0.0),
        ("Legacy", 0.0),
        ("Pro", 50.0),
    ]


@pytest.mark.asyncio
async def test_metric_on_reference_attribute_not_grouped(ref_client):
    """Without a grouping the metric reads the label through the plan join.
    Only Pro events count, so the total is 50."""
    sql = await metrics_sql(ref_client, metric="v3.rl_pro_amount")
    assert_sql_equal(
        sql,
        f"""
        WITH v3_rl_events AS (
          SELECT plan_key, amount
          FROM default.v3.rl_events
        ),
        v3_rl_plan AS (
          SELECT plan_key, plan_label
          FROM default.v3.rl_plans
        ),
        rl_events_0 AS (
          SELECT
            SUM(CASE WHEN t2.plan_label = 'Pro' THEN t1.amount ELSE 0 END)
              {PRO_COLUMN}
          FROM v3_rl_events t1
          LEFT OUTER JOIN v3_rl_plan t2 ON t1.plan_key = t2.plan_key
        )
        SELECT SUM(rl_events_0.{PRO_COLUMN}) AS rl_pro_amount
        FROM rl_events_0
        """,
    )
    assert run_in_duckdb(sql) == [(50.0,)]


@pytest.mark.asyncio
async def test_metric_on_reference_attribute_grouped_by_role(ref_client):
    """Grouping by the role-qualified label joins the plan dimension once and
    the metric reads the same join. Each label stays its own group."""
    sql = await metrics_sql(ref_client, [ALT_LABEL], metric="v3.rl_pro_amount")
    assert_sql_equal(
        sql,
        f"""
        WITH v3_rl_events AS (
          SELECT plan_key, amount
          FROM default.v3.rl_events
        ),
        v3_rl_plan AS (
          SELECT plan_key, plan_label
          FROM default.v3.rl_plans
        ),
        rl_events_0 AS (
          SELECT t2.plan_label plan_label_alt,
            SUM(CASE WHEN t2.plan_label = 'Pro' THEN t1.amount ELSE 0 END)
              {PRO_COLUMN}
          FROM v3_rl_events t1
          LEFT OUTER JOIN v3_rl_plan t2 ON t1.plan_key = t2.plan_key
          GROUP BY t2.plan_label
        )
        SELECT rl_events_0.plan_label_alt AS plan_label_alt,
          SUM(rl_events_0.{PRO_COLUMN}) AS rl_pro_amount
        FROM rl_events_0
        GROUP BY rl_events_0.plan_label_alt
        """,
    )
    assert sorted(run_in_duckdb(sql), key=str) == [
        ("Basic", 0.0),
        ("Pro", 50.0),
        (None, 0.0),
    ]


@pytest.mark.asyncio
async def test_metric_on_joined_attribute_grouped_by_it(ref_client):
    """With no reference column the metric and the grouping share one join
    and the tier stays in the SELECT and GROUP BY once. The SQL is the same
    as before the reference preference."""
    sql = await metrics_sql(ref_client, [PLAN_TIER], metric="v3.rl_tier_amount")
    assert_sql_equal(
        sql,
        f"""
        WITH v3_rl_events AS (
          SELECT plan_key, amount
          FROM default.v3.rl_events
        ),
        v3_rl_plan AS (
          SELECT plan_key, plan_tier
          FROM default.v3.rl_plans
        ),
        rl_events_0 AS (
          SELECT t2.plan_tier,
            SUM(CASE WHEN t2.plan_tier = 'tier-b' THEN t1.amount ELSE 0 END)
              {TIER_COLUMN}
          FROM v3_rl_events t1
          LEFT OUTER JOIN v3_rl_plan t2 ON t1.plan_key = t2.plan_key
          GROUP BY t2.plan_tier
        )
        SELECT rl_events_0.plan_tier AS plan_tier,
          SUM(rl_events_0.{TIER_COLUMN}) AS rl_tier_amount
        FROM rl_events_0
        GROUP BY rl_events_0.plan_tier
        """,
    )
    assert sorted(run_in_duckdb(sql), key=str) == [
        ("tier-a", 0.0),
        ("tier-b", 50.0),
        (None, 0.0),
    ]
