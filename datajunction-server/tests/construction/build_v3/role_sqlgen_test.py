"""
Role-qualified filters and dimensions return the right rows.

Each test runs the generated SQL on tiny sqlite tables and checks the rows,
covering role paths next to plain roles, grouping by one role while
filtering another, and parent nodes that link one dimension under different
role names.
"""

import sqlite3

import pytest

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group

ENDPOINTS = ["measures", "metrics"]

# Orders: 1 = c1 Canada->US 10, 2 = c2 Canada->Mexico 20,
# 3 = c1 US->Canada 40, 4 = c2 Mexico->US 80. c1 lives in Canada, c2 in the US.
ORDER_TABLES = """
CREATE TABLE locations (
    location_id INTEGER, postal_code TEXT, city TEXT, region TEXT, country TEXT
);
INSERT INTO locations VALUES
    (1, 'p1', 'c1', 'r1', 'Canada'),
    (2, 'p2', 'c2', 'r2', 'United States'),
    (3, 'p3', 'c3', 'r3', 'Mexico');
CREATE TABLE customers (
    customer_id INTEGER, name TEXT, email TEXT, registration_date INTEGER,
    location_id INTEGER
);
INSERT INTO customers VALUES (1, 'c1', 'e1', 1, 1), (2, 'c2', 'e2', 1, 2);
CREATE TABLE orders (
    order_id INTEGER, customer_id INTEGER, order_date INTEGER,
    from_location_id INTEGER, to_location_id INTEGER, status TEXT
);
INSERT INTO orders VALUES
    (1, 1, 1, 1, 2, 's'), (2, 2, 1, 1, 3, 's'),
    (3, 1, 1, 2, 1, 's'), (4, 2, 1, 3, 2, 's');
CREATE TABLE order_items (
    order_id INTEGER, line_number INTEGER, product_id INTEGER, quantity INTEGER,
    unit_price REAL
);
INSERT INTO order_items VALUES
    (1, 1, 1, 1, 10), (2, 1, 1, 1, 20), (3, 1, 1, 1, 40), (4, 1, 1, 1, 80);
"""

# Parent A links locations as role x, parent B as role y.
TWO_PARENT_TABLES = """
CREATE TABLE locations (
    location_id INTEGER, postal_code TEXT, city TEXT, region TEXT, country TEXT
);
INSERT INTO locations VALUES
    (1, 'p1', 'c1', 'r1', 'Canada'),
    (2, 'p2', 'c2', 'r2', 'United States'),
    (3, 'p3', 'c3', 'r3', 'Mexico');
CREATE TABLE dates (
    date_id INTEGER, date_value TEXT, day_of_week INTEGER, week INTEGER,
    month INTEGER, quarter INTEGER, year INTEGER
);
INSERT INTO dates VALUES (1, '2026-01-01', 1, 1, 1, 1, 2026);
CREATE TABLE rp_sales_a (loc_id INTEGER, date_id INTEGER, amount REAL);
INSERT INTO rp_sales_a VALUES (1, 1, 10), (2, 1, 20), (3, 1, 40);
CREATE TABLE rp_sales_b (loc_id INTEGER, date_id INTEGER, amount REAL);
INSERT INTO rp_sales_b VALUES (1, 1, 1), (2, 1, 2), (3, 1, 4);
"""


class KnownWrongResult(Exception):
    """The generated SQL returns a known wrong result."""


def expect_equal(actual, expected):
    """Raise KnownWrongResult when a 200 response gives the wrong result."""
    if actual != expected:
        raise KnownWrongResult(f"actual {actual!r} != expected {expected!r}")


def execute(sql: str, fixture_sql: str) -> list[tuple]:
    """Run SQL against small physical tables, omitting the fixture catalog."""
    with sqlite3.connect(":memory:") as db:
        db.executescript(fixture_sql)
        return db.execute(sql.replace("default.v3.", "")).fetchall()


async def post(client, path, body):
    response = await client.post(path, json=body)
    assert response.status_code in (200, 201), response.json()


async def order_rows(client, endpoint, dimensions, filters) -> list[tuple]:
    """Rows of total_revenue by dimensions, sorted."""
    response = await client.get(
        f"/sql/{endpoint}/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": dimensions,
            "filters": filters,
        },
    )
    assert response.status_code == 200, response.json()
    data = response.json()
    sql = get_first_grain_group(data)["sql"] if endpoint == "measures" else data["sql"]
    return sorted(execute(sql, ORDER_TABLES))


# ---------------------------------------------------------------------------
# Scenario 3: role paths next to plain roles of the same dimension
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_group_by_role_path_filter_plain_role(client_with_build_v3, endpoint):
    """Filtering `[from]` must not touch the `[customer->home]` join."""
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.location.country[customer->home]"],
        ["v3.location.country[from] = 'Canada'"],
    )
    assert rows == [("Canada", 10.0), ("United States", 20.0)]


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_group_by_plain_role_filter_role_path(client_with_build_v3, endpoint):
    """Filtering `[customer->home]` must not touch the `[from]` join."""
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.location.country[from]"],
        ["v3.location.country[customer->home] = 'Canada'"],
    )
    assert rows == [("Canada", 10.0), ("United States", 40.0)]


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_role_path_and_plain_role_filters_with_third_role_group_by(
    client_with_build_v3,
    endpoint,
):
    """Each of two filters on one dimension reaches only its own role."""
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.location.country[to]"],
        [
            "v3.location.country[customer->home] = 'United States'",
            "v3.location.country[from] = 'Canada'",
        ],
    )
    assert rows == [("Mexico", 20.0)]


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_role_path_group_by_with_both_roles_and_path_filter(
    client_with_build_v3,
    endpoint,
):
    """A path and a plain role in the group by, filtered on the path."""
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.location.country[customer->home]", "v3.location.country[from]"],
        ["v3.location.country[customer->home] = 'Canada'"],
    )
    assert rows == [("Canada", "Canada", 10.0), ("Canada", "United States", 40.0)]


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_plain_role_of_customer_with_role_path_filter(
    client_with_build_v3,
    endpoint,
):
    """`[customer]` on the customer dimension next to a `[customer->home]` filter."""
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.customer.name[customer]"],
        ["v3.location.country[customer->home] = 'Canada'"],
    )
    assert rows == [("c1", 50.0)]


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_role_path_group_by_with_plain_role_filter_on_customer(
    client_with_build_v3,
    endpoint,
):
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.location.country[customer->home]"],
        ["v3.customer.name[customer] = 'c2'"],
    )
    assert rows == [("United States", 100.0)]


@pytest.mark.asyncio
async def test_role_path_group_by_with_plain_role_filter_sql(client_with_build_v3):
    """Full SQL: the `[from]` filter sits on its own join, apart from the path."""
    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.location.country[customer->home]"],
            "filters": ["v3.location.country[from] = 'Canada'"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        get_first_grain_group(response.json())["sql"],
        """
        WITH
        v3_customer AS (
          SELECT customer_id, location_id FROM default.v3.customers
        ),
        v3_location AS (
          SELECT location_id, country FROM default.v3.locations
        ),
        v3_order_details AS (
          SELECT o.customer_id, o.from_location_id,
            oi.quantity * oi.unit_price AS line_total
          FROM default.v3.orders o
          JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        )
        SELECT t3.country country_home,
          SUM(t1.line_total) line_total_sum_e1f61696
        FROM v3_order_details t1
        LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
        LEFT OUTER JOIN v3_location t3 ON t2.location_id = t3.location_id
        LEFT OUTER JOIN v3_location t4 ON t1.from_location_id = t4.location_id
        WHERE t4.country = 'Canada'
        GROUP BY t3.country
        """,
    )


# ---------------------------------------------------------------------------
# Scenario 4: group by one role, filter another role of the same dimension
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_group_by_from_filter_to(client_with_build_v3, endpoint):
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.location.country[from]"],
        ["v3.location.country[to] = 'United States'"],
    )
    assert rows == [("Canada", 10.0), ("Mexico", 80.0)]


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_group_by_to_filter_from(client_with_build_v3, endpoint):
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.location.country[to]"],
        ["v3.location.country[from] = 'Canada'"],
    )
    assert rows == [("Mexico", 20.0), ("United States", 10.0)]


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_group_by_from_filter_to_in_list(client_with_build_v3, endpoint):
    """A non-equality filter on the other role."""
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.location.country[from]"],
        ["v3.location.country[to] IN ('Canada', 'Mexico')"],
    )
    assert rows == [("Canada", 20.0), ("United States", 40.0)]


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_group_by_from_filter_from_and_to(client_with_build_v3, endpoint):
    """Filters on the grouped role and on the other role together."""
    rows = await order_rows(
        client_with_build_v3,
        endpoint,
        ["v3.location.country[from]"],
        [
            "v3.location.country[from] IN ('Canada', 'Mexico')",
            "v3.location.country[to] = 'United States'",
        ],
    )
    assert rows == [("Canada", 10.0), ("Mexico", 80.0)]


@pytest.mark.asyncio
async def test_group_by_from_filter_to_sql(client_with_build_v3):
    """Full SQL: the `[to]` filter applies to the `[to]` join only."""
    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.location.country[from]"],
            "filters": ["v3.location.country[to] = 'United States'"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        get_first_grain_group(response.json())["sql"],
        """
        WITH
        v3_location AS (
          SELECT location_id, country FROM default.v3.locations
        ),
        v3_order_details AS (
          SELECT o.from_location_id, o.to_location_id,
            oi.quantity * oi.unit_price AS line_total
          FROM default.v3.orders o
          JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        )
        SELECT t2.country country_from,
          SUM(t1.line_total) line_total_sum_e1f61696
        FROM v3_order_details t1
        LEFT OUTER JOIN v3_location t2 ON t1.from_location_id = t2.location_id
        LEFT OUTER JOIN v3_location t3 ON t1.to_location_id = t3.location_id
        WHERE t3.country = 'United States'
        GROUP BY t2.country
        """,
    )


# ---------------------------------------------------------------------------
# Scenario 5: parent nodes linking one dimension under different role names
# ---------------------------------------------------------------------------


async def build_two_parents(client):
    """`rp_sales_a` links v3.location as role x, `rp_sales_b` as role y.

    Both link v3.date without a role, as the dimension they share.
    """
    for suffix, role in (("a", "x"), ("b", "y")):
        await post(
            client,
            "/nodes/source/",
            {
                "name": f"v3.rp_sales_{suffix}",
                "columns": [
                    {"name": "loc_id", "type": "int"},
                    {"name": "date_id", "type": "int"},
                    {"name": "amount", "type": "double"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": f"rp_sales_{suffix}",
            },
        )
        await post(
            client,
            f"/nodes/v3.rp_sales_{suffix}/link/",
            {
                "dimension_node": "v3.location",
                "join_type": "left",
                "join_on": f"v3.rp_sales_{suffix}.loc_id = v3.location.location_id",
                "role": role,
            },
        )
        await post(
            client,
            f"/nodes/v3.rp_sales_{suffix}/link/",
            {
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": f"v3.rp_sales_{suffix}.date_id = v3.date.date_id",
            },
        )
        await post(
            client,
            "/nodes/metric/",
            {
                "name": f"v3.rp_{suffix}_total",
                "query": f"SELECT SUM(amount) FROM v3.rp_sales_{suffix}",
                "mode": "published",
            },
        )


async def measures_by_parent(client, metrics, dimensions, filters):
    """Response status and (columns, rows) per parent from /sql/measures/v3."""
    response = await client.get(
        "/sql/measures/v3/",
        params={"metrics": metrics, "dimensions": dimensions, "filters": filters},
    )
    assert response.status_code < 500, response.text
    if response.status_code != 200:
        return response.status_code, {}
    groups = {}
    for group in response.json()["grain_groups"]:
        parent = "a" if "rp_sales_a" in group["sql"] else "b"
        groups[parent] = (
            [column["name"] for column in group["columns"]],
            sorted(execute(group["sql"], TWO_PARENT_TABLES)),
        )
    return 200, groups


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "suffix,role,expected",
    [("a", "x", 10.0), ("b", "y", 1.0)],
)
@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_single_parent_filters_on_its_own_role(
    client_with_build_v3,
    endpoint,
    suffix,
    role,
    expected,
):
    await build_two_parents(client_with_build_v3)
    response = await client_with_build_v3.get(
        f"/sql/{endpoint}/v3/",
        params={
            "metrics": [f"v3.rp_{suffix}_total"],
            "dimensions": [],
            "filters": [f"v3.location.country[{role}] = 'Canada'"],
        },
    )
    assert response.status_code == 200, response.json()
    data = response.json()
    sql = get_first_grain_group(data)["sql"] if endpoint == "measures" else data["sql"]
    assert execute(sql, TWO_PARENT_TABLES) == [(expected,)]


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=KnownWrongResult,
    reason="filtering roles x and y together ANDs both on every parent (country = 'Canada' AND country = 'United States')",
)
async def test_measures_each_role_filter_reaches_the_parent_with_that_role(
    client_with_build_v3,
):
    """Filters on x and y: A gets the x filter, B the y filter, or a clean error."""
    await build_two_parents(client_with_build_v3)
    status, groups = await measures_by_parent(
        client_with_build_v3,
        ["v3.rp_a_total", "v3.rp_b_total"],
        [],
        [
            "v3.location.country[x] = 'Canada'",
            "v3.location.country[y] = 'United States'",
        ],
    )
    if status != 200:
        assert 400 <= status < 500, status
        return
    expect_equal(
        {parent: rows for parent, (_, rows) in groups.items()},
        {"a": [(10.0,)], "b": [(2.0,)]},
    )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=KnownWrongResult,
    reason="the x filter is applied to parent B through its role y join",
)
async def test_measures_role_filter_on_parent_without_that_role(
    client_with_build_v3,
):
    """A filter on role x must not filter parent B through its role y."""
    await build_two_parents(client_with_build_v3)
    status, groups = await measures_by_parent(
        client_with_build_v3,
        ["v3.rp_a_total", "v3.rp_b_total"],
        [],
        ["v3.location.country[x] = 'Canada'"],
    )
    if status != 200:
        assert 400 <= status < 500, status
        return
    # Parent B is skipped (left unfiltered) or dropped, never filtered by y.
    expect_equal(
        (groups["a"][1], groups.get("b", (None, [(7.0,)]))[1]),
        ([(10.0,)], [(7.0,)]),
    )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=KnownWrongResult,
    reason="parent B is grouped by its role y join labelled country_x",
)
async def test_measures_group_by_role_on_parent_without_that_role(
    client_with_build_v3,
):
    """Grouping by role x must not group parent B by its role y."""
    await build_two_parents(client_with_build_v3)
    status, groups = await measures_by_parent(
        client_with_build_v3,
        ["v3.rp_a_total", "v3.rp_b_total"],
        ["v3.location.country[x]"],
        [],
    )
    if status != 200:
        assert 400 <= status < 500, status
        return
    expect_equal(
        groups["a"][1],
        [("Canada", 10.0), ("Mexico", 40.0), ("United States", 20.0)],
    )
    if "b" in groups:
        expect_equal("country_x" in groups["b"][0], False)


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=KnownWrongResult,
    reason="filtering roles x and y together ANDs both on every parent (country = 'Canada' AND country = 'United States')",
)
async def test_metrics_role_filters_with_shared_dimension(client_with_build_v3):
    """/sql/metrics/v3: x filter reaches A, y filter reaches B, or a clean error."""
    await build_two_parents(client_with_build_v3)
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.rp_a_total", "v3.rp_b_total"],
            "dimensions": ["v3.date.year"],
            "filters": [
                "v3.location.country[x] = 'Canada'",
                "v3.location.country[y] = 'United States'",
            ],
        },
    )
    if response.status_code != 200:
        assert 400 <= response.status_code < 500, response.text
        return
    expect_equal(
        execute(response.json()["sql"], TWO_PARENT_TABLES),
        [(2026, 10.0, 2.0)],
    )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=KnownWrongResult,
    reason="the x filter is applied to parent B through its role y join",
)
async def test_metrics_role_filter_on_parent_without_that_role(client_with_build_v3):
    """/sql/metrics/v3: an x filter must not filter parent B through role y."""
    await build_two_parents(client_with_build_v3)
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.rp_a_total", "v3.rp_b_total"],
            "dimensions": ["v3.date.year"],
            "filters": ["v3.location.country[x] = 'Canada'"],
        },
    )
    if response.status_code != 200:
        assert 400 <= response.status_code < 500, response.text
        return
    expect_equal(
        execute(response.json()["sql"], TWO_PARENT_TABLES),
        [(2026, 10.0, 7.0)],
    )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=KnownWrongResult,
    reason="parent B is grouped by its role y join labelled country_x",
)
async def test_metrics_group_by_role_on_parent_without_that_role(
    client_with_build_v3,
):
    """/sql/metrics/v3: grouping by role x must not group parent B by role y."""
    await build_two_parents(client_with_build_v3)
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.rp_a_total", "v3.rp_b_total"],
            "dimensions": ["v3.location.country[x]"],
        },
    )
    assert response.status_code < 500, response.text
    expect_equal(response.status_code // 100, 4)
