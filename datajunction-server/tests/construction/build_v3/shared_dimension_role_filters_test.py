"""
Filters on different roles of one dimension each stay on their own join.

A fact linking the same dimension twice (`from` and `to` locations) reads one
dimension CTE through two joins. Pushing both role filters into that CTE
required every row to match both (`country = 'Canada' AND country =
'United States'`), and the outer filter reused one table alias for both roles.
"""

import sqlite3

import pytest

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group

FROM_FILTER = "v3.location.country[from] = 'Canada'"
TO_FILTER = "v3.location.country[to] = 'United States'"


def run_in_sqlite(sql: str) -> list[tuple]:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE locations (location_id INT, country TEXT);
        INSERT INTO locations VALUES (1, 'Canada'), (2, 'United States'), (3, 'Mexico');
        CREATE TABLE orders (order_id INT, from_location_id INT, to_location_id INT);
        INSERT INTO orders VALUES (1, 1, 2), (2, 2, 1), (3, 1, 3), (4, 1, 2);
        CREATE TABLE order_items (order_id INT, quantity INT, unit_price REAL);
        INSERT INTO order_items VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 2, 5);
        CREATE TABLE places (place_id INT, country TEXT);
        INSERT INTO places VALUES (1, 'Canada'), (2, 'United States'), (3, 'Mexico');
        CREATE TABLE legs (from_loc INT, to_loc INT, carrier_id INT, amount REAL);
        INSERT INTO legs VALUES (1, 2, 1, 10), (2, 1, 1, 20), (1, 3, 2, 30), (3, 2, 2, 5);
        CREATE TABLE carriers (carrier_id INT, from_loc INT);
        INSERT INTO carriers VALUES (1, 7), (2, 8);
        CREATE TABLE trips (from_place_id INT, to_place_id INT, amount REAL);
        INSERT INTO trips VALUES (1, 2, 10), (2, 1, 20), (1, 3, 30), (1, 2, 5);
        """,
    )
    return conn.execute(
        sql.replace("default.v3.", "").replace("analytics.dim.", ""),
    ).fetchall()


@pytest.mark.asyncio
async def test_two_roles_of_one_dimension_keep_their_own_filter_and_alias(
    client_with_build_v3,
):
    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.location.country[from]", "v3.location.country[to]"],
            "filters": [FROM_FILTER, TO_FILTER],
        },
    )
    assert response.status_code == 200, response.json()
    sql = get_first_grain_group(response.json())["sql"]
    assert_sql_equal(
        sql,
        """
        WITH
        v3_location AS (
          SELECT location_id, country
          FROM default.v3.locations
        ),
        v3_order_details AS (
          SELECT o.from_location_id,
            o.to_location_id,
            oi.quantity * oi.unit_price AS line_total
          FROM default.v3.orders o
          JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        )
        SELECT t2.country country_from,
          t3.country country_to,
          SUM(t1.line_total) line_total_sum_e1f61696
        FROM v3_order_details t1
        LEFT OUTER JOIN v3_location t2 ON t1.from_location_id = t2.location_id
        LEFT OUTER JOIN v3_location t3 ON t1.to_location_id = t3.location_id
        WHERE t2.country = 'Canada' AND t3.country = 'United States'
        GROUP BY t2.country, t3.country
        """,
    )
    assert run_in_sqlite(sql) == [("Canada", "United States", 20.0)]


@pytest.mark.asyncio
async def test_one_role_filter_is_not_pushed_into_a_cte_joined_under_two_roles(
    client_with_build_v3,
):
    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.location.country[from]", "v3.location.country[to]"],
            "filters": [FROM_FILTER],
        },
    )
    assert response.status_code == 200, response.json()
    sql = get_first_grain_group(response.json())["sql"]
    assert_sql_equal(
        sql,
        """
        WITH
        v3_location AS (
          SELECT location_id, country
          FROM default.v3.locations
        ),
        v3_order_details AS (
          SELECT o.from_location_id,
            o.to_location_id,
            oi.quantity * oi.unit_price AS line_total
          FROM default.v3.orders o
          JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        )
        SELECT t2.country country_from,
          t3.country country_to,
          SUM(t1.line_total) line_total_sum_e1f61696
        FROM v3_order_details t1
        LEFT OUTER JOIN v3_location t2 ON t1.from_location_id = t2.location_id
        LEFT OUTER JOIN v3_location t3 ON t1.to_location_id = t3.location_id
        WHERE t2.country = 'Canada'
        GROUP BY t2.country, t3.country
        """,
    )
    assert sorted(run_in_sqlite(sql)) == [
        ("Canada", "Mexico", 30.0),
        ("Canada", "United States", 20.0),
    ]


@pytest.mark.asyncio
async def test_metrics_sql_keeps_each_role_filter_on_its_join(client_with_build_v3):
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.location.country[from]"],
            "filters": [FROM_FILTER, TO_FILTER],
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    assert_sql_equal(
        sql,
        """
        WITH
        v3_location AS (
          SELECT location_id, country
          FROM default.v3.locations
        ),
        v3_order_details AS (
          SELECT o.from_location_id,
            o.to_location_id,
            oi.quantity * oi.unit_price AS line_total
          FROM default.v3.orders o
          JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        order_details_0 AS (
          SELECT t2.country country_from,
            SUM(t1.line_total) line_total_sum_e1f61696
          FROM v3_order_details t1
          LEFT OUTER JOIN v3_location t2 ON t1.from_location_id = t2.location_id
          LEFT OUTER JOIN v3_location t3 ON t1.to_location_id = t3.location_id
          WHERE t2.country = 'Canada' AND t3.country = 'United States'
          GROUP BY t2.country
        )
        SELECT order_details_0.country_from AS country_from,
          SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
        FROM order_details_0
        GROUP BY order_details_0.country_from
        """,
    )


async def build_trips_with_place_roles(client):
    """`v3.place` reads another transform and is linked twice, as `from` and `to`."""
    for name, columns, table in [
        (
            "v3.src_trips",
            [
                {"name": "from_place_id", "type": "int"},
                {"name": "to_place_id", "type": "int"},
                {"name": "amount", "type": "double"},
            ],
            "trips",
        ),
        (
            "v3.src_places",
            [
                {"name": "place_id", "type": "int"},
                {"name": "country", "type": "string"},
            ],
            "places",
        ),
    ]:
        resp = await client.post(
            "/nodes/source/",
            json={
                "name": name,
                "columns": columns,
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": table,
            },
        )
        assert resp.status_code in (200, 201), resp.json()
    for kind, name, query, extra in [
        (
            "transform",
            "v3.trips",
            "SELECT from_place_id, to_place_id, amount FROM v3.src_trips",
            {},
        ),
        (
            "transform",
            "v3.place_base",
            "SELECT place_id, country FROM v3.src_places",
            {},
        ),
        (
            "dimension",
            "v3.place",
            "SELECT place_id, country FROM v3.place_base",
            {"primary_key": ["place_id"]},
        ),
    ]:
        resp = await client.post(
            f"/nodes/{kind}/",
            json={"name": name, "query": query, "mode": "published", **extra},
        )
        assert resp.status_code == 201, resp.json()
    for role, column in [("from", "from_place_id"), ("to", "to_place_id")]:
        resp = await client.post(
            "/nodes/v3.trips/link/",
            json={
                "dimension_node": "v3.place",
                "join_type": "left",
                "join_on": f"v3.trips.{column} = v3.place.place_id",
                "role": role,
            },
        )
        assert resp.status_code in (200, 201), resp.json()
    resp = await client.post(
        "/nodes/metric/",
        json={
            "name": "v3.trip_amount",
            "query": "SELECT SUM(amount) FROM v3.trips",
            "mode": "published",
        },
    )
    assert resp.status_code == 201, resp.json()


@pytest.mark.asyncio
async def test_role_filters_are_not_pushed_into_transforms_below_a_shared_dimension(
    client_with_build_v3,
):
    await build_trips_with_place_roles(client_with_build_v3)
    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.trip_amount"],
            "dimensions": ["v3.place.country[from]", "v3.place.country[to]"],
            "filters": [
                "v3.place.country[from] = 'Canada'",
                "v3.place.country[to] = 'United States'",
            ],
        },
    )
    assert response.status_code == 200, response.json()
    sql = get_first_grain_group(response.json())["sql"]
    assert_sql_equal(
        sql,
        """
        WITH
        v3_place_base AS (
          SELECT place_id, country
          FROM default.v3.places
        ),
        v3_trips AS (
          SELECT from_place_id, to_place_id, amount
          FROM default.v3.trips
        ),
        v3_place AS (
          SELECT place_id, country
          FROM v3_place_base
        )
        SELECT t2.country country_from,
          t3.country country_to,
          SUM(t1.amount) amount_sum_HASH
        FROM v3_trips t1
        LEFT OUTER JOIN v3_place t2 ON t1.from_place_id = t2.place_id
        LEFT OUTER JOIN v3_place t3 ON t1.to_place_id = t3.place_id
        WHERE t2.country = 'Canada' AND t3.country = 'United States'
        GROUP BY t2.country, t3.country
        """,
        normalize_aliases=True,
    )
    assert run_in_sqlite(sql) == [("Canada", "United States", 15.0)]


async def build_legs_with_materialized_parent(client):
    """`v3.legs` links `v3.place_dim` as `from` then `to`, and joins `v3.carrier`,
    which has a column named like the first link's key."""
    for name, columns, table in [
        (
            "v3.src_legs",
            [
                {"name": "from_loc", "type": "int"},
                {"name": "to_loc", "type": "int"},
                {"name": "carrier_id", "type": "int"},
                {"name": "amount", "type": "double"},
            ],
            "legs",
        ),
        (
            "v3.src_locs",
            [{"name": "loc_id", "type": "int"}, {"name": "label", "type": "string"}],
            "locs",
        ),
        (
            "v3.src_carriers",
            [
                {"name": "carrier_id", "type": "int"},
                {"name": "from_loc", "type": "int"},
            ],
            "carriers",
        ),
    ]:
        resp = await client.post(
            "/nodes/source/",
            json={
                "name": name,
                "columns": columns,
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": table,
            },
        )
        assert resp.status_code in (200, 201), resp.json()
    for kind, name, query, extra in [
        (
            "transform",
            "v3.legs",
            "SELECT from_loc, to_loc, carrier_id, amount FROM v3.src_legs",
            {},
        ),
        (
            "dimension",
            "v3.place_dim",
            "SELECT loc_id, label FROM v3.src_locs",
            {"primary_key": ["loc_id"]},
        ),
        (
            "dimension",
            "v3.carrier",
            "SELECT carrier_id, from_loc FROM v3.src_carriers",
            {"primary_key": ["carrier_id"]},
        ),
    ]:
        resp = await client.post(
            f"/nodes/{kind}/",
            json={"name": name, "query": query, "mode": "published", **extra},
        )
        assert resp.status_code == 201, resp.json()
    for dimension, join_on, role in [
        ("v3.place_dim", "v3.legs.from_loc = v3.place_dim.loc_id", "from"),
        ("v3.place_dim", "v3.legs.to_loc = v3.place_dim.loc_id", "to"),
        ("v3.carrier", "v3.legs.carrier_id = v3.carrier.carrier_id", None),
    ]:
        resp = await client.post(
            "/nodes/v3.legs/link/",
            json={
                "dimension_node": dimension,
                "join_type": "left",
                "join_on": join_on,
                **({"role": role} if role else {}),
            },
        )
        assert resp.status_code in (200, 201), resp.json()
    resp = await client.post(
        "/nodes/metric/",
        json={
            "name": "v3.leg_amount",
            "query": "SELECT SUM(amount) FROM v3.legs",
            "mode": "published",
        },
    )
    assert resp.status_code == 201, resp.json()
    resp = await client.post(
        "/data/v3.legs/availability/",
        json={
            "catalog": "analytics",
            "schema_": "dim",
            "table": "legs",
            "valid_through_ts": 9999999999,
        },
    )
    assert resp.status_code == 200, resp.json()


@pytest.mark.asyncio
async def test_outer_filter_keeps_the_column_of_its_own_role(client_with_build_v3):
    await build_legs_with_materialized_parent(client_with_build_v3)
    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.leg_amount"],
            "dimensions": ["v3.place_dim.loc_id[to]", "v3.carrier.from_loc"],
            "filters": ["v3.place_dim.loc_id[to] = 2"],
        },
    )
    assert response.status_code == 200, response.json()
    sql = get_first_grain_group(response.json())["sql"]
    assert_sql_equal(
        sql,
        """
        WITH
        v3_carrier AS (
          SELECT carrier_id, from_loc
          FROM default.v3.carriers
        )
        SELECT t1.to_loc loc_id_to,
          t2.from_loc,
          SUM(t1.amount) amount_sum_HASH
        FROM analytics.dim.legs t1
        LEFT OUTER JOIN v3_carrier t2 ON t1.carrier_id = t2.carrier_id
        WHERE t1.to_loc = 2
        GROUP BY t1.to_loc, t2.from_loc
        """,
        normalize_aliases=True,
    )
    assert sorted(run_in_sqlite(sql)) == [(2, 7, 10.0), (2, 8, 5.0)]
