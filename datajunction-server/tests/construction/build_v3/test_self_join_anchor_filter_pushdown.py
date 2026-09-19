"""
Filter pushdown into a dimension whose own query self-joins one table.

A month-to-date bridge is built by self-joining a date dimension: the ``a`` side
supplies the anchor date, the ``b`` side enumerates every date from the start of
that month through the anchor. Both output columns therefore trace back to the
same physical column on the same physical table, distinguished only by alias.

Filtering on the anchor column should constrain the ``a`` side alone. The period
side has to stay free, because it is what gives each anchor its window.
"""

import pytest
from httpx import AsyncClient

from tests.construction.build_v3 import assert_sql_equal


async def _setup_mtd_bridge(client: AsyncClient) -> None:
    """Create a date table, orders table, self-joined MTD bridge, transform, metric."""
    response = await client.post(
        "/nodes/source/",
        json={
            "name": "default.date_table",
            "display_name": "Date Table",
            "catalog": "default",
            "schema_": "public",
            "table": "dates",
            "columns": [
                {"name": "date_int", "type": "int"},
                {"name": "first_date_of_month", "type": "int"},
            ],
        },
    )
    assert response.status_code in (200, 201), response.text

    response = await client.post(
        "/nodes/source/",
        json={
            "name": "default.orders_table",
            "display_name": "Orders Table",
            "catalog": "default",
            "schema_": "public",
            "table": "orders",
            "columns": [
                {"name": "order_id", "type": "int"},
                {"name": "order_date", "type": "int"},
                {"name": "discount", "type": "double"},
            ],
        },
    )
    assert response.status_code in (200, 201), response.text

    # The self-join: `a` anchors, `b` enumerates the month-to-date window.
    response = await client.post(
        "/nodes/dimension/",
        json={
            "name": "default.date_mtd_bridge",
            "display_name": "Date MTD Bridge",
            "mode": "published",
            "primary_key": ["anchor_date", "period_date"],
            "query": (
                "SELECT a.date_int AS anchor_date, b.date_int AS period_date "
                "FROM default.date_table a "
                "INNER JOIN default.date_table b "
                "ON b.date_int BETWEEN a.first_date_of_month AND a.date_int"
            ),
        },
    )
    assert response.status_code in (200, 201), response.text

    # Orders join the bridge on the PERIOD side, and expose the ANCHOR side, so
    # each order appears once per window that contains it.
    response = await client.post(
        "/nodes/transform/",
        json={
            "name": "default.orders_mtd",
            "display_name": "Orders MTD",
            "mode": "published",
            "query": (
                "SELECT o.order_id, o.order_date, b.anchor_date, o.discount "
                "FROM default.orders_table o "
                "INNER JOIN default.date_mtd_bridge b "
                "ON o.order_date = b.period_date"
            ),
        },
    )
    assert response.status_code in (200, 201), response.text

    response = await client.post(
        "/nodes/metric/",
        json={
            "name": "default.avg_discount_mtd",
            "display_name": "Avg Discount MTD",
            "mode": "published",
            "query": (
                "SELECT SUM(discount) * 1.0 / NULLIF(COUNT(*), 0) "
                "FROM default.orders_mtd"
            ),
        },
    )
    assert response.status_code in (200, 201), response.text


@pytest.mark.asyncio
async def test_anchor_filter_does_not_leak_onto_period_side(
    client_with_service_setup: AsyncClient,
) -> None:
    """A filter on the anchor column must not also constrain the period column.

    Both columns resolve to `date_int` on `default.date_table`, so a pushdown that
    matches on bare column name and then replicates itself across every alias of
    that table lands the predicate on the period side too. That collapses each
    month-to-date window to just the filtered dates.
    """
    await _setup_mtd_bridge(client_with_service_setup)

    response = await client_with_service_setup.get(
        "/sql/metrics/v3",
        params={
            "metrics": ["default.avg_discount_mtd"],
            "dimensions": ["default.orders_mtd.anchor_date"],
            "filters": ["default.orders_mtd.anchor_date IN (20180208, 20180210)"],
        },
    )
    assert response.status_code == 200, response.text
    sql = response.json()["sql"]

    assert_sql_equal(
        sql,
        """
        WITH default_date_mtd_bridge AS (
          SELECT a.date_int AS anchor_date, b.date_int AS period_date
          FROM default.public.dates a
          INNER JOIN default.public.dates b
            ON b.date_int BETWEEN a.first_date_of_month AND a.date_int
          WHERE a.date_int IN (20180208, 20180210)
        ),
        default_orders_mtd AS (
          SELECT b.anchor_date, o.discount
          FROM default.public.orders o
          INNER JOIN default_date_mtd_bridge b ON o.order_date = b.period_date
          WHERE b.anchor_date IN (20180208, 20180210)
        ),
        orders_mtd_0 AS (
          SELECT
            t1.anchor_date,
            SUM(t1.discount) AS discount_sum_HASH,
            COUNT(*) AS count_HASH
          FROM default_orders_mtd t1
          GROUP BY t1.anchor_date
        )
        SELECT
          orders_mtd_0.anchor_date AS anchor_date,
          SUM(orders_mtd_0.discount_sum_HASH) * 1.0
            / NULLIF(SUM(orders_mtd_0.count_HASH), 0) AS avg_discount_mtd
        FROM orders_mtd_0
        WHERE orders_mtd_0.anchor_date IN (20180208, 20180210)
        GROUP BY orders_mtd_0.anchor_date
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
async def test_two_table_bridge_is_unaffected(
    client_with_service_setup: AsyncClient,
) -> None:
    """Control: the same query shape over two DIFFERENT tables filters correctly.

    Identical bridge, identical filter, identical column names -- the only change
    is that the two sides read different physical tables. The anchor predicate then
    lands on the anchor side alone, which isolates "both aliases resolve to the same
    physical table" as the trigger rather than the self-join shape itself.
    """
    for table, name in (
        ("dates", "default.date_table"),
        ("dates_b", "default.date_table_b"),
    ):
        response = await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": name,
                "display_name": name,
                "catalog": "default",
                "schema_": "public",
                "table": table,
                "columns": [
                    {"name": "date_int", "type": "int"},
                    {"name": "first_date_of_month", "type": "int"},
                ],
            },
        )
        assert response.status_code in (200, 201), response.text

    response = await client_with_service_setup.post(
        "/nodes/source/",
        json={
            "name": "default.orders_table",
            "display_name": "Orders Table",
            "catalog": "default",
            "schema_": "public",
            "table": "orders",
            "columns": [
                {"name": "order_id", "type": "int"},
                {"name": "order_date", "type": "int"},
                {"name": "discount", "type": "double"},
            ],
        },
    )
    assert response.status_code in (200, 201), response.text

    response = await client_with_service_setup.post(
        "/nodes/dimension/",
        json={
            "name": "default.date_mtd_bridge_two_table",
            "display_name": "Date MTD Bridge (two tables)",
            "mode": "published",
            "primary_key": ["anchor_date", "period_date"],
            "query": (
                "SELECT a.date_int AS anchor_date, b.date_int AS period_date "
                "FROM default.date_table a "
                "INNER JOIN default.date_table_b b "
                "ON b.date_int BETWEEN a.first_date_of_month AND a.date_int"
            ),
        },
    )
    assert response.status_code in (200, 201), response.text

    response = await client_with_service_setup.post(
        "/nodes/transform/",
        json={
            "name": "default.orders_mtd_two_table",
            "display_name": "Orders MTD (two tables)",
            "mode": "published",
            "query": (
                "SELECT o.order_id, o.order_date, b.anchor_date, o.discount "
                "FROM default.orders_table o "
                "INNER JOIN default.date_mtd_bridge_two_table b "
                "ON o.order_date = b.period_date"
            ),
        },
    )
    assert response.status_code in (200, 201), response.text

    response = await client_with_service_setup.post(
        "/nodes/metric/",
        json={
            "name": "default.avg_discount_mtd_two_table",
            "display_name": "Avg Discount MTD (two tables)",
            "mode": "published",
            "query": (
                "SELECT SUM(discount) * 1.0 / NULLIF(COUNT(*), 0) "
                "FROM default.orders_mtd_two_table"
            ),
        },
    )
    assert response.status_code in (200, 201), response.text

    response = await client_with_service_setup.get(
        "/sql/metrics/v3",
        params={
            "metrics": ["default.avg_discount_mtd_two_table"],
            "dimensions": ["default.orders_mtd_two_table.anchor_date"],
            "filters": [
                "default.orders_mtd_two_table.anchor_date IN (20180208, 20180210)",
            ],
        },
    )
    assert response.status_code == 200, response.text
    sql = response.json()["sql"]

    assert_sql_equal(
        sql,
        """
        WITH default_date_mtd_bridge_two_table AS (
          SELECT a.date_int AS anchor_date, b.date_int AS period_date
          FROM default.public.dates a
          INNER JOIN default.public.dates_b b
            ON b.date_int BETWEEN a.first_date_of_month AND a.date_int
          WHERE a.date_int IN (20180208, 20180210)
        ),
        default_orders_mtd_two_table AS (
          SELECT b.anchor_date, o.discount
          FROM default.public.orders o
          INNER JOIN default_date_mtd_bridge_two_table b ON o.order_date = b.period_date
          WHERE b.anchor_date IN (20180208, 20180210)
        ),
        orders_mtd_two_table_0 AS (
          SELECT
            t1.anchor_date,
            SUM(t1.discount) AS discount_sum_HASH,
            COUNT(*) AS count_HASH
          FROM default_orders_mtd_two_table t1
          GROUP BY t1.anchor_date
        )
        SELECT
          orders_mtd_two_table_0.anchor_date AS anchor_date,
          SUM(orders_mtd_two_table_0.discount_sum_HASH) * 1.0
            / NULLIF(SUM(orders_mtd_two_table_0.count_HASH), 0) AS avg_discount_mtd_two_table
        FROM orders_mtd_two_table_0
        WHERE orders_mtd_two_table_0.anchor_date IN (20180208, 20180210)
        GROUP BY orders_mtd_two_table_0.anchor_date
        """,
        normalize_aliases=True,
    )
