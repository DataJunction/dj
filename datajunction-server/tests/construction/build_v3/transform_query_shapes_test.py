"""Tests for unusual transform-query shapes in build_v3 SQL generation.

Covers shapes beyond the standard SELECT-FROM-JOIN mold: derived tables
in FROM, window-function projections.  These paths go through the CTE
builder and projection-map logic and used to be untested.

Self-joins are exercised in ``test_self_join_sql_generation.py``;
set-operation bodies live in ``set_operations_test.py``.
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient


@pytest_asyncio.fixture
async def client_with_edge_shapes(client_with_build_v3: AsyncClient):
    """Adds transforms with unusual query shapes + metrics on top of them."""
    r1 = await client_with_build_v3.post(
        "/nodes/transform/",
        json={
            "name": "v3.orders_via_derived_table",
            "description": "FROM (SELECT ... FROM src) derived",
            "query": """
                SELECT inner_q.order_id, inner_q.customer_id, inner_q.order_date
                FROM (
                    SELECT order_id, customer_id, order_date, status
                    FROM v3.src_orders
                    WHERE status IS NOT NULL
                ) inner_q
            """,
            "mode": "published",
            "primary_key": ["order_id"],
        },
    )
    assert r1.status_code == 201, r1.json()

    r2 = await client_with_build_v3.post(
        "/nodes/transform/",
        json={
            "name": "v3.orders_ranked",
            "description": "Projects ROW_NUMBER() OVER (...) AS rn",
            "query": """
                SELECT
                    order_id,
                    customer_id,
                    order_date,
                    status,
                    ROW_NUMBER() OVER (
                        PARTITION BY customer_id ORDER BY order_date
                    ) AS rn
                FROM v3.src_orders
            """,
            "mode": "published",
            "primary_key": ["order_id"],
        },
    )
    assert r2.status_code == 201, r2.json()

    r3 = await client_with_build_v3.post(
        "/nodes/transform/",
        json={
            "name": "v3.orders_self_joined",
            "description": "Self-join with 2+ conditions in ON",
            "query": """
                SELECT
                    a.order_id,
                    a.customer_id,
                    a.order_date AS curr_date,
                    b.order_date AS prev_date
                FROM v3.src_orders a
                JOIN v3.src_orders b
                  ON a.customer_id = b.customer_id
                 AND a.order_date > b.order_date
            """,
            "mode": "published",
            "primary_key": ["order_id"],
        },
    )
    assert r3.status_code == 201, r3.json()

    for name, query in [
        (
            "v3.derived_table_count",
            "SELECT COUNT(*) FROM v3.orders_via_derived_table",
        ),
        ("v3.ranked_count", "SELECT COUNT(*) FROM v3.orders_ranked"),
        ("v3.self_join_count", "SELECT COUNT(*) FROM v3.orders_self_joined"),
    ]:
        r = await client_with_build_v3.post(
            "/nodes/metric/",
            json={"name": name, "query": query, "mode": "published"},
        )
        assert r.status_code == 201, r.json()
    return client_with_build_v3


class TestTransformQueryShapes:
    """Transform queries that don't fit the simple SELECT-FROM-JOIN mold."""

    @pytest.mark.asyncio
    async def test_transform_with_derived_table_in_from(
        self, client_with_edge_shapes,
    ):
        """Transform whose FROM is ``(SELECT ... FROM src) inner_q``.
        Column resolution must still find the underlying columns.
        """
        response = await client_with_edge_shapes.get(
            "/sql/metrics/v3/",
            params={"metrics": ["v3.derived_table_count"]},
        )
        assert response.status_code == 200, response.json()
        sql = response.json()["sql"]
        assert "inner_q" in sql
        assert "v3.src_orders" in sql or "default.v3.orders" in sql

    @pytest.mark.asyncio
    async def test_transform_with_window_function_projection(
        self, client_with_edge_shapes,
    ):
        """Transform projects ``ROW_NUMBER() OVER (...) AS rn``.  Metric
        aggregation on this transform must still generate valid SQL — the
        window expression survives into the CTE body.
        """
        response = await client_with_edge_shapes.get(
            "/sql/metrics/v3/",
            params={"metrics": ["v3.ranked_count"]},
        )
        assert response.status_code == 200, response.json()
        sql = response.json()["sql"]
        assert "ROW_NUMBER()" in sql.upper() or "ROW_NUMBER (" in sql.upper()

    @pytest.mark.asyncio
    async def test_transform_with_self_join_multi_condition_on(
        self, client_with_edge_shapes,
    ):
        """Self-join in the transform QUERY body with ``ON a.x = b.x
        AND a.y > b.y``.  Both conditions must be preserved in the CTE
        body; the multi-ref filter-pushdown fix would've broken if it
        regressed onto the ON clause.
        """
        response = await client_with_edge_shapes.get(
            "/sql/metrics/v3/",
            params={"metrics": ["v3.self_join_count"]},
        )
        assert response.status_code == 200, response.json()
        sql = response.json()["sql"]
        assert "a.customer_id = b.customer_id" in sql
        assert "a.order_date > b.order_date" in sql
