"""Tests for set-operation (UNION/INTERSECT/EXCEPT) transform bodies.

A transform whose query is a set operation stresses paths that usually
only see a single SELECT: projection inspection, column pruning, filter
pushdown, alias registry.  Filter pushdown refuses set-op CTEs
(``_cte_has_set_operation``) because ``_inject_filter_into_where`` only
mutates the first arm's WHERE and ``_build_cte_projection_map`` only sees
the first arm's projection — partial pushdown would silently produce
wrong data.
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient

from tests.construction.build_v3 import assert_sql_equal


@pytest_asyncio.fixture
async def client_with_union_transform(client_with_build_v3: AsyncClient):
    """Adds a UNION-ALL transform + a metric that reads from it.

    Both arms select from v3.src_orders (same source) so the schemas are
    identical — exercises the set-op code path without a second source.
    """
    resp = await client_with_build_v3.post(
        "/nodes/transform/",
        json={
            "name": "v3.orders_unified",
            "description": "Orders union'd across two arbitrary partitions "
            "to exercise the set-op code path.",
            "query": """
                SELECT order_id, customer_id, order_date, status
                FROM v3.src_orders
                WHERE status = 'completed'
                UNION ALL
                SELECT order_id, customer_id, order_date, status
                FROM v3.src_orders
                WHERE status = 'shipped'
            """,
            "mode": "published",
            "primary_key": ["order_id"],
        },
    )
    assert resp.status_code == 201, resp.json()
    resp = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.unified_order_count",
            "query": "SELECT COUNT(DISTINCT order_id) FROM v3.orders_unified",
            "mode": "published",
        },
    )
    assert resp.status_code == 201, resp.json()
    return client_with_build_v3


class TestSetOperationTransforms:
    """Transforms whose body is a UNION/INTERSECT/EXCEPT."""

    @pytest.mark.xfail(
        strict=True,
        reason="Column pruning applies to only the first arm of a UNION ALL, "
        "emitting arms with mismatched column counts (arm 1 pruned, arm 2 "
        "intact) — invalid SQL.  The correct behavior is to skip pruning "
        "entirely for set-op transform bodies so both arms preserve their "
        "original projection.  Pinned here so the fix flips it to pass.",
    )
    @pytest.mark.asyncio
    async def test_union_all_transform_metric_generates_sql(
        self,
        client_with_union_transform,
    ):
        """A metric built on a UNION-ALL transform generates SQL without
        choking on the set-op body — both arms must land in the CTE
        preserving the same column count.
        """
        response = await client_with_union_transform.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.unified_order_count"],
                "dimensions": ["v3.orders_unified.status"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            response.json()["sql"],
            """
            WITH
            v3_orders_unified AS (
              SELECT order_id, customer_id, order_date, status
              FROM default.v3.orders
              WHERE status = 'completed'
              UNION ALL
              SELECT order_id, customer_id, order_date, status
              FROM default.v3.orders
              WHERE status = 'shipped'
            ),
            orders_unified_0 AS (
              SELECT t1.status, t1.order_id
              FROM v3_orders_unified t1
              GROUP BY t1.status, t1.order_id
            )
            SELECT orders_unified_0.status AS status,
              COUNT(DISTINCT orders_unified_0.order_id) AS unified_order_count
            FROM orders_unified_0
            GROUP BY orders_unified_0.status
            """,
        )

    @pytest.mark.xfail(
        strict=True,
        reason="Same set-op column-pruning bug as above.  The filter-pushdown "
        "refusal for set-op CTEs is correct (filter stays on outer query "
        "only), but the CTE body still has mismatched arm column counts. "
        "Flip to passing when the pruner skips set-op bodies.",
    )
    @pytest.mark.asyncio
    async def test_union_transform_filter_stays_on_outer_query(
        self,
        client_with_union_transform,
    ):
        """Filter on a column of a UNION-ALL transform must NOT be pushed
        into either arm.  The filter lands on the outer query's WHERE; the
        set-op arms keep their original predicates untouched.
        """
        response = await client_with_union_transform.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.unified_order_count"],
                "dimensions": ["v3.orders_unified.status"],
                "filters": ["v3.orders_unified.status = 'completed'"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            response.json()["sql"],
            """
            WITH
            v3_orders_unified AS (
              SELECT order_id, customer_id, order_date, status
              FROM default.v3.orders
              WHERE status = 'completed'
              UNION ALL
              SELECT order_id, customer_id, order_date, status
              FROM default.v3.orders
              WHERE status = 'shipped'
            ),
            orders_unified_0 AS (
              SELECT t1.status, t1.order_id
              FROM v3_orders_unified t1
              WHERE t1.status = 'completed'
              GROUP BY t1.status, t1.order_id
            )
            SELECT orders_unified_0.status AS status,
              COUNT(DISTINCT orders_unified_0.order_id) AS unified_order_count
            FROM orders_unified_0
            WHERE orders_unified_0.status = 'completed'
            GROUP BY orders_unified_0.status
            """,
        )
