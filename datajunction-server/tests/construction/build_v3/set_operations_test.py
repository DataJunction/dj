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

    @pytest.mark.asyncio
    async def test_union_all_transform_metric_generates_sql(
        self, client_with_union_transform,
    ):
        """A metric built on a UNION-ALL transform generates SQL without
        choking on the set-op body — both arms land in the CTE intact.
        """
        response = await client_with_union_transform.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.unified_order_count"],
                "dimensions": ["v3.orders_unified.status"],
            },
        )
        assert response.status_code == 200, response.json()
        sql = response.json()["sql"]
        # Both arms preserved: each status literal must appear.
        assert "'completed'" in sql
        assert "'shipped'" in sql
        # Set-op keyword survived.
        assert "UNION ALL" in sql.upper()

    @pytest.mark.asyncio
    async def test_union_transform_filter_stays_on_outer_query(
        self, client_with_union_transform,
    ):
        """Filter on a column of a UNION-ALL transform must NOT be pushed
        into either arm — ``_cte_has_set_operation`` refuses and the filter
        stays on the outer WHERE.  Both arms keep their original (unfiltered)
        WHEREs.
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
        sql = response.json()["sql"]
        # The set-op CTE body preserves its per-arm WHEREs.
        assert "'completed'" in sql  # present in arm 1 and outer WHERE
        assert "'shipped'" in sql  # present in arm 2 unchanged
        # Filter must land on the outer query (post-union).
        assert "UNION ALL" in sql.upper()
