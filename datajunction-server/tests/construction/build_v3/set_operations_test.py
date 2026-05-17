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

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group


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
              GROUP BY t1.status, t1.order_id
            )
            SELECT orders_unified_0.status AS status,
              COUNT(DISTINCT orders_unified_0.order_id) AS unified_order_count
            FROM orders_unified_0
            WHERE orders_unified_0.status = 'completed'
            GROUP BY orders_unified_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_only_dim_link_on_one_arm_source_only(
        self,
        client_with_build_v3,
    ):
        """Partial-source pushdown into a UNION-ALL transform.

        Two sources, ``v3.src_orders_us`` and ``v3.src_orders_eu``, both
        carry an ``order_date`` column.  Only the US source has a
        dimension link to ``v3.audit_date_dim``.  The transform UNION-ALLs
        both.  A filter on the dim must be pushed into ONLY the arm
        reading from the linked source — the other arm has no formal
        binding to the dim and must stay untouched.
        """
        client = client_with_build_v3

        resp = await client.post(
            "/nodes/source/",
            json={
                "name": "v3.src_audit_dates",
                "description": "audit dates",
                "columns": [{"name": "dateint", "type": "int"}],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "audit_dates",
            },
        )
        assert resp.status_code in (200, 201), resp.json()
        resp = await client.post(
            "/nodes/dimension/",
            json={
                "name": "v3.audit_date_dim",
                "description": "Audit date dim",
                "query": "SELECT dateint FROM v3.src_audit_dates",
                "mode": "published",
                "primary_key": ["dateint"],
            },
        )
        assert resp.status_code in (200, 201), resp.json()

        for name, table in (
            ("v3.src_orders_us", "orders_us"),
            ("v3.src_orders_eu", "orders_eu"),
        ):
            resp = await client.post(
                "/nodes/source/",
                json={
                    "name": name,
                    "description": f"Orders for {table}",
                    "columns": [
                        {"name": "order_id", "type": "int"},
                        {"name": "order_date", "type": "int"},
                        {"name": "status", "type": "string"},
                    ],
                    "mode": "published",
                    "catalog": "default",
                    "schema_": "v3",
                    "table": table,
                },
            )
            assert resp.status_code in (200, 201), resp.json()

        # Only the US source carries the dim link.
        resp = await client.post(
            "/nodes/v3.src_orders_us/link",
            json={
                "dimension_node": "v3.audit_date_dim",
                "join_type": "inner",
                "join_on": "v3.src_orders_us.order_date = v3.audit_date_dim.dateint",
            },
        )
        assert resp.status_code in (200, 201), resp.json()

        resp = await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.orders_global",
                "description": "Union of US and EU orders",
                # Linked source is in the SECOND (non-leading) arm so the
                # pushdown goes through the direct-arm-WHERE-mutation path.
                "query": """
                    SELECT order_id, status FROM v3.src_orders_eu
                    UNION ALL
                    SELECT order_id, status FROM v3.src_orders_us
                """,
                "mode": "published",
                "primary_key": ["order_id"],
            },
        )
        assert resp.status_code in (200, 201), resp.json()

        resp = await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.global_order_count",
                "query": "SELECT COUNT(DISTINCT order_id) FROM v3.orders_global",
                "mode": "published",
            },
        )
        assert resp.status_code in (200, 201), resp.json()

        response = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.global_order_count"],
                "dimensions": ["v3.orders_global.status"],
                "filters": ["v3.audit_date_dim.dateint >= 20260101"],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        # The filter lands in the US arm (linked to the dim) only;
        # the EU arm stays untouched.  US is the non-leading arm, so this
        # exercises the direct-arm-WHERE-mutation path.
        assert_sql_equal(
            sql,
            """
            WITH v3_orders_global AS (
                SELECT order_id, status
                FROM default.v3.orders_eu
                UNION ALL
                SELECT order_id, status
                FROM default.v3.orders_us
                WHERE src_orders_us.order_date >= 20260101
            )
            SELECT t1.status, t1.order_id
            FROM v3_orders_global t1
            GROUP BY t1.status, t1.order_id
            """,
        )
