"""
A CTE that enters the build only as a transitive dependency still constrains
what its upstream CTEs may prune.
"""

import pytest

from tests.construction.build_v3 import assert_sql_equal
from tests.construction.build_v3.projection_invariant import unprojected_references


class TestTransitiveConsumerProjection:
    """
    ``fact`` joins to ``item`` for one attribute, so ``item``'s CTE is pruned to
    that attribute plus the join key. ``fact`` also reads from ``bundles``, which
    is not a requested dimension and reaches ``item`` only through its own query
    — the shape that used to lose a column.
    """

    async def _setup(self, client, ns: str):
        resp = await client.post(f"/namespaces/{ns}/")
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            "/nodes/source/",
            json={
                "name": f"{ns}.src_item",
                "catalog": "default",
                "schema_": ns,
                "table": "src_item",
                "columns": [
                    {"name": "item_id", "type": "int"},
                    {"name": "kind_code", "type": "string"},
                    {"name": "bundle_id", "type": "int"},
                ],
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            "/nodes/source/",
            json={
                "name": f"{ns}.src_event",
                "catalog": "default",
                "schema_": ns,
                "table": "src_event",
                "columns": [
                    {"name": "event_id", "type": "int"},
                    {"name": "item_id", "type": "int"},
                    {"name": "amount", "type": "double"},
                ],
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # Dimension: projects bundle_id alongside the requested attribute.
        resp = await client.post(
            "/nodes/dimension/",
            json={
                "name": f"{ns}.item",
                "mode": "published",
                "primary_key": ["item_id"],
                "query": (
                    f"SELECT item_id, bundle_id, "
                    f"CASE WHEN kind_code = 'AUTO' THEN 'auto' "
                    f"ELSE 'manual' END AS channel "
                    f"FROM {ns}.src_item"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # Transform reading bundle_id from the dimension node. It is never a
        # requested dimension, so nothing on a join path scans it.
        resp = await client.post(
            "/nodes/transform/",
            json={
                "name": f"{ns}.bundles",
                "mode": "published",
                "query": (
                    f"SELECT i.item_id, i.bundle_id AS bundle FROM {ns}.item AS i"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # Fact joins the transform, pulling it into the CTE closure.
        resp = await client.post(
            "/nodes/transform/",
            json={
                "name": f"{ns}.fact",
                "mode": "published",
                "query": (
                    f"SELECT e.event_id, e.item_id, e.amount, b.bundle "
                    f"FROM {ns}.src_event AS e "
                    f"LEFT JOIN {ns}.bundles AS b "
                    f"ON e.item_id = b.item_id"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            f"/nodes/{ns}.fact/link",
            json={
                "dimension_node": f"{ns}.item",
                "join_type": "left",
                "join_on": f"{ns}.fact.item_id = {ns}.item.item_id",
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            "/nodes/metric/",
            json={
                "name": f"{ns}.total_amount",
                "mode": "published",
                "query": f"SELECT SUM(amount) FROM {ns}.fact",
            },
        )
        assert resp.status_code in (200, 201), resp.text

    @pytest.mark.asyncio
    async def test_transitive_consumer_column_survives_pruning(
        self,
        client_with_service_setup,
    ):
        client = client_with_service_setup
        ns = "ctetrans"
        await self._setup(client, ns)

        resp = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": [f"{ns}.total_amount"],
                "dimensions": [f"{ns}.item.channel"],
            },
        )
        assert resp.status_code == 200, resp.text
        sql = resp.json()["grain_groups"][0]["sql"]

        # ``bundles`` is itself pruned to what ``fact`` reads from it, which
        # narrows its own demand on ``item`` — so ``item`` need not keep
        # ``bundle_id``. It does keep ``channel``, which the outer select asks
        # for through a separate join.
        assert_sql_equal(
            sql,
            """
            WITH ctetrans_item AS (
              SELECT item_id,
                CASE WHEN kind_code = 'AUTO' THEN 'auto' ELSE 'manual' END AS channel
              FROM default.ctetrans.src_item
            ),
            ctetrans_bundles AS (
              SELECT i.item_id FROM ctetrans_item AS i
            ),
            ctetrans_fact AS (
              SELECT e.item_id, e.amount
              FROM default.ctetrans.src_event AS e
              LEFT JOIN ctetrans_bundles AS b ON e.item_id = b.item_id
            )
            SELECT t2.channel, SUM(t1.amount) AS amount_sum_3ade80bc
            FROM ctetrans_fact t1
            LEFT OUTER JOIN ctetrans_item t2 ON t1.item_id = t2.item_id
            GROUP BY t2.channel
            """,
            normalize_aliases=True,
        )
        assert unprojected_references(sql) == set(), sql
