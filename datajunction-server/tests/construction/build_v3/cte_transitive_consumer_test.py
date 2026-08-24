"""
A CTE that enters the build only as a transitive dependency still constrains
what its upstream CTEs may prune.
"""

import pytest


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
                    f"SELECT i.item_id, i.bundle_id AS bundle "
                    f"FROM {ns}.item AS i"
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

        item_cte = sql.split(f"{ns}_bundles")[0]
        assert "channel" in item_cte, sql
        assert "bundle_id" in item_cte, (
            f"{ns}.item CTE pruned bundle_id, but {ns}.bundles "
            f"selects it from that CTE:\n\n{sql}"
        )
