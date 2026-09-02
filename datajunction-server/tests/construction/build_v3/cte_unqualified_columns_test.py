"""
Columns a CTE reads without a table qualifier still hold its source open.

The projection pruner attributes a column to a node by its qualifier, so a bare
``code_a`` used to belong to nobody and the CTE producing it dropped it. These
tests state the invariant directly: whatever a CTE reads from another CTE, that
other CTE projects.
"""

import pytest

from tests.construction.build_v3.projection_invariant import unprojected_references


class TestUnqualifiedColumnsSurvivePruning:
    """
    ``child_dim`` unions two parents and reads ``parent_dim``'s columns bare, so
    the pruner has to keep them even though nothing says where they come from.
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
                    {"name": "code_a", "type": "string"},
                    {"name": "code_b", "type": "string"},
                    {"name": "code_c", "type": "string"},
                    {"name": "region", "type": "string"},
                ],
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            "/nodes/source/",
            json={
                "name": f"{ns}.src_partner_item",
                "catalog": "default",
                "schema_": ns,
                "table": "src_partner_item",
                "columns": [
                    {"name": "external_item_id", "type": "int"},
                    {"name": "is_special", "type": "bool"},
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

        resp = await client.post(
            "/nodes/dimension/",
            json={
                "name": f"{ns}.parent_dim",
                "mode": "published",
                "primary_key": ["item_id"],
                "query": (
                    f"SELECT item_id, code_a, code_b, code_c, region FROM {ns}.src_item"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            "/nodes/dimension/",
            json={
                "name": f"{ns}.partner_dim",
                "mode": "published",
                "primary_key": ["external_item_id"],
                "query": (
                    f"SELECT external_item_id, is_special FROM {ns}.src_partner_item"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # Every code_* here is bare: no alias, no node qualifier.
        resp = await client.post(
            "/nodes/dimension/",
            json={
                "name": f"{ns}.child_dim",
                "mode": "published",
                "primary_key": ["item_id"],
                "query": (
                    f"SELECT item_id, "
                    f"CASE WHEN code_a = 'PROMO' THEN 'promo' "
                    f"WHEN code_b = 'DIRECT' AND code_c = 'OPEN' THEN 'direct' "
                    f"ELSE 'other' END AS channel "
                    f"FROM {ns}.parent_dim "
                    f"UNION ALL "
                    f"SELECT external_item_id AS item_id, "
                    f"CASE WHEN is_special THEN 'special' ELSE 'other' END AS channel "
                    f"FROM {ns}.partner_dim"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            "/nodes/transform/",
            json={
                "name": f"{ns}.fact",
                "mode": "published",
                "query": f"SELECT event_id, item_id, amount FROM {ns}.src_event",
            },
        )
        assert resp.status_code in (200, 201), resp.text

        for dimension in ("parent_dim", "child_dim"):
            resp = await client.post(
                f"/nodes/{ns}.fact/link",
                json={
                    "dimension_node": f"{ns}.{dimension}",
                    "join_type": "left",
                    "join_on": f"{ns}.fact.item_id = {ns}.{dimension}.item_id",
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
    async def test_every_cte_projects_what_its_readers_ask_for(
        self,
        client_with_service_setup,
    ):
        client = client_with_service_setup
        ns = "cteunqual"
        await self._setup(client, ns)

        resp = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": [f"{ns}.total_amount"],
                "dimensions": [f"{ns}.parent_dim.region", f"{ns}.child_dim.channel"],
            },
        )
        assert resp.status_code == 200, resp.text
        sql = resp.json()["grain_groups"][0]["sql"]

        assert unprojected_references(sql) == set(), sql


class TestPassThroughColumnSurvivesPruning:
    """
    ``parent_dim`` derives ``channel`` itself and ``child_dim`` passes it
    through, naming it bare. The reference carries no qualifier, so pruning the
    parent for an unrelated dimension used to drop the column out from under it.
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
                    {"name": "code_a", "type": "string"},
                    {"name": "region", "type": "string"},
                ],
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            "/nodes/source/",
            json={
                "name": f"{ns}.src_partner_item",
                "catalog": "default",
                "schema_": ns,
                "table": "src_partner_item",
                "columns": [
                    {"name": "external_item_id", "type": "int"},
                    {"name": "is_special", "type": "bool"},
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

        # The parent derives `channel`, so the child has a column to pass through.
        resp = await client.post(
            "/nodes/dimension/",
            json={
                "name": f"{ns}.parent_dim",
                "mode": "published",
                "primary_key": ["item_id"],
                "query": (
                    f"SELECT item_id, "
                    f"CASE WHEN code_a = 'PROMO' THEN 'promo' ELSE 'other' END AS channel, "
                    f"region FROM {ns}.src_item"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            "/nodes/dimension/",
            json={
                "name": f"{ns}.partner_dim",
                "mode": "published",
                "primary_key": ["external_item_id"],
                "query": (
                    f"SELECT external_item_id, is_special FROM {ns}.src_partner_item"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # `channel` is named bare: no alias, no node qualifier.
        resp = await client.post(
            "/nodes/dimension/",
            json={
                "name": f"{ns}.child_dim",
                "mode": "published",
                "primary_key": ["item_id"],
                "query": (
                    f"SELECT item_id, channel FROM {ns}.parent_dim "
                    f"UNION ALL "
                    f"SELECT external_item_id AS item_id, "
                    f"CASE WHEN is_special THEN 'special' ELSE 'other' END AS channel "
                    f"FROM {ns}.partner_dim"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client.post(
            "/nodes/transform/",
            json={
                "name": f"{ns}.fact",
                "mode": "published",
                "query": f"SELECT event_id, item_id, amount FROM {ns}.src_event",
            },
        )
        assert resp.status_code in (200, 201), resp.text

        for dimension in ("parent_dim", "child_dim"):
            resp = await client.post(
                f"/nodes/{ns}.fact/link",
                json={
                    "dimension_node": f"{ns}.{dimension}",
                    "join_type": "left",
                    "join_on": f"{ns}.fact.item_id = {ns}.{dimension}.item_id",
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
    async def test_parent_keeps_the_column_its_child_passes_through(
        self,
        client_with_service_setup,
    ):
        client = client_with_service_setup
        ns = "ctepassthru"
        await self._setup(client, ns)

        # Asking for a dimension from the parent as well is what turns pruning
        # on for it; `child_dim.channel` alone leaves the parent unpruned.
        resp = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": [f"{ns}.total_amount"],
                "dimensions": [f"{ns}.parent_dim.region", f"{ns}.child_dim.channel"],
            },
        )
        assert resp.status_code == 200, resp.text
        sql = resp.json()["grain_groups"][0]["sql"]

        assert unprojected_references(sql) == set(), sql
