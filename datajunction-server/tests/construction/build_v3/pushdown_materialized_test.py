"""
Hidden-key filter pushdown must not be consumed when the upstream node that
would hold the predicate is never built because a materialized table sits
above it.
"""

import pytest

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group

UNREACHABLE = (
    "Cannot find join path from v3.account_events to dimension "
    "v3.audit_date_dim. Please create a dimension link between these nodes."
)


async def _post(client, path, payload):
    response = await client.post(path, json=payload)
    assert response.status_code < 300, response.json()


async def _build_graph(client, link_source: bool):
    """
    audit_log (linked to audit_date_dim via audit_date) ->
    [audit_clean] -> account_events -> total_event_count.
    ``audit_date`` is not projected by account_events, so a filter on the
    date dimension has to be pushed upstream.
    """
    await _post(
        client,
        "/nodes/source/",
        {
            "name": "v3.src_audit_dates",
            "columns": [{"name": "dateint", "type": "int"}],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "audit_dates",
        },
    )
    await _post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.audit_date_dim",
            "query": "SELECT dateint FROM v3.src_audit_dates",
            "mode": "published",
            "primary_key": ["dateint"],
        },
    )
    await _post(
        client,
        "/nodes/source/",
        {
            "name": "v3.src_audit_log",
            "columns": [
                {"name": "audit_date", "type": "int"},
                {"name": "account_id", "type": "int"},
                {"name": "event_type", "type": "string"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "audit_log",
        },
    )
    await _post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.audit_clean",
            "query": (
                "SELECT audit_date, account_id, event_type FROM v3.src_audit_log"
            ),
            "mode": "published",
        },
    )
    link_from = "v3.src_audit_log" if link_source else "v3.audit_clean"
    await _post(
        client,
        f"/nodes/{link_from}/link",
        {
            "dimension_node": "v3.audit_date_dim",
            "join_type": "inner",
            "join_on": f"{link_from}.audit_date = v3.audit_date_dim.dateint",
        },
    )
    await _post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.account_events",
            "query": (
                "SELECT account_id, event_type, COUNT(*) AS event_count "
                "FROM v3.audit_clean GROUP BY account_id, event_type"
            ),
            "mode": "published",
            "primary_key": ["account_id", "event_type"],
        },
    )
    await _post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.total_event_count",
            "query": "SELECT SUM(event_count) FROM v3.account_events",
            "mode": "published",
        },
    )


async def _materialize(client, node: str, table: str):
    await _post(
        client,
        f"/data/{node}/availability/",
        {
            "catalog": "analytics",
            "schema_": "warehouse",
            "table": table,
            "valid_through_ts": 9999999999,
        },
    )


async def _request_filtered(client):
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.total_event_count"],
            "dimensions": ["v3.account_events.event_type"],
            "filters": ["v3.audit_date_dim.dateint >= 20260101"],
        },
    )
    return response


class TestPushdownTargetNotBuilt:
    @pytest.mark.asyncio
    async def test_rejected_when_fact_materialized(self, client_with_build_v3):
        """The fact itself reads a materialized table, so the upstream
        predicate target is never built."""
        await _build_graph(client_with_build_v3, link_source=True)
        await _materialize(client_with_build_v3, "v3.account_events", "events_mat")
        response = await _request_filtered(client_with_build_v3)
        assert response.status_code == 422, response.json()
        assert response.json()["message"] == UNREACHABLE

    @pytest.mark.asyncio
    async def test_rejected_when_intermediate_materialized(
        self,
        client_with_build_v3,
    ):
        """A materialized node sits between the fact and the linked
        transform that would hold the predicate."""
        await _build_graph(client_with_build_v3, link_source=False)
        await _materialize(client_with_build_v3, "v3.audit_clean", "clean_mat")
        response = await _request_filtered(client_with_build_v3)
        assert response.status_code == 422, response.json()
        assert response.json()["message"] == UNREACHABLE

    @pytest.mark.asyncio
    async def test_filter_pushed_when_nothing_materialized(
        self,
        client_with_build_v3,
    ):
        """Control: with no materialized node the predicate lands in the
        linked transform's CTE."""
        await _build_graph(client_with_build_v3, link_source=False)
        response = await _request_filtered(client_with_build_v3)
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH v3_audit_clean AS (
                SELECT account_id, event_type
                FROM default.v3.audit_log
                WHERE audit_date >= 20260101
            ),
            v3_account_events AS (
                SELECT account_id, event_type, COUNT(*) AS event_count
                FROM v3_audit_clean
                GROUP BY account_id, event_type
            )
            SELECT t1.event_type, SUM(t1.event_count) event_count_sum_HASH
            FROM v3_account_events t1
            GROUP BY t1.event_type
            """,
            normalize_aliases=True,
        )


async def _build_union_graph(client):
    """
    Two linked transforms feed a UNION ALL fact that does not project
    ``audit_date``, so the date filter must reach both branches.
    """
    await _post(
        client,
        "/nodes/source/",
        {
            "name": "v3.src_audit_dates",
            "columns": [{"name": "dateint", "type": "int"}],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "audit_dates",
        },
    )
    await _post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.audit_date_dim",
            "query": "SELECT dateint FROM v3.src_audit_dates",
            "mode": "published",
            "primary_key": ["dateint"],
        },
    )
    for branch in ("a", "b"):
        await _post(
            client,
            "/nodes/source/",
            {
                "name": f"v3.src_audit_log_{branch}",
                "columns": [
                    {"name": "audit_date", "type": "int"},
                    {"name": "account_id", "type": "int"},
                    {"name": "event_type", "type": "string"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": f"audit_log_{branch}",
            },
        )
        await _post(
            client,
            "/nodes/transform/",
            {
                "name": f"v3.audit_clean_{branch}",
                "query": (
                    "SELECT audit_date, account_id, event_type "
                    f"FROM v3.src_audit_log_{branch}"
                ),
                "mode": "published",
            },
        )
        await _post(
            client,
            f"/nodes/v3.audit_clean_{branch}/link",
            {
                "dimension_node": "v3.audit_date_dim",
                "join_type": "inner",
                "join_on": (
                    f"v3.audit_clean_{branch}.audit_date = v3.audit_date_dim.dateint"
                ),
            },
        )
    await _post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.account_events",
            "query": (
                "SELECT account_id, event_type, 1 AS event_count "
                "FROM v3.audit_clean_a "
                "UNION ALL "
                "SELECT account_id, event_type, 1 AS event_count "
                "FROM v3.audit_clean_b"
            ),
            "mode": "published",
            "primary_key": ["account_id", "event_type"],
        },
    )
    await _post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.total_event_count",
            "query": "SELECT SUM(event_count) FROM v3.account_events",
            "mode": "published",
        },
    )


class TestPushdownUnionBranches:
    @pytest.mark.asyncio
    async def test_rejected_when_one_union_branch_materialized(
        self,
        client_with_build_v3,
    ):
        """One UNION ALL branch cannot receive the filter, so the filter
        must not be dropped from the outer query."""
        await _build_union_graph(client_with_build_v3)
        await _materialize(client_with_build_v3, "v3.audit_clean_a", "clean_a_mat")
        response = await _request_filtered(client_with_build_v3)
        assert response.status_code == 422, response.json()
        assert response.json()["message"] == UNREACHABLE

    @pytest.mark.asyncio
    async def test_filter_pushed_into_both_union_branches(
        self,
        client_with_build_v3,
    ):
        """Control: nothing materialized, both branches get the filter."""
        await _build_union_graph(client_with_build_v3)
        response = await _request_filtered(client_with_build_v3)
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH v3_audit_clean_a AS (
                SELECT event_type
                FROM default.v3.audit_log_a
                WHERE audit_date >= 20260101
            ),
            v3_audit_clean_b AS (
                SELECT event_type
                FROM default.v3.audit_log_b
                WHERE audit_date >= 20260101
            ),
            v3_account_events AS (
                SELECT event_type, 1 AS event_count
                FROM v3_audit_clean_a
                UNION ALL
                SELECT event_type, 1 AS event_count
                FROM v3_audit_clean_b
            )
            SELECT t1.event_type, SUM(t1.event_count) event_count_sum_HASH
            FROM v3_account_events t1
            GROUP BY t1.event_type
            """,
            normalize_aliases=True,
        )
