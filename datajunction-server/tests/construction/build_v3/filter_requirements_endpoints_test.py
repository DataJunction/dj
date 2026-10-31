"""Filter requirements through the HTTP endpoints."""

import json
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest

from datajunction_server.models.query import QueryWithResults
from datajunction_server.typing import QueryState
from datajunction_server.utils import get_query_service_client

from . import assert_sql_equal


def _expected_sql(order_date, metric_alias):
    return f"""
    WITH
    v3_location AS (
      SELECT location_id, country FROM default.v3.locations
    ),
    v3_order_details AS (
      SELECT o.from_location_id, oi.quantity * oi.unit_price AS line_total
      FROM default.v3.orders o
      JOIN default.v3.order_items oi ON o.order_id = oi.order_id
      WHERE o.order_date = {order_date}
    ),
    order_details_0 AS (
      SELECT t2.country AS country_from, SUM(t1.line_total) line_total_sum_e1f61696
      FROM v3_order_details t1
      LEFT OUTER JOIN v3_location t2 ON t1.from_location_id = t2.location_id
      GROUP BY t2.country
    )
    SELECT order_details_0.country_from AS country_from,
      SUM(order_details_0.line_total_sum_e1f61696) AS {metric_alias}
    FROM order_details_0
    GROUP BY order_details_0.country_from
    """


async def _make_metric(client, name, **fields):
    query = (await client.get("/nodes/v3.total_revenue/")).json()["query"]
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": "d",
            "query": query,
            "mode": "published",
            **fields,
        },
    )
    assert response.status_code == 201, response.text


@pytest.mark.asyncio
async def test_sql_rejects_missing_required_filter(client_with_build_v3):
    await _make_metric(
        client_with_build_v3,
        "v3.ep_required",
        required_filters=[{"dimension": "v3.date.date_id[order]"}],
    )
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.ep_required"],
            "dimensions": ["v3.location.country[from]"],
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Missing required filter: one of v3.date.date_id[order] "
        "(required by metric v3.ep_required)"
    )


@pytest.mark.asyncio
async def test_default_does_not_weaken_a_caller_or_filter(client_with_build_v3):
    await _make_metric(
        client_with_build_v3,
        "v3.ep_or",
        required_filters=[
            {
                "dimension": "v3.date.date_id[order]",
                "default": "v3.date.date_id[order] = 20250101",
            },
        ],
    )
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.ep_or"],
            "dimensions": ["v3.location.country[from]"],
            "filters": [
                "v3.order_details.status = 'a' OR v3.order_details.status = 'b'",
            ],
        },
    )
    assert response.status_code == 200
    assert_sql_equal(
        response.json()["sql"],
        _expected_sql(20250101, "ep_or").replace(
            "WHERE o.order_date = 20250101",
            "WHERE (o.status = 'a' OR o.status = 'b') AND o.order_date = 20250101",
        ),
    )


@pytest.mark.asyncio
async def test_sql_applies_default_and_reports_it(client_with_build_v3):
    await _make_metric(
        client_with_build_v3,
        "v3.ep_default",
        required_filters=[
            {
                "dimension": "v3.date.date_id[order]",
                "default": "v3.date.date_id[order] = 20250101",
            },
        ],
    )
    params = {"metrics": ["v3.ep_default"], "dimensions": ["v3.location.country[from]"]}
    response = await client_with_build_v3.get("/sql/metrics/v3/", params=params)
    assert response.status_code == 200
    body = response.json()
    assert_sql_equal(body["sql"], _expected_sql(20250101, "ep_default"))
    assert [(w["code"], w["message"]) for w in body["warnings"]] == [
        (
            "DEFAULT_FILTER_APPLIED",
            "Applied default filter v3.date.date_id[order] = 20250101 "
            "(from metric v3.ep_default)",
        ),
    ]

    overridden = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={**params, "filters": ["v3.date.date_id[order] = 20240101"]},
    )
    assert_sql_equal(
        overridden.json()["sql"],
        _expected_sql(20240101, "ep_default"),
    )
    assert overridden.json()["warnings"] == []


@pytest.mark.asyncio
async def test_metric_without_requirements_is_unchanged(client_with_build_v3):
    params = {
        "metrics": ["v3.total_revenue"],
        "dimensions": ["v3.location.country[from]"],
        "filters": ["v3.date.date_id[order] = 20240101"],
    }
    response = await client_with_build_v3.get("/sql/metrics/v3/", params=params)
    assert response.status_code == 200
    body = response.json()
    assert_sql_equal(body["sql"], _expected_sql(20240101, "total_revenue"))
    assert body["warnings"] == []


@pytest.mark.asyncio
async def test_node_sql_endpoint_enforces(client_with_build_v3):
    await _make_metric(
        client_with_build_v3,
        "v3.ep_node",
        required_filters=[{"dimension": "v3.date.date_id[order]"}],
    )
    response = await client_with_build_v3.get(
        "/sql/v3.ep_node/",
        params={"dimensions": ["v3.location.country[from]"]},
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Missing required filter: one of v3.date.date_id[order] "
        "(required by metric v3.ep_node)"
    )


@pytest.mark.asyncio
async def test_data_endpoint_enforces_and_returns_warnings(client_with_build_v3):
    client = client_with_build_v3
    await _make_metric(
        client,
        "v3.ep_data",
        recommended_filters=[{"dimension": "v3.date.date_id[order]"}],
    )
    mock_qs_client = MagicMock()

    async def submit_query(query_create, request_headers=None):
        return QueryWithResults(
            id="test-query-id",
            submitted_query=query_create.submitted_query,
            state=QueryState.FINISHED,
            results=[],
        )

    mock_qs_client.submit_query = submit_query
    client.app.dependency_overrides[get_query_service_client] = lambda: mock_qs_client
    try:
        response = await client.get(
            "/data/",
            params={
                "metrics": ["v3.ep_data"],
                "dimensions": ["v3.location.country[from]"],
            },
        )
    finally:
        del client.app.dependency_overrides[get_query_service_client]
    assert response.status_code == 200, response.text
    assert [(w["code"], w["message"]) for w in response.json()["warnings"]] == [
        (
            "RECOMMENDED_FILTER_MISSING",
            "Missing recommended filter: one of v3.date.date_id[order] "
            "(recommended by metric v3.ep_data)",
        ),
    ]


DEFAULT_ENTRY = {
    "dimension": "v3.date.date_id[order]",
    "default": "v3.date.date_id[order] = 20250101",
}
DJSQL = "SELECT v3.{metric}, v3.location.country[from] FROM metrics GROUP BY v3.location.country[from]"


def _applied(metric):
    return [
        (
            "DEFAULT_FILTER_APPLIED",
            "Applied default filter v3.date.date_id[order] = 20250101 "
            f"(from metric v3.{metric})",
        ),
    ]


@asynccontextmanager
async def _capture_submitted_sql(client):
    """Replace the query service with one that records submitted SQL."""
    submitted = []

    async def submit_query(query_create, request_headers=None):
        submitted.append(query_create.submitted_query)
        return QueryWithResults(
            id="test-query-id",
            submitted_query=query_create.submitted_query,
            state=QueryState.FINISHED,
            results=[],
        )

    async def get_query(query_id, request_headers=None):
        return QueryWithResults(
            id=query_id,
            submitted_query="",
            state=QueryState.FINISHED,
            results=[],
        )

    mock_qs_client = MagicMock()
    mock_qs_client.submit_query = submit_query
    mock_qs_client.get_query = get_query
    client.app.dependency_overrides[get_query_service_client] = lambda: mock_qs_client
    try:
        yield submitted
    finally:
        del client.app.dependency_overrides[get_query_service_client]


@pytest.mark.asyncio
async def test_stream_endpoint_enforces_and_applies_defaults(client_with_build_v3):
    client = client_with_build_v3
    await _make_metric(
        client,
        "v3.ep_stream_required",
        required_filters=[{"dimension": "v3.date.date_id[order]"}],
    )
    await _make_metric(client, "v3.ep_stream", required_filters=[DEFAULT_ENTRY])
    params = {"dimensions": ["v3.location.country[from]"]}

    rejected = await client.get(
        "/stream/",
        params={**params, "metrics": ["v3.ep_stream_required"]},
    )
    assert rejected.status_code == 422
    assert rejected.json()["message"] == (
        "Missing required filter: one of v3.date.date_id[order] "
        "(required by metric v3.ep_stream_required)"
    )

    async with _capture_submitted_sql(client) as submitted:
        async with client.stream(
            "GET",
            "/stream/",
            params={**params, "metrics": ["v3.ep_stream"]},
        ) as response:
            assert response.status_code == 200
            await response.aread()
    assert len(submitted) == 1
    assert_sql_equal(submitted[0], _expected_sql(20250101, "ep_stream"))


@pytest.mark.asyncio
async def test_djsql_endpoint_enforces_and_applies_defaults(client_with_build_v3):
    client = client_with_build_v3
    await _make_metric(
        client,
        "v3.ep_djsql_required",
        required_filters=[{"dimension": "v3.date.date_id[order]"}],
    )
    await _make_metric(client, "v3.ep_djsql", required_filters=[DEFAULT_ENTRY])

    rejected = await client.get(
        "/djsql/",
        params={"query": DJSQL.format(metric="ep_djsql_required")},
    )
    assert rejected.status_code == 422
    assert rejected.json()["message"] == (
        "Missing required filter: one of v3.date.date_id[order] "
        "(required by metric v3.ep_djsql_required)"
    )

    response = await client.get(
        "/djsql/",
        params={"query": DJSQL.format(metric="ep_djsql")},
    )
    assert response.status_code == 200
    assert_sql_equal(response.json()["sql"], _expected_sql(20250101, "ep_djsql"))
    assert [(w["code"], w["message"]) for w in response.json()["warnings"]] == (
        _applied("ep_djsql")
    )


@pytest.mark.asyncio
async def test_djsql_data_endpoint_applies_defaults_and_returns_warnings(
    client_with_build_v3,
):
    client = client_with_build_v3
    await _make_metric(
        client,
        "v3.ep_djsql_data_required",
        required_filters=[{"dimension": "v3.date.date_id[order]"}],
    )
    await _make_metric(client, "v3.ep_djsql_data", required_filters=[DEFAULT_ENTRY])

    rejected = await client.get(
        "/djsql/data",
        params={"query": DJSQL.format(metric="ep_djsql_data_required")},
    )
    assert rejected.status_code == 422
    assert rejected.json()["message"] == (
        "Missing required filter: one of v3.date.date_id[order] "
        "(required by metric v3.ep_djsql_data_required)"
    )

    async with _capture_submitted_sql(client) as submitted:
        response = await client.get(
            "/djsql/data",
            params={"query": DJSQL.format(metric="ep_djsql_data")},
        )
    assert response.status_code == 200, response.text
    assert len(submitted) == 1
    assert_sql_equal(submitted[0], _expected_sql(20250101, "ep_djsql_data"))
    assert [(w["code"], w["message"]) for w in response.json()["warnings"]] == (
        _applied("ep_djsql_data")
    )


@pytest.mark.asyncio
async def test_node_data_endpoint_returns_applied_default(client_with_build_v3):
    client = client_with_build_v3
    await _make_metric(client, "v3.ep_node_data", required_filters=[DEFAULT_ENTRY])
    async with _capture_submitted_sql(client) as submitted:
        response = await client.get(
            "/data/v3.ep_node_data/",
            params={"dimensions": ["v3.location.country[from]"]},
        )
    assert response.status_code == 200, response.text
    assert len(submitted) == 1
    assert_sql_equal(submitted[0], _expected_sql(20250101, "ep_node_data"))
    assert [(w["code"], w["message"]) for w in response.json()["warnings"]] == (
        _applied("ep_node_data")
    )


@pytest.mark.asyncio
async def test_node_sql_reflects_edited_requirements(client_with_build_v3):
    client = client_with_build_v3
    await _make_metric(client, "v3.ep_cached", required_filters=[DEFAULT_ENTRY])
    params = {"dimensions": ["v3.location.country[from]"]}

    first = await client.get("/sql/v3.ep_cached/", params=params)
    assert first.status_code == 200
    assert_sql_equal(first.json()["sql"], _expected_sql(20250101, "ep_cached"))
    assert [(w["code"], w["message"]) for w in first.json()["warnings"]] == (
        _applied("ep_cached")
    )

    repeated = await client.get("/sql/v3.ep_cached/", params=params)
    assert repeated.json() == first.json()

    patched = await client.patch(
        "/nodes/v3.ep_cached/",
        json={"required_filters": [{"dimension": "v3.date.date_id[order]"}]},
    )
    assert patched.status_code == 200, patched.text

    second = await client.get("/sql/v3.ep_cached/", params=params)
    assert second.status_code == 422
    assert second.json()["message"] == (
        "Missing required filter: one of v3.date.date_id[order] "
        "(required by metric v3.ep_cached)"
    )


async def _make_chain(client, prefix):
    """Create metrics a -> b -> c, each derived from the one before."""
    await _make_metric(client, f"v3.{prefix}_a")
    for name, parent in (("b", "a"), ("c", "b")):
        response = await client.post(
            "/nodes/metric/",
            json={
                "name": f"v3.{prefix}_{name}",
                "description": "d",
                "query": f"SELECT v3.{prefix}_{parent} * 2",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.text


def _chain_sql(metric_alias, order_date=None):
    where = f"WHERE o.order_date = {order_date}" if order_date else ""
    return f"""
    WITH
    v3_location AS (
      SELECT location_id, country FROM default.v3.locations
    ),
    v3_order_details AS (
      SELECT o.from_location_id, oi.quantity * oi.unit_price AS line_total
      FROM default.v3.orders o
      JOIN default.v3.order_items oi ON o.order_id = oi.order_id
      {where}
    ),
    order_details_0 AS (
      SELECT t2.country AS country_from, SUM(t1.line_total) line_total_sum_e1f61696
      FROM v3_order_details t1
      LEFT OUTER JOIN v3_location t2 ON t1.from_location_id = t2.location_id
      GROUP BY t2.country
    )
    SELECT order_details_0.country_from AS country_from,
      SUM(order_details_0.line_total_sum_e1f61696) * 2 * 2 AS {metric_alias}
    FROM order_details_0
    GROUP BY order_details_0.country_from
    """


def _missing(metric):
    return f"Missing required filter: one of v3.date.date_id[order] (required by metric {metric})"


@pytest.mark.asyncio
async def test_node_sql_sees_upstream_requirement_edits(client_with_build_v3):
    client = client_with_build_v3
    await _make_chain(client, "chain_sql")
    params = {"dimensions": ["v3.location.country[from]"]}

    first = await client.get("/sql/v3.chain_sql_c/", params=params)
    assert first.status_code == 200, first.text
    assert_sql_equal(first.json()["sql"], _chain_sql("chain_sql_c"))
    assert first.json()["warnings"] == []

    patched = await client.patch(
        "/nodes/v3.chain_sql_a/",
        json={"required_filters": [DEFAULT_ENTRY]},
    )
    assert patched.status_code == 200, patched.text

    # The first call builds the SQL; the second reads it from the cache.
    for _ in range(2):
        response = await client.get("/sql/v3.chain_sql_c/", params=params)
        assert response.status_code == 200, response.text
        assert_sql_equal(response.json()["sql"], _chain_sql("chain_sql_c", 20250101))
        assert [(w["code"], w["message"]) for w in response.json()["warnings"]] == (
            _applied("chain_sql_a")
        )

    patched = await client.patch(
        "/nodes/v3.chain_sql_a/",
        json={"required_filters": [{"dimension": "v3.date.date_id[order]"}]},
    )
    assert patched.status_code == 200, patched.text
    rejected = await client.get("/sql/v3.chain_sql_c/", params=params)
    assert rejected.status_code == 422
    assert rejected.json()["message"] == _missing("v3.chain_sql_a")


@pytest.mark.asyncio
async def test_node_data_sees_upstream_requirement_edits(client_with_build_v3):
    client = client_with_build_v3
    await _make_chain(client, "chain_data")
    params = {"dimensions": ["v3.location.country[from]"]}

    async with _capture_submitted_sql(client) as submitted:
        first = await client.get("/data/v3.chain_data_c/", params=params)
        assert first.status_code == 200, first.text
        assert first.json()["warnings"] == []

        patched = await client.patch(
            "/nodes/v3.chain_data_a/",
            json={"required_filters": [DEFAULT_ENTRY]},
        )
        assert patched.status_code == 200, patched.text
        for _ in range(2):
            response = await client.get("/data/v3.chain_data_c/", params=params)
            assert response.status_code == 200, response.text
            assert [
                (w["code"], w["message"]) for w in response.json()["warnings"]
            ] == _applied("chain_data_a")

        patched = await client.patch(
            "/nodes/v3.chain_data_a/",
            json={"required_filters": [{"dimension": "v3.date.date_id[order]"}]},
        )
        assert patched.status_code == 200, patched.text
        rejected = await client.get("/data/v3.chain_data_c/", params=params)
        assert rejected.status_code == 422
        assert rejected.json()["message"] == _missing("v3.chain_data_a")

    assert len(submitted) == 3
    assert_sql_equal(submitted[0], _chain_sql("chain_data_c"))
    assert_sql_equal(submitted[1], _chain_sql("chain_data_c", 20250101))
    assert_sql_equal(submitted[2], _chain_sql("chain_data_c", 20250101))


@pytest.mark.asyncio
async def test_node_sql_reports_recommended_once(client_with_build_v3):
    client = client_with_build_v3
    await _make_chain(client, "chain_rec")
    patched = await client.patch(
        "/nodes/v3.chain_rec_a/",
        json={"recommended_filters": [{"dimension": "v3.date.date_id[order]"}]},
    )
    assert patched.status_code == 200, patched.text
    expected = [
        (
            "RECOMMENDED_FILTER_MISSING",
            "Missing recommended filter: one of v3.date.date_id[order] "
            "(recommended by metric v3.chain_rec_a)",
        ),
    ]
    # Built once, then read from the cache.
    for _ in range(2):
        response = await client.get(
            "/sql/v3.chain_rec_c/",
            params={"dimensions": ["v3.location.country[from]"]},
        )
        assert response.status_code == 200, response.text
        assert [
            (w["code"], w["message"]) for w in response.json()["warnings"]
        ] == expected


CHAIN_CUBE_SQL = """
WITH
v3_order_details AS (
  SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total
  FROM default.v3.orders o
  JOIN default.v3.order_items oi ON o.order_id = oi.order_id
  WHERE o.order_date = {date}
),
v3_product AS (
  SELECT product_id, category FROM default.v3.products
),
order_details_0 AS (
  SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
  FROM v3_order_details t1
  LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
  GROUP BY t2.category
)
SELECT order_details_0.category AS category,
  SUM(order_details_0.line_total_sum_e1f61696) * 2 * 2 AS chain_cube_c
FROM order_details_0
GROUP BY order_details_0.category
"""


@pytest.mark.asyncio
async def test_cube_node_sql_sees_upstream_default_edits(client_with_build_v3):
    client = client_with_build_v3
    await _make_chain(client, "chain_cube")
    patched = await client.patch(
        "/nodes/v3.chain_cube_a/",
        json={"required_filters": [DEFAULT_ENTRY]},
    )
    assert patched.status_code == 200, patched.text
    response = await client.post(
        "/nodes/cube/",
        json={
            "name": "v3.chain_cube_cube",
            "metrics": ["v3.chain_cube_c"],
            "dimensions": ["v3.product.category"],
            "mode": "published",
            "description": "d",
        },
    )
    assert response.status_code == 201, response.text

    # Built once, then read from the cache.
    for _ in range(2):
        response = await client.get("/sql/v3.chain_cube_cube/")
        assert response.status_code == 200, response.text
        assert_sql_equal(response.json()["sql"], CHAIN_CUBE_SQL.format(date=20250101))
        assert [(w["code"], w["message"]) for w in response.json()["warnings"]] == (
            _applied("chain_cube_a")
        )

    patched = await client.patch(
        "/nodes/v3.chain_cube_a/",
        json={
            "required_filters": [
                {
                    "dimension": "v3.date.date_id[order]",
                    "default": "v3.date.date_id[order] = 20240101",
                },
            ],
        },
    )
    assert patched.status_code == 200, patched.text
    response = await client.get("/sql/v3.chain_cube_cube/")
    assert response.status_code == 200, response.text
    assert_sql_equal(response.json()["sql"], CHAIN_CUBE_SQL.format(date=20240101))
    assert [(w["code"], w["message"]) for w in response.json()["warnings"]] == [
        (
            "DEFAULT_FILTER_APPLIED",
            "Applied default filter v3.date.date_id[order] = 20240101 "
            "(from metric v3.chain_cube_a)",
        ),
    ]


async def _stream_events(client, url, params):
    """Return the (event, data) pairs of a server-sent event stream."""
    events = []
    event = data = None
    async with client.stream("GET", url, params=params) as response:
        assert response.status_code == 200
        async for line in response.aiter_lines():
            if line.startswith("event: "):
                event = line.removeprefix("event: ")
            elif line.startswith("data: "):
                data = line.removeprefix("data: ")
            elif not line and event:
                events.append((event, data))
                event = data = None
    return events


def _query_events(submitted_sql):
    """The two query events of a stream that finishes on the first poll."""
    return [
        (
            "message",
            json.dumps(
                QueryWithResults(
                    id="test-query-id",
                    submitted_query=submitted_sql,
                    state=QueryState.FINISHED,
                    results=[],
                ).json(),
            ),
        ),
        (
            "message",
            json.dumps(
                QueryWithResults(
                    id="test-query-id",
                    submitted_query="",
                    state=QueryState.FINISHED,
                    results=[],
                ).json(),
            ),
        ),
    ]


RECOMMENDED_ENTRY = {"dimension": "v3.product.category"}


def _stream_warnings(metric):
    return [
        {
            "code": "RECOMMENDED_FILTER_MISSING",
            "message": "Missing recommended filter: one of v3.product.category "
            f"(recommended by metric {metric})",
            "debug": None,
        },
        {
            "code": "DEFAULT_FILTER_APPLIED",
            "message": "Applied default filter v3.date.date_id[order] = 20250101 "
            f"(from metric {metric})",
            "debug": None,
        },
    ]


@pytest.mark.parametrize(
    "url, params",
    [
        (
            "/stream/",
            {"metrics": ["v3.ep_sse"], "dimensions": ["v3.location.country[from]"]},
        ),
        ("/djsql/stream/", {"query": DJSQL.format(metric="ep_sse")}),
        ("/stream/v3.ep_sse", {"dimensions": ["v3.location.country[from]"]}),
    ],
)
@pytest.mark.asyncio
async def test_streams_send_warnings_first(client_with_build_v3, url, params):
    client = client_with_build_v3
    await _make_metric(
        client,
        "v3.ep_sse",
        required_filters=[DEFAULT_ENTRY],
        recommended_filters=[RECOMMENDED_ENTRY],
    )
    async with _capture_submitted_sql(client) as submitted:
        events = await _stream_events(client, url, params)
    assert len(submitted) == 1
    assert [(name, json.loads(data)) for name, data in events[:1]] == [
        ("warnings", _stream_warnings("v3.ep_sse")),
    ]
    assert events[1:] == _query_events(submitted[0])


@pytest.mark.parametrize(
    "url, params",
    [
        (
            "/stream/",
            {
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.location.country[from]"],
            },
        ),
        ("/djsql/stream/", {"query": DJSQL.format(metric="total_revenue")}),
        ("/stream/v3.total_revenue", {"dimensions": ["v3.location.country[from]"]}),
    ],
)
@pytest.mark.asyncio
async def test_streams_without_entries_send_no_warnings(
    client_with_build_v3,
    url,
    params,
):
    async with _capture_submitted_sql(client_with_build_v3) as submitted:
        events = await _stream_events(client_with_build_v3, url, params)
    assert len(submitted) == 1
    assert events == _query_events(submitted[0])
