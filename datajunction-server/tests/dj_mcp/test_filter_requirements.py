"""Filter requirements through the MCP tools."""

from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from datajunction_server.construction.build_v3 import prepare
from datajunction_server.mcp import context, server, tools
from datajunction_server.models.query import QueryWithResults
from datajunction_server.typing import QueryState

RULE = "-" * 60
DEFAULT_WARNING = (
    "  • Applied default filter v3.date.date_id = 20250101 (from metric v3.mcp_default)"
)


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


@pytest_asyncio.fixture
async def mcp_client_with_metric(client_with_build_v3, session):
    await _make_metric(
        client_with_build_v3,
        "v3.mcp_default",
        required_filters=[
            {"dimension": "v3.date.date_id", "default": "v3.date.date_id = 20250101"},
        ],
    )
    await _make_metric(
        client_with_build_v3,
        "v3.mcp_required",
        required_filters=[{"dimension": "v3.date.date_id"}],
    )
    await _make_metric(
        client_with_build_v3,
        "v3.mcp_recommended",
        recommended_filters=[{"dimension": "v3.location.country"}],
    )
    token = context._session_var.set(session)
    try:
        yield client_with_build_v3
    finally:
        context._session_var.reset(token)


def _tail(text, marker):
    lines = text.split("\n")
    return lines[lines.index(marker) :]


@pytest.mark.asyncio
async def test_build_metric_sql_prints_applied_default(mcp_client_with_metric):
    text = await tools.build_metric_sql(["v3.mcp_default"], ["v3.location.country"])
    assert _tail(text, "Output Columns:") == [
        "Output Columns:",
        RULE,
        "  • country: string (semantic: v3.location.country)",
        "  • mcp_default: double (semantic: v3.mcp_default)",
        "",
        "Warnings:",
        RULE,
        DEFAULT_WARNING,
    ]
    assert [line for line in text.split("\n") if "o.order_date" in line] == [
        " WHERE  o.order_date = 20250101",
    ]


@pytest.mark.asyncio
async def test_build_metric_sql_prints_recommended_warning(mcp_client_with_metric):
    text = await tools.build_metric_sql(
        ["v3.mcp_recommended"],
        ["v3.location.country"],
    )
    assert _tail(text, "Warnings:") == [
        "Warnings:",
        RULE,
        "  • Missing recommended filter: one of v3.location.country "
        "(recommended by metric v3.mcp_recommended)",
    ]


@pytest.mark.asyncio
async def test_build_metric_sql_without_requirements_has_no_warnings(
    mcp_client_with_metric,
):
    text = await tools.build_metric_sql(
        ["v3.total_revenue"],
        ["v3.location.country"],
        ["v3.date.date_id = 20240101"],
    )
    assert _tail(text, "Output Columns:") == [
        "Output Columns:",
        RULE,
        "  • country: string (semantic: v3.location.country)",
        "  • total_revenue: double (semantic: v3.total_revenue)",
    ]


@pytest.mark.asyncio
async def test_build_metric_sql_surfaces_required_filter_error(
    mcp_client_with_metric,
):
    result = await server.call_tool(
        "build_metric_sql",
        {"metrics": ["v3.mcp_required"], "dimensions": ["v3.location.country"]},
    )
    assert [item.text for item in result] == [
        "Error executing build_metric_sql: Missing required filter: one of "
        "v3.date.date_id (required by metric v3.mcp_required)\n"
        "The following error happened:\n"
        "- Missing required filter: one of v3.date.date_id "
        "(required by metric v3.mcp_required) (error code: 210)",
    ]


@pytest.mark.asyncio
async def test_node_details_list_filter_requirements(mcp_client_with_metric):
    text = await tools.get_node_details("v3.mcp_default")
    lines = text.split("\n")
    start = lines.index("Required Filters:")
    assert lines[start : start + 3] == [
        "Required Filters:",
        "  • v3.date.date_id (default: v3.date.date_id = 20250101)"
        " (from metric v3.mcp_default)",
        "",
    ]
    assert "Recommended Filters:" not in lines


@pytest.mark.asyncio
async def test_node_details_list_recommended_filters(mcp_client_with_metric):
    text = await tools.get_node_details("v3.mcp_recommended")
    lines = text.split("\n")
    start = lines.index("Recommended Filters:")
    assert lines[start : start + 3] == [
        "Recommended Filters:",
        "  • v3.location.country (from metric v3.mcp_recommended)",
        "",
    ]
    assert "Required Filters:" not in lines


@pytest.mark.asyncio
async def test_node_details_list_inherited_filters(mcp_client_with_metric):
    await _make_metric(
        mcp_client_with_metric,
        "v3.mcp_derived",
        required_filters=[{"dimension": "v3.date.date_id"}],
    )
    response = await mcp_client_with_metric.post(
        "/nodes/metric/",
        json={
            "name": "v3.mcp_top",
            "description": "d",
            "query": "SELECT v3.mcp_derived / v3.mcp_recommended",
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.text
    text = await tools.get_node_details("v3.mcp_top")
    assert _tail(text, "Required Filters:")[:6] == [
        "Required Filters:",
        "  • v3.date.date_id (from metric v3.mcp_derived)",
        "",
        "Recommended Filters:",
        "  • v3.location.country (from metric v3.mcp_recommended)",
        "",
    ]


@pytest.mark.asyncio
async def test_node_details_without_requirements_has_no_block(
    mcp_client_with_metric,
):
    text = await tools.get_node_details("v3.total_revenue")
    assert [line for line in text.split("\n") if "Filters:" in line] == []


class _FakeQueryService:
    def __init__(self):
        self.submitted = []

    async def submit_query(self, query_create):
        self.submitted.append(query_create)
        return QueryWithResults(
            id="q-1",
            submitted_query=query_create.submitted_query,
            state=QueryState.FINISHED,
            results=[
                {
                    "sql": query_create.submitted_query,
                    "columns": [],
                    "rows": [["US", 1.0]],
                    "row_count": 1,
                },
            ],
            errors=[],
        )


@pytest.mark.asyncio
async def test_get_metric_data_applies_default_and_prints_warning(
    mcp_client_with_metric,
    monkeypatch,
):
    service = _FakeQueryService()
    monkeypatch.setattr(tools, "get_query_service_client", lambda settings: service)
    text = await tools.get_metric_data(["v3.mcp_default"], ["v3.location.country"])
    assert _tail(text, "Warnings:") == ["Warnings:", RULE, DEFAULT_WARNING]
    assert len(service.submitted) == 1
    submitted_sql = service.submitted[0].submitted_query
    assert [line for line in submitted_sql.split("\n") if "o.order_date" in line] == [
        " WHERE  o.order_date = 20250101",
    ]


@pytest.mark.asyncio
async def test_get_metric_data_uses_effective_filters_for_engine_choice(
    mcp_client_with_metric,
    monkeypatch,
):
    seen = {}

    async def fake_resolve(**kwargs):
        seen["filters"] = kwargs["filters"]
        raise ValueError("stop here")

    monkeypatch.setattr(prepare, "resolve_dialect_and_engine_for_metrics", fake_resolve)
    await tools.get_metric_data(["v3.mcp_default"], ["v3.location.country"])
    assert seen == {"filters": ["v3.date.date_id = 20250101"]}


@pytest.mark.asyncio
async def test_get_metric_data_rejects_missing_required_filter(
    mcp_client_with_metric,
    monkeypatch,
):
    monkeypatch.setattr(tools, "get_query_service_client", lambda settings: MagicMock())
    result = await server.call_tool(
        "get_metric_data",
        {"metrics": ["v3.mcp_required"], "dimensions": ["v3.location.country"]},
    )
    assert [item.text for item in result] == [
        "Error executing get_metric_data: Missing required filter: one of "
        "v3.date.date_id (required by metric v3.mcp_required)\n"
        "The following error happened:\n"
        "- Missing required filter: one of v3.date.date_id "
        "(required by metric v3.mcp_required) (error code: 210)",
    ]
