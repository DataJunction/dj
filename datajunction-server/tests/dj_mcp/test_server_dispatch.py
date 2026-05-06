"""
Unit tests for the MCP ``call_tool`` dispatcher in ``mcp/server.py``.

The dispatcher's job is wiring: route a tool name + arguments dict to the
right ``tools.*`` function with the right keyword arguments. We test it
in isolation by stubbing ``tools`` and asserting the forwarded call shape
plus the wrapped TextContent response.
"""

from typing import List
from unittest.mock import AsyncMock

import mcp.types as types
import pytest

from datajunction_server.mcp import server, tools


@pytest.fixture
def patched_tools(monkeypatch):
    """Stub every tool function on the ``tools`` module so the dispatcher
    routes against fakes instead of real internals."""
    fakes = {
        name: AsyncMock(return_value=f"<{name} result>")
        for name in (
            "list_namespaces",
            "search_nodes",
            "get_node_details",
            "get_common",
            "build_metric_sql",
            "get_metric_data",
            "get_query_plan",
            "get_node_lineage",
            "get_node_dimensions",
        )
    }
    for name, fake in fakes.items():
        monkeypatch.setattr(tools, name, fake)
    # visualize_metrics returns its own list[TextContent], not a str.
    fakes["visualize_metrics"] = AsyncMock(
        return_value=[types.TextContent(type="text", text="<chart>")],
    )
    monkeypatch.setattr(tools, "visualize_metrics", fakes["visualize_metrics"])
    return fakes


@pytest.mark.asyncio
async def test_list_namespaces_dispatch(patched_tools) -> None:
    result = await server.call_tool("list_namespaces", {})
    assert result == [types.TextContent(type="text", text="<list_namespaces result>")]
    patched_tools["list_namespaces"].assert_awaited_once_with()


@pytest.mark.asyncio
async def test_search_nodes_dispatch(patched_tools) -> None:
    result = await server.call_tool(
        "search_nodes",
        {
            "query": "revenue",
            "node_type": "metric",
            "namespace": "finance",
            "tags": ["core"],
            "statuses": ["valid"],
            "mode": "published",
            "owned_by": "alice",
            "has_materialization": True,
            "limit": 10,
            "prefer_main_branch": False,
        },
    )
    assert result == [types.TextContent(type="text", text="<search_nodes result>")]
    patched_tools["search_nodes"].assert_awaited_once_with(
        query="revenue",
        node_type="metric",
        namespace="finance",
        tags=["core"],
        statuses=["valid"],
        mode="published",
        owned_by="alice",
        has_materialization=True,
        limit=10,
        prefer_main_branch=False,
    )


@pytest.mark.asyncio
async def test_get_node_details_dispatch(patched_tools) -> None:
    result = await server.call_tool("get_node_details", {"name": "n.foo"})
    assert result == [types.TextContent(type="text", text="<get_node_details result>")]
    patched_tools["get_node_details"].assert_awaited_once_with(name="n.foo")


@pytest.mark.asyncio
async def test_get_common_dispatch(patched_tools) -> None:
    result = await server.call_tool(
        "get_common",
        {"metrics": ["m1"], "dimensions": None},
    )
    assert result == [types.TextContent(type="text", text="<get_common result>")]
    patched_tools["get_common"].assert_awaited_once_with(
        metrics=["m1"],
        dimensions=None,
    )


@pytest.mark.asyncio
async def test_build_metric_sql_dispatch(patched_tools) -> None:
    result = await server.call_tool(
        "build_metric_sql",
        {
            "metrics": ["m1"],
            "dimensions": ["d1"],
            "filters": ["x = 1"],
            "orderby": ["m1 DESC"],
            "limit": 5,
            "dialect": "spark",
            "use_materialized": False,
        },
    )
    assert result == [types.TextContent(type="text", text="<build_metric_sql result>")]
    patched_tools["build_metric_sql"].assert_awaited_once_with(
        metrics=["m1"],
        dimensions=["d1"],
        filters=["x = 1"],
        orderby=["m1 DESC"],
        limit=5,
        dialect="spark",
        use_materialized=False,
    )


@pytest.mark.asyncio
async def test_get_metric_data_dispatch(patched_tools) -> None:
    result = await server.call_tool(
        "get_metric_data",
        {"metrics": ["m1"], "dimensions": ["d1"], "limit": 10},
    )
    assert result == [types.TextContent(type="text", text="<get_metric_data result>")]
    patched_tools["get_metric_data"].assert_awaited_once_with(
        metrics=["m1"],
        dimensions=["d1"],
        filters=None,
        orderby=None,
        limit=10,
    )


@pytest.mark.asyncio
async def test_get_query_plan_dispatch(patched_tools) -> None:
    result = await server.call_tool(
        "get_query_plan",
        {
            "metrics": ["m1"],
            "include_temporal_filters": True,
            "lookback_window": "3 DAY",
        },
    )
    assert result == [types.TextContent(type="text", text="<get_query_plan result>")]
    patched_tools["get_query_plan"].assert_awaited_once_with(
        metrics=["m1"],
        dimensions=None,
        filters=None,
        dialect=None,
        use_materialized=True,
        include_temporal_filters=True,
        lookback_window="3 DAY",
    )


@pytest.mark.asyncio
async def test_get_node_lineage_dispatch(patched_tools) -> None:
    result = await server.call_tool(
        "get_node_lineage",
        {"node_name": "n.foo", "direction": "upstream", "max_depth": 3},
    )
    assert result == [types.TextContent(type="text", text="<get_node_lineage result>")]
    patched_tools["get_node_lineage"].assert_awaited_once_with(
        node_name="n.foo",
        direction="upstream",
        max_depth=3,
    )


@pytest.mark.asyncio
async def test_get_node_dimensions_dispatch(patched_tools) -> None:
    result = await server.call_tool("get_node_dimensions", {"node_name": "n.foo"})
    assert result == [
        types.TextContent(type="text", text="<get_node_dimensions result>"),
    ]
    patched_tools["get_node_dimensions"].assert_awaited_once_with(node_name="n.foo")


@pytest.mark.asyncio
async def test_visualize_metrics_dispatch(patched_tools) -> None:
    """``visualize_metrics`` returns its own list[TextContent] — the dispatcher
    forwards it directly without wrapping."""
    result = await server.call_tool(
        "visualize_metrics",
        {
            "metrics": ["m1"],
            "dimensions": ["d1"],
            "chart_type": "bar",
            "title": "My chart",
        },
    )
    assert result == [types.TextContent(type="text", text="<chart>")]
    patched_tools["visualize_metrics"].assert_awaited_once_with(
        metrics=["m1"],
        dimensions=["d1"],
        filters=None,
        orderby=None,
        limit=100,
        chart_type="bar",
        title="My chart",
        y_min=None,
    )


@pytest.mark.asyncio
async def test_unknown_tool_dispatch(patched_tools) -> None:
    result = await server.call_tool("does_not_exist", {})
    assert result == [
        types.TextContent(type="text", text="Unknown tool: does_not_exist"),
    ]


@pytest.mark.asyncio
async def test_dispatch_wraps_tool_exceptions(monkeypatch) -> None:
    """A tool raising should produce a TextContent error block, not propagate."""
    boom = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(tools, "list_namespaces", boom)

    result = await server.call_tool("list_namespaces", {})
    assert result == [
        types.TextContent(
            type="text",
            text="Error executing list_namespaces: boom",
        ),
    ]


@pytest.mark.asyncio
async def test_list_resources_returns_empty() -> None:
    """No DJ resources are exposed today."""
    result = await server.list_resources()
    assert result == []


@pytest.mark.asyncio
async def test_read_resource_returns_not_found() -> None:
    """Until DJ exposes resources, every URI is unknown."""
    result = await server.read_resource("dj://anything")
    assert result == "Resource not found: dj://anything"


@pytest.mark.asyncio
async def test_list_tools_returns_full_surface() -> None:
    """The tool surface is the contract every MCP client codes against —
    lock the names so any change is intentional."""
    result: List[types.Tool] = await server.list_tools()
    assert {t.name for t in result} == {
        "list_namespaces",
        "search_nodes",
        "get_node_details",
        "get_common",
        "build_metric_sql",
        "get_metric_data",
        "get_node_lineage",
        "get_node_dimensions",
        "get_query_plan",
        "visualize_metrics",
    }
