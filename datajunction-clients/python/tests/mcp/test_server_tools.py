"""
Tests for MCP server tool execution
"""

from unittest.mock import patch

import pytest

from datajunction.mcp.server import call_tool, list_resources, read_resource


@pytest.mark.asyncio
async def test_call_tool_list_namespaces():
    """Test calling list_namespaces tool"""
    with patch("datajunction.mcp.server.tools.list_namespaces") as mock_list:
        mock_list.return_value = "Namespace 1\nNamespace 2"

        result = await call_tool("list_namespaces", {})

        assert len(result) == 1
        assert result[0].type == "text"
        assert "Namespace" in result[0].text
        mock_list.assert_called_once()


@pytest.mark.asyncio
async def test_call_tool_search_nodes():
    """Test calling search_nodes tool"""
    with patch("datajunction.mcp.server.tools.search_nodes") as mock_search:
        mock_search.return_value = "Found 2 nodes:\n1. metric1\n2. metric2"

        result = await call_tool(
            "search_nodes",
            {
                "query": "revenue",
                "node_type": "metric",
                "namespace": "default",
                "limit": 10,
            },
        )

        assert len(result) == 1
        assert result[0].type == "text"
        assert "Found 2 nodes" in result[0].text
        mock_search.assert_called_once_with(
            query="revenue",
            node_type="metric",
            namespace="default",
            limit=10,
        )


@pytest.mark.asyncio
async def test_call_tool_search_nodes_minimal_args():
    """Test search_nodes with only required args"""
    with patch("datajunction.mcp.server.tools.search_nodes") as mock_search:
        mock_search.return_value = "Results"

        result = await call_tool("search_nodes", {"query": "test"})

        assert len(result) == 1
        mock_search.assert_called_once_with(
            query="test",
            node_type=None,
            namespace=None,
            limit=100,  # default
        )


@pytest.mark.asyncio
async def test_call_tool_get_node_details():
    """Test calling get_node_details tool"""
    with patch("datajunction.mcp.server.tools.get_node_details") as mock_get:
        mock_get.return_value = "Node details for default.revenue"

        result = await call_tool("get_node_details", {"name": "default.revenue"})

        assert len(result) == 1
        assert "Node details" in result[0].text
        mock_get.assert_called_once_with(name="default.revenue")


@pytest.mark.asyncio
async def test_call_tool_get_common_dimensions():
    """Test calling get_common_dimensions tool"""
    with patch("datajunction.mcp.server.tools.get_common_dimensions") as mock_dims:
        mock_dims.return_value = "Common dimensions:\n- date\n- region"

        result = await call_tool(
            "get_common_dimensions",
            {"metric_names": ["metric1", "metric2"]},
        )

        assert len(result) == 1
        assert "Common dimensions" in result[0].text
        mock_dims.assert_called_once_with(metric_names=["metric1", "metric2"])


@pytest.mark.asyncio
async def test_call_tool_build_metric_sql():
    """Test calling build_metric_sql tool"""
    with patch("datajunction.mcp.server.tools.build_metric_sql") as mock_sql:
        mock_sql.return_value = "SELECT * FROM table"

        result = await call_tool(
            "build_metric_sql",
            {
                "metrics": ["default.revenue"],
                "dimensions": ["default.date"],
                "filters": ["date > '2024-01-01'"],
                "orderby": ["date DESC"],
                "limit": 100,
                "dialect": "spark",
                "use_materialized": True,
            },
        )

        assert len(result) == 1
        assert "SELECT" in result[0].text
        mock_sql.assert_called_once_with(
            metrics=["default.revenue"],
            dimensions=["default.date"],
            filters=["date > '2024-01-01'"],
            orderby=["date DESC"],
            limit=100,
            dialect="spark",
            use_materialized=True,
        )


@pytest.mark.asyncio
async def test_call_tool_build_metric_sql_minimal():
    """Test build_metric_sql with minimal args"""
    with patch("datajunction.mcp.server.tools.build_metric_sql") as mock_sql:
        mock_sql.return_value = "SQL"

        result = await call_tool("build_metric_sql", {"metrics": ["metric1"]})

        assert len(result) == 1
        mock_sql.assert_called_once_with(
            metrics=["metric1"],
            dimensions=None,
            filters=None,
            orderby=None,
            limit=None,
            dialect=None,
            use_materialized=True,  # default
        )


@pytest.mark.asyncio
async def test_call_tool_get_metric_data():
    """Test calling get_metric_data tool"""
    with patch("datajunction.mcp.server.tools.get_metric_data") as mock_data:
        mock_data.return_value = "Query Results:\nRow 1: value1\nRow 2: value2"

        result = await call_tool(
            "get_metric_data",
            {
                "metrics": ["default.revenue"],
                "dimensions": ["default.date"],
                "filters": ["region = 'US'"],
                "orderby": ["date"],
                "limit": 50,
                "use_materialized": False,
            },
        )

        assert len(result) == 1
        assert "Query Results" in result[0].text
        mock_data.assert_called_once_with(
            metrics=["default.revenue"],
            dimensions=["default.date"],
            filters=["region = 'US'"],
            orderby=["date"],
            limit=50,
            use_materialized=False,
        )


@pytest.mark.asyncio
async def test_call_tool_get_metric_data_minimal():
    """Test get_metric_data with minimal args"""
    with patch("datajunction.mcp.server.tools.get_metric_data") as mock_data:
        mock_data.return_value = "Data"

        result = await call_tool("get_metric_data", {"metrics": ["metric1"]})

        assert len(result) == 1
        mock_data.assert_called_once_with(
            metrics=["metric1"],
            dimensions=None,
            filters=None,
            orderby=None,
            limit=None,
            use_materialized=True,  # default
        )


@pytest.mark.asyncio
async def test_call_tool_unknown():
    """Test calling unknown tool"""
    result = await call_tool("nonexistent_tool", {})

    assert len(result) == 1
    assert result[0].type == "text"
    assert "Unknown tool: nonexistent_tool" in result[0].text


@pytest.mark.asyncio
async def test_call_tool_error_handling():
    """Test tool execution error handling"""
    with patch("datajunction.mcp.server.tools.search_nodes") as mock_search:
        mock_search.side_effect = Exception("API connection failed")

        result = await call_tool("search_nodes", {"query": "test"})

        assert len(result) == 1
        assert result[0].type == "text"
        assert "Error executing search_nodes" in result[0].text
        assert "API connection failed" in result[0].text


@pytest.mark.asyncio
async def test_call_tool_missing_required_arg():
    """Test tool with missing required argument"""
    with patch("datajunction.mcp.server.tools.get_node_details") as mock_get:
        # Simulate KeyError when required arg is missing
        mock_get.side_effect = KeyError("name")

        result = await call_tool("get_node_details", {})

        assert len(result) == 1
        assert "Error executing" in result[0].text


@pytest.mark.asyncio
async def test_list_resources():
    """Test listing MCP resources"""
    resources = await list_resources()

    assert isinstance(resources, list)
    # Currently returns empty list as resources not implemented
    assert len(resources) == 0


@pytest.mark.asyncio
async def test_read_resource():
    """Test reading MCP resource"""
    result = await read_resource("dj://nodes")

    assert isinstance(result, str)
    assert "not found" in result.lower() or "resource" in result.lower()


@pytest.mark.asyncio
async def test_read_resource_catalog():
    """Test reading catalog resource"""
    result = await read_resource("dj://catalog")

    assert isinstance(result, str)


@pytest.mark.asyncio
async def test_call_tool_with_none_values():
    """Test tool call with explicit None values"""
    with patch("datajunction.mcp.server.tools.build_metric_sql") as mock_sql:
        mock_sql.return_value = "SQL"

        result = await call_tool(
            "build_metric_sql",
            {
                "metrics": ["metric1"],
                "dimensions": None,
                "filters": None,
                "orderby": None,
                "limit": None,
                "dialect": None,
            },
        )

        assert len(result) == 1
        mock_sql.assert_called_once()


@pytest.mark.asyncio
async def test_list_tools_handler():
    """Test the list_tools handler returns correct tool definitions"""
    from datajunction.mcp import server

    # Find and call the list_tools decorated function directly
    # The @app.list_tools() decorator registers the function, but we can still call it
    list_tools_func = None
    for name in dir(server):
        obj = getattr(server, name)
        if callable(obj) and name == "list_tools":
            list_tools_func = obj
            break

    assert list_tools_func is not None, "list_tools function not found"

    # Call the function directly
    tools = await list_tools_func()

    # Verify we get the expected tools
    assert isinstance(tools, list)
    assert len(tools) == 6  # Should have 6 tools
    tool_names = [tool.name for tool in tools]
    assert "list_namespaces" in tool_names
    assert "search_nodes" in tool_names
    assert "get_node_details" in tool_names
    assert "get_common_dimensions" in tool_names
    assert "build_metric_sql" in tool_names
    assert "get_metric_data" in tool_names
