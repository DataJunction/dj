"""
Tests for MCP tool implementations
"""
import pytest
from unittest.mock import AsyncMock, patch

from datajunction_server.mcp import tools


@pytest.mark.asyncio
async def test_search_nodes_success():
    """Test successful node search"""
    mock_response = {
        "findNodes": [
            {
                "name": "finance.daily_revenue",
                "type": "METRIC",
                "createdAt": "2024-01-01T00:00:00Z",
                "current": {
                    "displayName": "Daily Revenue",
                    "description": "Total revenue per day",
                    "status": "VALID",
                    "mode": "PUBLISHED",
                },
                "tags": [{"name": "finance", "tagType": "category"}],
                "owners": [{"username": "admin", "email": "admin@example.com"}],
            }
        ]
    }

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.search_nodes(query="revenue")

        assert "finance.daily_revenue" in result
        assert "Daily Revenue" in result
        assert "Total revenue per day" in result
        mock_client.query.assert_called_once()


@pytest.mark.asyncio
async def test_search_nodes_not_found():
    """Test node search with no results"""
    mock_response = {"findNodes": []}

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.search_nodes(query="nonexistent")

        assert "No nodes found" in result


@pytest.mark.asyncio
async def test_get_node_details_success():
    """Test getting node details"""
    mock_response = {
        "findNodes": [
            {
                "name": "finance.daily_revenue",
                "type": "METRIC",
                "createdAt": "2024-01-01T00:00:00Z",
                "current": {
                    "displayName": "Daily Revenue",
                    "description": "Total revenue per day",
                    "status": "VALID",
                    "mode": "PUBLISHED",
                    "query": "SELECT date, SUM(amount) FROM transactions GROUP BY date",
                    "metricMetadata": {
                        "direction": "HIGHER_IS_BETTER",
                        "unit": {"name": "currency", "label": "USD"},
                    },
                    "columns": [
                        {"name": "date", "type": "DATE"},
                        {"name": "amount", "type": "DECIMAL"},
                    ],
                    "parents": [],
                },
                "tags": [],
                "owners": [],
            }
        ],
        "commonDimensions": [
            {
                "name": "core.date",
                "type": "DIMENSION",
                "dimensionNode": {
                    "name": "core.date",
                    "current": {
                        "description": "Date dimension",
                        "displayName": "Date",
                    },
                },
            }
        ],
    }

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.get_node_details(name="finance.daily_revenue")

        assert "finance.daily_revenue" in result
        assert "Daily Revenue" in result
        assert "SELECT date, SUM(amount)" in result
        assert "Available Dimensions" in result


@pytest.mark.asyncio
async def test_get_common_dimensions_success():
    """Test getting common dimensions"""
    mock_response = {
        "commonDimensions": [
            {
                "name": "core.date",
                "type": "DIMENSION",
                "dimensionNode": {
                    "name": "core.date",
                    "current": {
                        "description": "Date dimension",
                        "displayName": "Date",
                    },
                },
            },
            {
                "name": "core.region",
                "type": "DIMENSION",
                "dimensionNode": {
                    "name": "core.region",
                    "current": {
                        "description": "Geographic region",
                        "displayName": "Region",
                    },
                },
            },
        ]
    }

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.get_common_dimensions(
            metric_names=["finance.revenue", "growth.users"]
        )

        assert "Found 2 common dimensions" in result
        assert "core.date" in result
        assert "core.region" in result


@pytest.mark.asyncio
async def test_build_metric_sql_success():
    """Test SQL generation"""
    mock_response = {
        "measuresSql": [
            {
                "node": {"name": "finance.daily_revenue"},
                "sql": "SELECT date, region, SUM(amount) as revenue FROM transactions GROUP BY date, region",
                "dialect": "SPARK",
                "columns": [
                    {"name": "date", "type": "DATE"},
                    {"name": "region", "type": "VARCHAR"},
                    {"name": "revenue", "type": "DECIMAL"},
                ],
                "upstreamTables": ["finance.transactions"],
                "errors": [],
            }
        ]
    }

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.build_metric_sql(
            metrics=["finance.daily_revenue"],
            dimensions=["core.date", "core.region"],
        )

        assert "SELECT date, region, SUM(amount)" in result
        assert "Dialect: SPARK" in result
        assert "Output Columns" in result


@pytest.mark.asyncio
async def test_api_error_handling():
    """Test that API errors are handled gracefully"""
    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.side_effect = Exception("Connection failed")
        mock_get_client.return_value = mock_client

        result = await tools.search_nodes(query="test")

        assert "Error" in result
        assert "Connection failed" in result
