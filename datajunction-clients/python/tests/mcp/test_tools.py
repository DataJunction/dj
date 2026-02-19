"""
Tests for MCP tool implementations
"""

import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from datajunction.mcp import tools


# ============================================================================
# DJGraphQLClient Tests
# ============================================================================


@pytest.mark.asyncio
async def test_client_with_api_token():
    """Test client initialization with provided API token"""
    with patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token_123",
            dj_username=None,
            dj_password=None,
            request_timeout=30.0,
        )

        client = tools.DJGraphQLClient()
        await client._ensure_token()

        assert client._token == "test_token_123"
        assert client._token_initialized


@pytest.mark.asyncio
async def test_client_with_username_password_login():
    """Test client initialization with username/password login"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.cookies = {"__dj": "jwt_token_from_login"}
    mock_response.raise_for_status = MagicMock()

    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token=None,
            dj_username="test_user",
            dj_password="test_pass",
            request_timeout=30.0,
        )

        mock_http_client = AsyncMock()
        mock_http_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        client = tools.DJGraphQLClient()
        await client._ensure_token()

        assert client._token == "jwt_token_from_login"
        assert client._token_initialized
        mock_http_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_client_login_failure():
    """Test client handles login failure gracefully"""
    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token=None,
            dj_username="test_user",
            dj_password="wrong_pass",
            request_timeout=30.0,
        )

        mock_http_client = AsyncMock()
        mock_http_client.post.side_effect = httpx.HTTPStatusError(
            "401 Unauthorized",
            request=MagicMock(),
            response=MagicMock(status_code=401, text="Invalid credentials"),
        )
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        client = tools.DJGraphQLClient()
        await client._ensure_token()

        # Should not have token but should be initialized
        assert client._token is None
        assert client._token_initialized


@pytest.mark.asyncio
async def test_client_query_with_graphql_errors():
    """Test client handles GraphQL errors"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "errors": [{"message": "Node not found"}, {"message": "Invalid query"}],
    }
    mock_response.raise_for_status = MagicMock()

    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_http_client = AsyncMock()
        mock_http_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        client = tools.DJGraphQLClient()

        with pytest.raises(Exception, match="GraphQL errors"):
            await client.query("query { test }")


@pytest.mark.asyncio
async def test_client_query_http_error():
    """Test client handles HTTP errors"""
    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_http_client = AsyncMock()
        mock_http_client.post.side_effect = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=MagicMock(),
            response=MagicMock(status_code=500, text="Server error"),
        )
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        client = tools.DJGraphQLClient()

        with pytest.raises(Exception, match="API request failed: 500"):
            await client.query("query { test }")


@pytest.mark.asyncio
async def test_client_query_request_error():
    """Test client handles connection errors"""
    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_http_client = AsyncMock()
        mock_http_client.post.side_effect = httpx.ConnectError(
            "Connection refused",
        )
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        client = tools.DJGraphQLClient()

        with pytest.raises(Exception, match="Failed to connect to DJ API"):
            await client.query("query { test }")


# ============================================================================
# list_namespaces Tests
# ============================================================================


@pytest.mark.asyncio
async def test_list_namespaces_success():
    """Test successful namespace listing"""
    mock_response = {
        "findNodes": [
            {"name": "finance.metrics.revenue"},
            {"name": "finance.metrics.cost"},
            {"name": "finance.dimensions.date"},
            {"name": "core.dimensions.region"},
            {"name": "core.dimensions.country"},
            {"name": "core.dimensions.city"},
        ],
    }

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.list_namespaces()

        assert "Available Namespaces:" in result
        assert "finance.metrics (2 nodes)" in result
        assert "finance.dimensions (1 nodes)" in result
        assert "core.dimensions (3 nodes)" in result


@pytest.mark.asyncio
async def test_list_namespaces_empty():
    """Test namespace listing with no nodes"""
    mock_response = {"findNodes": []}

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.list_namespaces()

        assert "No namespaces found" in result


@pytest.mark.asyncio
async def test_list_namespaces_error():
    """Test namespace listing error handling"""
    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.side_effect = Exception("API error")
        mock_get_client.return_value = mock_client

        result = await tools.list_namespaces()

        assert "Error" in result
        assert "API error" in result


# ============================================================================
# search_nodes Tests
# ============================================================================


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
            },
        ],
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
async def test_search_nodes_with_filters():
    """Test node search with type and namespace filters"""
    mock_response = {"findNodes": []}

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.search_nodes(
            query="revenue",
            node_type="metric",
            namespace="finance",
            limit=50,
        )

        # Verify the GraphQL query was called with correct variables
        call_args = mock_client.query.call_args
        variables = call_args[0][1]
        assert variables["fragment"] == "revenue"
        assert variables["nodeTypes"] == ["METRIC"]
        assert variables["namespace"] == "finance"
        assert variables["limit"] == 50

        # Verify result format
        assert "No nodes found" in result


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
async def test_search_nodes_error():
    """Test node search error handling"""
    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.side_effect = Exception("Connection failed")
        mock_get_client.return_value = mock_client

        result = await tools.search_nodes(query="test")

        assert "Error" in result
        assert "Connection failed" in result


# ============================================================================
# get_node_details Tests
# ============================================================================


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
                        {"name": "date", "type": "DATE", "displayName": "Date"},
                        {"name": "amount", "type": "DECIMAL", "displayName": "Amount"},
                    ],
                    "parents": [
                        {"name": "finance.transactions", "type": "SOURCE"},
                    ],
                },
                "tags": [
                    {
                        "name": "finance",
                        "tagType": "category",
                        "description": "Finance metrics",
                    },
                ],
                "owners": [
                    {
                        "username": "admin",
                        "email": "admin@example.com",
                        "name": "Admin User",
                    },
                ],
            },
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
            },
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
        assert "core.date" in result


@pytest.mark.asyncio
async def test_get_node_details_not_found():
    """Test getting details for non-existent node"""
    mock_response = {"findNodes": [], "commonDimensions": []}

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.get_node_details(name="nonexistent.node")

        assert "not found" in result


@pytest.mark.asyncio
async def test_get_node_details_error():
    """Test node details error handling"""
    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.side_effect = Exception("GraphQL error")
        mock_get_client.return_value = mock_client

        result = await tools.get_node_details(name="test.node")

        assert "Error" in result
        assert "GraphQL error" in result


# ============================================================================
# get_common_dimensions Tests
# ============================================================================


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
        ],
    }

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.get_common_dimensions(
            metric_names=["finance.revenue", "growth.users"],
        )

        assert "Found 2 common dimensions" in result
        assert "core.date" in result
        assert "core.region" in result


@pytest.mark.asyncio
async def test_get_common_dimensions_none():
    """Test getting common dimensions when none exist"""
    mock_response = {"commonDimensions": []}

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.get_common_dimensions(
            metric_names=["finance.revenue", "growth.users"],
        )

        assert "No common dimensions found" in result


@pytest.mark.asyncio
async def test_get_common_dimensions_error():
    """Test common dimensions error handling"""
    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.side_effect = Exception("Query failed")
        mock_get_client.return_value = mock_client

        result = await tools.get_common_dimensions(
            metric_names=["finance.revenue"],
        )

        assert "Error" in result
        assert "Query failed" in result


# ============================================================================
# build_metric_sql Tests
# ============================================================================


@pytest.mark.asyncio
async def test_build_metric_sql_success():
    """Test SQL generation"""
    mock_response_json = {
        "sql": "SELECT date, region, SUM(amount) as revenue FROM transactions GROUP BY date, region",
        "dialect": "spark",
        "cube_name": "finance.revenue_cube",
        "columns": [
            {"name": "date", "type": "DATE", "semantic_name": "core.date"},
            {"name": "region", "type": "VARCHAR", "semantic_name": "core.region"},
            {"name": "revenue", "type": "DECIMAL", "semantic_name": "finance.revenue"},
        ],
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_json
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.build_metric_sql(
            metrics=["finance.daily_revenue"],
            dimensions=["core.date", "core.region"],
            filters=["date >= '2024-01-01'"],
            orderby=["date DESC"],
            limit=100,
            dialect="spark",
        )

        assert "SELECT date, region, SUM(amount)" in result
        assert "Dialect: spark" in result
        assert "Cube: finance.revenue_cube" in result
        assert "Output Columns" in result
        assert "semantic: core.date" in result


@pytest.mark.asyncio
async def test_build_metric_sql_http_error():
    """Test SQL generation HTTP error handling"""
    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = httpx.HTTPStatusError(
            "400 Bad Request",
            request=MagicMock(),
            response=MagicMock(status_code=400, text="Invalid metric"),
        )
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.build_metric_sql(metrics=["invalid.metric"])

        assert "Error" in result
        assert "400" in result


@pytest.mark.asyncio
async def test_build_metric_sql_generic_error():
    """Test SQL generation generic error handling"""
    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock(side_effect=Exception("Connection lost"))
        mock_get_client.return_value = mock_client

        result = await tools.build_metric_sql(metrics=["finance.revenue"])

        assert "Error" in result
        assert "Connection lost" in result


# ============================================================================
# get_metric_data Tests
# ============================================================================


@pytest.mark.asyncio
async def test_get_metric_data_success():
    """Test getting metric data"""
    # Mock SQL response for materialization check
    mock_sql_response = MagicMock()
    mock_sql_response.status_code = 200
    mock_sql_response.json.return_value = {
        "sql": "SELECT * FROM preagg_cube",  # Contains "preagg" = materialized
        "columns": [],
        "dialect": "spark",
    }
    mock_sql_response.raise_for_status = MagicMock()

    # Mock data response
    mock_response_json = {
        "id": "query_123",
        "state": "FINISHED",
        "results": [
            {"date": "2024-01-01", "region": "US", "revenue": 10000},
            {"date": "2024-01-02", "region": "US", "revenue": 12000},
            {"date": "2024-01-01", "region": "EU", "revenue": 8000},
        ],
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_json
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = [mock_sql_response, mock_response]
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_metric_data(
            metrics=["finance.daily_revenue"],
            dimensions=["core.date", "core.region"],
            limit=100,
        )

        assert "Query Results:" in result
        assert "Query State: FINISHED" in result
        assert "Query ID: query_123" in result
        assert "Row Count: 3" in result
        assert "2024-01-01" in result
        assert "revenue: 10000" in result


@pytest.mark.asyncio
async def test_get_metric_data_many_rows():
    """Test getting metric data with more than 10 rows (shows truncation)"""
    # Mock SQL response for materialization check
    mock_sql_response = MagicMock()
    mock_sql_response.status_code = 200
    mock_sql_response.json.return_value = {
        "sql": "SELECT * FROM preagg_cube",  # Contains "preagg" = materialized
        "columns": [],
        "dialect": "spark",
    }
    mock_sql_response.raise_for_status = MagicMock()

    # Mock data response
    mock_results = [
        {"date": f"2024-01-{i:02d}", "revenue": i * 1000} for i in range(1, 21)
    ]
    mock_response_json = {
        "id": "query_456",
        "state": "FINISHED",
        "results": mock_results,
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_json
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = [mock_sql_response, mock_response]
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_metric_data(metrics=["finance.revenue"])

        assert "Row Count: 20" in result
        assert "... and 10 more rows" in result


@pytest.mark.asyncio
async def test_get_metric_data_no_results():
    """Test getting metric data with no results"""
    # Mock SQL response for materialization check
    mock_sql_response = MagicMock()
    mock_sql_response.status_code = 200
    mock_sql_response.json.return_value = {
        "sql": "SELECT * FROM preagg_cube",  # Contains "preagg" = materialized
        "columns": [],
        "dialect": "spark",
    }
    mock_sql_response.raise_for_status = MagicMock()

    # Mock data response
    mock_response_json = {
        "id": "query_789",
        "state": "FINISHED",
        "results": [],
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_json
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = [mock_sql_response, mock_response]
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_metric_data(metrics=["finance.revenue"])

        assert "No results returned" in result


@pytest.mark.asyncio
async def test_get_metric_data_with_errors():
    """Test getting metric data with errors in response"""
    # Mock SQL response for materialization check
    mock_sql_response = MagicMock()
    mock_sql_response.status_code = 200
    mock_sql_response.json.return_value = {
        "sql": "SELECT * FROM preagg_cube",  # Contains "preagg" = materialized
        "columns": [],
        "dialect": "spark",
    }
    mock_sql_response.raise_for_status = MagicMock()

    # Mock data response
    mock_response_json = {
        "id": "query_error",
        "state": "FAILED",
        "results": [],
        "errors": ["Invalid dimension", "Query timeout"],
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_json
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = [mock_sql_response, mock_response]
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_metric_data(metrics=["finance.revenue"])

        assert "Errors:" in result
        assert "Invalid dimension" in result
        assert "Query timeout" in result


@pytest.mark.asyncio
async def test_get_metric_data_http_error():
    """Test metric data HTTP error handling"""
    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=MagicMock(),
            response=MagicMock(status_code=500, text="Database error"),
        )
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_metric_data(metrics=["finance.revenue"])

        assert "Error" in result
        assert "500" in result


@pytest.mark.asyncio
async def test_get_metric_data_generic_error():
    """Test metric data generic error handling"""
    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock(side_effect=Exception("Network error"))
        mock_get_client.return_value = mock_client

        result = await tools.get_metric_data(metrics=["finance.revenue"])

        assert "Error" in result
        assert "Network error" in result


# ============================================================================
# Additional Edge Cases for Full Coverage
# ============================================================================


@pytest.mark.asyncio
async def test_client_with_no_credentials():
    """Test client initialization without any credentials"""
    with patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token=None,
            dj_username=None,
            dj_password=None,
            request_timeout=30.0,
        )

        client = tools.DJGraphQLClient()
        await client._ensure_token()

        assert client._token is None
        assert client._token_initialized


@pytest.mark.asyncio
async def test_client_login_no_token_in_cookie():
    """Test client handles login success but no __dj cookie"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.cookies = {}  # No __dj cookie
    mock_response.text = "Login successful"
    mock_response.raise_for_status = MagicMock()

    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token=None,
            dj_username="test_user",
            dj_password="test_pass",
            request_timeout=30.0,
        )

        mock_http_client = AsyncMock()
        mock_http_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        client = tools.DJGraphQLClient()
        await client._ensure_token()

        assert client._token is None
        assert client._token_initialized


@pytest.mark.asyncio
async def test_client_login_generic_exception():
    """Test client handles generic exception during login"""
    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token=None,
            dj_username="test_user",
            dj_password="test_pass",
            request_timeout=30.0,
        )

        mock_http_client = AsyncMock()
        mock_http_client.post.side_effect = Exception("Network timeout")
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        client = tools.DJGraphQLClient()
        await client._ensure_token()

        assert client._token is None
        assert client._token_initialized


@pytest.mark.asyncio
async def test_client_get_headers_without_token():
    """Test _get_headers when no token is available"""
    with patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token=None,
            request_timeout=30.0,
        )

        client = tools.DJGraphQLClient()
        headers = client._get_headers()

        assert "Authorization" not in headers
        assert headers["Content-Type"] == "application/json"


@pytest.mark.asyncio
async def test_client_get_auth():
    """Test _get_auth returns None (deprecated method)"""
    with patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )

        client = tools.DJGraphQLClient()
        auth = client._get_auth()

        assert auth is None


@pytest.mark.asyncio
async def test_get_client_singleton():
    """Test get_client returns same instance (singleton pattern)"""
    # Reset the global client
    tools._client = None

    client1 = tools.get_client()
    client2 = tools.get_client()

    assert client1 is client2

    # Clean up
    tools._client = None


@pytest.mark.asyncio
async def test_client_query_success_with_data():
    """Test successful GraphQL query with data"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": {"findNodes": []},
    }
    mock_response.raise_for_status = MagicMock()

    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_http_client = AsyncMock()
        mock_http_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        client = tools.DJGraphQLClient()
        result = await client.query("query { test }")

        assert result == {"findNodes": []}


# ============================================================================
# Edge Case Tests for Better Coverage
# ============================================================================


@pytest.mark.asyncio
async def test_client_token_already_initialized():
    """Test that _ensure_token returns early when token is already initialized"""
    with patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="existing_token",
            dj_username=None,
            dj_password=None,
        )

        client = tools.DJGraphQLClient()
        # First initialization
        await client._ensure_token()
        assert client._token == "existing_token"

        # Mark as initialized
        client._token_initialized = True

        # Second call should return early (covers lines 45-48)
        await client._ensure_token()

        # Token should remain the same
        assert client._token == "existing_token"


@pytest.mark.asyncio
async def test_build_metric_sql_api_error():
    """Test build_metric_sql when API returns error"""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Server error",
        request=MagicMock(),
        response=mock_response,
    )

    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client

        client = tools.DJGraphQLClient()

        # Should raise exception (covers error path lines 498->500)
        with pytest.raises(Exception):
            await client.build_metric_sql(
                metrics=["default.revenue"],
            )


@pytest.mark.asyncio
async def test_build_metric_sql_with_error_response():
    """Test build_metric_sql when SQL contains errors"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "sql": [
            {
                "sql": "SELECT * FROM table",
                "errors": [
                    {"message": "Table not found"},
                ],
            },
        ],
    }
    mock_response.raise_for_status = MagicMock()

    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        result = await tools.build_metric_sql(metrics=["test.metric"])

        assert "Error occurred" in result
        assert "Context: Building SQL for metrics: test.metric" in result


@pytest.mark.asyncio
async def test_get_metric_data_query_failed():
    """Test get_metric_data when query execution fails"""
    # Mock SQL response for materialization check
    mock_sql_response = MagicMock()
    mock_sql_response.status_code = 200
    mock_sql_response.json.return_value = {
        "sql": "SELECT * FROM preagg_cube",  # Contains "preagg" = materialized
        "columns": [],
        "dialect": "spark",
    }
    mock_sql_response.raise_for_status = MagicMock()

    # Mock data response with FAILED state
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "state": "FAILED",
        "errors": ["Query execution failed: timeout"],
        "results": [],
    }
    mock_response.raise_for_status = MagicMock()

    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_client = AsyncMock()
        mock_client.get.side_effect = [mock_sql_response, mock_response]
        mock_client_class.return_value.__aenter__.return_value = mock_client

        result = await tools.get_metric_data(metrics=["test.metric"])

        # Should handle failed state (covers lines 579->583)
        assert "FAILED" in result or "failed" in result.lower()


@pytest.mark.asyncio
async def test_get_metric_data_network_error():
    """Test get_metric_data when network error occurs"""
    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.ConnectError("Connection refused")
        mock_client_class.return_value.__aenter__.return_value = mock_client

        client = tools.DJGraphQLClient()

        # Should raise exception
        with pytest.raises(Exception):
            await client.get_metric_data(metrics=["test.metric"])


@pytest.mark.asyncio
async def test_graphql_query_with_http_error():
    """Test GraphQL query when HTTP error occurs"""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Not found",
        request=MagicMock(),
        response=mock_response,
    )

    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client

        client = tools.DJGraphQLClient()

        with pytest.raises(Exception):
            await client.query("{ nodes { name } }")


@pytest.mark.asyncio
async def test_build_metric_sql_empty_response():
    """Test build_metric_sql with empty SQL response"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"sql": []}
    mock_response.raise_for_status = MagicMock()

    with (
        patch("datajunction.mcp.tools.get_mcp_settings") as mock_settings,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
            dj_api_token="test_token",
            request_timeout=30.0,
        )

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        result = await tools.build_metric_sql(metrics=["test.metric"])
        assert "❌ Error occurred" in result


@pytest.mark.asyncio
async def test_build_metric_sql_columns_with_semantic_name():
    """Test build_metric_sql explicitly with columns that have semantic_name"""
    mock_response_json = {
        "sql": "SELECT city, count FROM table",
        "dialect": "spark",
        "columns": [
            {"name": "city", "type": "STRING", "semantic_name": "location.city"},
            {"name": "count", "type": "INTEGER"},  # No semantic_name
        ],
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_json
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.build_metric_sql(metrics=["test.metric"])

        # Verify both columns are in output
        assert "city: STRING (semantic: location.city)" in result
        assert "count: INTEGER" in result
        assert "(semantic: location.city)" in result  # Explicit check for semantic info


# ============================================================================
# get_node_lineage Tests
# ============================================================================


@pytest.mark.asyncio
async def test_get_node_lineage_both_directions():
    """Test get_node_lineage with both upstream and downstream"""
    upstream_response = MagicMock()
    upstream_response.status_code = 200
    upstream_response.json.return_value = [
        {"name": "upstream1", "type": "source", "status": "valid"},
        {"name": "upstream2", "type": "transform", "status": "valid"},
    ]
    upstream_response.raise_for_status = MagicMock()

    downstream_response = MagicMock()
    downstream_response.status_code = 200
    downstream_response.json.return_value = [
        {"name": "downstream1", "type": "metric", "status": "valid"},
    ]
    downstream_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = [upstream_response, downstream_response]
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_lineage(
            node_name="test.node",
            direction="both",
        )

        assert "Upstream Dependencies (2 nodes)" in result
        assert "upstream1 (source)" in result
        assert "upstream2 (transform)" in result
        assert "Downstream Dependencies (1 nodes)" in result
        assert "downstream1 (metric)" in result


@pytest.mark.asyncio
async def test_get_node_lineage_upstream_only():
    """Test get_node_lineage with upstream only"""
    upstream_response = MagicMock()
    upstream_response.status_code = 200
    upstream_response.json.return_value = [
        {"name": "upstream1", "type": "source", "status": "valid"},
    ]
    upstream_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = upstream_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_lineage(
            node_name="test.node",
            direction="upstream",
        )

        assert "Upstream Dependencies" in result
        assert "upstream1 (source)" in result
        assert "Downstream Dependencies" not in result


@pytest.mark.asyncio
async def test_get_node_lineage_with_max_depth():
    """Test get_node_lineage with max_depth parameter"""
    upstream_response = MagicMock()
    upstream_response.status_code = 200
    upstream_response.json.return_value = []
    upstream_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = upstream_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_lineage(
            node_name="test.node",
            direction="upstream",
            max_depth=2,
        )

        # Verify max_depth is passed in params
        mock_http_client.get.assert_called_once()
        call_kwargs = mock_http_client.get.call_args
        assert call_kwargs[1]["params"]["max_depth"] == 2
        assert "Upstream Dependencies" in result


@pytest.mark.asyncio
async def test_get_node_lineage_error():
    """Test get_node_lineage handles errors gracefully"""
    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = httpx.HTTPStatusError(
            "404 Not Found",
            request=MagicMock(),
            response=MagicMock(status_code=404, text="Node not found"),
        )
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_lineage(node_name="nonexistent.node")

        assert "❌ Error occurred" in result
        assert "Getting lineage for node: nonexistent.node" in result


# ============================================================================
# get_node_dimensions Tests
# ============================================================================


@pytest.mark.asyncio
async def test_get_node_dimensions_success():
    """Test get_node_dimensions returns formatted dimension list"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"name": "dim1.attr1", "type": "dimension", "path": ["node1", "dim1"]},
        {"name": "dim2.attr2", "type": "dimension", "path": ["node1", "node2", "dim2"]},
        {"name": "dim3.attr3", "type": "dimension"},
    ]
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_dimensions(node_name="test.node")

        assert "Dimensions for: test.node" in result
        assert "Total: 3 dimensions" in result
        assert "dim1.attr1 (dimension)" in result
        assert "via: node1 → dim1" in result
        assert "dim2.attr2 (dimension)" in result
        assert "via: node1 → node2 → dim2" in result
        assert "dim3.attr3 (dimension)" in result


@pytest.mark.asyncio
async def test_get_node_dimensions_empty():
    """Test get_node_dimensions with no dimensions available"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_dimensions(node_name="test.source")

        assert "Total: 0 dimensions" in result
        assert "(no dimensions available)" in result


@pytest.mark.asyncio
async def test_get_node_dimensions_error():
    """Test get_node_dimensions handles errors gracefully"""
    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = httpx.HTTPStatusError(
            "404 Not Found",
            request=MagicMock(),
            response=MagicMock(status_code=404, text="Node not found"),
        )
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_dimensions(node_name="nonexistent.node")

        assert "❌ Error occurred" in result
        assert "Getting dimensions for node: nonexistent.node" in result


@pytest.mark.asyncio
async def test_list_namespaces_with_single_part_names():
    """Test list_namespaces with nodes that have no namespace (no dots)"""
    mock_response = {
        "findNodes": [
            {"name": "node1"},  # No dot - should be ignored
            {"name": "default.metric1"},
            {"name": "default.metric2"},
        ],
    }

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.list_namespaces()

        # Should only count "default" namespace, ignore "node1"
        assert "default (2 nodes)" in result
        assert "node1" not in result


@pytest.mark.asyncio
async def test_search_nodes_with_git_info():
    """Test search_nodes formatting with git repository info"""
    mock_response = {
        "findNodes": [
            {
                "name": "finance.revenue",
                "type": "metric",
                "currentVersion": {
                    "displayName": "Revenue",
                    "description": "Total revenue",
                    "status": "valid",
                    "mode": "published",
                },
                "gitInfo": {
                    "repo": "company/finance-metrics",
                    "branch": "main",
                },
                "tags": [],
            },
        ],
    }

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.search_nodes(query="revenue")

        assert "[git: company/finance-metrics @ main]" in result


@pytest.mark.asyncio
async def test_search_nodes_namespace_resolution_to_main():
    """Test that namespace gets resolved to .main when it exists"""
    search_response = {
        "findNodes": [
            {
                "name": "finance.main.revenue",
                "type": "metric",
                "currentVersion": {
                    "status": "valid",
                    "mode": "published",
                },
                "tags": [],
            },
        ],
    }

    with (
        patch("datajunction.mcp.tools.list_namespaces") as mock_list,
        patch.object(tools, "get_client") as mock_get_client,
    ):
        # Return format that matches the regex pattern (without bullet points at line start)
        mock_list.return_value = (
            "Available Namespaces:\n\nfinance.main (1 nodes)\nfinance.feature (1 nodes)"
        )
        mock_client = AsyncMock()
        mock_client.query.return_value = search_response
        mock_get_client.return_value = mock_client

        result = await tools.search_nodes(
            query="revenue",
            namespace="finance",
            prefer_main_branch=True,
        )

        assert "finance.main.revenue" in result


@pytest.mark.asyncio
async def test_search_nodes_namespace_resolution_exception():
    """Test namespace resolution handles exceptions gracefully"""
    search_response = {
        "findNodes": [
            {
                "name": "finance.revenue",
                "type": "metric",
                "currentVersion": {"status": "valid", "mode": "published"},
                "tags": [],
            },
        ],
    }

    with (
        patch("datajunction.mcp.tools.list_namespaces") as mock_list,
        patch.object(tools, "get_client") as mock_get_client,
    ):
        mock_list.side_effect = Exception("Connection failed")
        mock_client = AsyncMock()
        mock_client.query.return_value = search_response
        mock_get_client.return_value = mock_client

        # Should continue with original namespace despite exception
        result = await tools.search_nodes(
            query="revenue",
            namespace="finance",
            prefer_main_branch=True,
        )

        assert "finance.revenue" in result


@pytest.mark.asyncio
async def test_search_nodes_namespace_resolution_no_match():
    """Test namespace resolution when list_namespaces returns non-matching lines"""
    search_response = {
        "findNodes": [
            {
                "name": "finance.revenue",
                "type": "metric",
                "currentVersion": {"status": "valid", "mode": "published"},
                "tags": [],
            },
        ],
    }

    with (
        patch("datajunction.mcp.tools.list_namespaces") as mock_list,
        patch.object(tools, "get_client") as mock_get_client,
    ):
        # Return text that doesn't match the pattern
        mock_list.return_value = "Available Namespaces:\n\nNo namespaces found"
        mock_client = AsyncMock()
        mock_client.query.return_value = search_response
        mock_get_client.return_value = mock_client

        result = await tools.search_nodes(
            query="revenue",
            namespace="finance",
            prefer_main_branch=True,
        )

        # Should use original namespace when no match found
        assert "finance.revenue" in result


@pytest.mark.asyncio
async def test_get_node_lineage_downstream_with_max_depth():
    """Test get_node_lineage with downstream direction and max_depth"""
    downstream_response = MagicMock()
    downstream_response.status_code = 200
    downstream_response.json.return_value = [
        {"name": "downstream1", "type": "metric", "status": "valid"},
    ]
    downstream_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = downstream_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_lineage(
            node_name="test.node",
            direction="downstream",
            max_depth=3,
        )

        assert "Downstream Dependencies (1 nodes)" in result
        assert "downstream1" in result
        # Verify max_depth was passed in params
        mock_http_client.get.assert_called_once()
        call_kwargs = mock_http_client.get.call_args
        assert call_kwargs[1]["params"]["max_depth"] == 3


@pytest.mark.asyncio
async def test_get_node_lineage_no_downstream():
    """Test get_node_lineage with empty downstream results"""
    downstream_response = MagicMock()
    downstream_response.status_code = 200
    downstream_response.json.return_value = []  # Empty list
    downstream_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = downstream_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_lineage(
            node_name="test.node",
            direction="downstream",
        )

        assert "Downstream Dependencies (0 nodes)" in result
        assert "(none)" in result


@pytest.mark.asyncio
async def test_get_node_lineage_general_exception():
    """Test get_node_lineage handles non-HTTP exceptions"""
    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = ValueError("Invalid data")
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_lineage(node_name="test.node")

        assert "❌ Error occurred" in result
        assert "Invalid data" in result


@pytest.mark.asyncio
async def test_get_node_dimensions_with_type():
    """Test get_node_dimensions with dimension type field"""
    dimensions_response = MagicMock()
    dimensions_response.status_code = 200
    dimensions_response.json.return_value = [
        {
            "name": "date",
            "type": "temporal",
            "path": ["orders", "date_dim"],
        },
    ]
    dimensions_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = dimensions_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_dimensions(node_name="test.metric")

        assert "date (temporal)" in result
        assert "orders → date_dim" in result


@pytest.mark.asyncio
async def test_get_node_dimensions_general_exception():
    """Test get_node_dimensions handles non-HTTP exceptions"""
    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.side_effect = TypeError("Type error")
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_dimensions(node_name="test.node")

        assert "❌ Error occurred" in result
        assert "Type error" in result


@pytest.mark.asyncio
async def test_list_namespaces_with_none_names():
    """Test list_namespaces with nodes that have None names"""
    mock_response = {
        "findNodes": [
            {"name": None},  # None name - should be skipped
            {"name": ""},  # Empty name - should be skipped
            {"name": "default.metric1"},
        ],
    }

    with patch.object(tools, "get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.query.return_value = mock_response
        mock_get_client.return_value = mock_client

        result = await tools.list_namespaces()

        # Should only count "default" namespace
        assert "default (1 nodes)" in result


@pytest.mark.asyncio
async def test_search_nodes_namespace_no_main_branch():
    """Test namespace resolution when .main branch doesn't exist"""
    search_response = {
        "findNodes": [
            {
                "name": "finance.revenue",
                "type": "metric",
                "currentVersion": {"status": "valid", "mode": "published"},
                "tags": [],
            },
        ],
    }

    with (
        patch("datajunction.mcp.tools.list_namespaces") as mock_list,
        patch.object(tools, "get_client") as mock_get_client,
    ):
        # Return namespaces without .main variant
        mock_list.return_value = (
            "  • finance.dev (1 nodes)\n  • finance.staging (1 nodes)"
        )
        mock_client = AsyncMock()
        mock_client.query.return_value = search_response
        mock_get_client.return_value = mock_client

        result = await tools.search_nodes(
            query="revenue",
            namespace="finance",
            prefer_main_branch=True,
        )

        # Should use original namespace when .main doesn't exist
        assert "finance.revenue" in result


@pytest.mark.asyncio
async def test_get_node_dimensions_without_type_but_with_path():
    """Test get_node_dimensions with dimension that has path but no type"""
    dimensions_response = MagicMock()
    dimensions_response.status_code = 200
    dimensions_response.json.return_value = [
        {
            "name": "region",
            # No type field
            "path": ["orders", "location", "region"],
        },
    ]
    dimensions_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_client._get_headers = MagicMock(return_value={})
        mock_get_client.return_value = mock_client

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = dimensions_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_node_dimensions(node_name="test.metric")

        # Should show dimension name and path, but not type
        assert "region" in result
        assert "orders → location → region" in result
        # Should not have type in parentheses
        assert "region (" not in result
