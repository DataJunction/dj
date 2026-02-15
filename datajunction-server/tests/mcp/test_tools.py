"""
Tests for MCP tool implementations
"""

import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from datajunction_server.mcp import tools


# ============================================================================
# DJGraphQLClient Tests
# ============================================================================


@pytest.mark.asyncio
async def test_client_with_api_token():
    """Test client initialization with provided API token"""
    with patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings:
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
        patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings,
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
        patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings,
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
        patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings,
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
        patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings,
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
        patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings,
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
            {"namespace": "finance.metrics"},
            {"namespace": "finance.metrics"},
            {"namespace": "finance.dimensions"},
            {"namespace": "core.dimensions"},
            {"namespace": "core.dimensions"},
            {"namespace": "core.dimensions"},
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
        mock_http_client.get.return_value = mock_response
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
        mock_http_client.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_metric_data(metrics=["finance.revenue"])

        assert "Row Count: 20" in result
        assert "... and 10 more rows" in result


@pytest.mark.asyncio
async def test_get_metric_data_no_results():
    """Test getting metric data with no results"""
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
        mock_http_client.get.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.get_metric_data(metrics=["finance.revenue"])

        assert "No results returned" in result


@pytest.mark.asyncio
async def test_get_metric_data_with_errors():
    """Test getting metric data with errors in response"""
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
        mock_http_client.get.return_value = mock_response
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
    with patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings:
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
        patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings,
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
        patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings,
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
    with patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings:
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
    with patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings:
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
        patch("datajunction_server.mcp.tools.get_mcp_settings") as mock_settings,
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
