"""
Tests for visualize_metrics MCP tool
"""

import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from datajunction.mcp import tools
import mcp.types as types


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_plotext():
    """Fixture to create a mock plotext module"""
    mock_plt = MagicMock()
    # Set return values to None for most methods (like real plotext)
    mock_plt.clear_figure.return_value = None
    mock_plt.plot.return_value = None
    mock_plt.bar.return_value = None
    mock_plt.scatter.return_value = None
    mock_plt.title.return_value = None
    mock_plt.xlabel.return_value = None
    mock_plt.ylabel.return_value = None
    mock_plt.ylim.return_value = None
    mock_plt.xticks.return_value = None
    mock_plt.legend.return_value = None
    mock_plt.plot_size.return_value = None
    # build() returns the chart string
    mock_plt.build.return_value = "Mock ASCII Chart\n===\n"
    return mock_plt


# ============================================================================
# visualize_metrics Tests
# ============================================================================


@pytest.mark.asyncio
async def test_visualize_metrics_success(mock_plotext):
    """Test successful visualization with valid data"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    ["20251215", 100.5],
                    ["20251216", 105.2],
                    ["20251217", 103.8],
                    ["20251218", 110.1],
                    ["20251219", 108.5],
                ],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "revenue", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(
            metrics=["demo.revenue"],
            dimensions=["common.date"],
            limit=10,
        )

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Mock ASCII Chart" in result[0].text
        assert "revenue" in result[0].text
        assert "5 points" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_with_y_min(mock_plotext):
    """Test visualization with custom y_min parameter"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [["20251215", 100.5], ["20251216", 105.2]],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        mock_plotext.clear_figure = MagicMock()
        mock_plotext.plot = MagicMock()
        mock_plotext.title = MagicMock()
        mock_plotext.xlabel = MagicMock()
        mock_plotext.ylabel = MagicMock()
        mock_plotext.ylim = MagicMock()
        mock_plotext.plot_size = MagicMock()
        mock_plotext.build.return_value = "Chart"

        result = await tools.visualize_metrics(
            metrics=["demo.metric"],
            y_min=0,
        )

        # Verify ylim was called with 0
        mock_plotext.ylim.assert_called_once_with(0, None)
        assert len(result) == 1


@pytest.mark.asyncio
async def test_visualize_metrics_no_data(mock_plotext):
    """Test visualization when no data is returned"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {"results": []}
    mock_data_response.raise_for_status = MagicMock()

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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        assert "No results returned" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_empty_rows(mock_plotext):
    """Test visualization when result has no rows"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        assert "No data rows returned" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_with_nulls(mock_plotext):
    """Test visualization handles null values correctly"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    ["20251215", 100.5],
                    ["20251216", None],  # Null value
                    ["20251217", 103.8],
                ],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        mock_plotext.clear_figure = MagicMock()
        mock_plotext.plot = MagicMock()
        mock_plotext.title = MagicMock()
        mock_plotext.xlabel = MagicMock()
        mock_plotext.ylabel = MagicMock()
        mock_plotext.plot_size = MagicMock()
        mock_plotext.build.return_value = "Chart"

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        # Should still create chart with gaps
        mock_plotext.plot.assert_called_once()


@pytest.mark.asyncio
async def test_visualize_metrics_bar_chart(mock_plotext):
    """Test bar chart visualization"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [["A", 10], ["B", 20], ["C", 15]],
                "columns": [
                    {"name": "category", "type": "string"},
                    {"name": "count", "type": "int"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        mock_plotext.clear_figure = MagicMock()
        mock_plotext.bar = MagicMock()
        mock_plotext.title = MagicMock()
        mock_plotext.xlabel = MagicMock()
        mock_plotext.ylabel = MagicMock()
        mock_plotext.plot_size = MagicMock()
        mock_plotext.build.return_value = "Bar Chart"

        result = await tools.visualize_metrics(
            metrics=["demo.count"],
            chart_type="bar",
        )

        assert len(result) == 1
        mock_plotext.bar.assert_called_once()
        assert "bar" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_scatter_chart(mock_plotext):
    """Test scatter plot visualization"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [[1, 10], [2, 20], [3, 15]],
                "columns": [
                    {"name": "x", "type": "int"},
                    {"name": "y", "type": "int"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        mock_plotext.clear_figure = MagicMock()
        mock_plotext.scatter = MagicMock()
        mock_plotext.title = MagicMock()
        mock_plotext.xlabel = MagicMock()
        mock_plotext.ylabel = MagicMock()
        mock_plotext.plot_size = MagicMock()
        mock_plotext.build.return_value = "Scatter Plot"

        result = await tools.visualize_metrics(
            metrics=["demo.y"],
            chart_type="scatter",
        )

        assert len(result) == 1
        mock_plotext.scatter.assert_called_once()
        assert "scatter" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_http_error(mock_plotext):
    """Test visualization handles HTTP errors"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 500
    mock_data_response.text = "Internal Server Error"
    mock_data_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Server error",
        request=MagicMock(),
        response=mock_data_response,
    )

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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        assert "❌ Error occurred" in result[0].text
        assert "500" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_general_exception(mock_plotext):
    """Test visualization handles general exceptions"""
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
        mock_http_client.get.side_effect = ValueError("Unexpected error")
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        assert "❌ Error occurred" in result[0].text
        assert "Unexpected error" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_plotext_not_installed(mock_plotext):
    """Test visualization when plotext is not installed"""
    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient"),
    ):
        mock_client = AsyncMock()
        mock_client._ensure_token = AsyncMock()
        mock_client.settings = MagicMock(
            dj_api_url="http://localhost:8000",
            request_timeout=30.0,
        )
        mock_get_client.return_value = mock_client

        # Mock import to fail
        with patch(
            "builtins.__import__",
            side_effect=ImportError("No module named 'plotext'"),
        ):
            result = await tools.visualize_metrics(metrics=["demo.metric"])

            assert len(result) == 1
            assert "plotext is not installed" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_date_parsing(mock_plotext):
    """Test that dateint values are parsed correctly as dates"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    ["20251215", 100.5],
                    ["20251220", 105.2],  # 5 day gap
                    ["20251221", 103.8],
                ],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        mock_plotext.clear_figure = MagicMock()
        mock_plotext.plot = MagicMock()
        mock_plotext.title = MagicMock()
        mock_plotext.xlabel = MagicMock()
        mock_plotext.ylabel = MagicMock()
        mock_plotext.xticks = MagicMock()
        mock_plotext.plot_size = MagicMock()
        mock_plotext.build.return_value = "Chart"

        result = await tools.visualize_metrics(
            metrics=["demo.metric"],
            dimensions=["common.dateint"],
        )

        assert len(result) == 1
        # Should parse dates and create proper x-axis
        # X values should be: 0, 5, 6 (preserving the 5-day gap)
        call_args = mock_plotext.plot.call_args
        x_values = call_args[0][0]
        # First value is 0, second should be 5 (5 days later), third should be 6
        assert x_values == [0, 5, 6]


@pytest.mark.asyncio
async def test_visualize_metrics_multiple_metrics(mock_plotext):
    """Test visualization with multiple metrics shows legend"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    ["20251215", 100.5, 50.2],
                    ["20251216", 105.2, 52.1],
                ],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric1", "type": "float"},
                    {"name": "metric2", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        mock_plotext.clear_figure = MagicMock()
        mock_plotext.plot = MagicMock()
        mock_plotext.title = MagicMock()
        mock_plotext.xlabel = MagicMock()
        mock_plotext.ylabel = MagicMock()
        mock_plotext.legend = MagicMock()
        mock_plotext.plot_size = MagicMock()
        mock_plotext.build.return_value = "Chart"

        result = await tools.visualize_metrics(
            metrics=["demo.metric1", "demo.metric2"],
        )

        assert len(result) == 1
        # Should call plot twice (once per metric)
        assert mock_plotext.plot.call_count == 2
        # Should show legend for multiple metrics
        mock_plotext.legend.assert_called_once()


@pytest.mark.asyncio
async def test_visualize_metrics_numeric_dateint(mock_plotext):
    """Test visualization with numeric dateint values"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    [20251215, 100.5],  # Numeric instead of string
                    [20251216, 105.2],
                ],
                "columns": [
                    {"name": "dateint", "type": "int"},
                    {"name": "metric", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        assert "Mock ASCII Chart" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_invalid_dates(mock_plotext):
    """Test visualization with invalid date values"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    ["invalid", 100.5],
                    ["20251216", 105.2],
                    ["bad_date", 103.8],
                ],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        # Should still create chart, using indices for invalid dates
        assert "Mock ASCII Chart" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_no_valid_dates(mock_plotext):
    """Test visualization when no valid dates can be parsed"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    ["A", 100.5],
                    ["B", 105.2],
                    ["C", 103.8],
                ],
                "columns": [
                    {"name": "category", "type": "string"},
                    {"name": "metric", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        # Should use indices instead of dates
        assert "Mock ASCII Chart" in result[0].text


@pytest.mark.asyncio
async def test_visualize_metrics_non_numeric_values(mock_plotext):
    """Test visualization with non-numeric y values"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    ["20251215", "invalid"],
                    ["20251216", 105.2],
                    ["20251217", None],
                ],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric", "type": "string"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        # Should handle non-numeric values gracefully
        mock_plotext.plot.assert_called_once()


@pytest.mark.asyncio
async def test_visualize_metrics_all_invalid_data(mock_plotext):
    """Test visualization when all data is invalid"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    ["20251215", "invalid"],
                    ["20251216", "bad"],
                    ["20251217", "wrong"],
                ],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric", "type": "string"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.metric"])

        assert len(result) == 1
        # Should return error when no valid data
        # The warning log should have been triggered


@pytest.mark.asyncio
async def test_visualize_metrics_custom_title(mock_plotext):
    """Test visualization with custom title"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [["A", 10], ["B", 20]],
                "columns": [
                    {"name": "x", "type": "string"},
                    {"name": "y", "type": "int"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(
            metrics=["demo.y"],
            title="Custom Chart Title",
        )

        assert len(result) == 1
        # Should call title with custom string
        mock_plotext.title.assert_called_once_with("Custom Chart Title")


@pytest.mark.asyncio
async def test_visualize_metrics_x_value_is_none(mock_plotext):
    """Test when x-axis column exists but has None value (line 970)"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    [None, 100.5],  # x value is None
                    ["20251216", 105.2],
                    [None, 103.8],  # x value is None
                ],
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(
            metrics=["demo.metric"],
            dimensions=["common.dateint"],
        )

        assert len(result) == 1
        # Should use indices for None x-values


@pytest.mark.asyncio
async def test_visualize_metrics_string_numbers(mock_plotext):
    """Test with string numbers that can convert to float (line 1037)"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    ["A", "100.5"],  # String number
                    ["B", "105.2"],  # String number
                    ["C", "103.8"],  # String number
                ],
                "columns": [
                    {"name": "x", "type": "string"},
                    {"name": "y", "type": "string"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(metrics=["demo.y"])

        assert len(result) == 1
        # Should successfully convert string numbers to floats
        mock_plotext.plot.assert_called_once()


@pytest.mark.asyncio
async def test_visualize_metrics_many_valid_dates(mock_plotext):
    """Test with >10 valid dates to trigger x-axis formatting (lines 1078-1090)"""
    # Create 15 valid dates in YYYYMMDD format
    rows = [[f"202512{i:02d}", 100.0 + i] for i in range(15, 30)]

    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": rows,
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "revenue", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(
            metrics=["demo.revenue"],
            dimensions=["common.dateint"],
        )

        assert len(result) == 1
        # Should format x-axis labels with dates
        assert mock_plotext.xticks.called
        # Verify it was called with formatted date labels
        call_args = mock_plotext.xticks.call_args
        if call_args:
            x_positions, x_labels = call_args[0]
            # Should have date labels in MM/DD format
            assert len(x_labels) > 0


@pytest.mark.asyncio
async def test_visualize_metrics_scatter_with_valid_data(mock_plotext):
    """Test scatter plot with valid data to ensure branch coverage (line 1060)"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    [1, 10.5],
                    [2, 20.3],
                    [3, 15.7],
                    [4, 25.2],
                    [5, 18.9],
                ],
                "columns": [
                    {"name": "x", "type": "int"},
                    {"name": "y", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(
            metrics=["demo.y"],
            chart_type="scatter",
        )

        assert len(result) == 1
        # Should call scatter with data
        mock_plotext.scatter.assert_called_once()
        call_args = mock_plotext.scatter.call_args
        x_values, y_values = call_args[0][:2]
        # Verify we have the data
        assert len(x_values) == 5
        assert len(y_values) == 5


@pytest.mark.asyncio
async def test_visualize_metrics_scatter_multiple_metrics(mock_plotext):
    """Test scatter plot with multiple metrics to hit loop branch (1060->1021)"""
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": [
                    [1, 10.5, 20.3],
                    [2, 20.3, 25.7],
                    [3, 15.7, 18.2],
                ],
                "columns": [
                    {"name": "x", "type": "int"},
                    {"name": "metric1", "type": "float"},
                    {"name": "metric2", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(
            metrics=["demo.metric1", "demo.metric2"],
            chart_type="scatter",
        )

        assert len(result) == 1
        # Should call scatter twice (once per metric)
        assert mock_plotext.scatter.call_count == 2


@pytest.mark.asyncio
async def test_visualize_metrics_mixed_valid_invalid_dates(mock_plotext):
    """Test with >10 points and mixed valid/invalid dates (line 1089)"""
    # Mix of valid dates and invalid values
    rows = [
        ["20251215", 100.0],
        ["20251216", 101.0],
        ["invalid", 102.0],  # Invalid date
        ["20251218", 103.0],
        ["20251219", 104.0],
        ["bad_date", 105.0],  # Invalid date
        ["20251221", 106.0],
        ["20251222", 107.0],
        ["20251223", 108.0],
        ["", 109.0],  # Empty string
        ["20251225", 110.0],
        ["20251226", 111.0],
        ["20251227", 112.0],
    ]

    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = {
        "results": [
            {
                "rows": rows,
                "columns": [
                    {"name": "dateint", "type": "string"},
                    {"name": "metric", "type": "float"},
                ],
            },
        ],
    }
    mock_data_response.raise_for_status = MagicMock()

    with (
        patch.object(tools, "get_client") as mock_get_client,
        patch("httpx.AsyncClient") as mock_client_class,
        patch.dict("sys.modules", {"plotext": mock_plotext}),
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
        mock_http_client.get.return_value = mock_data_response
        mock_client_class.return_value.__aenter__.return_value = mock_http_client

        result = await tools.visualize_metrics(
            metrics=["demo.metric"],
            dimensions=["common.dateint"],
        )

        assert len(result) == 1
        # Should format x-axis with mix of dates and fallback strings
        assert mock_plotext.xticks.called
