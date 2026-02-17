"""
Tests for MCP formatters
"""

from datajunction.mcp.formatters import (
    format_dimensions_compatibility,
    format_error,
    format_node_details,
    format_nodes_list,
    format_sql_response,
)


def test_format_sql_response_empty():
    """Test formatting empty SQL results"""
    result = format_sql_response([])
    assert result == "No SQL generated."


def test_format_sql_response_none():
    """Test formatting None SQL results"""
    result = format_sql_response(None)
    assert result == "No SQL generated."


def test_format_sql_response_single_query():
    """Test formatting single SQL result with all fields"""
    sql_results = [
        {
            "node": {"name": "default.avg_repair_price"},
            "dialect": "spark",
            "sql": "SELECT city, AVG(price) as avg_price FROM repairs GROUP BY city",
            "columns": [
                {"name": "city", "type": "string"},
                {"name": "avg_price", "type": "double"},
            ],
            "upstreamTables": ["default.repairs", "default.cities"],
        },
    ]

    result = format_sql_response(sql_results)
    assert "Source Node: default.avg_repair_price" in result
    assert "Dialect: spark" in result
    assert "SELECT city, AVG(price)" in result
    assert "city (string)" in result
    assert "avg_price (double)" in result
    assert "Upstream Tables: default.repairs, default.cities" in result


def test_format_sql_response_multiple_queries():
    """Test formatting multiple SQL results"""
    sql_results = [
        {
            "node": {"name": "metric1"},
            "sql": "SELECT 1",
            "dialect": "spark",
            "columns": [],
        },
        {
            "node": {"name": "metric2"},
            "sql": "SELECT 2",
            "dialect": "trino",
            "columns": [],
        },
    ]

    result = format_sql_response(sql_results)
    assert "Query 1 of 2" in result
    assert "Query 2 of 2" in result
    assert "metric1" in result
    assert "metric2" in result
    assert "=" * 60 in result  # Separator


def test_format_sql_response_with_errors():
    """Test formatting SQL with errors"""
    sql_results = [
        {
            "node": {"name": "test.metric"},
            "sql": "SELECT * FROM table",
            "dialect": "spark",
            "errors": [
                {"message": "Warning: missing column definition"},
                {"message": "Error: syntax issue detected"},
            ],
        },
    ]

    result = format_sql_response(sql_results)
    assert "⚠️ Warnings:" in result
    assert "Warning: missing column definition" in result
    assert "Error: syntax issue detected" in result


def test_format_sql_response_minimal():
    """Test formatting SQL with minimal fields"""
    sql_results = [
        {
            "sql": "SELECT 1",
        },
    ]

    result = format_sql_response(sql_results)
    assert "Source Node: unknown" in result
    assert "Dialect: unknown" in result
    assert "SELECT 1" in result


def test_format_sql_response_no_columns():
    """Test formatting SQL without columns"""
    sql_results = [
        {
            "node": {"name": "test"},
            "sql": "SELECT 1",
            "dialect": "spark",
        },
    ]

    result = format_sql_response(sql_results)
    assert "Output Columns" not in result


def test_format_sql_response_no_upstream():
    """Test formatting SQL without upstream tables"""
    sql_results = [
        {
            "node": {"name": "test"},
            "sql": "SELECT 1",
            "dialect": "spark",
        },
    ]

    result = format_sql_response(sql_results)
    assert "Upstream Tables" not in result


def test_format_nodes_list_empty():
    """Test formatting empty nodes list"""
    result = format_nodes_list([])
    assert result == "No nodes found."


def test_format_nodes_list_single():
    """Test formatting single node"""
    nodes = [
        {
            "name": "default.revenue",
            "type": "metric",
            "current": {
                "status": "valid",
                "mode": "published",
                "description": "Total revenue",
            },
        },
    ]

    result = format_nodes_list(nodes)
    assert "Found 1 nodes:" in result
    assert "• default.revenue (metric)" in result
    assert "Description: Total revenue" in result
    assert "Status: valid" in result
    assert "Mode: published" in result


def test_format_nodes_list_multiple():
    """Test formatting multiple nodes"""
    nodes = [
        {
            "name": "metric1",
            "type": "metric",
            "current": {"status": "valid", "mode": "published"},
        },
        {
            "name": "metric2",
            "type": "dimension",
            "current": {"status": "invalid", "mode": "draft"},
        },
    ]

    result = format_nodes_list(nodes)
    assert "Found 2 nodes" in result
    assert "metric1" in result
    assert "metric2" in result


def test_format_nodes_list_with_tags():
    """Test formatting nodes with tags"""
    nodes = [
        {
            "name": "test.node",
            "type": "metric",
            "current": {"status": "valid", "mode": "published"},
            "tags": [{"name": "finance"}, {"name": "kpi"}],
        },
    ]

    result = format_nodes_list(nodes)
    assert "Tags: finance, kpi" in result


def test_format_nodes_list_no_description():
    """Test formatting node without description"""
    nodes = [
        {
            "name": "test",
            "type": "metric",
            "current": {"status": "valid", "mode": "published"},
        },
    ]

    result = format_nodes_list(nodes)
    assert "Description: No description" in result


def test_format_node_details_full():
    """Test formatting node with all details"""
    node = {
        "name": "default.revenue",
        "type": "metric",
        "current": {
            "status": "valid",
            "mode": "published",
            "displayName": "Total Revenue",
            "description": "Sum of all revenue",
            "query": "SELECT SUM(amount) as revenue FROM sales",
            "columns": [
                {"name": "revenue", "type": "double"},
            ],
            "metricMetadata": {
                "direction": "higher_is_better",
                "unit": {"label": "USD"},
            },
            "dimension_links": [
                {"dimension": {"name": "default.date"}},
                {"dimension": {"name": "default.region"}},
            ],
        },
    }

    result = format_node_details(node)
    assert "Node: default.revenue" in result
    assert "Type: metric" in result
    assert "Status: valid" in result
    assert "Mode: published" in result
    assert "Display Name: Total Revenue" in result
    assert "Description: Sum of all revenue" in result
    assert "SQL Definition:" in result
    assert "SELECT SUM(amount) as revenue FROM sales" in result
    assert "Columns (1):" in result
    assert "revenue (double)" in result
    assert "Direction: higher_is_better" in result
    assert "Unit: USD" in result


def test_format_node_details_minimal():
    """Test formatting node with minimal fields"""
    node = {
        "name": "test.node",
        "type": "source",
        "current": {},
    }

    result = format_node_details(node)
    assert "test.node" in result
    assert "source" in result


def test_format_node_details_no_sql():
    """Test formatting node without SQL"""
    node = {
        "name": "test",
        "type": "metric",
        "current": {},
    }

    result = format_node_details(node)
    assert "SQL Definition:" not in result


def test_format_dimensions_compatibility_with_dimensions():
    """Test formatting dimensions compatibility"""
    metrics = ["metric1", "metric2"]
    common_dimensions = [
        {
            "name": "default.date",
            "type": "dimension",
        },
        {
            "name": "default.region",
            "type": "dimension",
        },
    ]

    result = format_dimensions_compatibility(metrics, common_dimensions)
    assert "Found 2 common dimensions" in result
    assert "default.date" in result
    assert "default.region" in result


def test_format_dimensions_compatibility_empty():
    """Test formatting when no common dimensions"""
    metrics = ["metric1", "metric2"]
    common_dimensions = []

    result = format_dimensions_compatibility(metrics, common_dimensions)
    assert "No common dimensions" in result


def test_format_error_simple():
    """Test formatting simple error"""
    result = format_error("Something went wrong")
    assert "❌ Error occurred" in result
    assert "Message: Something went wrong" in result


def test_format_error_with_context():
    """Test formatting error with context string"""
    result = format_error("Connection failed", "Connecting to database")
    assert "❌ Error occurred" in result
    assert "Context: Connecting to database" in result
    assert "Message: Connection failed" in result


def test_format_error_no_context():
    """Test formatting error without context"""
    result = format_error("API timeout")
    assert "❌ Error occurred" in result
    assert "Message: API timeout" in result
    assert "Context:" not in result


def test_format_node_details_many_columns():
    """Test formatting node with more than 10 columns"""
    node = {
        "name": "test.node",
        "type": "source",
        "current": {
            "columns": [{"name": f"col{i}", "type": "string"} for i in range(15)],
        },
    }

    result = format_node_details(node)
    assert "Columns (15):" in result
    assert "... and 5 more columns" in result


def test_format_node_details_many_parents():
    """Test formatting node with more than 5 parents"""
    node = {
        "name": "test.node",
        "type": "transform",
        "current": {
            "parents": [{"name": f"parent{i}"} for i in range(8)],
        },
    }

    result = format_node_details(node)
    assert "Upstream Dependencies: 8" in result
    assert "... and 3 more" in result


def test_format_node_details_with_dimensions():
    """Test formatting node with dimensions parameter"""
    node = {
        "name": "test.metric",
        "type": "metric",
        "current": {},
    }
    dimensions = [
        {
            "name": "default.date",
            "type": "timestamp",
            "dimensionNode": {
                "current": {"description": "Date dimension"},
            },
        },
        {
            "name": "default.region",
            # No type
            "dimensionNode": {
                "current": {},  # No description
            },
        },
        {
            "name": "default.country",
            # No dimensionNode at all
        },
    ]

    result = format_node_details(node, dimensions)
    assert "Available Dimensions (3):" in result
    assert "default.date" in result
    assert "Type: timestamp" in result
    assert "Description: Date dimension" in result
    assert "default.region" in result
    assert "default.country" in result


def test_format_node_details_metric_metadata_partial():
    """Test formatting node with partial metric metadata"""
    node = {
        "name": "test.metric",
        "type": "metric",
        "current": {
            "metricMetadata": {
                "direction": "higher_is_better",
                # No unit
            },
        },
    }

    result = format_node_details(node)
    assert "Direction: higher_is_better" in result
    assert "Unit:" not in result


def test_format_node_details_metric_metadata_unit_only():
    """Test formatting node with only unit in metric metadata"""
    node = {
        "name": "test.metric",
        "type": "metric",
        "current": {
            "metricMetadata": {
                # No direction
                "unit": {"name": "dollars"},
            },
        },
    }

    result = format_node_details(node)
    assert "Direction:" not in result
    assert "Unit: dollars" in result


def test_format_dimensions_compatibility_with_partial_dimensions():
    """Test formatting dimensions with missing nested fields"""
    metrics = ["metric1", "metric2"]
    common_dimensions = [
        {
            "name": "default.date",
            "dimensionNode": {
                "current": {},  # No description
            },
        },
        {
            "name": "default.region",
            # No dimensionNode
        },
    ]

    result = format_dimensions_compatibility(metrics, common_dimensions)
    assert "Found 2 common dimensions" in result
    assert "default.date" in result
    assert "default.region" in result
