"""Test BaseQueryServiceClient abstract class."""

import pytest

from datajunction_server.query_clients.base import BaseQueryServiceClient
from datajunction_server.models.query import QueryCreate


class MockQueryServiceClient(BaseQueryServiceClient):
    """Mock implementation for testing."""

    def get_columns_for_table(
        self,
        catalog,
        schema,
        table,
        request_headers=None,
        engine=None,
    ):
        return []


class AbstractOnlyClient(BaseQueryServiceClient):
    """Implementation that doesn't override get_columns_for_table to test abstract method."""

    pass


def test_base_client_abstract_methods():
    """Test that BaseQueryServiceClient defines abstract methods correctly."""
    client = MockQueryServiceClient()

    # Only get_columns_for_table is implemented
    assert client.get_columns_for_table("cat", "sch", "tbl") == []

    # Other methods should raise NotImplementedError
    query_create = QueryCreate(
        submitted_query="SELECT 1",
        catalog_name="test_catalog",
        engine_name="test_engine",
        engine_version="v1",
    )

    with pytest.raises(NotImplementedError):
        client.create_view("test_view", query_create)

    with pytest.raises(NotImplementedError):
        client.submit_query(query_create)

    with pytest.raises(NotImplementedError):
        client.get_query("query_id")

    with pytest.raises(NotImplementedError):
        client.materialize(None)

    with pytest.raises(NotImplementedError):
        client.materialize_cube(None)

    with pytest.raises(NotImplementedError):
        client.refresh_cube_materialization("cube")

    with pytest.raises(NotImplementedError):
        client.deactivate_materialization("node", "mat")

    with pytest.raises(NotImplementedError):
        client.get_materialization_info("node", "v1", "SOURCE", "mat")

    with pytest.raises(NotImplementedError):
        client.run_backfill("node", "v1", "SOURCE", "mat", [])

    # Pre-aggregation methods should raise NotImplementedError
    with pytest.raises(NotImplementedError):
        client.materialize_preagg(None)

    with pytest.raises(NotImplementedError):
        client.deactivate_preagg_workflow("test_table")

    with pytest.raises(NotImplementedError):
        client.run_preagg_backfill(None)


def test_base_client_error_messages():
    """Test that error messages include class name."""
    client = MockQueryServiceClient()

    query_create = QueryCreate(
        submitted_query="SELECT 1",
        catalog_name="test_catalog",
        engine_name="test_engine",
        engine_version="v1",
    )

    try:
        client.create_view("test", query_create)
    except NotImplementedError as e:
        assert "MockQueryServiceClient" in str(e)
        assert "does not support view creation" in str(e)

    try:
        client.submit_query(query_create)
    except NotImplementedError as e:
        assert "MockQueryServiceClient" in str(e)
        assert "does not support query submission" in str(e)


def test_refresh_cube_materialization_error_message():
    """Test that refresh_cube_materialization error message includes class name."""
    client = MockQueryServiceClient()

    try:
        client.refresh_cube_materialization("cube")
    except NotImplementedError as e:
        assert "MockQueryServiceClient" in str(e)
        assert "does not support cube materialization refresh" in str(e)


def test_preagg_error_messages():
    """Test that preagg error messages include class name."""
    client = MockQueryServiceClient()

    try:
        client.materialize_preagg(None)
    except NotImplementedError as e:
        assert "MockQueryServiceClient" in str(e)
        assert "does not support pre-aggregation materialization" in str(e)

    try:
        client.deactivate_preagg_workflow("test_table")
    except NotImplementedError as e:
        assert "MockQueryServiceClient" in str(e)
        assert "does not support pre-aggregation workflows" in str(e)

    try:
        client.run_preagg_backfill(None)
    except NotImplementedError as e:
        assert "MockQueryServiceClient" in str(e)
        assert "does not support pre-aggregation backfill" in str(e)


def test_abstract_method_coverage():
    """Test abstract method implementation to hit the pass statement."""
    # Use the MockQueryServiceClient which does implement get_columns_for_table
    client = MockQueryServiceClient()

    # But call the actual abstract method implementation from the base class
    # This should hit the pass statement in the abstract method
    result = BaseQueryServiceClient.__dict__["get_columns_for_table"](
        client,
        "cat",
        "sch",
        "tbl",
    )

    # The abstract method should return None due to the pass statement
    assert result is None


def test_get_columns_for_tables_batch_fallback():
    """Test default implementation falls back to individual calls."""
    from unittest.mock import MagicMock, Mock

    client = MockQueryServiceClient()

    # Create mock columns instead of actual Column objects
    mock_col1 = Mock()
    mock_col1.name = "col1"
    mock_col2 = Mock()
    mock_col2.name = "col2"
    mock_col3 = Mock()
    mock_col3.name = "col3"

    # Mock get_columns_for_table to return different columns for each table
    def mock_get_columns(catalog, schema, table, request_headers=None, engine=None):
        if table == "table1":
            return [mock_col1, mock_col2]
        elif table == "table2":
            return [mock_col3]
        return []

    client.get_columns_for_table = MagicMock(side_effect=mock_get_columns)

    # Call batch method
    result = client.get_columns_for_tables_batch(
        tables=[
            ("cat1", "sch1", "table1"),
            ("cat1", "sch1", "table2"),
        ],
    )

    # Verify individual calls were made
    assert client.get_columns_for_table.call_count == 2

    # Verify results
    assert len(result) == 2
    assert len(result[("cat1", "sch1", "table1")]) == 2
    assert result[("cat1", "sch1", "table1")][0].name == "col1"
    assert len(result[("cat1", "sch1", "table2")]) == 1
    assert result[("cat1", "sch1", "table2")][0].name == "col3"


def test_get_columns_for_tables_batch_fallback_with_exception():
    """Test default implementation handles exceptions gracefully."""
    from unittest.mock import MagicMock

    client = MockQueryServiceClient()

    # Mock get_columns_for_table to raise exception for one table
    def mock_get_columns(catalog, schema, table, request_headers=None, engine=None):
        if table == "table1":
            raise Exception("Connection error")
        return []

    client.get_columns_for_table = MagicMock(side_effect=mock_get_columns)

    # Call batch method - should not raise, should return empty list for failed table
    result = client.get_columns_for_tables_batch(
        tables=[
            ("cat1", "sch1", "table1"),
            ("cat1", "sch1", "table2"),
        ],
    )

    # Verify both calls were attempted
    assert client.get_columns_for_table.call_count == 2

    # Failed table should have empty list
    assert result[("cat1", "sch1", "table1")] == []
    # Successful table should have empty list (as per mock)
    assert result[("cat1", "sch1", "table2")] == []


def test_run_cube_backfill_not_implemented():
    """Test that run_cube_backfill raises NotImplementedError."""
    client = MockQueryServiceClient()

    with pytest.raises(NotImplementedError) as exc_info:
        client.run_cube_backfill(None)

    assert "MockQueryServiceClient" in str(exc_info.value)
    assert "does not support cube backfill" in str(exc_info.value)
