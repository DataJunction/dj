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
