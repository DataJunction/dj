"""Test query client optional dependencies."""

from datajunction_server.query_clients import (
    BaseQueryServiceClient,
    HttpQueryServiceClient,
)


def test_base_client_import():
    """Test that base client imports without any optional dependencies."""
    assert BaseQueryServiceClient is not None
    assert HttpQueryServiceClient is not None


def test_snowflake_client_import_handling():
    """Test that SnowflakeClient handles missing dependency gracefully."""
    try:
        from datajunction_server.query_clients import SnowflakeClient

        # If import succeeds, the dependency is available
        assert SnowflakeClient is not None
    except ImportError:
        # This is expected when snowflake-connector-python is not installed
        pass


def test_snowflake_client_error_message():
    """Test that SnowflakeClient gives helpful error message when dependency missing."""
    try:
        from datajunction_server.query_clients import SnowflakeClient

        # If import succeeds, try to instantiate (which should check dependencies)
        try:
            SnowflakeClient(account="test", user="test")
        except ImportError as e:
            assert "datajunction-server[snowflake]" in str(e)
    except ImportError:
        # Import failed at module level, which is also fine
        pass


def test_client_factory_error_handling():
    """Test that client factory provides helpful error messages."""
    from datajunction_server.config import QueryClientConfig
    from datajunction_server.utils import _create_configured_query_client

    # Test with missing dependency
    config = QueryClientConfig(
        type="snowflake",
        connection={"account": "test", "user": "test"},
    )

    try:
        client = _create_configured_query_client(config)
        # If this succeeds, dependency is available
        assert client is not None
    except ValueError as e:
        # Should get a helpful error message about installation
        assert "datajunction-server[snowflake]" in str(e)
    except ImportError:
        # Direct import error is also acceptable
        pass


def test_http_client_always_works():
    """Test that HTTP client works without optional dependencies."""
    from datajunction_server.config import QueryClientConfig
    from datajunction_server.utils import _create_configured_query_client

    config = QueryClientConfig(type="http", connection={"uri": "http://test:8001"})

    client = _create_configured_query_client(config)
    assert isinstance(client, HttpQueryServiceClient)
