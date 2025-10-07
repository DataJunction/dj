"""Test SnowflakeClient."""

from unittest.mock import MagicMock, patch
import pytest


def test_snowflake_client_import_error():
    """Test SnowflakeClient handles missing snowflake-connector-python."""
    with patch(
        "datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE",
        False,
    ):
        from datajunction_server.query_clients.snowflake import SnowflakeClient

        with pytest.raises(ImportError) as exc_info:
            SnowflakeClient(account="test", user="test")

        assert "datajunction-server[snowflake]" in str(exc_info.value)
        assert "snowflake-connector-python" in str(exc_info.value)


def test_get_database_from_engine_empty_database():
    """Test _get_database_from_engine when path parsing yields empty database name."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient

    with patch(
        "datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE",
        True,
    ):
        client = SnowflakeClient(
            account="test_account",
            user="test_user",
            password="test_pass",
            database="default_db",
        )

        # Mock an engine with a URI that has a path but empty database
        mock_engine = MagicMock()
        mock_engine.uri = "snowflake://user:pass@account//"

        result = client._get_database_from_engine(mock_engine, "fallback_catalog")
        assert result == "default_db"


def test_get_database_from_engine_with_query_params():
    """Test _get_database_from_engine extracting database from query parameters."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient

    with patch(
        "datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE",
        True,
    ):
        client = SnowflakeClient(
            account="test_account",
            user="test_user",
            password="test_pass",
            database="default_db",
        )

        # Mock an engine with database in query parameters
        mock_engine = MagicMock()
        mock_engine.uri = "snowflake://user:pass@account?database=query_db&warehouse=wh"

        result = client._get_database_from_engine(mock_engine, "fallback_catalog")
        assert result == "query_db"
