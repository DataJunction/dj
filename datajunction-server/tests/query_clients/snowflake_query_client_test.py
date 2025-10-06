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


def test_get_columns_for_table_unexpected_error():
    """Test get_columns_for_table with an unexpected exception."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.errors import DJQueryServiceClientException

    with patch(
        "datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE",
        True,
    ):
        with patch("datajunction_server.query_clients.snowflake.snowflake") as mock_sf:
            client = SnowflakeClient(
                account="test_account",
                user="test_user",
                password="test_pass",
            )

            # Mock connection that raises an unexpected exception
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = RuntimeError("Unexpected error")
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_sf.connector.connect.return_value = mock_conn

            with pytest.raises(DJQueryServiceClientException) as exc_info:
                client.get_columns_for_table("catalog", "schema", "table")

            assert "Unexpected error retrieving columns" in str(exc_info.value)
