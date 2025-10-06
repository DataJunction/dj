"""Test SnowflakeClient."""

from unittest.mock import patch
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
