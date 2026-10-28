"""Test SnowflakeClient."""

from unittest.mock import MagicMock, patch
from types import SimpleNamespace

import pytest

from datajunction_server.models.query import QueryCreate
from datajunction_server.typing import QueryState


@pytest.mark.asyncio
async def test_submit_query_returns_rows_and_closes_connection():
    from datajunction_server.query_clients.snowflake import SnowflakeClient

    with patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True):
        client = SnowflakeClient(account="test", user="test")
    cursor = MagicMock()
    cursor.description = [SimpleNamespace(name="total", type_code="NUMBER")]
    cursor.fetchall.return_value = [(42,)]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    client._get_connection = MagicMock(return_value=connection)
    query = QueryCreate(
        catalog_name="warehouse",
        engine_name="snowflake",
        engine_version="1",
        submitted_query="SELECT 42 AS total",
    )

    result = await client.submit_query(query)

    assert result.state == QueryState.FINISHED
    assert result.results.root[0].rows == [(42,)]
    assert result.results.root[0].columns[0].name == "total"
    cursor.execute.assert_called_once_with(query.submitted_query)
    connection.close.assert_called_once()


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


def test_map_snowflake_type_to_dj_parameterized_types():
    """Test _map_snowflake_type_to_dj with parameterized NUMBER, DECIMAL, and NUMERIC types."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.sql.parsing.types import DecimalType

    with patch(
        "datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE",
        True,
    ):
        client = SnowflakeClient(
            account="test_account",
            user="test_user",
            password="test_pass",
        )

        # Test NUMBER with precision and scale
        result = client._map_snowflake_type_to_dj("NUMBER(10,2)")
        assert isinstance(result, DecimalType)
        assert result.precision == 10
        assert result.scale == 2

        # Test DECIMAL with precision and scale
        result = client._map_snowflake_type_to_dj("DECIMAL(5,3)")
        assert isinstance(result, DecimalType)
        assert result.precision == 5
        assert result.scale == 3

        # Test NUMERIC with precision and scale
        result = client._map_snowflake_type_to_dj("NUMERIC(8,1)")
        assert isinstance(result, DecimalType)
        assert result.precision == 8
        assert result.scale == 1

        # Test with precision only (scale defaults to 0)
        result = client._map_snowflake_type_to_dj("NUMBER(15)")
        assert isinstance(result, DecimalType)
        assert result.precision == 15
        assert result.scale == 0

        # Test lowercase type names
        result = client._map_snowflake_type_to_dj("decimal(12,4)")
        assert isinstance(result, DecimalType)
        assert result.precision == 12
        assert result.scale == 4
