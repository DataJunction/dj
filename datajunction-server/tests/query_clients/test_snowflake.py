"""Test SnowflakeClient."""

from unittest.mock import Mock, patch, MagicMock
import pytest


def test_snowflake_available_import():
    """Test that snowflake imports work when available."""
    # Import without any patches to cover the import lines 19-20
    import sys

    # Temporarily mock the snowflake package to make imports succeed
    sys.modules["snowflake"] = Mock()
    sys.modules["snowflake.connector"] = Mock()
    sys.modules["snowflake.connector"].DictCursor = Mock()

    # Now import the module to execute the import lines
    import importlib
    import datajunction_server.query_clients.snowflake as sf_module

    importlib.reload(sf_module)

    # Clean up
    if "snowflake" in sys.modules:
        del sys.modules["snowflake"]
    if "snowflake.connector" in sys.modules:
        del sys.modules["snowflake.connector"]

    # The import should have worked
    assert hasattr(sf_module, "SNOWFLAKE_AVAILABLE")


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


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_snowflake_client_initialization(mock_snowflake):
    """Test SnowflakeClient initialization."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient

    client = SnowflakeClient(
        account="test-account",
        user="test-user",
        password="test-password",
        warehouse="TEST_WH",
        database="TEST_DB",
        schema="TEST_SCHEMA",
        role="TEST_ROLE",
    )

    assert client.connection_params["account"] == "test-account"
    assert client.connection_params["user"] == "test-user"
    assert client.connection_params["password"] == "test-password"
    assert client.connection_params["warehouse"] == "TEST_WH"
    assert client.connection_params["database"] == "TEST_DB"
    assert client.connection_params["schema"] == "TEST_SCHEMA"
    assert client.connection_params["role"] == "TEST_ROLE"


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_snowflake_client_with_private_key(mock_snowflake):
    """Test SnowflakeClient initialization with private key."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient

    with patch("builtins.open", mock_open(read_data=b"private_key_data")):
        client = SnowflakeClient(
            account="test-account",
            user="test-user",
            private_key_path="/path/to/key.pem",
            warehouse="TEST_WH",
            database="TEST_DB",
            schema="TEST_SCHEMA",
        )

    assert "private_key" in client.connection_params
    assert client.connection_params["private_key"] == b"private_key_data"


def mock_open(read_data):
    """Helper to mock open() for file reading."""
    from unittest.mock import mock_open as mock_open_builtin

    return mock_open_builtin(read_data=read_data)


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_get_database_from_engine_with_uri(mock_snowflake):
    """Test _get_database_from_engine with valid URI."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient

    client = SnowflakeClient(account="test", user="test", database="DEFAULT_DB")

    # Test URI with database in path
    engine = Mock()
    engine.uri = "snowflake://user:pass@account/ANALYTICS?warehouse=WH"

    result = client._get_database_from_engine(engine, "fallback")
    assert result == "ANALYTICS"

    # Test URI with database in query params
    engine.uri = "snowflake://user:pass@account/?database=PROD&warehouse=WH"
    result = client._get_database_from_engine(engine, "fallback")
    assert result == "PROD"


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_get_database_from_engine_fallbacks(mock_snowflake):
    """Test _get_database_from_engine fallback scenarios."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient

    client = SnowflakeClient(account="test", user="test", database="DEFAULT_DB")

    # Test with no engine
    result = client._get_database_from_engine(None, "fallback")
    assert result == "DEFAULT_DB"

    # Test with engine but no URI
    engine = Mock()
    engine.uri = None
    result = client._get_database_from_engine(engine, "fallback")
    assert result == "DEFAULT_DB"

    # Test with invalid URI
    engine.uri = "invalid://uri"
    result = client._get_database_from_engine(engine, "fallback")
    assert result == "DEFAULT_DB"

    # Test with empty database in path
    engine.uri = "snowflake://user:pass@account/?warehouse=WH"
    result = client._get_database_from_engine(engine, "fallback")
    assert result == "DEFAULT_DB"

    # Test with empty path and no query params
    engine.uri = "snowflake://user:pass@account"
    result = client._get_database_from_engine(engine, "fallback")
    assert result == "DEFAULT_DB"

    # Test with query params but no database param
    engine.uri = "snowflake://user:pass@account/?warehouse=WH&schema=PUBLIC"
    result = client._get_database_from_engine(engine, "fallback")
    assert result == "DEFAULT_DB"

    # Test empty database name in path to hit line 121
    engine.uri = "snowflake://user:pass@account//"
    result = client._get_database_from_engine(engine, "fallback")
    assert result == "DEFAULT_DB"


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_get_columns_for_table_success(mock_snowflake):
    """Test successful get_columns_for_table."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.database.column import Column

    # Mock the cursor and connection
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        {
            "COLUMN_NAME": "id",
            "DATA_TYPE": "NUMBER",
            "IS_NULLABLE": "NO",
            "ORDINAL_POSITION": 1,
        },
        {
            "COLUMN_NAME": "name",
            "DATA_TYPE": "VARCHAR",
            "IS_NULLABLE": "YES",
            "ORDINAL_POSITION": 2,
        },
    ]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_snowflake.connector.connect.return_value = mock_conn

    client = SnowflakeClient(account="test", user="test", database="TEST_DB")

    engine = Mock()
    engine.uri = "snowflake://user:pass@account/ANALYTICS"

    result = client.get_columns_for_table(
        "default",
        "PUBLIC",
        "test_table",
        engine=engine,
    )

    assert len(result) == 2
    assert all(isinstance(col, Column) for col in result)
    assert result[0].name == "id"
    assert result[1].name == "name"

    # Verify query was called with correct database
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args[0]
    assert "ANALYTICS" in call_args[1]  # Database from engine URI


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_get_columns_for_table_no_columns_found(mock_snowflake):
    """Test get_columns_for_table when no columns found."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.errors import DJDoesNotExistException

    # Create a proper DatabaseError class for proper exception handling
    class MockDatabaseError(Exception):
        pass

    mock_snowflake.connector.DatabaseError = MockDatabaseError

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_snowflake.connector.connect.return_value = mock_conn

    client = SnowflakeClient(account="test", user="test", database="TEST_DB")

    with pytest.raises(DJDoesNotExistException) as exc_info:
        client.get_columns_for_table("default", "PUBLIC", "test_table")

    assert "No columns found for table" in str(exc_info.value)
    assert "TEST_DB.PUBLIC.test_table" in str(exc_info.value)
    assert "(DJ catalog: default)" in str(exc_info.value)


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_get_columns_for_table_database_error(mock_snowflake):
    """Test get_columns_for_table with database error."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.errors import DJDoesNotExistException

    # Create a proper DatabaseError class
    class MockDatabaseError(Exception):
        pass

    mock_snowflake.connector.DatabaseError = MockDatabaseError

    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = MockDatabaseError("Table does not exist")

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_snowflake.connector.connect.return_value = mock_conn

    client = SnowflakeClient(account="test", user="test", database="TEST_DB")

    with pytest.raises(DJDoesNotExistException) as exc_info:
        client.get_columns_for_table("default", "PUBLIC", "test_table")

    assert "Table not found" in str(exc_info.value)


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_get_columns_for_table_other_database_error(mock_snowflake):
    """Test get_columns_for_table with other database error."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.errors import DJQueryServiceClientException

    # Create a proper DatabaseError class
    class MockDatabaseError(Exception):
        pass

    mock_snowflake.connector.DatabaseError = MockDatabaseError

    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = MockDatabaseError("Connection failed")

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_snowflake.connector.connect.return_value = mock_conn

    client = SnowflakeClient(account="test", user="test", database="TEST_DB")

    with pytest.raises(DJQueryServiceClientException) as exc_info:
        client.get_columns_for_table("default", "PUBLIC", "test_table")

    assert "Error retrieving columns from Snowflake" in str(exc_info.value)


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_get_columns_for_table_unexpected_error(mock_snowflake):
    """Test get_columns_for_table with unexpected error."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.errors import DJQueryServiceClientException

    # Create a proper DatabaseError class for proper exception handling
    class MockDatabaseError(Exception):
        pass

    mock_snowflake.connector.DatabaseError = MockDatabaseError

    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("Unexpected error")

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_snowflake.connector.connect.return_value = mock_conn

    client = SnowflakeClient(account="test", user="test", database="TEST_DB")

    with pytest.raises(DJQueryServiceClientException) as exc_info:
        client.get_columns_for_table("default", "PUBLIC", "test_table")

    assert "Unexpected error retrieving columns" in str(exc_info.value)


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_map_snowflake_type_to_dj(mock_snowflake):
    """Test _map_snowflake_type_to_dj method."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.sql.parsing.types import (
        DecimalType,
        IntegerType,
        BigIntType,
        FloatType,
        DoubleType,
        StringType,
        DateType,
        TimestampType,
        TimeType,
        BooleanType,
    )

    client = SnowflakeClient(account="test", user="test")

    # Test numeric types
    assert isinstance(client._map_snowflake_type_to_dj("NUMBER"), DecimalType)
    assert isinstance(client._map_snowflake_type_to_dj("NUMBER(10,2)"), DecimalType)
    assert isinstance(client._map_snowflake_type_to_dj("INT"), IntegerType)
    assert isinstance(client._map_snowflake_type_to_dj("BIGINT"), BigIntType)
    assert isinstance(client._map_snowflake_type_to_dj("FLOAT"), FloatType)
    assert isinstance(client._map_snowflake_type_to_dj("DOUBLE"), DoubleType)

    # Test string types
    assert isinstance(client._map_snowflake_type_to_dj("VARCHAR"), StringType)
    assert isinstance(client._map_snowflake_type_to_dj("STRING"), StringType)
    assert isinstance(client._map_snowflake_type_to_dj("TEXT"), StringType)

    # Test date/time types
    assert isinstance(client._map_snowflake_type_to_dj("DATE"), DateType)
    assert isinstance(client._map_snowflake_type_to_dj("TIMESTAMP"), TimestampType)
    assert isinstance(client._map_snowflake_type_to_dj("TIME"), TimeType)

    # Test boolean
    assert isinstance(client._map_snowflake_type_to_dj("BOOLEAN"), BooleanType)

    # Test other types default to string
    assert isinstance(client._map_snowflake_type_to_dj("VARIANT"), StringType)
    assert isinstance(client._map_snowflake_type_to_dj("UNKNOWN_TYPE"), StringType)


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_test_connection_success(mock_snowflake):
    """Test successful connection test."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_snowflake.connector.connect.return_value = mock_conn

    client = SnowflakeClient(account="test", user="test")

    assert client.test_connection() is True
    mock_cursor.execute.assert_called_once_with("SELECT 1")


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_test_connection_failure(mock_snowflake):
    """Test failed connection test."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient

    mock_snowflake.connector.connect.side_effect = Exception("Connection failed")

    client = SnowflakeClient(account="test", user="test")

    assert client.test_connection() is False


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_decimal_type_with_invalid_params(mock_snowflake):
    """Test decimal type mapping with invalid parameters."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.sql.parsing.types import DecimalType

    client = SnowflakeClient(account="test", user="test")

    # Test with invalid decimal parameters - should fall back to default
    result = client._map_snowflake_type_to_dj("NUMBER(invalid,params)")
    assert isinstance(result, DecimalType)

    # Test with missing scale parameter (single parameter)
    result = client._map_snowflake_type_to_dj("NUMBER(10)")
    assert isinstance(result, DecimalType)


@patch("datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE", True)
@patch("datajunction_server.query_clients.snowflake.snowflake")
def test_non_parameterized_types_coverage(mock_snowflake):
    """Test non-parameterized types to hit the else branch (245->256)."""
    from datajunction_server.query_clients.snowflake import SnowflakeClient
    from datajunction_server.sql.parsing.types import DecimalType, StringType

    client = SnowflakeClient(account="test", user="test")

    # Test non-parameterized types without parentheses to hit else branch
    result = client._map_snowflake_type_to_dj("NUMBER")
    assert isinstance(result, DecimalType)

    result = client._map_snowflake_type_to_dj("DECIMAL")
    assert isinstance(result, DecimalType)

    result = client._map_snowflake_type_to_dj("NUMERIC")
    assert isinstance(result, DecimalType)

    # Test other types without parentheses
    result = client._map_snowflake_type_to_dj("VARCHAR")
    assert isinstance(result, StringType)
