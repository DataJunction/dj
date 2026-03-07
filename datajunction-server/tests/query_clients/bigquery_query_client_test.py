"""Tests for BigQueryClient."""

from unittest.mock import MagicMock, patch

import pytest


def test_bigquery_client_import_error():
    """BigQueryClient raises ImportError when google-cloud-bigquery is not installed."""
    with patch(
        "datajunction_server.query_clients.bigquery.BIGQUERY_AVAILABLE",
        False,
    ):
        from datajunction_server.query_clients.bigquery import BigQueryClient

        with pytest.raises(ImportError) as exc_info:
            BigQueryClient(project="my-project")

        assert "datajunction-server[bigquery]" in str(exc_info.value)
        assert "google-cloud-bigquery" in str(exc_info.value)


def _make_client(project="my-project", **kwargs):
    """Helper: create a BigQueryClient with BIGQUERY_AVAILABLE patched to True."""
    with patch(
        "datajunction_server.query_clients.bigquery.BIGQUERY_AVAILABLE",
        True,
    ):
        from datajunction_server.query_clients.bigquery import BigQueryClient

        return BigQueryClient(project=project, **kwargs)


def test_get_columns_for_table():
    """get_columns_for_table returns correct Column objects from INFORMATION_SCHEMA."""
    from datajunction_server.sql.parsing.types import (
        BigIntType,
        StringType,
        TimestampType,
    )

    client = _make_client()

    mock_row_1 = MagicMock()
    mock_row_1.column_name = "id"
    mock_row_1.data_type = "INT64"
    mock_row_1.ordinal_position = 1

    mock_row_2 = MagicMock()
    mock_row_2.column_name = "name"
    mock_row_2.data_type = "STRING"
    mock_row_2.ordinal_position = 2

    mock_row_3 = MagicMock()
    mock_row_3.column_name = "created_at"
    mock_row_3.data_type = "TIMESTAMP"
    mock_row_3.ordinal_position = 3

    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(
        return_value=iter([mock_row_1, mock_row_2, mock_row_3]),
    )

    mock_job = MagicMock()
    mock_job.result.return_value = mock_result

    mock_bq_client = MagicMock()
    mock_bq_client.query.return_value = mock_job

    with (
        patch(
            "datajunction_server.query_clients.bigquery.QueryJobConfig",
            MagicMock(),
        ),
        patch(
            "datajunction_server.query_clients.bigquery.ScalarQueryParameter",
            MagicMock(),
        ),
        patch.object(
            client,
            "_get_client",
            return_value=mock_bq_client,
        ),
    ):
        columns = client.get_columns_for_table(
            catalog="my-project",
            schema="my_dataset",
            table="my_table",
        )

    assert len(columns) == 3
    assert columns[0].name == "id"
    assert isinstance(columns[0].type, BigIntType)
    assert columns[0].order == 0

    assert columns[1].name == "name"
    assert isinstance(columns[1].type, StringType)
    assert columns[1].order == 1

    assert columns[2].name == "created_at"
    assert isinstance(columns[2].type, TimestampType)
    assert columns[2].order == 2


def test_get_columns_for_table_not_found():
    """get_columns_for_table raises DJDoesNotExistException when no columns are returned."""
    from datajunction_server.errors import DJDoesNotExistException

    client = _make_client()

    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))

    mock_job = MagicMock()
    mock_job.result.return_value = mock_result

    mock_bq_client = MagicMock()
    mock_bq_client.query.return_value = mock_job

    with (
        patch(
            "datajunction_server.query_clients.bigquery.QueryJobConfig",
            MagicMock(),
        ),
        patch(
            "datajunction_server.query_clients.bigquery.ScalarQueryParameter",
            MagicMock(),
        ),
        patch.object(
            client,
            "_get_client",
            return_value=mock_bq_client,
        ),
    ):
        with pytest.raises(DJDoesNotExistException):
            client.get_columns_for_table(
                catalog="my-project",
                schema="my_dataset",
                table="nonexistent_table",
            )


def test_get_columns_for_table_query_error():
    """get_columns_for_table wraps unexpected errors in DJQueryServiceClientException."""
    from datajunction_server.errors import DJQueryServiceClientException

    client = _make_client()

    mock_bq_client = MagicMock()
    mock_bq_client.query.side_effect = RuntimeError("network error")

    with (
        patch(
            "datajunction_server.query_clients.bigquery.QueryJobConfig",
            MagicMock(),
        ),
        patch(
            "datajunction_server.query_clients.bigquery.ScalarQueryParameter",
            MagicMock(),
        ),
        patch.object(
            client,
            "_get_client",
            return_value=mock_bq_client,
        ),
    ):
        with pytest.raises(DJQueryServiceClientException) as exc_info:
            client.get_columns_for_table(
                catalog="my-project",
                schema="my_dataset",
                table="my_table",
            )

    assert "network error" in str(exc_info.value)


def test_get_project_from_engine_with_uri():
    """_get_project_from_engine returns the netloc of the engine URI."""
    client = _make_client(project="default-project")

    engine = MagicMock()
    engine.uri = "bigquery://my-gcp-project"

    assert client._get_project_from_engine(engine, "catalog-alias") == "my-gcp-project"


def test_get_project_from_engine_no_engine():
    """_get_project_from_engine falls back to self.project when engine is None."""
    client = _make_client(project="default-project")

    assert client._get_project_from_engine(None, "catalog-alias") == "default-project"


def test_get_project_from_engine_no_uri():
    """_get_project_from_engine falls back to self.project when engine has no URI."""
    client = _make_client(project="default-project")

    engine = MagicMock()
    engine.uri = None

    assert client._get_project_from_engine(engine, "catalog-alias") == "default-project"


def test_get_columns_for_table_uses_engine_uri_project():
    """get_columns_for_table uses the project from the engine URI, not self.project."""
    from datajunction_server.sql.parsing.types import StringType

    client = _make_client(project="default-project")

    mock_row = MagicMock()
    mock_row.column_name = "name"
    mock_row.data_type = "STRING"
    mock_row.ordinal_position = 1

    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([mock_row]))

    mock_job = MagicMock()
    mock_job.result.return_value = mock_result

    mock_bq_client = MagicMock()
    mock_bq_client.query.return_value = mock_job

    engine = MagicMock()
    engine.uri = "bigquery://my-gcp-project"

    with (
        patch(
            "datajunction_server.query_clients.bigquery.QueryJobConfig",
            MagicMock(),
        ),
        patch(
            "datajunction_server.query_clients.bigquery.ScalarQueryParameter",
            MagicMock(),
        ),
        patch.object(
            client,
            "_get_client",
            return_value=mock_bq_client,
        ) as mock_get_client,
    ):
        columns = client.get_columns_for_table(
            catalog="catalog-alias",
            schema="my_dataset",
            table="my_table",
            engine=engine,
        )

    # Client should be created with the project from the engine URI
    mock_get_client.assert_called_once_with(project="my-gcp-project")
    assert len(columns) == 1
    assert isinstance(columns[0].type, StringType)


def test_map_bigquery_type_to_dj_integer_types():
    """_map_bigquery_type_to_dj maps all BigQuery integer types to BigIntType."""
    from datajunction_server.sql.parsing.types import BigIntType

    client = _make_client()

    for bq_type in (
        "INT64",
        "INT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
        "BYTEINT",
    ):
        result = client._map_bigquery_type_to_dj(bq_type)
        assert isinstance(result, BigIntType), f"Expected BigIntType for {bq_type}"


def test_map_bigquery_type_to_dj_float():
    """_map_bigquery_type_to_dj maps FLOAT64 and FLOAT to FloatType."""
    from datajunction_server.sql.parsing.types import FloatType

    client = _make_client()

    for bq_type in ("FLOAT64", "FLOAT"):
        result = client._map_bigquery_type_to_dj(bq_type)
        assert isinstance(result, FloatType), f"Expected FloatType for {bq_type}"


def test_map_bigquery_type_to_dj_decimal():
    """_map_bigquery_type_to_dj maps NUMERIC/DECIMAL/BIGNUMERIC/BIGDECIMAL to DecimalType."""
    from datajunction_server.sql.parsing.types import DecimalType

    client = _make_client()

    result = client._map_bigquery_type_to_dj("NUMERIC")
    assert isinstance(result, DecimalType)
    assert result.precision == 38
    assert result.scale == 9

    result = client._map_bigquery_type_to_dj("BIGNUMERIC")
    assert isinstance(result, DecimalType)
    # DJ's DecimalType caps precision at 38, so BIGNUMERIC uses max values
    assert result.precision == 38
    assert result.scale == 38

    # Parameterized variant should strip the parameters
    result = client._map_bigquery_type_to_dj("NUMERIC(10, 2)")
    assert isinstance(result, DecimalType)


def test_map_bigquery_type_to_dj_boolean():
    """_map_bigquery_type_to_dj maps BOOL and BOOLEAN to BooleanType."""
    from datajunction_server.sql.parsing.types import BooleanType

    client = _make_client()

    for bq_type in ("BOOL", "BOOLEAN"):
        result = client._map_bigquery_type_to_dj(bq_type)
        assert isinstance(result, BooleanType)


def test_map_bigquery_type_to_dj_string_and_bytes():
    """_map_bigquery_type_to_dj maps STRING and BYTES to StringType."""
    from datajunction_server.sql.parsing.types import StringType

    client = _make_client()

    for bq_type in ("STRING", "BYTES"):
        result = client._map_bigquery_type_to_dj(bq_type)
        assert isinstance(result, StringType)


def test_map_bigquery_type_to_dj_date_time():
    """_map_bigquery_type_to_dj maps date/time types correctly."""
    from datajunction_server.sql.parsing.types import DateType, TimeType, TimestampType

    client = _make_client()

    assert isinstance(client._map_bigquery_type_to_dj("DATE"), DateType)
    assert isinstance(client._map_bigquery_type_to_dj("TIME"), TimeType)
    assert isinstance(client._map_bigquery_type_to_dj("DATETIME"), TimestampType)
    assert isinstance(client._map_bigquery_type_to_dj("TIMESTAMP"), TimestampType)


def test_map_bigquery_type_to_dj_complex_types():
    """_map_bigquery_type_to_dj maps complex/semi-structured types to StringType."""
    from datajunction_server.sql.parsing.types import StringType

    client = _make_client()

    for bq_type in ("ARRAY", "STRUCT", "RECORD", "GEOGRAPHY", "JSON", "INTERVAL"):
        result = client._map_bigquery_type_to_dj(bq_type)
        assert isinstance(result, StringType), f"Expected StringType for {bq_type}"


def test_map_bigquery_type_to_dj_unknown_type():
    """_map_bigquery_type_to_dj falls back to StringType for unknown types."""
    from datajunction_server.sql.parsing.types import StringType

    client = _make_client()

    result = client._map_bigquery_type_to_dj("SOME_UNKNOWN_TYPE")
    assert isinstance(result, StringType)


def test_utils_create_bigquery_client():
    """_create_configured_query_client creates BigQueryClient for type='bigquery'."""
    from datajunction_server.config import QueryClientConfig
    from datajunction_server.utils import _create_configured_query_client

    with patch(
        "datajunction_server.query_clients.bigquery.BIGQUERY_AVAILABLE",
        True,
    ):
        config = QueryClientConfig(
            type="bigquery",
            connection={"project": "my-project"},
        )
        client = _create_configured_query_client(config)

    from datajunction_server.query_clients.bigquery import BigQueryClient

    assert isinstance(client, BigQueryClient)
    assert client.project == "my-project"


def test_utils_bigquery_missing_project():
    """_create_configured_query_client raises ValueError when 'project' is missing."""
    from datajunction_server.config import QueryClientConfig
    from datajunction_server.utils import _create_configured_query_client

    config = QueryClientConfig(
        type="bigquery",
        connection={},
    )
    with pytest.raises(ValueError, match="'project'"):
        _create_configured_query_client(config)


def test_get_client_no_credentials():
    """_get_client creates a Client with no credentials (uses ADC)."""
    client = _make_client()

    mock_bq_class = MagicMock()
    with patch(
        "datajunction_server.query_clients.bigquery.bigquery",
        mock_bq_class,
    ):
        client._get_client()

    mock_bq_class.Client.assert_called_once_with(
        project="my-project",
        credentials=None,
        location=None,
    )


def test_get_client_with_credentials_info():
    """_get_client creates credentials from credentials_info dict."""
    client = _make_client(credentials_info={"type": "service_account"})

    mock_sa = MagicMock()
    mock_bq = MagicMock()
    with (
        patch(
            "datajunction_server.query_clients.bigquery.service_account",
            mock_sa,
        ),
        patch(
            "datajunction_server.query_clients.bigquery.bigquery",
            mock_bq,
        ),
    ):
        client._get_client()

    mock_sa.Credentials.from_service_account_info.assert_called_once_with(
        {"type": "service_account"},
    )


def test_get_client_with_credentials_path():
    """_get_client creates credentials from a service account file path."""
    client = _make_client(credentials_path="/path/to/sa.json")

    mock_sa = MagicMock()
    mock_bq = MagicMock()
    with (
        patch(
            "datajunction_server.query_clients.bigquery.service_account",
            mock_sa,
        ),
        patch(
            "datajunction_server.query_clients.bigquery.bigquery",
            mock_bq,
        ),
    ):
        client._get_client()

    mock_sa.Credentials.from_service_account_file.assert_called_once_with(
        "/path/to/sa.json",
    )


def test_test_connection_success():
    """test_connection returns True when the query succeeds."""
    client = _make_client()

    mock_bq_client = MagicMock()
    mock_bq_client.query.return_value.result.return_value = iter([MagicMock()])

    with patch.object(client, "_get_client", return_value=mock_bq_client):
        result = client.test_connection()

    assert result is True
    mock_bq_client.query.assert_called_once_with("SELECT 1")
