"""Test HttpQueryServiceClient."""

from unittest.mock import Mock, patch


from datajunction_server.models.query import QueryCreate, QueryWithResults, QueryResults
from datajunction_server.query_clients.http import HttpQueryServiceClient
from datajunction_server.service_clients import QueryServiceClient


def test_http_client_initialization():
    """Test HttpQueryServiceClient initialization."""
    client = HttpQueryServiceClient("http://test:8001", retries=3)
    assert client._client is not None
    assert isinstance(client._client, QueryServiceClient)
    assert client.uri == "http://test:8001"


@patch("datajunction_server.service_clients.QueryServiceClient.get_columns_for_table")
def test_http_client_get_columns_for_table(mock_get_columns):
    """Test HttpQueryServiceClient.get_columns_for_table."""
    mock_columns = []
    mock_get_columns.return_value = mock_columns

    client = HttpQueryServiceClient("http://test:8001")
    engine = Mock()

    result = client.get_columns_for_table(
        catalog="test_cat",
        schema="test_sch",
        table="test_tbl",
        request_headers={"auth": "token"},
        engine=engine,
    )

    assert result == mock_columns
    mock_get_columns.assert_called_once_with(
        catalog="test_cat",
        schema="test_sch",
        table="test_tbl",
        request_headers={"auth": "token"},
        engine=engine,
    )


@patch("datajunction_server.service_clients.QueryServiceClient.create_view")
def test_http_client_create_view(mock_create_view):
    """Test HttpQueryServiceClient.create_view."""
    mock_create_view.return_value = "View created"

    client = HttpQueryServiceClient("http://test:8001")
    query_create = QueryCreate(
        submitted_query="SELECT 1",
        catalog_name="test_catalog",
        engine_name="test_engine",
        engine_version="v1",
    )

    result = client.create_view(
        view_name="test_view",
        query_create=query_create,
        request_headers={"auth": "token"},
    )

    assert result == "View created"
    mock_create_view.assert_called_once_with(
        view_name="test_view",
        query_create=query_create,
        request_headers={"auth": "token"},
    )


@patch("datajunction_server.service_clients.QueryServiceClient.submit_query")
def test_http_client_submit_query(mock_submit_query):
    """Test HttpQueryServiceClient.submit_query."""
    from datajunction_server.models.query import QueryResults

    mock_result = QueryWithResults(
        id="test_id",
        submitted_query="SELECT 1",
        executed_query="SELECT 1",
        state="FINISHED",
        results=QueryResults([]),
        errors=[],
    )
    mock_submit_query.return_value = mock_result

    client = HttpQueryServiceClient("http://test:8001")
    query_create = QueryCreate(
        submitted_query="SELECT 1",
        catalog_name="test_catalog",
        engine_name="test_engine",
        engine_version="v1",
    )

    result = client.submit_query(
        query_create=query_create,
        request_headers={"auth": "token"},
    )

    assert result == mock_result
    mock_submit_query.assert_called_once_with(
        query_create=query_create,
        request_headers={"auth": "token"},
    )


@patch("datajunction_server.service_clients.QueryServiceClient.get_query")
def test_http_client_get_query(mock_get_query):
    """Test HttpQueryServiceClient.get_query."""
    mock_result = QueryWithResults(
        id="test_id",
        submitted_query="SELECT 1",
        executed_query="SELECT 1",
        state="FINISHED",
        results=QueryResults([]),
        errors=[],
    )
    mock_get_query.return_value = mock_result

    client = HttpQueryServiceClient("http://test:8001")

    result = client.get_query(query_id="test_id", request_headers={"auth": "token"})

    assert result == mock_result
    mock_get_query.assert_called_once_with(
        query_id="test_id",
        request_headers={"auth": "token"},
    )


@patch("datajunction_server.service_clients.QueryServiceClient.materialize")
def test_http_client_materialize(mock_materialize):
    """Test HttpQueryServiceClient.materialize."""
    from datajunction_server.models.materialization import MaterializationInfo

    mock_result = MaterializationInfo(urls=[], output_tables=[])
    mock_materialize.return_value = mock_result

    client = HttpQueryServiceClient("http://test:8001")

    result = client.materialize(
        materialization_input=Mock(),
        request_headers={"auth": "token"},
    )

    assert result == mock_result


@patch("datajunction_server.service_clients.QueryServiceClient.materialize_cube")
def test_http_client_materialize_cube(mock_materialize_cube):
    """Test HttpQueryServiceClient.materialize_cube."""
    from datajunction_server.models.materialization import MaterializationInfo

    mock_result = MaterializationInfo(urls=[], output_tables=[])
    mock_materialize_cube.return_value = mock_result

    client = HttpQueryServiceClient("http://test:8001")

    result = client.materialize_cube(
        materialization_input=Mock(),
        request_headers={"auth": "token"},
    )

    assert result == mock_result


@patch(
    "datajunction_server.service_clients.QueryServiceClient.deactivate_materialization",
)
def test_http_client_deactivate_materialization(mock_deactivate):
    """Test HttpQueryServiceClient.deactivate_materialization."""
    from datajunction_server.models.materialization import MaterializationInfo

    mock_result = MaterializationInfo(urls=[], output_tables=[])
    mock_deactivate.return_value = mock_result

    client = HttpQueryServiceClient("http://test:8001")

    result = client.deactivate_materialization(
        node_name="test_node",
        materialization_name="test_mat",
        node_version="v1",
        request_headers={"auth": "token"},
    )

    assert result == mock_result


@patch(
    "datajunction_server.service_clients.QueryServiceClient.get_materialization_info",
)
def test_http_client_get_materialization_info(mock_get_info):
    """Test HttpQueryServiceClient.get_materialization_info."""
    from datajunction_server.models.materialization import MaterializationInfo
    from datajunction_server.models.node_type import NodeType

    mock_result = MaterializationInfo(urls=[], output_tables=[])
    mock_get_info.return_value = mock_result

    client = HttpQueryServiceClient("http://test:8001")

    result = client.get_materialization_info(
        node_name="test_node",
        node_version="v1",
        node_type=NodeType.SOURCE,
        materialization_name="test_mat",
        request_headers={"auth": "token"},
    )

    assert result == mock_result


@patch("datajunction_server.service_clients.QueryServiceClient.run_backfill")
def test_http_client_run_backfill(mock_run_backfill):
    """Test HttpQueryServiceClient.run_backfill."""
    from datajunction_server.models.materialization import MaterializationInfo
    from datajunction_server.models.node_type import NodeType

    mock_result = MaterializationInfo(urls=[], output_tables=[])
    mock_run_backfill.return_value = mock_result

    client = HttpQueryServiceClient("http://test:8001")

    result = client.run_backfill(
        node_name="test_node",
        node_version="v1",
        node_type=NodeType.SOURCE,
        materialization_name="test_mat",
        partitions=[],
        request_headers={"auth": "token"},
    )

    assert result == mock_result
