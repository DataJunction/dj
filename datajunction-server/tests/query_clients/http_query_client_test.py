"""Tests for HTTP query service client wrapper."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from datajunction_server.models.cube_materialization import (
    CubeMaterializationV2Input,
)
from datajunction_server.models.materialization import (
    MaterializationStrategy,
)
from datajunction_server.models.preaggregation import CubeBackfillInput
from datajunction_server.query_clients.http import HttpQueryServiceClient


class TestHttpQueryServiceClientCubeV2Methods:
    """Tests for HttpQueryServiceClient cube v2 methods."""

    @pytest.fixture
    def mock_client(self):
        """Create client with mocked underlying QueryServiceClient."""
        with patch(
            "datajunction_server.query_clients.http.QueryServiceClient",
        ) as MockClient:
            mock_inner = MagicMock()
            MockClient.return_value = mock_inner
            client = HttpQueryServiceClient(uri="http://test:8000")
            yield client, mock_inner

    def test_materialize_cube_v2(self, mock_client):
        """Test materialize_cube_v2 delegates to underlying client."""
        client, mock_inner = mock_client
        mock_inner.materialize_cube_v2.return_value = MagicMock(urls=["http://wf1"])

        input_data = CubeMaterializationV2Input(
            cube_name="test.cube",
            cube_version="v1",
            preagg_tables=[],
            combined_sql="SELECT 1",
            combined_columns=[],
            combined_grain=[],
            druid_datasource="test_ds",
            druid_spec={},
            timestamp_column="date_id",
            timestamp_format="yyyyMMdd",
            strategy=MaterializationStrategy.FULL,
            schedule="0 0 * * *",
        )
        client.materialize_cube_v2(input_data, request_headers={"X-Test": "1"})

        mock_inner.materialize_cube_v2.assert_called_once_with(
            materialization_input=input_data,
            request_headers={"X-Test": "1"},
        )

    def test_deactivate_cube_workflow(self, mock_client):
        """Test deactivate_cube_workflow delegates to underlying client."""
        client, mock_inner = mock_client
        mock_inner.deactivate_cube_workflow.return_value = {"status": "deactivated"}

        client.deactivate_cube_workflow("test.cube", request_headers={"X-Test": "1"})

        mock_inner.deactivate_cube_workflow.assert_called_once_with(
            cube_name="test.cube",
            version=None,
            request_headers={"X-Test": "1"},
        )

    def test_deactivate_cube_workflow_with_version(self, mock_client):
        """Test deactivate_cube_workflow with version delegates to underlying client."""
        client, mock_inner = mock_client
        mock_inner.deactivate_cube_workflow.return_value = {"status": "deactivated"}

        client.deactivate_cube_workflow(
            "test.cube",
            version="v3",
            request_headers={"X-Test": "1"},
        )

        mock_inner.deactivate_cube_workflow.assert_called_once_with(
            cube_name="test.cube",
            version="v3",
            request_headers={"X-Test": "1"},
        )

    def test_run_cube_backfill(self, mock_client):
        """Test run_cube_backfill delegates to underlying client."""
        client, mock_inner = mock_client
        mock_inner.run_cube_backfill.return_value = {"job_url": "http://job1"}

        backfill_input = CubeBackfillInput(
            cube_name="test.cube",
            cube_version="v1.0",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )
        client.run_cube_backfill(backfill_input, request_headers={"X-Test": "1"})

        mock_inner.run_cube_backfill.assert_called_once_with(
            backfill_input=backfill_input,
            request_headers={"X-Test": "1"},
        )

    def test_refresh_cube_materialization(self, mock_client):
        """Test refresh_cube_materialization delegates to underlying client."""
        client, mock_inner = mock_client
        mock_inner.refresh_cube_materialization.return_value = MagicMock(
            urls=["http://wf1"],
        )

        materializations = [
            {"name": "mat1", "job": "DruidCubeMaterializationJob"},
        ]
        client.refresh_cube_materialization(
            cube_name="test.cube",
            cube_version="v1.0",
            materializations=materializations,
            request_headers={"X-Test": "1"},
        )

        mock_inner.refresh_cube_materialization.assert_called_once_with(
            cube_name="test.cube",
            cube_version="v1.0",
            materializations=materializations,
            request_headers={"X-Test": "1"},
        )
