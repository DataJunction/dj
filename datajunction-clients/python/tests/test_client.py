"""Tests DJ client"""
import pandas
import pytest

from datajunction import DJClient
from datajunction.exceptions import DJClientException


class TestDJClient:  # pylint: disable=too-many-public-methods
    """
    Tests for DJ client functionality.
    """

    @pytest.fixture
    def client(self, session_with_examples):
        """
        Returns a DJ client instance
        """
        return DJClient(requests_session=session_with_examples)  # type: ignore

    def test_list_namespaces(self, client):
        """
        Check that `client.list_namespaces()` works as expected.
        """
        # full list
        expected = ["default", "foo.bar"]
        result = client.list_namespaces()
        assert result == expected

        # partial list
        partial = ["foo.bar"]
        result = client.list_namespaces(prefix="foo")
        assert result == partial

    def test_list_metrics(self, client):
        """
        Check that `client.list_metrics()` works as expected.
        """
        metrics = client.list_metrics()
        assert metrics == [
            "default.num_repair_orders",
            "default.avg_repair_price",
            "default.total_repair_cost",
            "default.avg_length_of_employment",
            "default.total_repair_order_discounts",
            "default.avg_repair_order_discounts",
            "default.avg_time_to_dispatch",
            "foo.bar.num_repair_orders",
            "foo.bar.avg_repair_price",
            "foo.bar.total_repair_cost",
            "foo.bar.avg_length_of_employment",
            "foo.bar.total_repair_order_discounts",
            "foo.bar.avg_repair_order_discounts",
            "foo.bar.avg_time_to_dispatch",
        ]

    def test_data(self, client):
        """
        Test data retreval for a metric and dimension(s)
        """
        # Retrieve data for a single metric
        result = client.data(
            metrics=["default.avg_repair_price"],
            dimensions=["default.hard_hat.city"],
        )

        expected_df = pandas.DataFrame.from_dict(
            {"default_DOT_avg_repair_price": [1.0, 2.0], "city": ["Foo", "Bar"]},
        )
        pandas.testing.assert_frame_equal(result, expected_df)

        # No data
        with pytest.raises(DJClientException) as exc_info:
            client.data(
                metrics=["default.avg_repair_price"],
                dimensions=["default.hard_hat.state"],
            )
        assert "No data for query!" in str(exc_info)

        # Error propagation
        with pytest.raises(DJClientException) as exc_info:
            client.data(
                metrics=["default.avg_repair_price"],
                dimensions=["default.hard_hat.postal_code"],
            )
        assert "Error response from query service" in str(exc_info)
