"""Tests DJ client"""

import pandas
import pytest

from datajunction import DJClient
from datajunction.exceptions import DJClientException
from datajunction.nodes import Cube, Dimension, Metric, Source, Transform


class TestDJClient:  # pylint: disable=too-many-public-methods
    """
    Tests for DJ client functionality.
    """

    @pytest.fixture
    def client(self, module__session_with_examples):
        """
        Returns a DJ client instance
        """
        return DJClient(requests_session=module__session_with_examples)  # type: ignore

    #
    # List basic objects: namespaces, dimensions, metrics, cubes
    #
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

    def test_list_dimensions(self, client):
        """
        Check that `client.list_dimensions()` works as expected.
        """
        # full list
        dims = client.list_dimensions()
        assert set(dims) == {
            "default.repair_order",
            "default.contractor",
            "default.hard_hat",
            "default.local_hard_hats",
            "default.us_state",
            "default.dispatcher",
            "default.municipality_dim",
            "foo.bar.repair_order",
            "foo.bar.contractor",
            "foo.bar.hard_hat",
            "foo.bar.local_hard_hats",
            "foo.bar.us_state",
            "foo.bar.dispatcher",
            "foo.bar.municipality_dim",
        }

        # partial list
        dims = client.list_dimensions(namespace="foo.bar")
        assert set(dims) == {
            "foo.bar.repair_order",
            "foo.bar.contractor",
            "foo.bar.hard_hat",
            "foo.bar.local_hard_hats",
            "foo.bar.us_state",
            "foo.bar.dispatcher",
            "foo.bar.municipality_dim",
        }

    def test_list_metrics(self, client):
        """
        Check that `client.list_metrics()` works as expected.
        """
        # full list
        metrics = client.list_metrics()
        assert set(metrics) == {
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
        }

        # partial list
        metrics = client.list_metrics(namespace="foo.bar")
        assert set(metrics) == {
            "foo.bar.num_repair_orders",
            "foo.bar.avg_repair_price",
            "foo.bar.total_repair_cost",
            "foo.bar.avg_length_of_employment",
            "foo.bar.total_repair_order_discounts",
            "foo.bar.avg_repair_order_discounts",
            "foo.bar.avg_time_to_dispatch",
        }

    def test_list_cubes(self, client):
        """
        Check that `client.list_cubes()` works as expected.
        """
        # full list
        cubes = client.list_cubes()
        assert set(cubes) == {"foo.bar.cube_one", "default.cube_two"}

        # partial list
        cubes = client.list_cubes(namespace="foo.bar")
        assert cubes == ["foo.bar.cube_one"]

    #
    # List other nodes: sources, transforms, all.
    #
    def test_list_sources(self, client):
        """
        Check that `client.list_sources()` works as expected.
        """
        # full list
        nodes = client.list_sources()
        assert set(nodes) == {
            "default.repair_orders",
            "default.repair_orders_foo",
            "default.repair_order_details",
            "default.repair_type",
            "default.contractors",
            "default.municipality_municipality_type",
            "default.municipality_type",
            "default.municipality",
            "default.dispatchers",
            "default.hard_hats",
            "default.hard_hat_state",
            "default.us_states",
            "default.us_region",
            "foo.bar.repair_orders",
            "foo.bar.repair_order_details",
            "foo.bar.repair_type",
            "foo.bar.contractors",
            "foo.bar.municipality_municipality_type",
            "foo.bar.municipality_type",
            "foo.bar.municipality",
            "foo.bar.dispatchers",
            "foo.bar.hard_hats",
            "foo.bar.hard_hat_state",
            "foo.bar.us_states",
            "foo.bar.us_region",
        }

        # partial list
        nodes = client.list_sources(namespace="foo.bar")
        assert set(nodes) == {
            "foo.bar.repair_orders",
            "foo.bar.repair_order_details",
            "foo.bar.repair_type",
            "foo.bar.contractors",
            "foo.bar.municipality_municipality_type",
            "foo.bar.municipality_type",
            "foo.bar.municipality",
            "foo.bar.dispatchers",
            "foo.bar.hard_hats",
            "foo.bar.hard_hat_state",
            "foo.bar.us_states",
            "foo.bar.us_region",
        }

    def test_list_transforms(self, client):
        """
        Check that `client.list_transforms)()` works as expected.
        """
        # full list
        nodes = client.list_transforms()
        assert set(nodes) == {
            "default.repair_orders_thin",
            "foo.bar.repair_orders_thin",
            "foo.bar.with_custom_metadata",
        }

        # partial list
        nodes = client.list_transforms(namespace="foo.bar")
        assert nodes == ["foo.bar.with_custom_metadata", "foo.bar.repair_orders_thin"]

    def test_list_nodes(self, client):
        """
        Check that `client.list_nodes)()` works as expected.
        """
        # full list
        nodes = client.list_nodes()
        assert set(nodes) == set(
            [
                "default.repair_orders",
                "default.repair_orders_foo",
                "default.repair_order_details",
                "default.repair_type",
                "default.contractors",
                "default.municipality_municipality_type",
                "default.municipality_type",
                "default.municipality",
                "default.dispatchers",
                "default.hard_hats",
                "default.hard_hat_state",
                "default.us_states",
                "default.us_region",
                "default.repair_order",
                "default.contractor",
                "default.hard_hat",
                "default.local_hard_hats",
                "default.us_state",
                "default.dispatcher",
                "default.municipality_dim",
                "default.num_repair_orders",
                "default.avg_repair_price",
                "default.total_repair_cost",
                "default.avg_length_of_employment",
                "default.total_repair_order_discounts",
                "default.avg_repair_order_discounts",
                "default.avg_time_to_dispatch",
                "foo.bar.repair_orders",
                "foo.bar.repair_order_details",
                "foo.bar.repair_type",
                "foo.bar.contractors",
                "foo.bar.municipality_municipality_type",
                "foo.bar.municipality_type",
                "foo.bar.municipality",
                "foo.bar.dispatchers",
                "foo.bar.hard_hats",
                "foo.bar.hard_hat_state",
                "foo.bar.us_states",
                "foo.bar.us_region",
                "foo.bar.repair_order",
                "foo.bar.contractor",
                "foo.bar.hard_hat",
                "foo.bar.local_hard_hats",
                "foo.bar.us_state",
                "foo.bar.dispatcher",
                "foo.bar.municipality_dim",
                "foo.bar.num_repair_orders",
                "foo.bar.avg_repair_price",
                "foo.bar.total_repair_cost",
                "foo.bar.avg_length_of_employment",
                "foo.bar.total_repair_order_discounts",
                "foo.bar.avg_repair_order_discounts",
                "foo.bar.avg_time_to_dispatch",
                "foo.bar.cube_one",
                "default.cube_two",
                "default.repair_orders_thin",
                "foo.bar.repair_orders_thin",
                "foo.bar.with_custom_metadata",
            ],
        )

        # partial list
        nodes = client.list_nodes(namespace="foo.bar")
        assert set(nodes) == set(
            [
                "foo.bar.repair_orders",
                "foo.bar.repair_order_details",
                "foo.bar.repair_type",
                "foo.bar.contractors",
                "foo.bar.municipality_municipality_type",
                "foo.bar.municipality_type",
                "foo.bar.municipality",
                "foo.bar.dispatchers",
                "foo.bar.hard_hats",
                "foo.bar.hard_hat_state",
                "foo.bar.us_states",
                "foo.bar.us_region",
                "foo.bar.repair_order",
                "foo.bar.contractor",
                "foo.bar.hard_hat",
                "foo.bar.local_hard_hats",
                "foo.bar.us_state",
                "foo.bar.dispatcher",
                "foo.bar.municipality_dim",
                "foo.bar.num_repair_orders",
                "foo.bar.avg_repair_price",
                "foo.bar.total_repair_cost",
                "foo.bar.avg_length_of_employment",
                "foo.bar.total_repair_order_discounts",
                "foo.bar.avg_repair_order_discounts",
                "foo.bar.avg_time_to_dispatch",
                "foo.bar.cube_one",
                "foo.bar.repair_orders_thin",
                "foo.bar.with_custom_metadata",
            ],
        )

    def test_find_nodes_with_dimension(self, client):
        """
        Check that `dimension.linked_nodes()` works as expected.
        """
        repair_order_dim = client.dimension("default.repair_order")
        assert repair_order_dim.linked_nodes() == [
            "default.repair_order_details",
            "default.avg_repair_price",
            "default.total_repair_cost",
            "default.total_repair_order_discounts",
            "default.avg_repair_order_discounts",
        ]

    def test_refresh_source_node(self, client):
        """
        Check that `Source.validate()` works as expected.
        """
        # change the source node
        source_node = client.source("default.repair_orders_foo")
        version_before = source_node.current_version
        response = source_node.validate()
        assert response == "valid"
        version_after = source_node.current_version
        assert version_before and version_after and version_before != version_after

        # change the source node (but not really)
        source_node = client.source("default.repair_orders_foo")
        version_before = source_node.current_version
        response = source_node.validate()
        assert response == "valid"
        version_after = source_node.current_version
        assert version_before and version_after and version_before == version_after

    #
    # Get common metrics and dimensions
    #
    def test_common_dimensions(self, client):
        """
        Test that getting common dimensions for metrics works
        """
        dims = client.common_dimensions(
            metrics=["default.num_repair_orders", "default.avg_repair_price"],
        )
        assert len(dims) == 28

    #
    # SQL and data
    #
    def test_data(self, client):
        """
        Test data retreval for a metric and dimension(s)
        """
        # Should throw error when no name or metrics are passed in
        with pytest.raises(DJClientException):
            client.node_data("")

        with pytest.raises(DJClientException):
            client.data([])

        # Retrieve data for a single metric
        expected_df = pandas.DataFrame.from_dict(
            {
                "default_DOT_hard_hat_DOT_city": ["Foo", "Bar"],
                "default_DOT_avg_repair_price": [1.0, 2.0],
            },
        )

        result = client.data(
            metrics=["default.avg_repair_price"],
            dimensions=["default.hard_hat.city"],
        )
        pandas.testing.assert_frame_equal(result, expected_df)

        # Retrieve data for a single node
        result = client.node_data(
            node_name="default.avg_repair_price",
            dimensions=["default.hard_hat.city"],
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
        # with pytest.raises(DJClientException) as exc_info:
        #     client.data(
        #         metrics=["default.avg_repair_price"],
        #         dimensions=["default.hard_hat.postal_code"],
        #     )
        # assert "Error response from query service" in str(exc_info)

    def test_sql(self, client):
        """
        Test SQL retrieval
        """
        # Retrieve sql for metrics
        result = client.sql(
            metrics=["default.avg_repair_price", "default.num_repair_orders"],
            dimensions=["default.hard_hat.city"],
            filters=["default.hard_hat.state = 'NY'"],
        )
        assert isinstance(result, str)

        # Retrieve sql for a node
        result = client.node_sql(
            node_name="default.repair_order_details",
            dimensions=["default.hard_hat.city"],
            filters=["default.hard_hat.state = 'NY'"],
        )
        assert isinstance(result, str)

        # Retrieve sql for a node (error)
        result = client.node_sql(
            node_name="default.repair_order_details",
            dimensions=["default.repair_order.repair_order_id1"],
            filters=["default.repair_order.repair_order_id = 1222"],
        )
        assert result["message"] == (
            "default.repair_order.repair_order_id1 are not available dimensions"
            " on default.repair_order_details"
        )

        # Retrieve measures sql for metrics
        result = client.sql(
            metrics=["default.avg_repair_price", "default.num_repair_orders"],
            dimensions=["default.hard_hat.city"],
            filters=["default.hard_hat.state = 'NY'"],
            measures=True,
        )
        for generated_sql in result:
            assert generated_sql["node"]["name"] in (
                "default.repair_order_details",
                "default.repair_orders",
            )
            assert isinstance(generated_sql["sql"], str)

    #
    # Data Catalog and Engines
    #
    def test_list_catalogs(self, client):
        """
        Check that `client.list_catalogs()` works as expected.
        """
        result = client.list_catalogs()
        assert set(result) == {"unknown", "draft", "default", "public"}

    def test_list_engines(self, client):
        """
        Check that `client.list_engines()` works as expected.
        """
        result = client.list_engines()
        assert result == [
            {"name": "spark", "version": "3.1.1"},
            {"name": "postgres", "version": "15.2"},
        ]

    def test_get_dag(self, client):
        """
        Check that `node.upstreams()`, `node.downstreams()`, and `node.dimensions()`
        all work as expected
        """
        num_repair_orders = client.metric("default.num_repair_orders")
        result = num_repair_orders.get_upstreams()
        assert result == ["default.repair_orders"]
        result = num_repair_orders.get_downstreams()
        assert result == ["default.cube_two"]
        result = num_repair_orders.get_dimensions()
        assert len(result) == 31

        hard_hat = client.dimension("default.hard_hat")
        result = hard_hat.get_upstreams()
        assert result == ["default.hard_hats"]
        result = hard_hat.get_downstreams()
        assert result == []
        result = hard_hat.get_dimensions()
        assert len(result) == 18

    def test_get_node(self, client):
        """
        Verifies that retrieving a node (of any type) works with:
            dj.node(<node_name>)
        """
        hard_hat = client.node("default.hard_hat")
        assert isinstance(hard_hat, Dimension)
        assert hard_hat.name == "default.hard_hat"

        num_repair_orders = client.node("default.num_repair_orders")
        assert isinstance(num_repair_orders, Metric)
        assert num_repair_orders.name == "default.num_repair_orders"

        repair_orders_thin = client.node("default.repair_orders_thin")
        assert isinstance(repair_orders_thin, Transform)
        assert repair_orders_thin.name == "default.repair_orders_thin"

        repair_orders = client.node("default.repair_orders")
        assert isinstance(repair_orders, Source)
        assert repair_orders.name == "default.repair_orders"

        cube_two = client.node("default.cube_two")
        assert isinstance(cube_two, Cube)
        assert cube_two.name == "default.cube_two"
        assert cube_two.metrics == ["default.num_repair_orders"]
        assert cube_two.dimensions == ["default.municipality_dim.local_region"]
