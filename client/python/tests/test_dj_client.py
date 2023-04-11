"""Tests DJ client"""
import pandas as pd
import pytest
import responses

from djclient.dj import Catalog, DJClient, Engine, Metric, Source, Transform
from djclient.exceptions import DJClientException


class TestDJClient:
    """
    Tests for DJ client functionality.
    """

    @pytest.fixture
    def client(self):
        """
        Returns a DJ client instance
        """
        return DJClient(uri="http://localhost:8000")

    def test_client_not_initialized(self):
        """
        Verify that it raises an exception when the DJ client isn't initialized.
        """
        source = Source(
            name="apples",
            description="A record of all apples in the store.",
            display_name="Apples",
            catalog="prod",
            schema_="store",
            table="apples",
        )
        with pytest.raises(DJClientException) as exc_info:
            source.publish()
        assert "DJ client not initialized!" in str(exc_info)

    @responses.activate
    def test_catalogs(self, client):
        """
        Check that `client.catalogs()` works as expected.
        """
        expected = [
            {
                "name": "prod",
                "engines": [{"name": "spark", "version": "123", "uri": "spark://"}],
            },
        ]
        responses.add(responses.GET, "http://localhost:8000/catalogs/", json=expected)
        result = client.catalogs()
        assert result == expected

    @responses.activate
    def test_engines(self, client):
        """
        Check that `client.engines()` works as expected.
        """
        expected = [{"name": "spark", "version": "123", "uri": "spark://"}]
        responses.add(responses.GET, "http://localhost:8000/engines/", json=expected)
        result = client.engines()
        assert result == expected

    @responses.activate
    def test_all_nodes(self, client):
        """
        Verifies that retrieving nodes with `client.nodes()` or node-type
        specific calls like `client.sources()` work.
        """
        expected = [
            {"name": "node1", "type": "source"},
            {"name": "node2", "type": "dimension"},
            {"name": "node3", "type": "transform"},
            {"name": "node4", "type": "metric"},
            {"name": "node5", "type": "cube"},
        ]
        responses.add(responses.GET, "http://localhost:8000/nodes/", json=expected)
        result = client.nodes()
        assert result == expected
        expected_names_only = ["node1", "node2", "node3", "node4", "node5"]
        result_names_only = client.nodes(names_only=True)
        assert result_names_only == expected_names_only

        # sources
        result = client.sources()
        assert result == [expected[0]]
        result_names_only = client.sources(names_only=True)
        assert result_names_only == ["node1"]

        # dimensions
        result = client.dimensions()
        assert result == [expected[1]]
        result_names_only = client.dimensions(names_only=True)
        assert result_names_only == ["node2"]

        # transforms
        result = client.transforms()
        assert result == [expected[2]]
        result_names_only = client.transforms(names_only=True)
        assert result_names_only == ["node3"]

        # metrics
        result = client.metrics()
        assert result == [expected[3]]
        result_names_only = client.metrics(names_only=True)
        assert result_names_only == ["node4"]

        # cubes
        result = client.cubes()
        assert result == [expected[4]]
        result_names_only = client.cubes(names_only=True)
        assert result_names_only == ["node5"]

    @responses.activate
    def test_delete_node(self, client):  # pylint: disable=unused-argument
        """
        Verifies that deleting a node works.
        """
        source = Source(
            name="apples",
            description="A record of all apples in the store.",
            display_name="Apples",
            catalog="prod",
            schema_="store",
            table="apples",
        )
        responses.add(
            responses.DELETE,
            "http://localhost:8000/nodes/apples/",
            status=204,
        )
        response = source.delete()
        assert response == "Successfully deleted `apples`"

    @responses.activate
    def test_create_node(self, client):  # pylint: disable=unused-argument
        """
        Verifies that retrieving nodes with `client.nodes()` or
        node-type specific calls like `client.sources()` work.
        """
        source = Source(
            name="apples",
            description="A record of all apples in the store.",
            display_name="Apples",
            catalog="prod",
            schema_="store",
            table="apples",
        )
        expected = {
            "name": "apples",
            "description": "A record of all apples in the store.",
            "type": "source",
            "mode": None,
            "display_name": "Apples",
            "availability": None,
            "tags": None,
            "catalog": "prod",
            "schema_": "store",
            "table": "apples",
            "columns": None,
        }

        responses.add(
            responses.POST,
            "http://localhost:8000/nodes/source/",
            json=expected,
        )
        responses.add(
            responses.GET,
            "http://localhost:8000/nodes/apples/",
            json={**expected, **{"node_revision_id": 1}},
        )
        source.publish()
        source.draft()

    @responses.activate
    def test_link_dimension(self, client):  # pylint: disable=unused-argument
        """
        Check that `client.engines()` works as expected.
        """
        expected = {"message": "success"}
        responses.add(
            responses.POST,
            "http://localhost:8000/nodes/fruit_purchases/columns/fruit/"
            "?dimension=fruits&dimension_column=fruit",
            json=expected,
        )
        transform_node = Transform(
            name="fruit_purchases",
            query="SELECT purchase_id, fruit, cost, cost_per_unit FROM purchase_records",
        )
        result = transform_node.link_dimension("fruit", "fruits", "fruit")
        assert result == expected

    @responses.activate
    def test_sql(self, client):  # pylint: disable=unused-argument
        """
        Check that `client.engines()` works as expected.
        """
        expected = {"sql": "SELECT count(*) FROM fruit_purchases WHERE fruit='apple'"}
        responses.add(
            responses.GET,
            "http://localhost:8000/sql/apple_count/",
            json=expected,
        )
        metric = Metric(
            name="apple_count",
            query="SELECT count(*) FROM fruit_purchases WHERE fruit='apple'",
        )
        result = metric.sql(dimensions=[], filters=[])
        assert result == expected["sql"]

    @responses.activate
    def test_data(self, client):  # pylint: disable=unused-argument
        """
        Check that `client.engines()` works as expected.
        """
        expected = {
            "results": [
                {
                    "sql": "SELECT count(*) FROM fruit_purchases WHERE fruit='apple'",
                    "columns": [{"name": "apple_count"}],
                    "rows": [[1], [2]],
                },
            ],
        }
        responses.add(
            responses.GET,
            "http://localhost:8000/data/apple_count/",
            json=expected,
        )
        metric = Metric(
            name="apple_count",
            query="SELECT count(*) FROM fruit_purchases WHERE fruit='apple'",
        )
        result = metric.data(dimensions=[], filters=[])
        assert isinstance(result, pd.DataFrame)

    @responses.activate
    def test_get_dimensions(self, client):  # pylint: disable=unused-argument
        """
        Check that `client.engines()` works as expected.
        """
        expected = {"dimensions": ["fruit"]}
        responses.add(
            responses.GET,
            "http://localhost:8000/metrics/apple_count/",
            json=expected,
        )
        metric = Metric(
            name="apple_count",
            query="SELECT count(*) FROM fruit_purchases WHERE fruit='apple'",
        )
        result = metric.dimensions()
        assert result == expected["dimensions"]

    @responses.activate
    def test_add_catalog(self, client):  # pylint: disable=unused-argument
        """
        Check that adding a catalog works.
        """
        expected = {}
        responses.add(
            responses.POST,
            "http://localhost:8000/catalogs/",
            json=expected,
        )
        catalog = Catalog(
            name="prodhive",
        )
        result = catalog.publish()
        assert result == expected

    @responses.activate
    def test_add_engine(self, client):  # pylint: disable=unused-argument
        """
        Check that adding an engine works.
        """
        expected = {}
        responses.add(
            responses.POST,
            "http://localhost:8000/engines/",
            json=expected,
        )
        engine = Engine(
            name="spark",
            version="2.4.4",
            uri="",
        )
        result = engine.publish()
        assert result == expected

    @responses.activate
    def test_add_engine_to_catalog(self, client):  # pylint: disable=unused-argument
        """
        Check that adding an engine works.
        """
        expected = {}
        responses.add(
            responses.POST,
            "http://localhost:8000/catalogs/prodhive/engines/",
            json=expected,
        )
        engine = Engine(
            name="prodhive",
            version="",
            uri="",
        )

        catalog = Catalog(
            name="prodhive",
        )
        result = catalog.add_engine(engine)
        assert result == expected

        result = catalog.add_engines([engine])
        assert result == expected
