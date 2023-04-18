"""Tests DJ client"""
import pandas as pd
import pytest
import responses

from djclient import DJClient
from djclient.dj import NodeMode
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

    @responses.activate
    def test_namespaces(self, client):
        """
        Check that `client.namespaces()` works as expected.
        """
        expected = [
            {
                "namespace": "default",
            },
        ]
        responses.add(
            responses.GET,
            "http://localhost:8000/namespaces/",
            json=expected,
        )
        result = client.namespaces()
        assert result == expected

    @responses.activate
    def test_nodes_in_namespace(self, client):
        """
        Check that `client.get_nodes_in_namespace()` works as expected.
        """
        expected = [
            {"name": "basic.example", "type": "source"},
            {"name": "basic.example_dimension", "type": "dimension"},
            {"name": "basic.example_metric", "type": "metric"},
            {"name": "basic.example_transform", "type": "transform"},
            {"name": "basic.example_cube", "type": "cube"},
        ]
        responses.add(
            responses.GET,
            "http://localhost:8000/namespaces/basic/",
            json=expected,
        )
        assert client.namespace("basic").nodes(names_only=True) == [
            "basic.example",
            "basic.example_dimension",
            "basic.example_metric",
            "basic.example_transform",
            "basic.example_cube",
        ]
        assert client.namespace("basic").sources(names_only=True) == [
            "basic.example",
        ]
        assert client.namespace("basic").dimensions(names_only=True) == [
            "basic.example_dimension",
        ]
        assert client.namespace("basic").metrics(names_only=True) == [
            "basic.example_metric",
        ]
        assert client.namespace("basic").transforms(names_only=True) == [
            "basic.example_transform",
        ]
        assert client.namespace("basic").cubes(names_only=True) == [
            "basic.example_cube",
        ]

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
            {
                "name": "node1",
                "type": "source",
                "catalog": "prod",
                "schema_": "random",
                "table": "test",
            },
            {"name": "node2", "type": "dimension", "query": "SELECT 1"},
            {"name": "node3", "type": "transform", "query": "SELECT 1"},
            {"name": "node4", "type": "metric", "query": "SELECT SUM(1)"},
            {
                "name": "node5",
                "type": "cube",
                "metrics": [],
                "dimensions": [],
                "filters": [],
            },
        ]
        responses.add(responses.GET, "http://localhost:8000/nodes/", json=expected)
        expected_names_only = ["node1", "node2", "node3", "node4", "node5"]
        result_names_only = client.nodes(names_only=True)
        assert result_names_only == expected_names_only

        # sources
        result = client.sources()
        assert result[0].name == "node1"
        assert result[0].catalog == "prod"
        assert result[0].schema_ == "random"
        assert result[0].table == "test"
        assert result[0].type == "source"
        result_names_only = client.sources(names_only=True)
        assert result_names_only == ["node1"]

        # dimensions
        result = client.dimensions()
        assert result[0].name == "node2"
        assert result[0].query == "SELECT 1"
        assert result[0].type == "dimension"
        result_names_only = client.dimensions(names_only=True)
        assert result_names_only == ["node2"]

        # transforms
        result = client.transforms()
        assert result[0].name == "node3"
        assert result[0].query == "SELECT 1"
        assert result[0].type == "transform"
        result_names_only = client.transforms(names_only=True)
        assert result_names_only == ["node3"]

        # metrics
        result = client.metrics()
        assert result[0].name == "node4"
        assert result[0].query == "SELECT SUM(1)"
        assert result[0].type == "metric"
        result_names_only = client.metrics(names_only=True)
        assert result_names_only == ["node4"]

        # cubes
        result = client.cubes()
        assert result[0].name == "node5"
        assert result[0].metrics == []
        assert result[0].type == "cube"
        result_names_only = client.cubes(names_only=True)
        assert result_names_only == ["node5"]

    @responses.activate
    def test_delete_node(self, client):  # pylint: disable=unused-argument
        """
        Verifies that deleting a node works.
        """
        responses.add(
            responses.GET,
            "http://localhost:8000/nodes/apples/",
            status=200,
            json={
                "name": "apples",
                "type": "source",
                "description": "A record of all apples in the store.",
                "display_name": "Apples",
                "catalog": "prod",
                "schema_": "store",
                "table": "apples",
            },
        )
        source = client.source(node_name="apples")
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
        Verifies that creating a new node works.
        """
        source = client.new_source(
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
        source.save(mode=NodeMode.PUBLISHED)

        transform = client.new_transform(
            name="orchards",
            description="An orchard transform",
            display_name="Orchards",
            query="SELECT 1",
        )
        expected = {
            "name": "orchards",
            "description": "An orchard transform",
            "type": "transform",
            "mode": "published",
            "display_name": "Orchards",
            "availability": None,
            "tags": None,
            "query": "SELECT 1",
        }
        responses.add(
            responses.GET,
            "http://localhost:8000/nodes/orchards/",
            status=404,
            json={"message": "not found"},
        )
        responses.add(
            responses.POST,
            "http://localhost:8000/nodes/transform/",
            json=expected,
        )
        transform.save(mode=NodeMode.PUBLISHED)

        dimension = client.new_dimension(
            name="records",
            description="A records dimension",
            display_name="Records",
            query="SELECT 1",
            primary_key=["id"],
        )
        expected = {
            "name": "records",
            "description": "A records dimension",
            "type": "dimension",
            "mode": "published",
            "display_name": "Records",
            "availability": None,
            "tags": None,
            "query": "SELECT 1",
            "primary_key": ["id"],
        }
        responses.add(
            responses.GET,
            "http://localhost:8000/nodes/records/",
            status=404,
            json={"message": "not found"},
        )
        responses.add(
            responses.POST,
            "http://localhost:8000/nodes/dimension/",
            json=expected,
        )
        dimension.save(mode=NodeMode.PUBLISHED)

        metric = client.new_metric(
            name="records_count",
            description="Number of records",
            display_name="Number of Records",
            query="SELECT COUNT(*) FROM records",
        )
        expected = {
            "name": "records_count",
            "description": "Number of records",
            "type": "metric",
            "mode": "published",
            "display_name": "Number of records",
            "availability": None,
            "tags": None,
            "query": "SELECT COUNT(*) FROM records",
        }
        responses.add(
            responses.GET,
            "http://localhost:8000/nodes/records_count/",
            status=404,
            json={"message": "not found"},
        )
        responses.add(
            responses.POST,
            "http://localhost:8000/nodes/metric/",
            json=expected,
        )
        metric.save(mode=NodeMode.PUBLISHED)

    @responses.activate
    def test_link_dimension(self, client):  # pylint: disable=unused-argument
        """
        Check linking dimensions works
        """
        responses.add(
            responses.GET,
            "http://localhost:8000/nodes/fruit/",
            json={
                "name": "fruit",
                "type": "dimension",
                "query": "SELECT 1",
            },
        )
        dimension_node = client.dimension(node_name="fruit")
        assert dimension_node.name == "fruit"

        responses.add(
            responses.GET,
            "http://localhost:8000/nodes/fruit_purchases/",
            json={
                "name": "fruit_purchases",
                "type": "transform",
                "query": "SELECT purchase_id, fruit, cost, cost_per_unit FROM purchase_records",
            },
        )
        transform_node = client.transform(node_name="fruit_purchases")

        expected = {"message": "success"}
        responses.add(
            responses.POST,
            "http://localhost:8000/nodes/fruit_purchases/columns/fruit/"
            "?dimension=fruits&dimension_column=fruit",
            json=expected,
        )
        result = transform_node.link_dimension("fruit", "fruits", "fruit")
        assert result == expected

    @pytest.fixture
    def apple_count_metric(self):
        """
        Metric response fixture
        """
        return responses.add(
            responses.GET,
            "http://localhost:8000/nodes/apple_count/",
            json={
                "name": "apple_count",
                "type": "metric",
                "query": "SELECT count(*) FROM fruit_purchases WHERE fruit='apple'",
            },
        )

    @responses.activate
    def test_sql(self, client, apple_count_metric):  # pylint: disable=unused-argument
        """
        Check that `client.engines()` works as expected.
        """
        expected = {"sql": "SELECT count(*) FROM fruit_purchases WHERE fruit='apple'"}
        responses.add(
            responses.GET,
            "http://localhost:8000/sql/apple_count/",
            json=expected,
        )
        metric = client.metric(node_name="apple_count")
        result = metric.sql(dimensions=[], filters=[])
        assert result == expected["sql"]

    @responses.activate
    def test_sql_failed(
        self,
        client,
        apple_count_metric,
    ):  # pylint: disable=unused-argument
        """
        Check that `client.engines()` works as expected.
        """
        responses.add(
            responses.GET,
            "http://localhost:8000/sql/apple_count/",
            json={"message": "Metric not found"},
            status=404,
        )
        metric = client.metric(node_name="apple_count")
        result = metric.sql(dimensions=[], filters=[])
        assert result == {"message": "Metric not found"}

    @responses.activate
    def test_data(self, client, apple_count_metric):  # pylint: disable=unused-argument
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
        metric = client.metric(node_name="apple_count")
        result = metric.data(dimensions=[], filters=[])
        assert isinstance(result, pd.DataFrame)

    @responses.activate
    def test_get_dimensions(
        self,
        client,
        apple_count_metric,
    ):  # pylint: disable=unused-argument
        """
        Check that `client.engines()` works as expected.
        """
        expected = {"dimensions": ["fruit"]}
        responses.add(
            responses.GET,
            "http://localhost:8000/metrics/apple_count/",
            json=expected,
        )
        metric = client.metric(node_name="apple_count")
        result = metric.dimensions()
        assert result == expected["dimensions"]

    @responses.activate
    def test_failure_modes(self, client):
        """
        Test some client failure modes when retrieving nodes.
        """
        responses.add(
            responses.GET,
            "http://localhost:8000/nodes/fruit/",
            status=404,
            json={
                "message": "Not found",
            },
        )
        with pytest.raises(DJClientException) as excinfo:
            client.transform(node_name="fruit")
        assert "No node with name fruit exists!" in str(excinfo)

        responses.add(
            responses.GET,
            "http://localhost:8000/nodes/fruit/",
            json={
                "name": "fruit",
                "type": "dimension",
                "query": "SELECT 1",
            },
        )
        with pytest.raises(DJClientException) as excinfo:
            client.transform(node_name="fruit")
        assert "A node with name fruit exists, but it is not a transform node!" in str(
            excinfo,
        )
