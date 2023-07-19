# pylint: disable=too-many-lines
"""
Tests for the nodes API.
"""
from typing import Any, Dict
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from datajunction_server.api.nodes import decompose_expression
from datajunction_server.models import Database, Table
from datajunction_server.models.column import Column
from datajunction_server.models.node import Node, NodeRevision, NodeStatus, NodeType
from datajunction_server.sql.parsing import ast, types
from datajunction_server.sql.parsing.types import IntegerType, StringType, TimestampType
from tests.sql.utils import compare_query_strings


def materialization_compare(response, expected):
    """Compares two materialization lists of json
    configs paying special attention to query comparison"""
    for (materialization_response, materialization_expected) in zip(response, expected):
        assert compare_query_strings(
            materialization_response["config"]["query"],
            materialization_expected["config"]["query"],
        )
        del materialization_response["config"]["query"]
        del materialization_expected["config"]["query"]
        assert materialization_response == materialization_expected


def test_read_node(client_with_examples: TestClient) -> None:
    """
    Test ``GET /nodes/{node_id}``.
    """
    response = client_with_examples.get("/nodes/default.repair_orders/")
    data = response.json()

    assert response.status_code == 200
    assert data["version"] == "v1.0"
    assert data["node_id"] == 1
    assert data["node_revision_id"] == 1
    assert data["type"] == "source"

    response = client_with_examples.get("/nodes/default.nothing/")
    data = response.json()

    assert response.status_code == 404
    assert data["message"] == "A node with name `default.nothing` does not exist."


def test_read_nodes(session: Session, client: TestClient) -> None:
    """
    Test ``GET /nodes/``.
    """
    node1 = Node(
        name="not-a-metric",
        type=NodeType.SOURCE,
        current_version="1",
    )
    node_rev1 = NodeRevision(
        node=node1,
        version="1",
        name=node1.name,
        type=node1.type,
    )
    node2 = Node(
        name="also-not-a-metric",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    node_rev2 = NodeRevision(
        name=node2.name,
        node=node2,
        version="1",
        query="SELECT 42 AS answer",
        type=node2.type,
        columns=[
            Column(name="answer", type=IntegerType()),
        ],
    )
    node3 = Node(name="a-metric", type=NodeType.METRIC, current_version="1")
    node_rev3 = NodeRevision(
        name=node3.name,
        node=node3,
        version="1",
        query="SELECT COUNT(*) FROM my_table",
        columns=[
            Column(name="_col0", type=IntegerType()),
        ],
        type=node3.type,
    )
    session.add(node_rev1)
    session.add(node_rev2)
    session.add(node_rev3)
    session.commit()

    response = client.get("/nodes/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 3

    nodes = {node["name"]: node for node in data}
    assert nodes["not-a-metric"]["query"] is None
    assert nodes["not-a-metric"]["version"] == "1"
    assert nodes["not-a-metric"]["display_name"] == "Not-A-Metric"
    assert not nodes["not-a-metric"]["columns"]
    assert nodes["not-a-metric"]["parents"] == []

    assert nodes["also-not-a-metric"]["query"] == "SELECT 42 AS answer"
    assert nodes["also-not-a-metric"]["display_name"] == "Also-Not-A-Metric"
    assert nodes["also-not-a-metric"]["columns"] == [
        {
            "name": "answer",
            "type": "int",
            "attributes": [],
            "dimension": None,
        },
    ]

    assert nodes["a-metric"]["query"] == "SELECT COUNT(*) FROM my_table"
    assert nodes["a-metric"]["display_name"] == "A-Metric"
    assert nodes["a-metric"]["columns"] == [
        {
            "name": "_col0",
            "type": "int",
            "attributes": [],
            "dimension": None,
        },
    ]
    assert nodes["a-metric"]["parents"] == []


class TestCreateOrUpdateNodes:  # pylint: disable=too-many-public-methods
    """
    Test ``POST /nodes/`` and ``PUT /nodes/{name}``.
    """

    @pytest.fixture
    def create_dimension_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a dimension node.
        """

        return {
            "description": "Country dimension",
            "query": "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country",
            "mode": "published",
            "name": "default.countries",
            "primary_key": ["country"],
        }

    @pytest.fixture
    def create_invalid_transform_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a transform node.
        """

        return {
            "name": "default.country_agg",
            "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM comments",
            "mode": "published",
            "description": "Distinct users per country",
            "columns": [
                {"name": "country", "type": "string"},
                {"name": "num_users", "type": "int"},
            ],
        }

    @pytest.fixture
    def create_transform_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a transform node.
        """

        return {
            "name": "default.country_agg",
            "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "mode": "published",
            "description": "Distinct users per country",
            "columns": [
                {"name": "country", "type": "string"},
                {"name": "num_users", "type": "int"},
            ],
        }

    @pytest.fixture
    def database(self, session: Session) -> Database:
        """
        A database fixture.
        """

        database = Database(name="postgres", URI="postgres://")
        session.add(database)
        session.commit()
        return database

    @pytest.fixture
    def source_node(self, session: Session, database: Database) -> Node:
        """
        A source node fixture.
        """

        table = Table(
            database=database,
            table="A",
            columns=[
                Column(name="ds", type=StringType()),
                Column(name="user_id", type=IntegerType()),
            ],
        )
        node = Node(
            name="basic.source.users",
            type=NodeType.SOURCE,
            current_version="1",
        )
        node_revision = NodeRevision(
            node=node,
            name=node.name,
            type=node.type,
            version="1",
            tables=[table],
            columns=[
                Column(name="id", type=IntegerType()),
                Column(name="full_name", type=StringType()),
                Column(name="age", type=IntegerType()),
                Column(name="country", type=StringType()),
                Column(name="gender", type=StringType()),
                Column(name="preferred_language", type=StringType()),
            ],
        )
        session.add(node_revision)
        session.commit()
        return node

    def test_deactivating_node(
        self,
        client_with_examples: TestClient,
    ):
        """
        Test deactivating a node
        """
        # Deactivate a node
        response = client_with_examples.post("/nodes/basic.source.users/deactivate/")
        assert response.status_code == 204
        # Check that then retrieving the node returns an error
        response = client_with_examples.get("/nodes/basic.source.users/")
        assert not response.ok
        assert response.json() == {
            "message": "A node with name `basic.source.users` does not exist.",
            "errors": [],
            "warnings": [],
        }
        # All downstream nodes should be invalid
        expected_downstreams = [
            "basic.dimension.users",
            "basic.transform.country_agg",
            "basic.dimension.countries",
            "basic.num_users",
        ]
        for downstream in expected_downstreams:
            response = client_with_examples.get(f"/nodes/{downstream}/")
            assert response.json()["status"] == NodeStatus.INVALID

            # The downstreams' status change should be recorded in their histories
            response = client_with_examples.get(f"/history?node={downstream}")
            assert [
                (activity["pre"], activity["post"])
                for activity in response.json()
                if activity["activity_type"] == "status_change"
            ] == [({"status": "valid"}, {"status": "invalid"})]

        # Trying to create the node again should reveal that the node exists but is deactivated
        response = client_with_examples.post(
            "/nodes/source/",
            json={
                "name": "basic.source.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "public",
                "schema_": "basic",
                "table": "dim_users",
            },
        )
        assert response.json() == {
            "message": "Node `basic.source.users` exists but has been deactivated.",
            "errors": [],
            "warnings": [],
        }

        # The deletion action should be recorded in the node's history
        response = client_with_examples.get("/history?node=basic.source.users")
        history = response.json()
        assert history == [
            {
                "activity_type": "create",
                "node": "basic.source.users",
                "created_at": mock.ANY,
                "details": {},
                "entity_name": "basic.source.users",
                "entity_type": "node",
                "id": mock.ANY,
                "post": {},
                "pre": {},
                "user": None,
            },
            {
                "activity_type": "delete",
                "node": "basic.source.users",
                "created_at": mock.ANY,
                "details": {},
                "entity_name": "basic.source.users",
                "entity_type": "node",
                "id": mock.ANY,
                "post": {},
                "pre": {},
                "user": None,
            },
        ]

    def test_deactivating_source_upstream_from_metric(
        self,
        client: TestClient,
    ):
        """
        Test deactivating a source that's upstream from a metric
        """
        response = client.post("/catalogs/", json={"name": "warehouse"})
        assert response.ok
        response = client.post("/namespaces/default/")
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "users",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of users",
                "query": "SELECT COUNT(DISTINCT id) FROM default.users",
                "mode": "published",
                "name": "default.num_users",
            },
        )
        assert response.ok
        # Deactivate the source node
        response = client.post("/nodes/default.users/deactivate/")
        assert response.ok
        # The downstream metric should have an invalid status
        assert (
            client.get("/nodes/default.num_users/").json()["status"]
            == NodeStatus.INVALID
        )
        response = client.get("/history?node=default.num_users")
        assert [
            (activity["pre"], activity["post"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == [({"status": "valid"}, {"status": "invalid"})]

        # Reactivate the source node
        response = client.post("/nodes/default.users/activate/")
        assert response.ok
        # Retrieving the reactivated node should work
        response = client.get("/nodes/default.users/")
        assert response.ok
        # The downstream metric should have been changed to valid
        response = client.get("/nodes/default.num_users/")
        assert response.json()["status"] == NodeStatus.VALID
        # Check activity history of downstream metric
        response = client.get("/history?node=default.num_users")
        assert [
            (activity["pre"], activity["post"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == [
            ({"status": "valid"}, {"status": "invalid"}),
            ({"status": "invalid"}, {"status": "valid"}),
        ]

    def test_deactivating_transform_upstream_from_metric(
        self,
        client: TestClient,
    ):
        """
        Test deactivating a transform that's upstream from a metric
        """
        response = client.post("/catalogs/", json={"name": "warehouse"})
        assert response.ok
        response = client.post("/namespaces/default/")
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "users",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/transform/",
            json={
                "name": "default.us_users",
                "description": "US users",
                "query": """
                    SELECT
                    id,
                    full_name,
                    age,
                    country,
                    gender,
                    preferred_language,
                    secret_number,
                    created_at,
                    post_processing_timestamp
                    FROM default.users
                    WHERE country = 'US'
                """,
                "mode": "published",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of US users",
                "query": "SELECT COUNT(DISTINCT id) FROM default.us_users",
                "mode": "published",
                "name": "default.num_us_users",
            },
        )
        assert response.ok
        # Create an invalid draft downstream node
        # so we can test that it stays invalid
        # when the upstream node is reactivated
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "An invalid node downstream of default.us_users",
                "query": "SELECT COUNT(DISTINCT non_existent_column) FROM default.us_users",
                "mode": "draft",
                "name": "default.invalid_metric",
            },
        )
        assert response.ok
        response = client.get("/nodes/default.invalid_metric/")
        assert response.ok
        assert response.json()["status"] == NodeStatus.INVALID
        # Deactivate the transform node
        response = client.post("/nodes/default.us_users/deactivate/")
        assert response.ok
        # Retrieving the deactivated node should respond that the node doesn't exist
        assert client.get("/nodes/default.us_users/").json()["message"] == (
            "A node with name `default.us_users` does not exist."
        )
        # The downstream metrics should have an invalid status
        assert (
            client.get("/nodes/default.num_us_users/").json()["status"]
            == NodeStatus.INVALID
        )
        assert (
            client.get("/nodes/default.invalid_metric/").json()["status"]
            == NodeStatus.INVALID
        )

        # Check history of downstream metrics
        response = client.get("/history?node=default.num_us_users")
        assert [
            (activity["pre"], activity["post"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == [({"status": "valid"}, {"status": "invalid"})]
        # No change recorded here because the metric was already invalid
        response = client.get("/history?node=default.invalid_metric")
        assert [
            (activity["pre"], activity["post"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == []

        # Reactivate the transform node
        response = client.post("/nodes/default.us_users/activate/")
        assert response.ok
        # Retrieving the reactivated node should work
        response = client.get("/nodes/default.us_users/")
        assert response.ok
        # Check history of the reactivated node
        response = client.get("/history?node=default.us_users")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("create", "node"), ("delete", "node"), ("restore", "node")]

        # This downstream metric should have been changed to valid
        response = client.get("/nodes/default.num_us_users/")
        assert response.json()["status"] == NodeStatus.VALID
        # Check history of downstream metric
        response = client.get("/history?node=default.num_us_users")
        assert [
            (activity["pre"], activity["post"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == [
            ({"status": "valid"}, {"status": "invalid"}),
            ({"status": "invalid"}, {"status": "valid"}),
        ]

        # The other downstream metric should have remained invalid
        response = client.get("/nodes/default.invalid_metric/")
        assert response.json()["status"] == NodeStatus.INVALID
        # Check history of downstream metric
        response = client.get("/history?node=default.invalid_metric")
        assert [
            (activity["pre"], activity["post"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == []

    def test_deactivating_linked_dimension(
        self,
        client: TestClient,
    ):
        """
        Test deactivating a dimension that's linked to columns on other nodes
        """
        response = client.post("/catalogs/", json={"name": "warehouse"})
        assert response.ok
        response = client.post("/namespaces/default/")
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "users",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/dimension/",
            json={
                "name": "default.us_users",
                "description": "US users",
                "query": """
                    SELECT
                    id,
                    full_name,
                    age,
                    country,
                    gender,
                    preferred_language,
                    secret_number,
                    created_at,
                    post_processing_timestamp
                    FROM default.users
                    WHERE country = 'US'
                """,
                "primary_key": ["id"],
                "mode": "published",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.messages",
                "description": "A table of user messages",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "user_id", "type": "int"},
                    {"name": "message", "type": "int"},
                    {"name": "posted_at", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "messages",
            },
        )
        assert response.ok
        # Create a metric on the source node
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of user messages",
                "query": "SELECT COUNT(DISTINCT id) FROM default.messages",
                "mode": "published",
                "name": "default.num_messages",
            },
        )
        assert response.ok

        # Create a metric on the source node w/ bound dimensions
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of user messages by id",
                "query": "SELECT COUNT(DISTINCT id) FROM default.messages",
                "mode": "published",
                "name": "default.num_messages_id",
                "required_dimensions": ["default.messages.id"],
            },
        )
        assert response.ok

        # Create a metric w/ bound dimensions that to not exist
        with pytest.raises(Exception) as exc:
            response = client.post(
                "/nodes/metric/",
                json={
                    "description": "Total number of user messages by id",
                    "query": "SELECT COUNT(DISTINCT id) FROM default.messages",
                    "mode": "published",
                    "name": "default.num_messages_id",
                    "required_dimensions": ["default.nothin.id"],
                },
            )
            assert "required dimensions that are not on parent nodes" in str(exc)

        # Create a metric on the source node w/ an invalid bound dimension
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of user messages by id",
                "query": "SELECT COUNT(DISTINCT id) FROM default.messages",
                "mode": "published",
                "name": "default.num_messages_id_invalid_dimension",
                "required_dimensions": ["default.messages.foo"],
            },
        )
        assert response.status_code == 400
        assert response.json() == {
            "message": "Node definition contains references to "
            "columns as required dimensions that are not on parent nodes.",
            "errors": [
                {
                    "code": 206,
                    "message": "Node definition contains references to columns "
                    "as required dimensions that are not on parent nodes.",
                    "debug": {"invalid_required_dimensions": ["default.messages.foo"]},
                    "context": "",
                },
            ],
            "warnings": [],
        }

        # Link the dimension to a column on the source node
        response = client.post(
            "/nodes/default.messages/columns/user_id/"
            "?dimension=default.us_users&dimension_column=id",
        )
        assert response.ok
        # The dimension's attributes should now be available to the metric
        response = client.get("/metrics/default.num_messages/")
        assert response.ok
        assert response.json()["dimensions"] == [
            {"name": "default.messages.user_id", "path": [], "type": "int"},
            {
                "name": "default.us_users.age",
                "path": ["default.messages.user_id"],
                "type": "int",
            },
            {
                "name": "default.us_users.country",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "name": "default.us_users.created_at",
                "path": ["default.messages.user_id"],
                "type": "timestamp",
            },
            {
                "name": "default.us_users.full_name",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "name": "default.us_users.gender",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "name": "default.us_users.id",
                "path": ["default.messages.user_id"],
                "type": "int",
            },
            {
                "name": "default.us_users.post_processing_timestamp",
                "path": ["default.messages.user_id"],
                "type": "timestamp",
            },
            {
                "name": "default.us_users.preferred_language",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "name": "default.us_users.secret_number",
                "path": ["default.messages.user_id"],
                "type": "float",
            },
        ]

        # Check history of the node with column dimension link
        response = client.get(
            "/history?node=default.messages",
        )
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("create", "node"), ("create", "link")]

        # Deactivate the dimension node
        response = client.post("/nodes/default.us_users/deactivate/")
        assert response.ok
        # Retrieving the deactivated node should respond that the node doesn't exist
        assert client.get("/nodes/default.us_users/").json()["message"] == (
            "A node with name `default.us_users` does not exist."
        )
        # The deactivated dimension's attributes should no longer be available to the metric
        response = client.get("/metrics/default.num_messages/")
        assert response.ok
        assert [
            {"path": [], "name": "default.messages.user_id", "type": "int"},
        ] == response.json()["dimensions"]
        # The metric should still be VALID
        response = client.get("/nodes/default.num_messages/")
        assert response.json()["status"] == NodeStatus.VALID
        # Reactivate the dimension node
        response = client.post("/nodes/default.us_users/activate/")
        assert response.ok
        # Retrieving the reactivated node should work
        response = client.get("/nodes/default.us_users/")
        assert response.ok
        # The dimension's attributes should now once again show for the linked metric
        response = client.get("/metrics/default.num_messages/")
        assert response.ok
        assert response.json()["dimensions"] == [
            {"name": "default.messages.user_id", "path": [], "type": "int"},
            {
                "name": "default.us_users.age",
                "path": ["default.messages.user_id"],
                "type": "int",
            },
            {
                "name": "default.us_users.country",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "name": "default.us_users.created_at",
                "path": ["default.messages.user_id"],
                "type": "timestamp",
            },
            {
                "name": "default.us_users.full_name",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "name": "default.us_users.gender",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "name": "default.us_users.id",
                "path": ["default.messages.user_id"],
                "type": "int",
            },
            {
                "name": "default.us_users.post_processing_timestamp",
                "path": ["default.messages.user_id"],
                "type": "timestamp",
            },
            {
                "name": "default.us_users.preferred_language",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "name": "default.us_users.secret_number",
                "path": ["default.messages.user_id"],
                "type": "float",
            },
        ]
        # The metric should still be VALID
        response = client.get("/nodes/default.num_messages/")
        assert response.json()["status"] == NodeStatus.VALID

    def test_activating_an_already_active_node(
        self,
        client: TestClient,
    ):
        """
        Test raising when activating an already active node
        """
        response = client.post("/catalogs/", json={"name": "warehouse"})
        assert response.ok
        response = client.post("/namespaces/default/")
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "users",
            },
        )
        assert response.ok
        response = client.post("/nodes/default.users/activate/")
        assert not response.ok
        assert response.json() == {
            "message": "Cannot activate `default.users`, node already active",
            "errors": [],
            "warnings": [],
        }

    def test_create_source_node_without_cols_or_query_service(
        self,
        client_with_examples: TestClient,
    ):
        """
        Trying to create a source node without columns and without
        a query service set up should fail.
        """
        basic_source_comments = {
            "name": "default.comments",
            "description": "A fact table with comments",
            "columns": [],
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "comments",
        }

        # Trying to create a source node without columns and without
        # a query service set up should fail
        response = client_with_examples.post(
            "/nodes/source/",
            json=basic_source_comments,
        )
        data = response.json()
        assert (
            data["message"] == "No table columns were provided and no query "
            "service is configured for table columns inference!"
        )
        assert response.status_code == 500

    def test_create_source_node_with_query_service(
        self,
        client_with_query_service: TestClient,
    ):
        """
        Creating a source node without columns but with a query service set should
        result in the source node columns being inferred via the query service.
        """
        basic_source_comments = {
            "name": "default.comments",
            "description": "A fact table with comments",
            "columns": [],
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "comments",
        }

        # Trying to create a source node without columns and without
        # a query service set up should fail
        response = client_with_query_service.post(
            "/nodes/source/",
            json=basic_source_comments,
        )
        data = response.json()
        assert data["name"] == "default.comments"
        assert data["type"] == "source"
        assert data["display_name"] == "Default: Comments"
        assert data["version"] == "v1.0"
        assert data["status"] == "valid"
        assert data["mode"] == "published"
        assert data["catalog"]["name"] == "public"
        assert data["schema_"] == "basic"
        assert data["table"] == "comments"
        assert data["columns"] == [
            {"name": "id", "type": "int", "attributes": [], "dimension": None},
            {"name": "user_id", "type": "int", "attributes": [], "dimension": None},
            {
                "name": "timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {"name": "text", "type": "string", "attributes": [], "dimension": None},
        ]
        assert response.status_code == 201

    def test_create_update_source_node(
        self,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test creating and updating a source node
        """
        basic_source_comments = {
            "name": "basic.source.comments",
            "description": "A fact table with comments",
            "columns": [
                {"name": "id", "type": "int"},
                {
                    "name": "user_id",
                    "type": "int",
                    "dimension": "basic.dimension.users",
                },
                {"name": "timestamp", "type": "timestamp"},
                {"name": "text", "type": "string"},
            ],
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "comments",
        }

        # Trying to create it again should fail
        response = client_with_examples.post(
            "/nodes/source/",
            json=basic_source_comments,
        )
        data = response.json()
        assert (
            data["message"]
            == "A node with name `basic.source.comments` already exists."
        )
        assert response.status_code == 409

        # Update node with a new description should create a new revision
        response = client_with_examples.patch(
            f"/nodes/{basic_source_comments['name']}/",
            json={
                "description": "New description",
                "display_name": "Comments facts",
            },
        )
        data = response.json()

        assert data["name"] == "basic.source.comments"
        assert data["display_name"] == "Comments facts"
        assert data["type"] == "source"
        assert data["version"] == "v1.1"
        assert data["description"] == "New description"

        # Try to update node with no changes
        response = client_with_examples.patch(
            f"/nodes/{basic_source_comments['name']}/",
            json={"description": "New description", "display_name": "Comments facts"},
        )
        new_data = response.json()
        assert data == new_data

        # Try to update a node with a table that has different columns
        response = client_with_examples.patch(
            f"/nodes/{basic_source_comments['name']}/",
            json={
                "columns": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "user_id",
                        "type": "int",
                        "dimension": "basic.dimension.users",
                    },
                    {"name": "timestamp", "type": "timestamp"},
                    {"name": "text_v2", "type": "string"},
                ],
            },
        )
        data = response.json()
        assert data["version"] == "v2.0"
        assert data["columns"] == [
            {"name": "id", "type": "int", "attributes": [], "dimension": None},
            {"name": "user_id", "type": "int", "attributes": [], "dimension": None},
            {
                "name": "timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {"name": "text_v2", "type": "string", "attributes": [], "dimension": None},
        ]

    def test_update_nonexistent_node(
        self,
        client: TestClient,
    ) -> None:
        """
        Test updating a non-existent node.
        """

        response = client.patch(
            "/nodes/something/",
            json={"description": "new"},
        )
        data = response.json()
        assert response.status_code == 404
        assert data["message"] == "A node with name `something` does not exist."

    def test_raise_on_source_node_with_no_catalog(
        self,
        client: TestClient,
    ) -> None:
        """
        Test raise on source node with no catalog
        """
        response = client.post(
            "/nodes/source/",
            json={
                "name": "basic.source.comments",
                "description": "A fact table with comments",
                "columns": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "user_id",
                        "type": "int",
                        "dimension": "basic.dimension.users",
                    },
                    {"name": "timestamp", "type": "timestamp"},
                    {"name": "text", "type": "string"},
                ],
                "mode": "published",
            },
        )
        assert not response.ok
        assert response.json() == {
            "detail": [
                {
                    "loc": ["body", "catalog"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["body", "schema_"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["body", "table"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
            ],
        }

    def test_create_invalid_transform_node(
        self,
        database: Database,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
        create_invalid_transform_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating an invalid transform node in draft and published modes.
        """
        client.post("/namespaces/default/")
        response = client.post(
            "/nodes/transform/",
            json=create_invalid_transform_node_payload,
        )
        data = response.json()
        assert response.status_code == 400
        assert (
            data["message"]
            == "Node definition contains references to nodes that do not exist"
        )

    def test_create_node_with_type_inference_failure(
        self,
        client_with_examples: TestClient,
    ):
        """
        Attempting to create a published metric where type inference fails should raise
        an appropriate error and fail.
        """
        response = client_with_examples.post(
            "/nodes/metric/",
            json={
                "description": "Average length of employment",
                "query": (
                    "SELECT avg(NOW() - hire_date + 1) as "
                    "default_DOT_avg_length_of_employment_plus_one "
                    "FROM foo.bar.hard_hats"
                ),
                "mode": "published",
                "name": "default.avg_length_of_employment_plus_one",
            },
        )
        data = response.json()
        assert data == {
            "message": (
                "Unable to infer type for some columns on node "
                "`default.avg_length_of_employment_plus_one`"
            ),
            "errors": [
                {
                    "code": 302,
                    "message": (
                        "Unable to infer type for some columns on node "
                        "`default.avg_length_of_employment_plus_one`"
                    ),
                    "debug": {
                        "columns": {
                            "default_DOT_avg_length_of_employment_plus_one": (
                                "Incompatible types in binary operation NOW() - "
                                "foo.bar.hard_hats.hire_date + 1. Got left timestamp, right int."
                            ),
                        },
                        "errors": [],
                    },
                    "context": "",
                },
            ],
            "warnings": [],
        }

    def test_create_update_transform_node(
        self,
        database: Database,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
        create_transform_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating and updating a transform node that references an existing source.
        """

        client.post("/namespaces/default/")
        # Create a transform node
        response = client.post(
            "/nodes/transform/",
            json=create_transform_node_payload,
        )
        data = response.json()
        assert data["name"] == "default.country_agg"
        assert data["display_name"] == "Default: Country Agg"
        assert data["type"] == "transform"
        assert data["description"] == "Distinct users per country"
        assert (
            data["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )
        assert data["status"] == "valid"
        assert data["columns"] == [
            {"name": "country", "type": "string", "attributes": [], "dimension": None},
            {
                "name": "num_users",
                "type": "bigint",
                "attributes": [],
                "dimension": None,
            },
        ]
        assert data["parents"] == [{"name": "basic.source.users"}]

        # Update the transform node with two minor changes
        response = client.patch(
            "/nodes/default.country_agg/",
            json={
                "description": "Some new description",
                "display_name": "Default: Country Aggregation by User",
            },
        )
        data = response.json()
        assert data["name"] == "default.country_agg"
        assert data["display_name"] == "Default: Country Aggregation by User"
        assert data["type"] == "transform"
        assert data["version"] == "v1.1"
        assert data["description"] == "Some new description"
        assert (
            data["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )
        assert data["status"] == "valid"

        # Try to update with a new query that references a non-existent source
        response = client.patch(
            "/nodes/default.country_agg/",
            json={
                "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM comments",
            },
        )
        data = response.json()
        assert (
            data["message"]
            == "Node definition contains references to nodes that do not exist"
        )

        # Try to update with a new query that references an existing source
        response = client.patch(
            "/nodes/default.country_agg/",
            json={
                "query": "SELECT country, COUNT(DISTINCT id) AS num_users, "
                "COUNT(*) AS num_entries FROM basic.source.users",
            },
        )
        data = response.json()
        assert data["version"] == "v2.0"
        assert (
            data["query"] == "SELECT country, COUNT(DISTINCT id) AS num_users, "
            "COUNT(*) AS num_entries FROM basic.source.users"
        )
        assert data["columns"] == [
            {"name": "country", "type": "string", "attributes": [], "dimension": None},
            {
                "name": "num_users",
                "type": "bigint",
                "attributes": [],
                "dimension": None,
            },
            {
                "name": "num_entries",
                "type": "bigint",
                "attributes": [],
                "dimension": None,
            },
        ]
        assert data["status"] == "valid"

        # Verify that asking for revisions for a non-existent transform fails
        response = client.get("/nodes/random_transform/revisions/")
        data = response.json()
        assert data["message"] == "A node with name `random_transform` does not exist."

        # Verify that all historical revisions are available for the node
        response = client.get("/nodes/default.country_agg/revisions/")
        data = response.json()
        assert {rev["version"]: rev["query"] for rev in data} == {
            "v1.0": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "v1.1": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "v2.0": "SELECT country, COUNT(DISTINCT id) AS num_users, COUNT(*) AS num_entries "
            "FROM basic.source.users",
        }
        assert {rev["version"]: rev["columns"] for rev in data} == {
            "v1.0": [
                {
                    "name": "country",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "num_users",
                    "type": "bigint",
                    "attributes": [],
                    "dimension": None,
                },
            ],
            "v1.1": [
                {
                    "name": "country",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "num_users",
                    "type": "bigint",
                    "attributes": [],
                    "dimension": None,
                },
            ],
            "v2.0": [
                {
                    "name": "country",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "num_users",
                    "type": "bigint",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "num_entries",
                    "type": "bigint",
                    "attributes": [],
                    "dimension": None,
                },
            ],
        }

    def test_create_dimension_node_fails(
        self,
        database: Database,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
    ):
        """
        Test various failure cases for dimension node creation.
        """
        client.post("/namespaces/default/")
        response = client.post(
            "/nodes/dimension/",
            json={
                "description": "Country dimension",
                "query": "SELECT country, COUNT(1) AS user_cnt "
                "FROM basic.source.users GROUP BY country",
                "mode": "published",
                "name": "countries",
            },
        )
        assert (
            response.json()["message"] == "Dimension nodes must define a primary key!"
        )

        response = client.post(
            "/nodes/dimension/",
            json={
                "description": "Country dimension",
                "query": "SELECT country, COUNT(1) AS user_cnt "
                "FROM basic.source.users GROUP BY country",
                "mode": "published",
                "name": "default.countries",
                "primary_key": ["country", "id"],
            },
        )
        assert response.json()["message"] == (
            "Some columns in the primary key [country,id] were not "
            "found in the list of available columns for the node "
            "default.countries."
        )

    def test_create_update_dimension_node(
        self,
        database: Database,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
        create_dimension_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating and updating a dimension node that references an existing source.
        """
        client.post("/namespaces/default/")
        response = client.post(
            "/nodes/dimension/",
            json=create_dimension_node_payload,
        )
        data = response.json()

        assert response.status_code == 201
        assert data["name"] == "default.countries"
        assert data["display_name"] == "Default: Countries"
        assert data["type"] == "dimension"
        assert data["version"] == "v1.0"
        assert data["description"] == "Country dimension"
        assert (
            data["query"] == "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country"
        )
        assert data["columns"] == [
            {
                "name": "country",
                "type": "string",
                "attributes": [
                    {"attribute_type": {"namespace": "system", "name": "primary_key"}},
                ],
                "dimension": None,
            },
            {"name": "user_cnt", "type": "bigint", "attributes": [], "dimension": None},
        ]

        # Test updating the dimension node with a new query
        response = client.patch(
            "/nodes/default.countries/",
            json={"query": "SELECT country FROM basic.source.users GROUP BY country"},
        )
        data = response.json()
        # Should result in a major version update due to the query change
        assert data["version"] == "v2.0"

        # The columns should have been updated
        assert data["columns"] == [
            {
                "name": "country",
                "type": "string",
                "attributes": [
                    {"attribute_type": {"namespace": "system", "name": "primary_key"}},
                ],
                "dimension": None,
            },
        ]

    def test_raise_on_multi_catalog_node(self, client_with_examples: TestClient):
        """
        Test raising when trying to select from multiple catalogs
        """
        response = client_with_examples.post(
            "/nodes/transform/",
            json={
                "query": (
                    "SELECT payment_id, payment_amount, customer_id, account_type "
                    "FROM default.revenue r LEFT JOIN basic.source.comments b on r.id = b.id"
                ),
                "description": "Multicatalog",
                "mode": "published",
                "name": "default.multicatalog",
            },
        )
        assert (
            "Cannot create nodes with multi-catalog dependencies"
            in response.json()["message"]
        )

    def test_updating_node_to_invalid_draft(
        self,
        database: Database,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
        create_dimension_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating an invalid node in draft mode
        """
        client.post("/namespaces/default/")
        response = client.post(
            "/nodes/dimension/",
            json=create_dimension_node_payload,
        )
        data = response.json()

        assert response.status_code == 201
        assert data["name"] == "default.countries"
        assert data["display_name"] == "Default: Countries"
        assert data["type"] == "dimension"
        assert data["version"] == "v1.0"
        assert data["description"] == "Country dimension"
        assert (
            data["query"] == "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country"
        )
        assert data["columns"] == [
            {
                "name": "country",
                "type": "string",
                "attributes": [
                    {"attribute_type": {"namespace": "system", "name": "primary_key"}},
                ],
                "dimension": None,
            },
            {"name": "user_cnt", "type": "bigint", "attributes": [], "dimension": None},
        ]

        response = client.patch(
            "/nodes/default.countries/",
            json={"mode": "draft"},
        )
        assert response.status_code == 200

        # Test updating the dimension node with an invalid query
        response = client.patch(
            "/nodes/default.countries/",
            json={"query": "SELECT country FROM missing_parent GROUP BY country"},
        )
        assert response.status_code == 200

        # Check that node is now a draft with an invalid status
        response = client.get("/nodes/default.countries")
        assert response.status_code == 200
        data = response.json()
        assert data["mode"] == "draft"
        assert data["status"] == "invalid"

    def test_upsert_materialization_config(  # pylint: disable=too-many-arguments
        self,
        client_with_query_service: TestClient,
    ) -> None:
        """
        Test creating & updating materialization config for a node.
        """
        # Setting the materialization config for a source node should fail
        response = client_with_query_service.post(
            "/nodes/basic.source.comments/materialization/",
            json={
                "engine": {
                    "name": "spark",
                    "version": "2.4.4",
                },
                "schedule": "0 * * * *",
                "config": {},
            },
        )
        assert response.status_code == 400
        assert (
            response.json()["message"]
            == "Cannot set materialization config for source node `basic.source.comments`!"
        )

        # Setting the materialization config for an engine that doesn't exist should fail
        response = client_with_query_service.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "engine": {
                    "name": "spark",
                    "version": "2.4.4",
                },
                "config": {},
                "schedule": "0 * * * *",
            },
        )
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Engine not found: `spark` version `2.4.4`"

    def test_update_node_query_with_materializations(
        self,
        client_with_query_service: TestClient,
    ):
        """
        Testing updating a node's query when the node already has materializations. The node's
        materializations should be updated based on the new query and rescheduled.
        """
        client_with_query_service.post(
            "/engines/",
            json={
                "name": "spark",
                "version": "2.4.4",
                "dialect": "spark",
            },
        )

        client_with_query_service.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "engine": {
                    "name": "spark",
                    "version": "2.4.4",
                },
                "config": {
                    "partitions": [
                        {
                            "name": "country",
                            "values": ["DE", "MY"],
                            "type_": "categorical",
                        },
                    ],
                },
                "schedule": "0 * * * *",
            },
        )
        client_with_query_service.patch(
            "/nodes/basic.transform.country_agg/",
            json={
                "query": (
                    "SELECT country, COUNT(DISTINCT id) AS num_users, "
                    "COUNT(DISTINCT preferred_language) AS languages "
                    "FROM basic.source.users GROUP BY 1"
                ),
            },
        )
        response = client_with_query_service.get("/nodes/basic.transform.country_agg/")
        node_output = response.json()
        assert node_output["materializations"] == [
            {
                "name": "country_3491792861",
                "engine": {
                    "name": "spark",
                    "version": "2.4.4",
                    "uri": None,
                    "dialect": "spark",
                },
                "config": {
                    "partitions": [
                        {
                            "name": "country",
                            "values": ["DE", "MY"],
                            "range": None,
                            "expression": None,
                            "type_": "categorical",
                        },
                    ],
                    "spark": {},
                    "query": "SELECT  basic_DOT_source_DOT_users.country,\n\t"
                    "COUNT( DISTINCT basic_DOT_source_DOT_users.preferred_language) "
                    "AS languages,\n\tCOUNT( DISTINCT basic_DOT_source_DOT_users.id) "
                    "AS num_users \n FROM basic.dim_users AS basic_DOT_source_DOT_users "
                    "\n WHERE  basic_DOT_source_DOT_users.country IN ('DE', 'MY') \n "
                    "GROUP BY  1\n\n",
                    "upstream_tables": ["public.basic.dim_users"],
                },
                "schedule": "0 * * * *",
                "job": "SparkSqlMaterializationJob",
            },
        ]

    def test_add_materialization_success(self, client_with_query_service: TestClient):
        """
        Verifies success cases of adding materialization config.
        """
        # Create the engine and check the existing transform node
        client_with_query_service.post(
            "/engines/",
            json={
                "name": "spark",
                "version": "2.4.4",
                "dialect": "spark",
            },
        )

        response = client_with_query_service.get("/nodes/basic.transform.country_agg/")
        old_node_data = response.json()
        assert old_node_data["version"] == "v1.0"
        assert old_node_data["materializations"] == []

        # Setting the materialization config should succeed
        response = client_with_query_service.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "engine": {
                    "name": "spark",
                    "version": "2.4.4",
                },
                "config": {
                    "partitions": [
                        {
                            "name": "country",
                            "values": ["DE", "MY"],
                            "type_": "categorical",
                        },
                    ],
                },
                "schedule": "0 * * * *",
            },
        )
        data = response.json()
        assert (
            data["message"] == "Successfully updated materialization config named "
            "`country_3491792861` for node `basic.transform.country_agg`"
        )

        # Check history of the node with materialization
        response = client_with_query_service.get(
            "/history?node=basic.transform.country_agg",
        )
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("create", "node"), ("create", "materialization")]

        # Setting it again should inform that it already exists
        response = client_with_query_service.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "engine": {
                    "name": "spark",
                    "version": "2.4.4",
                },
                "config": {
                    "partitions": [
                        {
                            "name": "country",
                            "values": ["DE", "MY"],
                            "type_": "categorical",
                        },
                    ],
                },
                "schedule": "0 * * * *",
            },
        )
        assert response.json() == {
            "info": {
                "output_tables": ["common.a", "common.b"],
                "urls": ["http://fake.url/job"],
            },
            "message": "The same materialization config with name "
            "`country_3491792861`already exists for node "
            "`basic.transform.country_agg` so no update was performed.",
        }

        # Setting the materialization config without partitions should succeed
        response = client_with_query_service.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "engine": {
                    "name": "spark",
                    "version": "2.4.4",
                },
                "config": {
                    "partitions": [],
                },
                "schedule": "0 * * * *",
            },
        )
        data = response.json()
        assert (
            data["message"] == "Successfully updated materialization config named "
            "`default` for node `basic.transform.country_agg`"
        )

        # Reading the node should yield the materialization config
        response = client_with_query_service.get("/nodes/basic.transform.country_agg/")
        data = response.json()
        assert data["version"] == "v1.0"
        materialization_compare(
            data["materializations"],
            [
                {
                    "name": "country_3491792861",
                    "engine": {
                        "name": "spark",
                        "version": "2.4.4",
                        "uri": None,
                        "dialect": "spark",
                    },
                    "config": {
                        "query": "SELECT  basic_DOT_source_DOT_users.country,\n\tCOUNT( "
                        "DISTINCT basic_DOT_source_DOT_users.id) AS num_users \n "
                        "FROM basic.dim_users AS basic_DOT_source_DOT_users \n WHERE"
                        "  basic_DOT_source_DOT_users.country IN ('DE', 'MY') \n "
                        "GROUP BY  1\n",
                        "partitions": [
                            {
                                "name": "country",
                                "values": ["DE", "MY"],
                                "range": None,
                                "type_": "categorical",
                                "expression": None,
                            },
                        ],
                        "spark": {},
                        "upstream_tables": ["public.basic.dim_users"],
                    },
                    "schedule": "0 * * * *",
                    "job": "SparkSqlMaterializationJob",
                },
                {
                    "config": {
                        "partitions": [],
                        "query": "SELECT  basic_DOT_source_DOT_users.country,\n"
                        "\tCOUNT( DISTINCT basic_DOT_source_DOT_users.id) AS "
                        "num_users \n"
                        " FROM basic.dim_users AS basic_DOT_source_DOT_users \n"
                        " GROUP BY  1\n",
                        "spark": {},
                        "upstream_tables": ["public.basic.dim_users"],
                    },
                    "engine": {
                        "dialect": "spark",
                        "name": "spark",
                        "uri": None,
                        "version": "2.4.4",
                    },
                    "job": "SparkSqlMaterializationJob",
                    "name": "default",
                    "schedule": "0 * * * *",
                },
            ],
        )

        # Setting the materialization config with a temporal partition should succeed
        response = client_with_query_service.post(
            "/nodes/default.hard_hat/materialization/",
            json={
                "engine": {
                    "name": "spark",
                    "version": "2.4.4",
                },
                "config": {
                    "partitions": [
                        {
                            "name": "country",
                            "values": ["DE", "MY"],
                            "type_": "categorical",
                        },
                        {
                            "name": "birth_date",
                            "range": (20010101, 20020101),
                            "type_": "temporal",
                        },
                        {
                            "name": "contractor_id",
                            "range": (1, 10),
                            "type_": "categorical",
                        },
                    ],
                },
                "schedule": "0 * * * *",
            },
        )
        data = response.json()
        assert (
            data["message"] == "Successfully updated materialization config named "
            "`country_birth_date_contractor_id_379232101` for node `default.hard_hat`"
        )

        # Check that the temporal partition is appended onto the list of partitions in the
        # materialization config but is not included directly in the materialization query
        response = client_with_query_service.get("/nodes/default.hard_hat/")
        data = response.json()
        assert data["version"] == "v1.0"
        materialization_compare(
            data["materializations"],
            [
                {
                    "name": "country_birth_date_contractor_id_379232101",
                    "engine": {
                        "name": "spark",
                        "version": "2.4.4",
                        "uri": None,
                        "dialect": "spark",
                    },
                    "config": {
                        "query": "SELECT  default_DOT_hard_hats.address,\n\tdefault_DOT_hard_hats."
                        "birth_date,\n\tdefault_DOT_hard_hats.city,\n\tdefault_DOT_hard_hats."
                        "contractor_id,\n\tdefault_DOT_hard_hats.country,\n\tdefault_DOT_hard"
                        "_hats.first_name,\n\tdefault_DOT_hard_hats.hard_hat_id,\n\tdefault_D"
                        "OT_hard_hats.hire_date,\n\tdefault_DOT_hard_hats.last_name,\n\tdefau"
                        "lt_DOT_hard_hats.manager,\n\tdefault_DOT_hard_hats.postal_code,\n\t"
                        "default_DOT_hard_hats.state,\n\tdefault_DOT_hard_hats.title \n FROM"
                        " roads.hard_hats AS default_DOT_hard_hats \n WHERE  default_DOT_har"
                        "d_hats.country IN ('DE', 'MY') AND default_DOT_hard_hats.contractor"
                        "_id BETWEEN 1 AND 10\n",
                        "partitions": [
                            {
                                "name": "country",
                                "values": ["DE", "MY"],
                                "range": None,
                                "type_": "categorical",
                                "expression": None,
                            },
                            {
                                "name": "birth_date",
                                "values": None,
                                "range": [20010101, 20020101],
                                "type_": "temporal",
                                "expression": None,
                            },
                            {
                                "name": "contractor_id",
                                "values": None,
                                "range": [1, 10],
                                "type_": "categorical",
                                "expression": None,
                            },
                        ],
                        "spark": {},
                        "upstream_tables": ["default.roads.hard_hats"],
                    },
                    "schedule": "0 * * * *",
                    "job": "SparkSqlMaterializationJob",
                },
            ],
        )

        # Check listing materializations of the node
        response = client_with_query_service.get(
            "/nodes/default.hard_hat/materializations/",
        )
        materialization_compare(
            response.json(),
            [
                {
                    "config": {
                        "partitions": [
                            {
                                "expression": None,
                                "name": "country",
                                "range": None,
                                "type_": "categorical",
                                "values": ["DE", "MY"],
                            },
                            {
                                "expression": None,
                                "name": "birth_date",
                                "range": [20010101, 20020101],
                                "type_": "temporal",
                                "values": None,
                            },
                            {
                                "expression": None,
                                "name": "contractor_id",
                                "range": [1, 10],
                                "type_": "categorical",
                                "values": None,
                            },
                        ],
                        "query": "SELECT  default_DOT_hard_hats.address,\n"
                        "\tdefault_DOT_hard_hats.birth_date,\n"
                        "\tdefault_DOT_hard_hats.city,\n"
                        "\tdefault_DOT_hard_hats.contractor_id,\n"
                        "\tdefault_DOT_hard_hats.country,\n"
                        "\tdefault_DOT_hard_hats.first_name,\n"
                        "\tdefault_DOT_hard_hats.hard_hat_id,\n"
                        "\tdefault_DOT_hard_hats.hire_date,\n"
                        "\tdefault_DOT_hard_hats.last_name,\n"
                        "\tdefault_DOT_hard_hats.manager,\n"
                        "\tdefault_DOT_hard_hats.postal_code,\n"
                        "\tdefault_DOT_hard_hats.state,\n"
                        "\tdefault_DOT_hard_hats.title \n"
                        " FROM roads.hard_hats AS default_DOT_hard_hats \n"
                        " WHERE  default_DOT_hard_hats.country IN ('DE', 'MY') "
                        "AND default_DOT_hard_hats.contractor_id BETWEEN 1 AND "
                        "10\n",
                        "spark": {},
                        "upstream_tables": ["default.roads.hard_hats"],
                    },
                    "engine": {
                        "dialect": "spark",
                        "name": "spark",
                        "uri": None,
                        "version": "2.4.4",
                    },
                    "job": "SparkSqlMaterializationJob",
                    "name": "country_birth_date_contractor_id_379232101",
                    "output_tables": ["common.a", "common.b"],
                    "schedule": "0 * * * *",
                    "urls": ["http://fake.url/job"],
                },
            ],
        )


class TestNodeColumnsAttributes:
    """
    Test ``POST /nodes/{name}/attributes/``.
    """

    @pytest.fixture
    def create_source_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a source node.
        """

        return {
            "name": "comments",
            "description": "A fact table with comments",
            "type": "source",
            "columns": [
                {"name": "id", "type": "int"},
                {
                    "name": "user_id",
                    "type": "int",
                    "dimension": "basic.dimension.users",
                },
                {"name": "event_timestamp", "type": "timestamp"},
                {"name": "post_processing_timestamp", "type": "timestamp"},
                {"name": "text", "type": "string"},
            ],
            "mode": "published",
        }

    @pytest.fixture
    def database(self, session: Session) -> Database:
        """
        A database fixture.
        """

        database = Database(name="postgres", URI="postgres://")
        session.add(database)
        session.commit()
        return database

    @pytest.fixture
    def source_node(self, session: Session, database: Database) -> Node:
        """
        A source node fixture.
        """

        table = Table(
            database=database,
            table="A",
            columns=[
                Column(name="ds", type=StringType()),
                Column(name="user_id", type=IntegerType()),
            ],
        )
        node = Node(
            name="basic.source.users",
            type=NodeType.SOURCE,
            current_version="1",
        )
        node_revision = NodeRevision(
            node=node,
            name=node.name,
            type=node.type,
            version="1",
            tables=[table],
            columns=[
                Column(name="id", type=IntegerType()),
                Column(name="created_at", type=TimestampType()),
                Column(name="full_name", type=StringType()),
                Column(name="age", type=IntegerType()),
                Column(name="country", type=StringType()),
                Column(name="gender", type=StringType()),
                Column(name="preferred_language", type=StringType()),
            ],
        )
        session.add(node_revision)
        session.commit()
        return node

    def test_set_columns_attributes(
        self,
        client_with_examples: TestClient,
    ):
        """
        Validate that setting column attributes on the node works.
        """
        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_namespace": "system",
                    "attribute_type_name": "primary_key",
                    "column_name": "id",
                },
            ],
        )
        data = response.json()
        assert data == [
            {
                "name": "id",
                "type": "int",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
            },
        ]

        # Set columns attributes
        response = client_with_examples.post(
            "/nodes/basic.dimension.users/attributes/",
            json=[
                {
                    "attribute_type_namespace": "system",
                    "attribute_type_name": "primary_key",
                    "column_name": "id",
                },
                {
                    "attribute_type_name": "effective_time",
                    "column_name": "created_at",
                },
            ],
        )
        data = response.json()
        assert data == [
            {
                "name": "id",
                "type": "int",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
            },
            {
                "name": "created_at",
                "type": "timestamp",
                "attributes": [
                    {
                        "attribute_type": {
                            "name": "effective_time",
                            "namespace": "system",
                        },
                    },
                ],
                "dimension": None,
            },
        ]

    def test_set_columns_attributes_failed(self, client_with_examples: TestClient):
        """
        Test setting column attributes with different failure modes.
        """
        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "effective_time",
                    "column_name": "event_timestamp",
                },
            ],
        )
        data = response.json()
        assert response.status_code == 500
        assert (
            data["message"]
            == "Attribute type `system.effective_time` not allowed on node type `source`!"
        )

        response = client_with_examples.get(
            "/nodes/basic.source.comments/",
        )

        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "primary_key",
                    "column_name": "nonexistent_col",
                },
            ],
        )
        assert response.status_code == 404
        data = response.json()
        assert data == {
            "message": "Column `nonexistent_col` does not exist on node `basic.source.comments`!",
            "errors": [],
            "warnings": [],
        }

        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "nonexistent_attribute",
                    "column_name": "id",
                },
            ],
        )
        assert response.status_code == 404
        data = response.json()
        assert data == {
            "message": "Attribute type `system.nonexistent_attribute` does not exist!",
            "errors": [],
            "warnings": [],
        }

        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "primary_key",
                    "column_name": "user_id",
                },
            ],
        )
        assert response.status_code == 201
        data = response.json()
        assert data == [
            {
                "name": "user_id",
                "type": "int",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": {"name": "basic.dimension.users"},
            },
        ]

        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "event_time",
                    "column_name": "event_timestamp",
                },
                {
                    "attribute_type_name": "event_time",
                    "column_name": "post_processing_timestamp",
                },
            ],
        )
        data = response.json()
        assert data == {
            "message": "The column attribute `event_time` is scoped to be unique to the "
            "`['node', 'column_type']` level, but there is more than one column"
            " tagged with it: `event_timestamp, post_processing_timestamp`",
            "errors": [],
            "warnings": [],
        }

        response = client_with_examples.get("/nodes/basic.source.comments/")
        data = response.json()
        assert data["columns"] == [
            {
                "name": "id",
                "type": "int",
                "attributes": [],
                "dimension": None,
            },
            {
                "name": "user_id",
                "type": "int",
                "attributes": [
                    {"attribute_type": {"namespace": "system", "name": "primary_key"}},
                ],
                "dimension": {"name": "basic.dimension.users"},
            },
            {
                "name": "timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {"name": "text", "type": "string", "attributes": [], "dimension": None},
            {
                "name": "event_timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {
                "name": "created_at",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {
                "name": "post_processing_timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
        ]


class TestValidateNodes:  # pylint: disable=too-many-public-methods
    """
    Test ``POST /nodes/validate/``.
    """

    def test_validating_a_valid_node(self, client_with_examples: TestClient) -> None:
        """
        Test validating a valid node
        """
        response = client_with_examples.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT payment_id FROM default.large_revenue_payments_only",
                "type": "transform",
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert len(data) == 5
        assert data["columns"] == [
            {
                "dimension_column": None,
                "dimension_id": None,
                "id": None,
                "name": "payment_id",
                "type": "int",
            },
        ]
        assert data["status"] == "valid"
        assert data["node_revision"]["status"] == "valid"
        assert data["dependencies"][0]["name"] == "default.large_revenue_payments_only"
        assert data["message"] == "Node `foo` is valid"
        assert data["node_revision"]["id"] is None
        assert data["node_revision"]["mode"] == "published"
        assert data["node_revision"]["name"] == "foo"
        assert (
            data["node_revision"]["query"]
            == "SELECT payment_id FROM default.large_revenue_payments_only"
        )

    def test_validating_an_invalid_node(self, client: TestClient) -> None:
        """
        Test validating an invalid node
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT bar FROM large_revenue_payments_only",
                "type": "transform",
            },
        )
        data = response.json()
        assert (
            data["message"]
            == "Node definition contains references to nodes that do not exist"
        )

    def test_validating_invalid_sql(self, client: TestClient) -> None:
        """
        Test validating an invalid node with invalid SQL
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SUPER invalid SQL query",
                "type": "transform",
            },
        )
        data = response.json()

        assert response.status_code == 500
        assert data == {
            "errors": [],
            "message": "('Parse error 1:0:', \"mismatched input 'SUPER' expecting "
            "{'(', 'ADD', 'ALTER', 'ANALYZE', 'CACHE', 'CLEAR', 'COMMENT', "
            "'COMMIT', 'CREATE', 'DELETE', 'DESC', 'DESCRIBE', 'DFS', 'DROP', "
            "'EXPLAIN', 'EXPORT', 'FROM', 'GRANT', 'IMPORT', 'INSERT', 'LIST', "
            "'LOAD', 'LOCK', 'MAP', 'MERGE', 'MSCK', 'REDUCE', 'REFRESH', "
            "'REPAIR', 'REPLACE', 'RESET', 'REVOKE', 'ROLLBACK', 'SELECT', "
            "'SET', 'SHOW', 'START', 'TABLE', 'TRUNCATE', 'UNCACHE', 'UNLOCK', "
            "'UPDATE', 'USE', 'VALUES', 'WITH'}\")",
            "warnings": [],
        }

    def test_validating_with_missing_parents(self, client: TestClient) -> None:
        """
        Test validating a node with a query that has missing parents
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT 1 FROM node_that_does_not_exist",
                "type": "transform",
            },
        )
        data = response.json()

        assert response.status_code == 400
        assert data == {
            "message": "Node definition contains references to nodes that do not exist",
            "errors": [
                {
                    "code": 301,
                    "message": "Node definition contains references to nodes that do not exist",
                    "debug": {"missing_parents": ["node_that_does_not_exist"]},
                    "context": "",
                },
            ],
            "warnings": [],
        }

    def test_allowing_missing_parents_for_draft_nodes(self, client: TestClient) -> None:
        """
        Test validating a draft node that's allowed to have missing parents
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT 1 FROM node_that_does_not_exist",
                "type": "transform",
                "mode": "draft",
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data["message"] == "Node `foo` is invalid"
        assert data["status"] == "invalid"
        assert data["node_revision"]["mode"] == "draft"
        assert data["node_revision"]["status"] == "invalid"
        assert data["columns"] == [
            {
                "id": None,
                "name": "col0",
                "type": "int",
                "dimension_id": None,
                "dimension_column": None,
            },
        ]

    def test_raise_when_trying_to_validate_a_source_node(
        self,
        client: TestClient,
    ) -> None:
        """
        Test validating a source node which is not possible
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo source node!",
                "type": "source",
                "columns": [
                    {"name": "payment_id", "type": "int"},
                    {"name": "payment_amount", "type": "float"},
                    {"name": "customer_id", "type": "int"},
                    {"name": "account_type", "type": "int"},
                ],
                "tables": [
                    {
                        "database_id": 1,
                        "catalog": "test",
                        "schema": "accounting",
                        "table": "revenue",
                    },
                ],
            },
        )
        data = response.json()

        assert response.status_code == 500
        assert data == {
            "message": "Source nodes cannot be validated",
            "errors": [],
            "warnings": [],
        }

    def test_adding_dimensions_to_node_columns(self, client_with_examples: TestClient):
        """
        Test linking dimensions to node columns
        """
        # Attach the payment_type dimension to the payment_type column on the revenue node
        response = client_with_examples.post(
            "/nodes/default.revenue/columns/payment_type/?dimension=default.payment_type",
        )
        data = response.json()
        assert data == {
            "message": (
                "Dimension node default.payment_type has been successfully "
                "linked to column payment_type on node default.revenue"
            ),
        }
        response = client_with_examples.get("/nodes/default.revenue")
        data = response.json()
        assert [
            col["dimension"]["name"] for col in data["columns"] if col["dimension"]
        ] == ["default.payment_type"]

        # Check that after deleting the dimension link, none of the columns have links
        response = client_with_examples.delete(
            "/nodes/default.revenue/columns/payment_type/?dimension=default.payment_type",
        )
        data = response.json()
        assert data == {
            "message": (
                "The dimension link on the node default.revenue's payment_type to "
                "default.payment_type has been successfully removed."
            ),
        }
        response = client_with_examples.get("/nodes/default.revenue")
        data = response.json()
        assert all(col["dimension"] is None for col in data["columns"])
        response = client_with_examples.get("/history?node=default.revenue")
        assert [
            (activity["activity_type"], activity["entity_type"])
            for activity in response.json()
        ] == [("create", "node"), ("create", "link"), ("delete", "link")]

        # Removing the dimension link again will result in no change
        response = client_with_examples.delete(
            "/nodes/default.revenue/columns/payment_type/?dimension=default.payment_type",
        )
        data = response.json()
        assert response.status_code == 304
        assert data == {
            "message": "No change was made to payment_type on node default.revenue as the"
            " specified dimension link to default.payment_type on None was not found.",
        }
        # Check history again, no change
        response = client_with_examples.get("/history?node=default.revenue")
        assert [
            (activity["activity_type"], activity["entity_type"])
            for activity in response.json()
        ] == [("create", "node"), ("create", "link"), ("delete", "link")]

        # Check that the proper error is raised when the column doesn't exist
        response = client_with_examples.post(
            "/nodes/default.revenue/columns/non_existent_column/?dimension=default.payment_type",
        )
        assert response.status_code == 404
        data = response.json()
        assert data["message"] == (
            "Column non_existent_column does not exist on node default.revenue"
        )

        # Add a dimension including a specific dimension column name
        response = client_with_examples.post(
            "/nodes/default.revenue/columns/payment_type/"
            "?dimension=default.payment_type"
            "&dimension_column=payment_type_name",
        )
        assert response.status_code == 422
        data = response.json()
        assert data["message"] == (
            "The column payment_type has type int and is being linked "
            "to the dimension default.payment_type via the dimension column "
            "payment_type_name, which has type string. These column "
            "types are incompatible and the dimension cannot be linked"
        )

        response = client_with_examples.post(
            "/nodes/default.revenue/columns/payment_type/?dimension=basic.dimension.users",
        )
        data = response.json()
        assert data["message"] == (
            "Cannot add dimension to column, because catalogs do not match: default, public"
        )

    def test_update_node_with_dimension_links(self, client_with_examples: TestClient):
        """
        When a node is updated with a new query, the original dimension links and attributes
        on its columns should be preserved where possible (that is, where the new and old
        columns have the same names).
        """
        client_with_examples.patch(
            "/nodes/default.hard_hat/",
            json={
                "query": """
                SELECT
                    hard_hat_id,
                    title,
                    state
                FROM default.hard_hats
                """,
            },
        )
        response = client_with_examples.get("/history?node=default.hard_hat")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [
            ("create", "node"),
            ("set_attribute", "column_attribute"),
            ("create", "link"),
            ("update", "node"),
        ]

        response = client_with_examples.get("/nodes/default.hard_hat").json()
        assert response["columns"] == [
            {
                "name": "hard_hat_id",
                "type": "int",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
            },
            {"name": "title", "type": "string", "attributes": [], "dimension": None},
            {
                "name": "state",
                "type": "string",
                "attributes": [],
                "dimension": {"name": "default.us_state"},
            },
        ]

        # Check history of the node with column attribute set
        response = client_with_examples.get(
            "/history?node=default.hard_hat",
        )
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [
            ("create", "node"),
            ("set_attribute", "column_attribute"),
            ("create", "link"),
            ("update", "node"),
        ]

    def test_update_dimension_remove_pk_column(self, client_with_examples: TestClient):
        """
        When a dimension node is updated with a new query that removes the original primary key
        column, either a new primary key must be set or the node will be set to invalid.
        """
        response = client_with_examples.patch(
            "/nodes/default.hard_hat/",
            json={
                "query": """
                SELECT
                    title,
                    state
                FROM default.hard_hats
                """,
                # "primary_key": ["title"],
            },
        )
        assert response.json()["status"] == "invalid"
        response = client_with_examples.patch(
            "/nodes/default.hard_hat/",
            json={
                "query": """
                SELECT
                    title,
                    state
                FROM default.hard_hats
                """,
                "primary_key": ["title"],
            },
        )
        assert response.json()["status"] == "valid"

    def test_node_downstreams(self, client_with_examples: TestClient):
        """
        Test getting downstream nodes of different node types.
        """
        response = client_with_examples.get(
            "/nodes/default.event_source/downstream/?node_type=metric",
        )
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.long_events_distinct_countries",
            "default.device_ids_count",
        }

        response = client_with_examples.get(
            "/nodes/default.event_source/downstream/?node_type=transform",
        )
        data = response.json()
        assert {node["name"] for node in data} == {"default.long_events"}

        response = client_with_examples.get(
            "/nodes/default.event_source/downstream/?node_type=dimension",
        )
        data = response.json()
        assert {node["name"] for node in data} == {"default.country_dim"}

        response = client_with_examples.get("/nodes/default.event_source/downstream/")
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.long_events_distinct_countries",
            "default.device_ids_count",
            "default.long_events",
            "default.country_dim",
        }

        response = client_with_examples.get(
            "/nodes/default.device_ids_count/downstream/",
        )
        data = response.json()
        assert data == []

        response = client_with_examples.get("/nodes/default.long_events/downstream/")
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.long_events_distinct_countries",
        }

    def test_node_upstreams(self, client_with_examples: TestClient):
        """
        Test getting upstream nodes of different node types.
        """
        response = client_with_examples.get(
            "/nodes/default.long_events_distinct_countries/upstream/",
        )
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.event_source",
            "default.long_events",
        }

    def test_list_node_dag(self, client_with_examples: TestClient):
        """
        Test getting the DAG for a node
        """
        response = client_with_examples.get(
            "/nodes/default.long_events_distinct_countries/dag",
        )
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.event_source",
            "default.long_events",
            "default.long_events_distinct_countries",
        }

        response = client_with_examples.get("/nodes/default.num_repair_orders/dag")
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.dispatcher",
            "default.hard_hat",
            "default.municipality_dim",
            "default.num_repair_orders",
            "default.repair_order",
            "default.repair_orders",
            "default.us_state",
        }

    def test_revalidating_existing_nodes(self, client_with_examples: TestClient):
        """
        Test revalidating all example nodes and confirm that they are set to valid
        """
        for node in client_with_examples.get("/nodes/").json():
            status = client_with_examples.post(
                f"/nodes/{node['name']}/validate/",
            ).json()["status"]
            assert status == "valid"
        # Confirm that they still show as valid server-side
        for node in client_with_examples.get("/nodes/").json():
            assert node["status"] == "valid"


def test_node_similarity(session: Session, client: TestClient):
    """
    Test determining node similarity based on their queries
    """
    source_data = Node(
        name="source_data",
        type=NodeType.SOURCE,
        current_version="1",
    )
    source_data_rev = NodeRevision(
        node=source_data,
        version="1",
        name=source_data.name,
        type=source_data.type,
    )
    a_transform = Node(
        name="a_transform",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    a_transform_rev = NodeRevision(
        name=a_transform.name,
        node=a_transform,
        version="1",
        query="SELECT 1 as num",
        type=a_transform.type,
        columns=[
            Column(name="num", type=IntegerType()),
        ],
    )
    another_transform = Node(
        name="another_transform",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    another_transform_rev = NodeRevision(
        name=another_transform.name,
        node=another_transform,
        version="1",
        query="SELECT 1 as num",
        type=another_transform.type,
        columns=[
            Column(name="num", type=IntegerType()),
        ],
    )
    yet_another_transform = Node(
        name="yet_another_transform",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    yet_another_transform_rev = NodeRevision(
        name=yet_another_transform.name,
        node=yet_another_transform,
        version="1",
        query="SELECT 2 as num",
        type=yet_another_transform.type,
        columns=[
            Column(name="num", type=IntegerType()),
        ],
    )
    session.add(source_data_rev)
    session.add(a_transform_rev)
    session.add(another_transform_rev)
    session.add(yet_another_transform_rev)
    session.commit()

    response = client.get("/nodes/similarity/a_transform/another_transform")
    assert response.status_code == 200
    data = response.json()
    assert data["similarity"] == 1.0

    response = client.get("/nodes/similarity/a_transform/yet_another_transform")
    assert response.status_code == 200
    data = response.json()
    assert data["similarity"] == 0.7142857142857143

    response = client.get("/nodes/similarity/yet_another_transform/another_transform")
    assert response.status_code == 200
    data = response.json()
    assert data["similarity"] == 0.7142857142857143

    # Check that the proper error is raised when using a source node
    response = client.get("/nodes/similarity/a_transform/source_data")
    assert response.status_code == 409
    data = response.json()
    assert data == {
        "message": "Cannot determine similarity of source nodes",
        "errors": [],
        "warnings": [],
    }


def test_resolving_downstream_status(client_with_examples: TestClient) -> None:
    """
    Test creating and updating a source node
    """
    # Create draft transform and metric nodes with missing parents
    transform1 = {
        "name": "default.comments_by_migrated_users",
        "description": "Comments by users who have already migrated",
        "query": "SELECT id, user_id FROM default.comments WHERE text LIKE '%migrated%'",
        "mode": "draft",
    }

    transform2 = {
        "name": "default.comments_by_users_pending_a_migration",
        "description": "Comments by users who have a migration pending",
        "query": "SELECT id, user_id FROM default.comments WHERE text LIKE '%migration pending%'",
        "mode": "draft",
    }

    transform3 = {
        "name": "default.comments_by_users_partially_migrated",
        "description": "Comments by users are partially migrated",
        "query": (
            "SELECT p.id, p.user_id FROM default.comments_by_users_pending_a_migration p "
            "INNER JOIN default.comments_by_migrated_users m ON p.user_id = m.user_id"
        ),
        "mode": "draft",
    }

    transform4 = {
        "name": "default.comments_by_banned_users",
        "description": "Comments by users are partially migrated",
        "query": (
            "SELECT id, user_id FROM default.comments AS comment "
            "INNER JOIN default.banned_users AS banned_users "
            "ON comments.user_id = banned_users.banned_user_id"
        ),
        "mode": "draft",
    }

    transform5 = {
        "name": "default.comments_by_users_partially_migrated_sample",
        "description": "Sample of comments by users are partially migrated",
        "query": "SELECT id, user_id, foo FROM default.comments_by_users_partially_migrated",
        "mode": "draft",
    }

    metric1 = {
        "name": "default.number_of_migrated_users",
        "description": "Number of migrated users",
        "query": "SELECT COUNT(DISTINCT user_id) FROM default.comments_by_migrated_users",
        "mode": "draft",
    }

    metric2 = {
        "name": "default.number_of_users_with_pending_migration",
        "description": "Number of users with a migration pending",
        "query": (
            "SELECT COUNT(DISTINCT user_id) FROM "
            "default.comments_by_users_pending_a_migration"
        ),
        "mode": "draft",
    }

    metric3 = {
        "name": "default.number_of_users_partially_migrated",
        "description": "Number of users partially migrated",
        "query": "SELECT COUNT(DISTINCT user_id) FROM default.comments_by_users_partially_migrated",
        "mode": "draft",
    }

    for node, node_type in [
        (transform1, NodeType.TRANSFORM),
        (transform2, NodeType.TRANSFORM),
        (transform3, NodeType.TRANSFORM),
        (transform4, NodeType.TRANSFORM),
        (transform5, NodeType.TRANSFORM),
        (metric1, NodeType.METRIC),
        (metric2, NodeType.METRIC),
        (metric3, NodeType.METRIC),
    ]:
        response = client_with_examples.post(
            f"/nodes/{node_type.value}/",
            json=node,
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == node["name"]
        assert data["mode"] == node["mode"]
        assert data["status"] == "invalid"

    # Add the missing parent
    missing_parent_node = {
        "name": "default.comments",
        "description": "A fact table with comments",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "user_id", "type": "int"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "text", "type": "string"},
        ],
        "mode": "published",
        "catalog": "public",
        "schema_": "basic",
        "table": "comments",
    }

    response = client_with_examples.post(
        "/nodes/source/",
        json=missing_parent_node,
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == missing_parent_node["name"]
    assert data["mode"] == missing_parent_node["mode"]

    # Check that downstream nodes have now been switched to a "valid" status
    for node in [transform1, transform2, transform3, metric1, metric2, metric3]:
        response = client_with_examples.get(f"/nodes/{node['name']}/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == node["name"]
        assert data["mode"] == node["mode"]  # make sure the mode hasn't been changed
        assert (
            data["status"] == "valid"
        )  # make sure the node's status has been updated to valid

    # Check that nodes still not valid have an invalid status
    for node in [transform4, transform5]:
        response = client_with_examples.get(f"/nodes/{node['name']}/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == node["name"]
        assert data["mode"] == node["mode"]  # make sure the mode hasn't been changed
        assert data["status"] == "invalid"


def test_decompose_expression():
    """
    Verify metric expression decomposition into measures for cubes
    """
    res = decompose_expression(ast.Number(value=5.5))
    assert res == (ast.Number(value=5.5), [])

    # Decompose `avg(orders)`
    res = decompose_expression(
        ast.Function(ast.Name("avg"), args=[ast.Column(ast.Name("orders"))]),
    )
    assert str(res[0]) == "sum(orders_sum) / count(orders_count)"
    assert [measure.alias_or_name.name for measure in res[1]] == [
        "orders_sum",
        "orders_count",
    ]

    # Decompose `avg(orders) + 5.5`
    res = decompose_expression(
        ast.BinaryOp(
            left=ast.Function(ast.Name("avg"), args=[ast.Column(ast.Name("orders"))]),
            right=ast.Number(value=5.5),
            op=ast.BinaryOpKind.Plus,
        ),
    )
    assert str(res[0]) == "sum(orders_sum) / count(orders_count) + 5.5"
    assert [measure.alias_or_name.name for measure in res[1]] == [
        "orders_sum",
        "orders_count",
    ]

    # Decompose `max(avg(orders_a) + avg(orders_b))`
    res = decompose_expression(
        ast.Function(
            ast.Name("max"),
            args=[
                ast.BinaryOp(
                    op=ast.BinaryOpKind.Plus,
                    left=ast.Function(
                        ast.Name("avg"),
                        args=[ast.Column(ast.Name("orders_a"))],
                    ),
                    right=ast.Function(
                        ast.Name("avg"),
                        args=[ast.Column(ast.Name("orders_b"))],
                    ),
                ),
            ],
        ),
    )
    assert (
        str(res[0]) == "max(sum(orders_a_sum) / count(orders_a_count) "
        "+ sum(orders_b_sum) / count(orders_b_count))"
    )

    # Decompose `sum(max(orders))`
    res = decompose_expression(
        ast.Function(
            ast.Name("sum"),
            args=[
                ast.Function(
                    ast.Name("max"),
                    args=[ast.Column(ast.Name("orders"))],
                ),
            ],
        ),
    )
    assert str(res[0]) == "sum(max(orders_max))"
    assert [measure.alias_or_name.name for measure in res[1]] == ["orders_max"]

    # Decompose `(max(orders) + min(validations))/sum(total)`
    res = decompose_expression(
        ast.BinaryOp(
            left=ast.BinaryOp(
                left=ast.Function(
                    ast.Name("max"),
                    args=[ast.Column(ast.Name("orders"))],
                ),
                right=ast.Function(
                    ast.Name("min"),
                    args=[ast.Column(ast.Name("validations"))],
                ),
                op=ast.BinaryOpKind.Plus,
            ),
            right=ast.Function(ast.Name("sum"), args=[ast.Column(ast.Name("total"))]),
            op=ast.BinaryOpKind.Divide,
        ),
    )
    assert str(res[0]) == "max(orders_max) + min(validations_min) / sum(total_sum)"
    assert [measure.alias_or_name.name for measure in res[1]] == [
        "orders_max",
        "validations_min",
        "total_sum",
    ]

    # Decompose `cast(sum(coalesce(has_ordered, 0.0)) as double)/count(total)`
    res = decompose_expression(
        ast.BinaryOp(
            left=ast.Cast(
                expression=ast.Function(
                    name=ast.Name("sum"),
                    args=[
                        ast.Function(
                            ast.Name("coalesce"),
                            args=[ast.Column(ast.Name("has_ordered")), ast.Number(0.0)],
                        ),
                    ],
                ),
                data_type=types.DoubleType(),
            ),
            right=ast.Function(
                name=ast.Name("sum"),
                args=[ast.Column(ast.Name("total"))],
            ),
            op=ast.BinaryOpKind.Divide,
        ),
    )
    assert str(res[0]) == "sum(has_ordered_sum) / sum(total_sum)"
    assert [measure.alias_or_name.name for measure in res[1]] == [
        "has_ordered_sum",
        "total_sum",
    ]
