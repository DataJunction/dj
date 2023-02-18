"""
Tests for the nodes API.
"""
# pylint: disable=too-many-lines
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from dj.models import Catalog, Database, Table
from dj.models.column import Column, ColumnType
from dj.models.node import Node, NodeRevision, NodeType


def test_read_node(session: Session, client: TestClient) -> None:
    """
    Test ``GET /nodes/{node_id}``.
    """
    node = Node(name="something", type=NodeType.SOURCE, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
    )
    session.add(node_revision)
    session.commit()

    response = client.get("/nodes/something/")
    data = response.json()

    assert response.status_code == 200
    assert data["version"] == "1"
    assert data["node_id"] == 1
    assert data["node_revision_id"] == 1
    assert data["type"] == "source"

    response = client.get("/nodes/nothing/")
    data = response.json()

    assert response.status_code == 404
    assert data["message"] == "A node with name `nothing` does not exist."


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
            Column(name="answer", type=ColumnType.INT),
        ],
    )
    node3 = Node(name="a-metric", type=NodeType.METRIC, current_version="1")
    node_rev3 = NodeRevision(
        name=node3.name,
        node=node3,
        version="1",
        query="SELECT COUNT(*) FROM my_table",
        columns=[
            Column(name="_col0", type=ColumnType.INT),
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

    assert nodes["also-not-a-metric"]["query"] == "SELECT 42 AS answer"
    assert nodes["also-not-a-metric"]["display_name"] == "Also-Not-A-Metric"
    assert nodes["also-not-a-metric"]["columns"] == [
        {
            "name": "answer",
            "type": "INT",
        },
    ]

    assert nodes["a-metric"]["query"] == "SELECT COUNT(*) FROM my_table"
    assert nodes["a-metric"]["display_name"] == "A-Metric"
    assert nodes["a-metric"]["columns"] == [
        {
            "name": "_col0",
            "type": "INT",
        },
    ]


class TestCreateOrUpdateNodes:
    """
    Test ``POST /nodes/`` and ``PUT /nodes/{name}``.
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
            "columns": {
                "id": {"type": "INT"},
                "user_id": {"type": "INT", "dimension": "basic.dimension.users"},
                "timestamp": {"type": "TIMESTAMP"},
                "text": {"type": "STR"},
            },
            "mode": "published",
        }

    @pytest.fixture
    def create_dimension_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a dimension node.
        """

        return {
            "description": "Country dimension",
            "type": "dimension",
            "query": "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country",
            "mode": "published",
            "name": "countries",
        }

    @pytest.fixture
    def create_invalid_transform_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a transform node.
        """

        return {
            "name": "country_agg",
            "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM comments",
            "type": "transform",
            "mode": "published",
            "description": "Distinct users per country",
            "columns": [
                {"name": "country", "type": "STR"},
                {"name": "num_users", "type": "INT"},
            ],
        }

    @pytest.fixture
    def create_transform_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a transform node.
        """

        return {
            "name": "country_agg",
            "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "type": "transform",
            "mode": "published",
            "description": "Distinct users per country",
            "columns": [
                {"name": "country", "type": "STR"},
                {"name": "num_users", "type": "INT"},
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
                Column(name="ds", type=ColumnType.STR),
                Column(name="user_id", type=ColumnType.INT),
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
                Column(name="id", type=ColumnType.INT),
                Column(name="full_name", type=ColumnType.STR),
                Column(name="age", type=ColumnType.INT),
                Column(name="country", type=ColumnType.STR),
                Column(name="gender", type=ColumnType.STR),
                Column(name="preferred_language", type=ColumnType.STR),
            ],
        )
        session.add(node_revision)
        session.commit()
        return node

    def test_create_update_source_node(
        self,
        client: TestClient,
        database: Database,  # pylint: disable=unused-argument
        create_source_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating and updating a source node
        """

        response = client.post(
            "/nodes/",
            json=create_source_node_payload,
        )
        data = response.json()

        assert data["name"] == "comments"
        assert data["type"] == "source"
        assert data["display_name"] == "Comments"
        assert data["version"] == "v1.0"
        assert data["node_id"] == 1
        assert data["description"] == "A fact table with comments"
        assert data["query"] is None
        assert data["columns"] == [
            {"name": "id", "type": "INT"},
            {"name": "user_id", "type": "INT"},
            {"name": "timestamp", "type": "TIMESTAMP"},
            {"name": "text", "type": "STR"},
        ]

        # Trying to create it again should fail
        response = client.post(
            "/nodes/",
            json=create_source_node_payload,
        )
        data = response.json()
        assert data["message"] == "A node with name `comments` already exists."
        assert response.status_code == 409

        # Update node with a new description should create a new revision
        response = client.patch(
            f"/nodes/{create_source_node_payload['name']}/",
            json={
                "description": "New description",
                "display_name": "Comments facts",
            },
        )
        data = response.json()

        assert data["name"] == "comments"
        assert data["display_name"] == "Comments facts"
        assert data["type"] == "source"
        assert data["version"] == "v1.1"
        assert data["node_id"] == 1
        assert data["description"] == "New description"

        # Try to update node with no changes
        response = client.patch(
            f"/nodes/{create_source_node_payload['name']}/",
            json={"description": "New description", "display_name": "Comments facts"},
        )
        new_data = response.json()
        assert data == new_data

        # Try to update a node with a table that has different columns
        response = client.patch(
            f"/nodes/{create_source_node_payload['name']}/",
            json={
                "columns": {
                    "id": {"type": "INT"},
                    "user_id": {"type": "INT", "dimension": "basic.dimension.users"},
                    "timestamp": {"type": "TIMESTAMP"},
                    "text_v2": {"type": "STR"},
                },
            },
        )
        data = response.json()
        assert data["version"] == "v2.0"
        assert data["columns"] == [
            {"name": "id", "type": "INT"},
            {"name": "user_id", "type": "INT"},
            {"name": "timestamp", "type": "TIMESTAMP"},
            {"name": "text_v2", "type": "STR"},
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

        response = client.post(
            "/nodes/",
            json=create_invalid_transform_node_payload,
        )
        data = response.json()
        assert response.status_code == 500
        assert (
            data["message"]
            == "Node definition contains references to nodes that do not exist"
        )

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

        # Create a transform node
        response = client.post(
            "/nodes/",
            json=create_transform_node_payload,
        )
        data = response.json()
        assert data["name"] == "country_agg"
        assert data["display_name"] == "Country Agg"
        assert data["type"] == "transform"
        assert data["description"] == "Distinct users per country"
        assert (
            data["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )
        assert data["columns"] == [
            {"name": "country", "type": "STR"},
            {"name": "num_users", "type": "INT"},
        ]
        assert data["tables"] == []

        # Update the transform node with two minor changes
        response = client.patch(
            "/nodes/country_agg/",
            json={
                "description": "Some new description",
                "display_name": "Country Aggregation by User",
            },
        )
        data = response.json()
        assert data["name"] == "country_agg"
        assert data["display_name"] == "Country Aggregation by User"
        assert data["type"] == "transform"
        assert data["version"] == "v1.1"
        assert data["description"] == "Some new description"
        assert (
            data["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )
        assert data["tables"] == []

        # Try to update with a new query that references a non-existent source
        response = client.patch(
            "/nodes/country_agg/",
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
            "/nodes/country_agg/",
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
            {"name": "country", "type": "STR"},
            {"name": "num_users", "type": "INT"},
            {"name": "num_entries", "type": "INT"},
        ]

        # Verify that asking for revisions for a non-existent transform fails
        response = client.get("/nodes/random_transform/revisions/")
        data = response.json()
        assert data["message"] == "A node with name `random_transform` does not exist."

        # Verify that all historical revisions are available for the node
        response = client.get("/nodes/country_agg/revisions/")
        data = response.json()
        assert {rev["version"]: rev["query"] for rev in data} == {
            "v1.0": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "v1.1": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "v2.0": "SELECT country, COUNT(DISTINCT id) AS num_users, COUNT(*) AS num_entries "
            "FROM basic.source.users",
        }
        assert {rev["version"]: rev["columns"] for rev in data} == {
            "v1.0": [
                {"name": "country", "type": "STR"},
                {"name": "num_users", "type": "INT"},
            ],
            "v1.1": [
                {"name": "country", "type": "STR"},
                {"name": "num_users", "type": "INT"},
            ],
            "v2.0": [
                {"name": "country", "type": "STR"},
                {"name": "num_users", "type": "INT"},
                {"name": "num_entries", "type": "INT"},
            ],
        }

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

        response = client.post(
            "/nodes/",
            json=create_dimension_node_payload,
        )
        data = response.json()

        assert response.status_code == 200
        assert data["name"] == "countries"
        assert data["display_name"] == "Countries"
        assert data["type"] == "dimension"
        assert data["version"] == "v1.0"
        assert data["description"] == "Country dimension"
        assert (
            data["query"] == "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country"
        )
        assert data["columns"] == [
            {"name": "country", "type": "STR"},
            {"name": "user_cnt", "type": "INT"},
        ]

        # Test updating the dimension node with a new query
        response = client.patch(
            "/nodes/countries/",
            json={"query": "SELECT country FROM basic.source.users GROUP BY country"},
        )
        data = response.json()
        # Should result in a major version update due to the query change
        assert data["version"] == "v2.0"

        # The columns should have been updated
        assert data["columns"] == [{"name": "country", "type": "STR"}]

    def test_upsert_materialization_config(  # pylint: disable=too-many-arguments
        self,
        database: Database,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
        create_source_node_payload: Dict[str, Any],
        create_transform_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating & updating materialization config for a node.
        """

        # Setting the materialization config for a source node should fail
        client.post("/nodes/", json=create_source_node_payload)
        response = client.post(
            "/nodes/comments/materialization/",
            json={"engine_name": "spark", "engine_version": "2.4.4", "config": "{}"},
        )
        assert response.status_code == 400
        assert (
            response.json()["message"]
            == "Cannot set materialization config for source node `comments`!"
        )

        # Setting the materialization config for an engine that doesn't exist should fail
        client.post("/nodes/", json=create_transform_node_payload)
        response = client.post(
            "/nodes/country_agg/materialization/",
            json={"engine_name": "spark", "engine_version": "2.4.4", "config": "{}"},
        )
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Engine not found: `spark` version `2.4.4`"

        # Create the engine and check the existing transform node
        client.post("/engines/", json={"name": "spark", "version": "2.4.4"})

        response = client.get("/nodes/country_agg/")
        old_node_data = response.json()
        assert old_node_data["version"] == "v1.0"
        assert old_node_data["materialization_configs"] == []

        # Setting the materialization config should succeed with a new node revision created.
        response = client.post(
            "/nodes/country_agg/materialization/",
            json={
                "engine_name": "spark",
                "engine_version": "2.4.4",
                "config": "blahblah",
            },
        )
        data = response.json()
        assert (
            data["message"]
            == "Successfully updated materialization config for node `country_agg`"
            " and engine `spark`."
        )

        # Reading the node should yield the materialization config and new revision.
        response = client.get("/nodes/country_agg/")
        data = response.json()
        assert data["version"] == "v2.0"
        assert data["materialization_configs"] == [
            {
                "config": "blahblah",
                "engine": {"name": "spark", "uri": None, "version": "2.4.4"},
            },
        ]
        assert old_node_data["node_revision_id"] < data["node_revision_id"]

        # Setting the same config should yield a message indicating so.
        response = client.post(
            "/nodes/country_agg/materialization/",
            json={
                "engine_name": "spark",
                "engine_version": "2.4.4",
                "config": "blahblah",
            },
        )
        assert response.status_code == 204

        data = response.json()
        assert (
            data["message"]
            == "The same materialization config provided already exists for node "
            "`country_agg` so no update was performed."
        )


class TestValidateNodes:  # pylint: disable=too-many-public-methods
    """
    Test ``POST /nodes/validate/``.
    """

    @pytest.fixture
    def session(self, session: Session) -> Session:
        """
        Add nodes to facilitate testing of the validation route
        """

        node1 = Node(
            name="revenue_source",
            type=NodeType.SOURCE,
            current_version="1",
        )
        node_rev1 = NodeRevision(
            name=node1.name,
            type=node1.type,
            node=node1,
            version="1",
            columns=[
                Column(name="payment_id", type=ColumnType.INT),
                Column(name="payment_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="account_type", type=ColumnType.STR),
            ],
        )
        node2 = Node(
            name="large_revenue_payments_only",
            type=NodeType.TRANSFORM,
            current_version="1",
        )
        node_rev2 = NodeRevision(
            node=node2,
            name=node2.name,
            type=node2.type,
            version="1",
            query=(
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM revenue_source WHERE payment_amount > 1000000"
            ),
            columns=[
                Column(name="payment_id", type=ColumnType.INT),
                Column(name="payment_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="account_type", type=ColumnType.STR),
            ],
        )

        node3 = Node(
            name="large_revenue_payments_and_business_only",
            type=NodeType.TRANSFORM,
            current_version="1",
        )
        node_rev3 = NodeRevision(
            node=node3,
            name=node3.name,
            type=node3.type,
            version="1",
            query=(
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM revenue_source WHERE payment_amount > 1000000 "
                "AND account_type = 'BUSINESS'"
            ),
            columns=[
                Column(name="payment_id", type=ColumnType.INT),
                Column(name="payment_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="account_type", type=ColumnType.STR),
            ],
        )
        session.add(node_rev1)
        session.add(node_rev2)
        session.add(node_rev3)
        session.commit()
        return session

    def test_validating_a_valid_node(self, client: TestClient) -> None:
        """
        Test validating a valid node
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT payment_id FROM large_revenue_payments_only",
                "type": "transform",
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert len(data) == 6
        assert data["columns"] == [
            {
                "dimension_column": None,
                "dimension_id": None,
                "id": None,
                "name": "payment_id",
                "type": "INT",
            },
        ]
        assert data["status"] == "valid"
        assert data["node_revision"]["status"] == "valid"
        assert data["dependencies"][0]["name"] == "large_revenue_payments_only"
        assert data["message"] == "Node `foo` is valid"
        assert data["node_revision"]["id"] is None
        assert data["node_revision"]["mode"] == "published"
        assert data["node_revision"]["name"] == "foo"
        assert (
            data["node_revision"]["query"]
            == "SELECT  large_revenue_payments_only.payment_id FROM large_revenue_payments_only"
        )
        assert data["node"]["type"] == "transform"

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

        assert response.status_code == 500
        assert data == {
            "message": "Cannot resolve type of column bar.",
            "errors": [],
            "warnings": [],
        }

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
            "message": "Query parsing failed.\n"
            "\tsql parser error: Expected an SQL statement, found: SUPER",
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

        assert response.status_code == 500
        assert data == {
            "message": "Node definition contains references to nodes that do not exist",
            "errors": [
                {
                    "code": 201,
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
        assert data["message"] == "Node `foo` is valid"
        assert data["status"] == "valid"
        assert data["node"]["name"] == "foo"
        assert data["node_revision"]["mode"] == "draft"
        assert data["node_revision"]["status"] == "valid"
        assert data["columns"] == [
            {
                "id": None,
                "name": "col0",
                "type": "INT",
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
                    {"name": "payment_id", "type": "INT"},
                    {"name": "payment_amount", "type": "FLOAT"},
                    {"name": "customer_id", "type": "INT"},
                    {"name": "account_type", "type": "INT"},
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

    def test_adding_tables_to_nodes(self, session: Session, client: TestClient):
        """
        Test adding tables to existing nodes
        """
        catalog = Catalog(name="test")
        session.add(catalog)
        session.commit()

        database = Database(name="postgres", URI="postgres://")
        session.add(database)
        session.commit()

        response = client.post(
            "/nodes/",
            json={
                "columns": {
                    "payment_id": {"type": "INT"},
                    "payment_type": {"type": "INT"},
                    "payment_amount": {"type": "FLOAT"},
                    "customer_id": {"type": "INT"},
                    "account_type": {"type": "STR"},
                },
                "description": "A source table for revenue data",
                "mode": "published",
                "name": "third_party_revenue",
                "type": "source",
            },
        )
        assert response.status_code == 200

        response = client.post(
            "/nodes/third_party_revenue/table/",
            json={
                "database_name": "postgres",
                "catalog_name": "test",
                "cost": 1.0,
                "schema": "accounting",
                "table": "revenue",
                "columns": [
                    {"name": "payment_id", "type": "INT"},
                    {"name": "payment_type", "type": "INT"},
                    {"name": "payment_amount", "type": "FLOAT"},
                    {"name": "customer_id", "type": "INT"},
                    {"name": "account_type", "type": "STR"},
                ],
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data == {
            "message": "Table revenue has been successfully linked to node third_party_revenue",
        }

        # Test adding a second table
        response = client.post(
            "/nodes/third_party_revenue/table/",
            json={
                "database_name": "postgres",
                "catalog_name": "test",
                "cost": 1.0,
                "schema": "accounting",
                "table": "third_party_revenue",
                "columns": [
                    {"name": "payment_id", "type": "INT"},
                    {"name": "payment_type", "type": "INT"},
                    {"name": "payment_amount", "type": "FLOAT"},
                    {"name": "customer_id", "type": "INT"},
                    {"name": "account_type", "type": "STR"},
                ],
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data == {
            "message": (
                "Table third_party_revenue has been successfully "
                "linked to node third_party_revenue"
            ),
        }

        # Test returning a 409 when the table already exists
        response = client.post(
            "/nodes/third_party_revenue/table/",
            json={
                "database_name": "postgres",
                "catalog_name": "test",
                "cost": 1.0,
                "schema": "accounting",
                "table": "revenue",
                "columns": [
                    {"name": "payment_id", "type": "INT"},
                    {"name": "payment_type", "type": "INT"},
                    {"name": "payment_amount", "type": "FLOAT"},
                    {"name": "customer_id", "type": "INT"},
                    {"name": "account_type", "type": "STR"},
                ],
            },
        )
        assert response.status_code == 409
        data = response.json()
        assert data == {
            "message": (
                "Table revenue in database postgres in catalog test already exists "
                "for node third_party_revenue"
            ),
            "errors": [],
            "warnings": [],
        }

    def test_adding_dimensions_to_node_columns(self, client: TestClient):
        """
        Test adding tables to existing nodes
        """

        response = client.post(
            "/nodes/",
            json={
                "columns": {
                    "payment_id": {"type": "INT"},
                    "payment_type": {"type": "INT"},
                    "payment_amount": {"type": "FLOAT"},
                    "customer_id": {"type": "INT"},
                    "account_type": {"type": "STR"},
                },
                "description": "A source table for revenue data",
                "mode": "published",
                "name": "company_revenue",
                "type": "source",
            },
        )
        assert response.status_code == 200

        response = client.post(
            "/nodes/",
            json={
                "columns": {
                    "id": {"type": "INT"},
                    "payment_type_name": {"type": "STR"},
                    "payment_type_classification": {"type": "INT"},
                },
                "description": "A source table for different types of payments",
                "mode": "published",
                "name": "payment_type_table",
                "type": "source",
            },
        )
        assert response.status_code == 200

        response = client.post(
            "/nodes/",
            json={
                "description": "Payment type dimensions",
                "query": (
                    "SELECT id, payment_type_name, payment_type_classification "
                    "FROM payment_type_table"
                ),
                "mode": "published",
                "name": "payment_type",
                "type": "dimension",
            },
        )
        assert response.status_code == 200

        # Attach the payment_type dimension to the payment_type column on the company_revenue node
        response = client.post(
            "/nodes/company_revenue/columns/payment_type/?dimension=payment_type",
        )
        data = response.json()
        assert data == {
            "message": (
                "Dimension node payment_type has been successfully "
                "linked to column payment_type on node company_revenue"
            ),
        }

        # Check that the proper error is raised when the column doesn't exist
        response = client.post(
            "/nodes/company_revenue/columns/non_existent_column/?dimension=payment_type",
        )
        assert response.status_code == 404
        data = response.json()
        assert data["message"] == (
            "Column non_existent_column does not exist on node company_revenue"
        )

        # Add a dimension including a specific dimension column name
        response = client.post(
            "/nodes/company_revenue/columns/payment_type/"
            "?dimension=payment_type"
            "&dimension_column=payment_type_name",
        )
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == (
            "Dimension node payment_type has been successfully "
            "linked to column payment_type on node company_revenue"
        )

        # Check that not including the dimension defaults it to the column name
        response = client.post("/nodes/company_revenue/columns/payment_type/")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == (
            "Dimension node payment_type has been successfully "
            "linked to column payment_type on node company_revenue"
        )

    def test_node_downstreams(self, client: TestClient):
        """
        Test getting downstream nodes of different node types.
        """

        client.post(
            "/nodes/",
            json={
                "name": "event_source",
                "description": "Events",
                "type": "source",
                "columns": {
                    "event_id": {"type": "INT"},
                    "event_latency": {"type": "INT"},
                    "device_id": {"type": "INT"},
                    "country": {"type": "STR", "dimension": "countries_dim"},
                },
                "mode": "published",
            },
        )

        client.post(
            "/nodes/",
            json={
                "name": "long_events",
                "description": "High-Latency Events",
                "type": "transform",
                "query": "SELECT event_id, event_latency, device_id, country "
                "FROM event_source WHERE event_latency > 1000000",
                "mode": "published",
            },
        )

        client.post(
            "/nodes/",
            json={
                "name": "country_dim",
                "description": "Country Dimension",
                "type": "dimension",
                "query": "SELECT country, COUNT(DISTINCT event_id) AS events_cnt "
                "FROM event_source GROUP BY country",
                "mode": "published",
            },
        )

        client.post(
            "/nodes/",
            json={
                "name": "device_ids_count",
                "description": "Number of Distinct Devices",
                "type": "metric",
                "query": "SELECT COUNT(DISTINCT device_id) " "FROM event_source",
                "mode": "published",
            },
        )

        client.post(
            "/nodes/",
            json={
                "name": "long_events_distinct_countries",
                "description": "Number of Distinct Countries for Long Events",
                "type": "metric",
                "query": "SELECT COUNT(DISTINCT country) " "FROM long_events",
                "mode": "published",
            },
        )

        response = client.get("/nodes/event_source/downstream/?node_type=metric")
        data = response.json()
        assert {node["name"] for node in data} == {
            "long_events_distinct_countries",
            "device_ids_count",
        }

        response = client.get("/nodes/event_source/downstream/?node_type=transform")
        data = response.json()
        assert {node["name"] for node in data} == {"long_events"}

        response = client.get("/nodes/event_source/downstream/?node_type=dimension")
        data = response.json()
        assert {node["name"] for node in data} == {"country_dim"}

        response = client.get("/nodes/event_source/downstream/")
        data = response.json()
        assert {node["name"] for node in data} == {
            "long_events_distinct_countries",
            "device_ids_count",
            "long_events",
            "country_dim",
        }

        response = client.get("/nodes/device_ids_count/downstream/")
        data = response.json()
        assert data == []

        response = client.get("/nodes/long_events/downstream/")
        data = response.json()
        assert {node["name"] for node in data} == {"long_events_distinct_countries"}


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
            Column(name="num", type=ColumnType.INT),
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
            Column(name="num", type=ColumnType.INT),
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
            Column(name="num", type=ColumnType.INT),
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
