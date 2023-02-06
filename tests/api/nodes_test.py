"""
Tests for the nodes API.
"""
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from dj.models import Database, Table
from dj.models.column import Column, ColumnType
from dj.models.node import Node, NodeRevision, NodeType


def test_read_node(session: Session, client: TestClient) -> None:
    """
    Test ``GET /nodes/{node_id}``.
    """
    ref_node = Node(name="something", type=NodeType.SOURCE, current_version=1)
    node = NodeRevision(
        name=ref_node.name,
        type=ref_node.type,
        reference_node=ref_node,
        version=1,
    )
    session.add(node)
    session.commit()

    response = client.get("/nodes/something/")
    data = response.json()

    assert response.status_code == 200

    assert data["current"]["version"] == "1"
    assert data["current_version"] == "1"
    assert len(data["revisions"]) == 1

    response = client.get("/nodes/nothing/")
    data = response.json()

    assert response.status_code == 404
    assert data["detail"] == "Node not found: `nothing`"


def test_read_nodes(session: Session, client: TestClient) -> None:
    """
    Test ``GET /nodes/``.
    """
    ref_node1 = Node(
        name="not-a-metric",
        type=NodeType.SOURCE,
        current_version=1,
    )
    node1 = NodeRevision(
        reference_node=ref_node1,
        version=1,
        name=ref_node1.name,
        type=ref_node1.type,
    )
    ref_node2 = Node(
        name="also-not-a-metric",
        type=NodeType.TRANSFORM,
        current_version=1,
    )
    node2 = NodeRevision(
        name=ref_node2.name,
        reference_node=ref_node2,
        version=1,
        query="SELECT 42 AS answer",
        type=ref_node2.type,
        columns=[
            Column(name="answer", type=ColumnType.INT),
        ],
    )
    ref_node3 = Node(name="a-metric", type=NodeType.METRIC, current_version=1)
    node3 = NodeRevision(
        name=ref_node3.name,
        reference_node=ref_node3,
        version=1,
        query="SELECT COUNT(*) FROM my_table",
        columns=[
            Column(name="_col0", type=ColumnType.INT),
        ],
        type=ref_node3.type,
    )
    session.add(node1)
    session.add(node2)
    session.add(node3)
    session.commit()

    response = client.get("/nodes/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 3

    nodes = {node["name"]: node for node in data}
    assert nodes["not-a-metric"]["current"]["query"] is None
    assert not nodes["not-a-metric"]["current"]["columns"]

    assert nodes["also-not-a-metric"]["current"]["query"] == "SELECT 42 AS answer"
    assert nodes["also-not-a-metric"]["current"]["columns"] == [
        {
            "name": "answer",
            "type": "INT",
        },
    ]

    assert nodes["a-metric"]["current"]["query"] == "SELECT COUNT(*) FROM my_table"
    assert nodes["a-metric"]["current"]["columns"] == [
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
                "timestamp": {"type": "DATETIME"},
                "text": {"type": "STR"},
            },
            "mode": "published",
            "tables": [
                {
                    "catalog": None,
                    "schema_": "basic",
                    "table": "comments",
                    "cost": 10.0,
                    "database_name": "postgres",
                },
            ],
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
            "columns": {
                "country": {"type": "STR"},
                "num_users": {"type": "INT"},
            },
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
            "columns": {
                "country": {"type": "STR"},
                "num_users": {"type": "INT"},
            },
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
        ref_node = Node(
            name="basic.source.users",
            type=NodeType.SOURCE,
            current_version=1,
        )
        node = NodeRevision(
            reference_node=ref_node,
            name=ref_node.name,
            type=ref_node.type,
            version=1,
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
        session.add(node)
        session.commit()
        return ref_node

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
        assert data["current_version"] == "1"
        assert data["current"]["name"] == "comments"
        assert data["current"]["version"] == "1"
        assert data["current"]["reference_node_id"] == 1
        assert data["current"]["description"] == "A fact table with comments"
        assert data["current"]["query"] is None
        assert data["current"]["columns"] == [
            {"name": "id", "type": "INT"},
            {"name": "user_id", "type": "INT"},
            {"name": "timestamp", "type": "DATETIME"},
            {"name": "text", "type": "STR"},
        ]

        # Trying to create it again should fail
        response = client.post(
            "/nodes/",
            json=create_source_node_payload,
        )
        data = response.json()
        assert data["message"] == "A node with name `comments` already exists."
        assert response.status_code == 500

        # Update node with a new description should create a new revision
        response = client.patch(
            f"/nodes/{create_source_node_payload['name']}/",
            json={
                "description": "New description",
            },
        )
        data = response.json()

        assert data["name"] == "comments"
        assert data["type"] == "source"
        assert data["current_version"] == "2"
        assert data["current"]["name"] == "comments"
        assert data["current"]["version"] == "2"
        assert data["current"]["reference_node_id"] == 1
        assert data["current"]["description"] == "New description"

        # Try to update node with no changes
        response = client.patch(
            f"/nodes/{create_source_node_payload['name']}/",
            json={"description": "New description"},
        )
        new_data = response.json()
        assert data == new_data

        # Update a node with a new table should create a new revision
        response = client.patch(
            f"/nodes/{create_source_node_payload['name']}/",
            json={
                "tables": [
                    {
                        "catalog": None,
                        "schema_": "basic",
                        "table": "commentsv2",
                        "cost": 10.0,
                        "database_name": "postgres",
                    },
                ],
            },
        )
        data = response.json()
        assert data["current_version"] == "3"
        assert data["current"]["version"] == "3"
        assert data["current"]["tables"][0]["table"] == "commentsv2"

        # Try to update a node with a table that has different columns
        response = client.patch(
            f"/nodes/{create_source_node_payload['name']}/",
            json={
                "columns": {
                    "id": {"type": "INT"},
                    "user_id": {"type": "INT", "dimension": "basic.dimension.users"},
                    "timestamp": {"type": "DATETIME"},
                    "text_v2": {"type": "STR"},
                },
            },
        )
        data = response.json()
        assert data["current_version"] == "4"
        assert data["current"]["version"] == "4"
        assert data["current"]["columns"] == [
            {"name": "id", "type": "INT"},
            {"name": "user_id", "type": "INT"},
            {"name": "timestamp", "type": "DATETIME"},
            {"name": "text_v2", "type": "STR"},
        ]

    def test_create_source_node_with_unregistered_databases(
        self,
        client: TestClient,
        create_source_node_payload: Dict[str, Any],
    ) -> None:
        """
        Creating a source node with a table in an unregistered database should fail.
        """

        response = client.post(
            "/nodes/",
            json=create_source_node_payload,
        )
        data = response.json()
        assert response.status_code == 500
        assert data["message"] == "Database(s) {'postgres'} not supported."

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

        response = client.post(
            "/nodes/",
            json=create_transform_node_payload,
        )
        data = response.json()
        assert data["name"] == "country_agg"
        assert data["type"] == "transform"
        assert data["current"]["description"] == "Distinct users per country"
        assert (
            data["current"]["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )
        assert data["current"]["columns"] == [
            {"name": "country", "type": "STR"},
            {"name": "num_users", "type": "INT"},
        ]
        assert data["current"]["tables"] == []

        response = client.patch(
            "/nodes/country_agg/",
            json={"description": "Some new description"},
        )
        data = response.json()
        assert data["name"] == "country_agg"
        assert data["type"] == "transform"
        assert data["current_version"] == "2"
        assert data["current"]["version"] == "2"
        assert data["current"]["description"] == "Some new description"
        assert (
            data["current"]["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )
        assert data["current"]["tables"] == []

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
        assert data["current_version"] == "3"
        assert data["current"]["version"] == "3"
        assert (
            data["current"]["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users, "
            "COUNT(*) AS num_entries FROM basic.source.users"
        )
        assert data["current"]["columns"] == [
            {"name": "country", "type": "STR"},
            {"name": "num_users", "type": "INT"},
            {"name": "num_entries", "type": "INT"},
        ]

        # Verify that all historical revisions are available for the node
        response = client.get("/nodes/country_agg/")
        data = response.json()
        assert len(data["revisions"]) == 3
        assert {rev["version"]: rev["query"] for rev in data["revisions"]} == {
            "1": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "2": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "3": "SELECT country, COUNT(DISTINCT id) AS num_users, COUNT(*) AS num_entries "
            "FROM basic.source.users",
        }
        assert {rev["version"]: rev["columns"] for rev in data["revisions"]} == {
            "1": [
                {"name": "country", "type": "STR"},
                {"name": "num_users", "type": "INT"},
            ],
            "2": [
                {"name": "country", "type": "STR"},
                {"name": "num_users", "type": "INT"},
            ],
            "3": [
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
        assert data["type"] == "dimension"
        assert data["current_version"] == "1"
        assert data["current"]["name"] == "countries"
        assert data["current"]["description"] == "Country dimension"
        assert (
            data["current"]["query"] == "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country"
        )
        assert data["current"]["columns"] == [
            {"name": "country", "type": "STR"},
            {"name": "user_cnt", "type": "INT"},
        ]

        # Test updating the dimension node with a new query
        response = client.patch(
            "/nodes/countries/",
            json={"query": "SELECT country FROM basic.source.users GROUP BY country"},
        )
        data = response.json()
        assert data["current_version"] == "2"
        assert data["current"]["version"] == "2"

        # The columns should have been updated
        assert data["current"]["columns"] == [{"name": "country", "type": "STR"}]


class TestValidateNodes:  # pylint: disable=too-many-public-methods
    """
    Test ``POST /nodes/validate/``.
    """

    @pytest.fixture
    def session(self, session: Session) -> Session:
        """
        Add nodes to facilitate testing of the validation route
        """

        ref_node1 = Node(
            name="revenue_source",
            type=NodeType.SOURCE,
            current_version=1,
        )
        node1 = NodeRevision(
            name=ref_node1.name,
            type=ref_node1.type,
            reference_node=ref_node1,
            version=1,
            columns=[
                Column(name="payment_id", type=ColumnType.INT),
                Column(name="payment_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="account_type", type=ColumnType.STR),
            ],
        )
        ref_node2 = Node(
            name="large_revenue_payments_only",
            type=NodeType.TRANSFORM,
            current_version=1,
        )
        node2 = NodeRevision(
            reference_node=ref_node2,
            name=ref_node2.name,
            type=ref_node2.type,
            version=1,
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

        ref_node3 = Node(
            name="large_revenue_payments_and_business_only",
            type=NodeType.TRANSFORM,
            current_version=1,
        )
        node3 = NodeRevision(
            reference_node=ref_node3,
            name=ref_node3.name,
            type=ref_node3.type,
            version=1,
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
        session.add(node1)
        session.add(node2)
        session.add(node3)
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
        assert data["node"]["status"] == "valid"
        assert data["dependencies"][0]["name"] == "large_revenue_payments_only"
        assert data["message"] == "Node `foo` is valid"
        assert data["node"]["id"] is None
        assert data["node"]["mode"] == "published"
        assert data["node"]["name"] == "foo"
        assert (
            data["node"]["query"]
            == "SELECT payment_id FROM large_revenue_payments_only"
        )
        assert data["ref_node"]["type"] == "transform"

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
        assert data["ref_node"]["name"] == "foo"
        assert data["node"]["mode"] == "draft"
        assert data["node"]["status"] == "valid"
        assert data["columns"] == [
            {
                "id": None,
                "name": "_col0",
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
