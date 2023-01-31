"""
Tests for the nodes API.
"""
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from dj.models.column import Column, ColumnType
from dj.models.node import Node, NodeType


def test_read_nodes(session: Session, client: TestClient) -> None:
    """
    Test ``GET /nodes/``.
    """
    node1 = Node(name="not-a-metric", type=NodeType.SOURCE)
    node2 = Node(
        name="also-not-a-metric",
        query="SELECT 42 AS answer",
        type=NodeType.TRANSFORM,
        columns=[
            Column(name="answer", type=ColumnType.INT),
        ],
    )
    node3 = Node(
        name="a-metric",
        query="SELECT COUNT(*) FROM my_table",
        columns=[
            Column(name="_col0", type=ColumnType.INT),
        ],
        type=NodeType.METRIC,
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

    assert nodes["not-a-metric"]["query"] is None
    assert not nodes["not-a-metric"]["columns"]

    assert nodes["also-not-a-metric"]["query"] == "SELECT 42 AS answer"
    assert nodes["also-not-a-metric"]["columns"] == [
        {
            "name": "answer",
            "type": "INT",
        },
    ]

    assert nodes["a-metric"]["query"] == "SELECT COUNT(*) FROM my_table"
    assert nodes["a-metric"]["columns"] == [
        {
            "name": "_col0",
            "type": "INT",
        },
    ]


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
            columns=[
                Column(name="payment_id", type=ColumnType.INT),
                Column(name="payment_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="account_type", type=ColumnType.STR),
            ],
        )
        node2 = Node(
            name="large_revenue_payments_only",
            query=(
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM revenue_source WHERE payment_amount > 1000000"
            ),
            type=NodeType.TRANSFORM,
            columns=[
                Column(name="payment_id", type=ColumnType.INT),
                Column(name="payment_amount", type=ColumnType.FLOAT),
                Column(name="customer_id", type=ColumnType.INT),
                Column(name="account_type", type=ColumnType.STR),
            ],
        )
        node3 = Node(
            name="large_revenue_payments_and_business_only",
            query=(
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM revenue_source WHERE payment_amount > 1000000 "
                "AND account_type = 'BUSINESS'"
            ),
            type=NodeType.TRANSFORM,
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
        assert len(data) == 5
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
