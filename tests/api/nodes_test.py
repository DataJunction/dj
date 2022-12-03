"""
Tests for the nodes API.
"""

from fastapi.testclient import TestClient
from sqlmodel import Session

from dj.api.nodes import NodeCreator
from dj.models.column import Column, ColumnType
from dj.models.node import Node, NodeEnvironment, NodeType


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


def test_create_nodes(client: TestClient) -> None:
    """
    Test ``POST /nodes/``.
    """
    # Create a source node
    source_create = {
        "name": "revenue",
        "description": "This is a source table containing revenue data about the product",
        "type": "source",
        "tables": {
            "postgres": [{"catalog": None, "schema_": "product", "table": "revenue"}],
        },
        "columns": {"country": {"type": "STR"}, "revenue": {"type": "FLOAT"}},
    }
    response = client.post("/nodes/", json=source_create)
    data = response.json()

    # Check the response after creating the source node
    assert response.status_code == 200
    assert len(data) == 8
    assert data["id"] == 1
    assert data["name"] == "revenue"
    assert (
        data["description"]
        == "This is a source table containing revenue data about the product"
    )
    assert data["type"] == "source"
    assert data["environment"] == "production"

    # Create a dimension node
    transform_create = NodeCreator(
        name="country_dim",
        description="This creates a country dimension node by pulling all distinct country names",
        type=NodeType.DIMENSION,
        query=(
            "SELECT DISTINCT country as id, SUM(revenue) as total_revenue FROM revenue GROUP BY id"
        ),
    )
    response = client.post("/nodes/", json=transform_create.dict())
    data = response.json()

    # Check the response after creating the dimension node
    assert response.status_code == 200
    assert len(data) == 8
    assert data["id"] == 2
    assert data["name"] == "country_dim"
    assert (
        data["description"]
        == "This creates a country dimension node by pulling all distinct country names"
    )
    assert data["type"] == "dimension"
    assert data["columns"] == [
        {
            "id": 1,
            "dimension_id": None,
            "dimension_column": None,
            "name": "country",
            "type": "STR",
        },
        {
            "id": 3,
            "dimension_id": None,
            "dimension_column": None,
            "name": "total_revenue",
            "type": "FLOAT",
        },
    ]

    # Create a transform node
    transform_create = NodeCreator(
        name="purchases_over_a_grand",
        description="This filters the revenue source node to only include purchases over $1,000",
        type=NodeType.TRANSFORM,
        query="SELECT country, revenue FROM revenue WHERE revenue > 1000.00",
    )
    response = client.post("/nodes/", json=transform_create.dict())
    data = response.json()

    # Check the response after creating the transform node
    assert response.status_code == 200
    assert len(data) == 8
    assert data["id"] == 3
    assert data["name"] == "purchases_over_a_grand"
    assert (
        data["description"]
        == "This filters the revenue source node to only include purchases over $1,000"
    )
    assert data["type"] == "transform"
    assert data["columns"] == [
        {
            "id": 1,
            "dimension_id": None,
            "dimension_column": None,
            "name": "country",
            "type": "STR",
        },
        {
            "id": 2,
            "dimension_id": None,
            "dimension_column": None,
            "name": "revenue",
            "type": "FLOAT",
        },
    ]

    # Create a metric node
    metric_create = NodeCreator(
        name="total_revenue_from_purchases_over_a_grand",
        description="A total of product revenue where only purchases over $1,000 are included",
        type=NodeType.METRIC,
        query="SELECT SUM(revenue) as total_revenue FROM purchases_over_a_grand",
    )
    response = client.post("/nodes/", json=metric_create.dict())
    data = response.json()

    # Check the response after creating the metric node
    assert response.status_code == 200
    assert len(data) == 8
    assert data["id"] == 4
    assert data["name"] == "total_revenue_from_purchases_over_a_grand"
    assert (
        data["description"]
        == "A total of product revenue where only purchases over $1,000 are included"
    )
    assert data["type"] == "metric"
    assert (
        data["query"]
        == "SELECT SUM(revenue) as total_revenue FROM purchases_over_a_grand"
    )
    assert data["columns"] == [
        {
            "id": 4,
            "dimension_id": None,
            "dimension_column": None,
            "name": "total_revenue",
            "type": "FLOAT",
        },
    ]


def test_raise_on_duplicate_node(client: TestClient) -> None:
    """
    Test that an error is raised when trying to add a node that already exists
    """
    # Create a source node
    node_create = {
        "name": "revenue",
        "description": "This is a source table containing revenue data about the product",
        "type": "source",
        "tables": {
            "postgres": [{"catalog": None, "schema_": "product", "table": "revenue"}],
        },
        "columns": {"country": {"type": "STR"}, "revenue": {"type": "FLOAT"}},
    }
    client.post("/nodes/", json=node_create)
    response = client.post(
        "/nodes/",
        json=node_create,
    )  # Try to add the same source node again
    data = response.json()

    assert response.status_code == 409
    assert data == {"detail": "Node already exists: revenue"}


def test_raise_when_columns_cannot_be_inferred(client: TestClient) -> None:
    """
    Test that an error is raised when the columns of a node cannot be inferred
    """
    # Create a transform node referencing a non-existent source node
    transform_create = NodeCreator(
        name="purchases_over_a_grand",
        description="This filters the revenue source node to only include purchases over $1,000",
        type=NodeType.TRANSFORM,
        query="SELECT country, revenue FROM revenue WHERE revenue > 1000.00",
    )
    response = client.post("/nodes/", json=transform_create.dict())
    data = response.json()

    assert response.status_code == 412
    assert data == {
        "detail": (
            "Cannot infer columns for purchases_over_a_grand: "
            'Unable to determine origin of column "country"'
        ),
    }


def test_not_raising_when_columns_cannot_be_inferred_in_staging(
    client: TestClient,
) -> None:
    """
    Test that an error is not raised in staging when the columns of a node cannot be inferred
    """
    # Create a transform node referencing a non-existent source node
    transform_create = NodeCreator(
        name="purchases_over_a_grand",
        description="This filters the revenue source node to only include purchases over $1,000",
        type=NodeType.TRANSFORM,
        environment=NodeEnvironment.STAGING,
        query="SELECT country, revenue FROM revenue WHERE revenue > 1000.00",
    )
    response = client.post("/nodes/", json=transform_create.dict())
    data = response.json()

    assert response.status_code == 200
    assert data["name"] == "purchases_over_a_grand"
