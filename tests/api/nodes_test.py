"""
Tests for the nodes API.
"""

from fastapi.testclient import TestClient
from sqlmodel import Session

from djqs.models.column import Column, ColumnType
from djqs.models.node import Node, NodeType


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
