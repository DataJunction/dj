"""
Tests for the nodes API.
"""

from fastapi.testclient import TestClient
from sqlmodel import Session

from datajunction.models.node import Node


def test_read_nodes(session: Session, client: TestClient) -> None:
    """
    Test ``GET /nodes/``.
    """
    node1 = Node(name="not-a-metric")
    node2 = Node(name="also-not-a-metric", expression="SELECT 42 AS answer")
    node3 = Node(name="a-metric", expression="SELECT COUNT(*) FROM my_table")
    session.add(node1)
    session.add(node2)
    session.add(node3)
    session.commit()

    response = client.get("/nodes/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 3

    nodes = {node["name"]: node for node in data}

    assert nodes["not-a-metric"]["expression"] is None
    assert not nodes["not-a-metric"]["columns"]

    assert nodes["also-not-a-metric"]["expression"] == "SELECT 42 AS answer"
    assert nodes["also-not-a-metric"]["columns"] == [
        {"id": None, "name": "answer", "table_id": None, "type": "INT"},
    ]

    assert nodes["a-metric"]["expression"] == "SELECT COUNT(*) FROM my_table"
    assert nodes["a-metric"]["columns"] == [
        {"id": None, "name": "_col0", "table_id": None, "type": "INT"},
    ]
