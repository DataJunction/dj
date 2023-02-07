"""
Tests for GQL nodes.
"""

from fastapi.testclient import TestClient
from sqlmodel import Session

from dj.models.column import Column, ColumnType
from dj.models.node import Node, NodeRevision, NodeType


def test_get_nodes(session: Session, client: TestClient) -> None:
    """
    Test ``get_nodes``.
    """
    node1 = Node(
        name="not-a-metric",
        type=NodeType.SOURCE,
        current_version="1",
    )
    node_rev1 = NodeRevision(node=node1, version="1")

    node2 = Node(
        name="also-not-a-metric",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    node_rev2 = NodeRevision(
        node=node2,
        version="1",
        query="SELECT 42 AS answer",
        columns=[
            Column(name="answer", type=ColumnType.INT),
        ],
    )
    node3 = Node(name="a-metric", type=NodeType.METRIC, current_version="1")
    node_rev3 = NodeRevision(
        node=node3,
        version="1",
        query="SELECT COUNT(*) FROM my_table",
        columns=[
            Column(name="_col0", type=ColumnType.INT),
        ],
    )
    session.add(node_rev1)
    session.add(node_rev2)
    session.add(node_rev3)
    session.commit()

    query = """
    {
        getNodes{
            name
            currentVersion
            current{
                version
                query
                columns{
                    name
                    type
                }
            }
        }
    }
    """

    response = client.post("/graphql", json={"query": query})
    data = response.json()["data"]["getNodes"]

    assert len(data) == 3

    nodes = {node["name"]: node for node in data}

    assert nodes["not-a-metric"]["currentVersion"] == "1"
    assert nodes["not-a-metric"]["current"]["query"] is None
    assert not nodes["not-a-metric"]["current"]["columns"]
    assert nodes["not-a-metric"]["current"]["version"] == "1"

    assert nodes["also-not-a-metric"]["current"]["query"] == "SELECT 42 AS answer"
    assert nodes["also-not-a-metric"]["current"]["columns"] == [
        {
            "name": "answer",
            "type": "INT",
        },
    ]
    assert nodes["also-not-a-metric"]["current"]["version"] == "1"
    assert nodes["also-not-a-metric"]["currentVersion"] == "1"

    assert nodes["a-metric"]["current"]["query"] == "SELECT COUNT(*) FROM my_table"
    assert nodes["a-metric"]["current"]["columns"] == [
        {
            "name": "_col0",
            "type": "INT",
        },
    ]
    assert nodes["a-metric"]["current"]["version"] == "1"
    assert nodes["a-metric"]["currentVersion"] == "1"
