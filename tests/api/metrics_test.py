"""
Tests for the metrics API.
"""


from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlmodel import Session

from dj.models.column import Column
from dj.models.node import Node, NodeRevision, NodeType
from dj.models.query import Database
from dj.models.table import Table
from dj.sql.parsing.backends.sqloxide import parse
from dj.typing import ColumnType


def test_read_metrics(session: Session, client: TestClient) -> None:
    """
    Test ``GET /metrics/``.
    """
    node1 = Node(
        name="not-a-metric",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    node_rev1 = NodeRevision(name=node1.name, node=node1, version=node1.current_version)

    node2 = Node(
        name="also-not-a-metric",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    node_rev2 = NodeRevision(
        name=node2.name,
        node=node2,
        query="SELECT 42",
        version="1",
    )

    node3 = Node(name="a-metric", type=NodeType.METRIC, current_version="1")
    node_rev3 = NodeRevision(
        name=node3.name,
        node=node3,
        version="1",
        query="SELECT COUNT(*) FROM my_table",
    )
    session.add(node_rev1)
    session.add(node_rev2)
    session.add(node_rev3)
    session.commit()

    response = client.get("/metrics/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "a-metric"
    assert data[0]["query"] == "SELECT COUNT(*) FROM my_table"


def test_read_metric(session: Session, client: TestClient) -> None:
    """
    Test ``GET /metric/{node_id}/``.
    """
    parent_rev = NodeRevision(
        name="parent",
        version="1",
        tables=[
            Table(
                database=Database(name="test", URI="sqlite://"),
                table="A",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="foo", type=ColumnType.FLOAT),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT),
            Column(name="foo", type=ColumnType.FLOAT),
        ],
    )
    parent_node = Node(
        name=parent_rev.name,
        type=NodeType.SOURCE,
        current_version="1",
    )
    parent_rev.node = parent_node

    child_node = Node(
        name="child",
        type=NodeType.METRIC,
        current_version="1",
    )
    child_rev = NodeRevision(
        name=child_node.name,
        node=child_node,
        version="1",
        query="SELECT COUNT(*) FROM parent",
        parents=[parent_node],
    )

    session.add(child_rev)
    session.commit()

    response = client.get("/metrics/child/")
    data = response.json()

    assert response.status_code == 200
    assert data["name"] == "child"
    assert data["query"] == "SELECT COUNT(*) FROM parent"
    assert data["dimensions"] == ["parent.ds", "parent.foo", "parent.user_id"]


def test_read_metrics_errors(session: Session, client: TestClient) -> None:
    """
    Test errors on ``GET /metrics/{node_id}/``.
    """
    database = Database(name="test", URI="sqlite://")
    node = Node(
        name="a-metric",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    node_revision = NodeRevision(
        name=node.name,
        node=node,
        version="1",
        query="SELECT 1 AS col",
    )
    session.add(database)
    session.add(node_revision)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    response = client.get("/metrics/foo")
    assert response.status_code == 404
    assert response.json() == {"detail": "Metric node not found: `foo`"}

    response = client.get("/metrics/a-metric")
    assert response.status_code == 400
    assert response.json() == {"detail": "Not a metric node: `a-metric`"}


def test_read_metrics_sql(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
) -> None:
    """
    Test ``GET /metrics/{node_id}/sql/``.
    """
    database = Database(name="test", URI="sqlite://")
    node = Node(name="a-metric", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        node=node,
        version="1",
        query="SELECT COUNT(*) FROM my_table",
        type=NodeType.METRIC,
    )
    session.add(database)
    session.add(node_revision)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    mocker.patch(
        "dj.api.helpers.build_node_for_database",
        return_value=(parse("SELECT COUNT(*) FROM my_table"), database),
    )

    response = client.get("/metrics/a-metric/sql/")
    assert response.json() == {
        "database_id": 1,
        "sql": "SELECT  COUNT(*) \n FROM my_table",
    }
