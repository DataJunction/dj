"""
Tests for the metrics API.
"""

from uuid import UUID

from fastapi.testclient import TestClient
from freezegun import freeze_time
from pytest_mock import MockerFixture
from sqlmodel import Session

from datajunction.models.column import Column
from datajunction.models.node import Node, NodeType
from datajunction.models.query import Database, QueryCreate, QueryWithResults
from datajunction.models.table import Table
from datajunction.typing import ColumnType


def test_read_metrics(session: Session, client: TestClient) -> None:
    """
    Test ``GET /metrics/``.
    """
    node1 = Node(name="not-a-metric")
    node2 = Node(name="also-not-a-metric", query="SELECT 42")
    node3 = Node(
        name="a-metric",
        query="SELECT COUNT(*) FROM my_table",
        type=NodeType.METRIC,
    )
    session.add(node1)
    session.add(node2)
    session.add(node3)
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
    parent = Node(
        name="parent",
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

    child = Node(
        name="child",
        query="SELECT COUNT(*) FROM parent",
        parents=[parent],
        type=NodeType.METRIC,
    )

    session.add(child)
    session.commit()

    response = client.get("/metrics/1/")
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
    node = Node(name="a-metric", query="SELECT 1 AS col")
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    response = client.get("/metrics/2")
    assert response.status_code == 404
    assert response.json() == {"detail": "Metric node not found"}

    response = client.get("/metrics/1")
    assert response.status_code == 400
    assert response.json() == {"detail": "Not a metric node"}


def test_read_metrics_data(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
) -> None:
    """
    Test ``GET /metrics/{node_id}/data/``.
    """
    database = Database(name="test", URI="sqlite://")
    node = Node(
        name="a-metric",
        query="SELECT COUNT(*) FROM my_table",
        type=NodeType.METRIC,
    )
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    create_query = QueryCreate(
        database_id=database.id,
        submitted_query="SELECT COUNT(*) FROM my_table",
    )
    mocker.patch(
        "datajunction.api.metrics.get_query_for_node",
        return_value=create_query,
    )
    uuid = UUID("74099c09-91f3-4df7-be9d-96a8075ff5a8")
    save_query_and_run = mocker.patch(
        "datajunction.api.metrics.save_query_and_run",
        return_value=QueryWithResults(
            database_id=1,
            id=uuid,
            submitted_query="SELECT COUNT(*) FROM my_table",
            results=[],
            errors=[],
        ),
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        client.get("/metrics/1/data/")

    save_query_and_run.assert_called()
    assert save_query_and_run.mock_calls[0].args[0] == create_query


def test_read_metrics_data_errors(session: Session, client: TestClient) -> None:
    """
    Test errors on ``GET /metrics/{node_id}/data/``.
    """
    database = Database(name="test", URI="sqlite://")
    node = Node(name="a-metric", query="SELECT 1 AS col")
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    response = client.get("/metrics/2/data/")
    assert response.status_code == 404
    assert response.json() == {"detail": "Metric node not found"}

    response = client.get("/metrics/1/data/")
    assert response.status_code == 400
    assert response.json() == {"detail": "Not a metric node"}


def test_read_metrics_sql(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
) -> None:
    """
    Test ``GET /metrics/{node_id}/sql/``.
    """
    database = Database(name="test", URI="sqlite://")
    node = Node(
        name="a-metric",
        query="SELECT COUNT(*) FROM my_table",
        type=NodeType.METRIC,
    )
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    create_query = QueryCreate(
        database_id=database.id,
        submitted_query="SELECT COUNT(*) FROM my_table",
    )
    mocker.patch(
        "datajunction.api.metrics.get_query_for_node",
        return_value=create_query,
    )

    response = client.get("/metrics/1/sql/")
    assert response.json() == {"database_id": 1, "sql": "SELECT COUNT(*) FROM my_table"}
