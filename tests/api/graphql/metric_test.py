"""
Test for GQL metrics.
"""

from uuid import UUID

from fastapi.testclient import TestClient
from freezegun import freeze_time
from pytest_mock import MockerFixture
from sqlmodel import Session

from dj.models.node import Node, NodeRevision, NodeType
from dj.models.query import Database, QueryCreate, QueryWithResults


def test_read_metrics(session: Session, client: TestClient):
    """
    Test ``read_metrics``.
    """
    ref_node1 = Node(name="not-a-metric", current_version=1)
    node1 = NodeRevision(reference_node=ref_node1, version=1)

    ref_node2 = Node(name="also-not-a-metric", current_version=1)
    node2 = NodeRevision(reference_node=ref_node2, version=1, query="select 42 as foo")

    ref_node3 = Node(name="a-metric", current_version=1, type=NodeType.METRIC)
    node3 = NodeRevision(
        reference_node=ref_node3,
        version=1,
        query="select count(*) from a_table",
        type=NodeType.METRIC,
    )

    session.add(node1)
    session.add(node2)
    session.add(node3)
    session.commit()

    query = """
    {
        readMetrics{
            name
            id
        }
    }
    """
    response = client.post("/graphql", json={"query": query})
    print("res", response.json())
    assert response.json() == {"data": {"readMetrics": [{"id": 3, "name": "a-metric"}]}}


def test_read_metric(session: Session, client: TestClient):
    """
    Test ``read_metric``.
    """
    ref_node1 = Node(name="not-a-metric", current_version=1)
    node1 = NodeRevision(reference_node=ref_node1, version=1)

    ref_node2 = Node(name="also-not-a-metric", current_version=1)
    node2 = NodeRevision(reference_node=ref_node2, version=1, query="select 42 as foo")

    ref_node3 = Node(name="a-metric", current_version=1, type=NodeType.METRIC)
    node3 = NodeRevision(
        reference_node=ref_node3,
        version=1,
        query="select count(*) from a_table",
        type=NodeType.METRIC,
    )

    session.add(node1)
    session.add(node2)
    session.add(node3)
    session.commit()

    query = """
    {
        readMetric(nodeName: "a-metric"){
            id,
            name
        }
    }
    """
    response = client.post("/graphql", json={"query": query})
    assert response.json() == {"data": {"readMetric": {"id": 3, "name": "a-metric"}}}


def test_read_metric_errors(session: Session, client: TestClient) -> None:
    """
    Test error response in ``read_metric``.
    """
    database = Database(name="test", URI="sqlite://")
    ref_node = Node(name="a-metric", current_version=1)
    node = NodeRevision(reference_node=ref_node, version=1, query="SELECT 1 AS col")
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    query = """
    {
        readMetric(nodeName: "foo"){
            id
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Metric node not found: `foo`"

    query = """
    {
        readMetric(nodeName: "a-metric"){
            id
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Not a metric node: `a-metric`"


def test_read_metrics_data(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
):
    """
    Test ``read_metrics_data``.
    """
    database = Database(name="test", URI="sqlite://")
    ref_node = Node(name="a-metric", current_version=1, type=NodeType.METRIC)
    node = NodeRevision(
        reference_node=ref_node,
        version=1,
        query="SELECT COUNT(*) FROM my_table",
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
        "dj.api.graphql.metric.get_query_for_node",
        return_value=create_query,
    )
    uuid = UUID("74099c09-91f3-4df7-be9d-96a8075ff5a8")
    save_query_and_run = mocker.patch(
        "dj.api.graphql.metric.save_query_and_run",
        return_value=QueryWithResults(
            database_id=1,
            id=uuid,
            submitted_query="SELECT COUNT(*) FROM my_table",
            results=[],
            errors=[],
        ),
    )

    query = """
    {
        readMetricsData(nodeName: "a-metric"){
            id
            submittedQuery
        }
    }
    """

    with freeze_time("2021-01-01T00:00:00Z"):
        client.post("/graphql", json={"query": query})

    save_query_and_run.assert_called()
    assert save_query_and_run.mock_calls[0].args[0] == create_query


def test_read_metrics_sql(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
):
    """
    Test ``read_metrics_sql``.
    """
    database = Database(name="test", URI="sqlite://")
    ref_node = Node(name="a-metric", current_version=1, type=NodeType.METRIC)
    node = NodeRevision(
        reference_node=ref_node,
        version=1,
        query="SELECT COUNT(*) FROM my_table",
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
        "dj.api.graphql.metric.get_query_for_node",
        return_value=create_query,
    )

    query = """
    {
        readMetricsSql(nodeName: "a-metric"){
            databaseId
            sql
        }
    }
    """

    response = client.post("/graphql", json={"query": query})
    assert response.json() == {
        "data": {
            "readMetricsSql": {"databaseId": 1, "sql": "SELECT COUNT(*) FROM my_table"},
        },
    }


def test_read_metrics_sql_errors(session: Session, client: TestClient):
    """
    Test error response in ``read_metrics_sql``.
    """

    database = Database(name="test", URI="sqlite://")
    ref_node = Node(name="a-metric", current_version=1)
    node = NodeRevision(reference_node=ref_node, version=1, query="SELECT 1 AS col")
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    query = """
    {
        readMetricsSql(nodeName: "a-metric"){
            sql
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Not a metric node: `a-metric`"
    query = """
    {
        readMetricsSql(nodeName: "a-metric"){
            sql
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Not a metric node: `a-metric`"


def test_read_metrics_data_errors(session: Session, client: TestClient):
    """
    Test error response in ``read_metrics_data``.
    """
    database = Database(name="test", URI="sqlite://")
    ref_node = Node(name="a-metric", current_version=1)
    node = NodeRevision(reference_node=ref_node, version=1, query="SELECT 1 AS col")
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    query = """
    {
        readMetricsData(nodeName: "a-metric"){
            id
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Not a metric node: `a-metric`"
    query = """
    {
        readMetricsData(nodeName: "a-metric"){
            id
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Not a metric node: `a-metric`"
