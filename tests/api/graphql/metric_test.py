"""
Test for GQL metrics.
"""


from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlmodel import Session

from dj.models.node import Node, NodeRevision, NodeType
from dj.models.query import Database, QueryCreate


def test_read_metrics(session: Session, client: TestClient):
    """
    Test ``read_metrics``.
    """
    node1 = Node(name="not-a-metric", current_version="1")
    node_rev1 = NodeRevision(name=node1.name, node=node1, version="1")

    node2 = Node(name="also-not-a-metric", current_version="1")
    node_rev2 = NodeRevision(
        name=node2.name,
        node=node2,
        version="1",
        query="select 42 as foo",
    )

    node3 = Node(name="a-metric", current_version="1", type=NodeType.METRIC)
    node_rev3 = NodeRevision(
        name=node3.name,
        node=node3,
        version="1",
        query="select count(*) from a_table",
        type=NodeType.METRIC,
    )

    session.add(node_rev1)
    session.add(node_rev2)
    session.add(node_rev3)
    session.commit()

    query = """
    {
        readMetrics{
            name
            displayName
            id
        }
    }
    """
    response = client.post("/graphql", json={"query": query})
    assert response.json() == {
        "data": {
            "readMetrics": [{"id": 3, "name": "a-metric", "displayName": "A-Metric"}],
        },
    }


def test_read_metric(session: Session, client: TestClient):
    """
    Test ``read_metric``.
    """
    node1 = Node(name="not-a-metric", current_version="1")
    node_rev1 = NodeRevision(name=node1.name, node=node1, version="1")

    node2 = Node(name="also-not-a-metric", current_version="1")
    node_rev2 = NodeRevision(
        name=node2.name,
        node=node2,
        version="1",
        query="select 42 as foo",
    )

    node3 = Node(name="a-metric", current_version="1", type=NodeType.METRIC)
    node_rev3 = NodeRevision(
        name=node3.name,
        node=node3,
        version="1",
        query="select count(*) from a_table",
        type=NodeType.METRIC,
    )

    session.add(node_rev1)
    session.add(node_rev2)
    session.add(node_rev3)
    session.commit()

    query = """
    {
        readMetric(nodeName: "a-metric"){
            id,
            name,
            displayName
        }
    }
    """
    response = client.post("/graphql", json={"query": query})
    assert response.json() == {
        "data": {
            "readMetric": {"id": 3, "name": "a-metric", "displayName": "A-Metric"},
        },
    }


def test_read_metric_errors(session: Session, client: TestClient) -> None:
    """
    Test error response in ``read_metric``.
    """
    database = Database(name="test", URI="sqlite://")
    node = Node(name="a-metric", current_version="1")
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

    query = """
    {
        readMetric(nodeName: "foo"){
            id
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert (
        response_json["errors"][0]["message"]
        == "A node with name `foo` does not exist."
    )

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


def test_read_metrics_sql(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
):
    """
    Test ``read_metrics_sql``.
    """
    database = Database(name="test", URI="sqlite://")
    node = Node(name="a-metric", current_version="1", type=NodeType.METRIC)
    node_revision = NodeRevision(
        name=node.name,
        node=node,
        version="1",
        query="SELECT COUNT(*) FROM my_table",
    )
    session.add(database)
    session.add(node_revision)
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
    node = Node(name="a-metric", current_version="1")
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
