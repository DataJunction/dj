from uuid import UUID

from fastapi.testclient import TestClient
from freezegun import freeze_time
from dj.models.node import Node, NodeType
from dj.models.query import Database, QueryCreate, QueryWithResults


def test_read_metrics(session: Session, client: TestClient):
    node1 = Node(name="not-a-metric")
    node2 = Node(name="also-not-a-metric", query="select 42 as foo")
    node3 = Node(
        name="a-metric",
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
    assert response.json() == {"data": {"readMetrics": [{"id": 3, "name": "a-metric"}]}}


def test_read_metric(session: Session, client: TestClient):
    node1 = Node(name="not-a-metric")
    node2 = Node(name="also-not-a-metric", query="select 42 as foo")
    node3 = Node(
        name="a-metric",
        query="select count(*) from a_table",
        type=NodeType.METRIC,
    )

    session.add(node1)
    session.add(node2)
    session.add(node3)
    session.commit()

    query = """
    {
        readMetric(nodeId: 3){
            id,
            name
        }
    }
    """
    response = client.post("/graphql", json={"query": query})
    assert response.json() == {"data": {"readMetric": {"id": 3, "name": "a-metric"}}}


def test_read_metric_errors(session: Session, client: TestClient) -> None:
    database = Database(name="test", URI="sqlite://")
    node = Node(name="a-metric", query="SELECT 1 AS col")
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    query = """
    {
        readMetric(nodeId: 2){
            id
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Metric node not found"
    query = """
    {
        readMetric(nodeId: 1){
            id
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Not a metric node"


def test_read_metrics_data(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
):
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
        readMetricsData(nodeId: 1){
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
        "dj.api.graphql.metric.get_query_for_node",
        return_value=create_query,
    )

    query = """
    {
        readMetricsSql(nodeId: 1){
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


def test_read_metrics_sql_errors(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
):

    database = Database(name="test", URI="sqlite://")
    node = Node(name="a-metric", query="SELECT 1 AS col")
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    query = """
    {
        readMetricsSql(nodeId: 2){
            sql
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Metric node not found"
    query = """
    {
        readMetricsSql(nodeId: 1){
            sql
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Not a metric node"


def test_read_metrics_data_errors(session: Session, client: TestClient):
    database = Database(name="test", URI="sqlite://")
    node = Node(name="a-metric", query="SELECT 1 AS col")
    session.add(database)
    session.add(node)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    query = """
    {
        readMetricsData(nodeId: 2){
            id
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Metric node not found"
    query = """
    {
        readMetricsData(nodeId: 1){
            id
        }
    }
    """

    response_json = client.post("/graphql", json={"query": query}).json()
    assert response_json["data"] is None
    assert response_json["errors"][0]["message"] == "Not a metric node"
