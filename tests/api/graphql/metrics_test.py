from sqlmodel import Session
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from uuid import UUID

from datajunction.models.node import Node, NodeType
from datajunction.models.query import Database, QueryCreate, QueryWithResults


def test_read_metrics(session: Session, client: TestClient):
    node1 = Node(name="not-a-metric")
    node2 = Node(name="also-not-a-metric", query="select 42 as foo")
    node3 = Node(name="a-metric", query="select count(*) from a_table", type=NodeType.METRIC)

    session.add(node1)
    session.add(node2)
    session.add(node3)
    session.commit()

    query="""
    {
        readMetrics{
            name
            id
        }
    }
    """
    response = client.post("/graphql", json={'query': query})
    assert response.json() == {'data': {'readMetrics': [{'id': 3, 'name': 'a-metric'}]}}

def test_read_metric(session: Session, client: TestClient):
    node1 = Node(name="not-a-metric")
    node2 = Node(name="also-not-a-metric", query="select 42 as foo")
    node3 = Node(name="a-metric", query="select count(*) from a_table", type=NodeType.METRIC)

    session.add(node1)
    session.add(node2)
    session.add(node3)
    session.commit()

    query="""
    {
        readMetric(nodeId: 3){
            id,
            name
        }
    }
    """
    response = client.post("/graphql", json={'query': query})
    assert response.json() == {'data': {'readMetric': {'id': 3, 'name': 'a-metric'}}} 

# def test_read_metrics_sql(
#     mocker: MockerFixture,
#     session: Session,
#     client: TestClient,
# ):

#     database = Database(id=0, name="test", URI="sqlite://")
#     node = Node(
#         name="a-metric",
#         query="SELECT COUNT(*) FROM my_table",
#         type=NodeType.METRIC,
#     )
#     session.add(database)
#     session.add(node)
#     session.execute("CREATE TABLE my_table (one TEXT)")
#     session.commit()

#     create_query = QueryCreate(
#         database_id=database.id,
#         submitted_query="SELECT COUNT(*) FROM my_table",
#     )
#     mocker.patch(
#         "datajunction.api.metrics.get_query_for_node",
#         return_value=create_query,
#     )

#     query="""
#     {
#         readMetricsSql(nodeId: 1, databaseId: 0){
#             databaseId,
#             sql
#         }
#     }
#     """
#     response = client.post("/graphql", json={'query': query})
#     assert response.json() == {"databaseId": 1, "sql": "SELECT COUNT(*) FROM my_table"}

