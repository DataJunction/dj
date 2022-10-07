from fastapi.testclient import TestClient
from freezegun import freeze_time
from sqlmodel import Session

from datajunction.models.query import Database


def test_submit_query(session: Session, client: TestClient) -> None:
    database = Database(name="test", URI="sqlite://")
    session.add(database)
    session.commit()
    session.refresh(database)

    query = """
    mutation{
        submitQuery(
            createQuery: {databaseId: 1, submittedQuery: "SELECT 1 AS col"}
      ) {
            databaseId
            catalog
            schema
            submittedQuery
            executedQuery
            scheduled
            started
            finished
            state
            progress
            errors
            results{
            _Root__{
                rows
                sql
                columns{
                    name
                    type
                }
            }
            }
        }
    }
    """

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post("/graphql", json={"query": query})

    data = response.json()["data"]["submitQuery"]

    assert data["databaseId"] == 1
    assert data["catalog"] is None
    assert data["schema"] is None
    assert data["submittedQuery"] == "SELECT 1 AS col"
    assert data["executedQuery"] == "SELECT 1 AS col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    results = data["results"]["_Root__"]
    assert len(results) == 1
    assert results[0]["sql"] == "SELECT 1 AS col"
    assert results[0]["columns"] == [{"name": "col", "type": "STR"}]
    assert results[0]["rows"] == [[1]]
    assert data["errors"] == []
