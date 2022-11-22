"""
Tests for GQL databases.
"""

from fastapi.testclient import TestClient
from sqlmodel import Session

from dj.models.database import Database


def test_get_databases(session: Session, client: TestClient):
    """
    Test ``get_databases``.
    """

    db1 = Database(id=1, name="db1", URI="")
    db2 = Database(id=2, name="db2", URI="")

    session.add(db1)
    session.add(db2)
    session.commit()

    query = """
    {
        getDatabases{
            id,
            name
        }
    }
    """

    response = client.post("/graphql", json={"query": query})
    assert response.json() == {
        "data": {"getDatabases": [{"id": 1, "name": "db1"}, {"id": 2, "name": "db2"}]},
    }
