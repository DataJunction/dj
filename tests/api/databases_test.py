"""
Tests for the FastAPI application.
"""


from fastapi.testclient import TestClient
from sqlmodel import Session

from datajunction.models.database import Database


def test_read_databases(session: Session, client: TestClient) -> None:
    """
    Test ``GET /databases/``.
    """
    database = Database(
        name="gsheets",
        description="A Google Sheets connector",
        URI="gsheets://",
        read_only=True,
    )
    session.add(database)
    session.commit()

    response = client.get("/databases/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 1
    assert data[0]["name"] == "gsheets"
    assert data[0]["URI"] == "gsheets://"
    assert data[0]["description"] == "A Google Sheets connector"
    assert data[0]["read_only"] is True
