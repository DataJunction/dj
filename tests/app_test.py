"""
Tests for the FastAPI application.
"""

from fastapi.testclient import TestClient
from freezegun import freeze_time
from pytest_mock import MockerFixture
from sqlmodel import Session

from datajunction.app import QueryCreate, process_query
from datajunction.models import Database, Query, QueryState


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


def test_submit_query(session: Session, client: TestClient) -> None:
    """
    Test ``POST /queries/``.
    """
    database = Database(name="test", URI="sqlite://")
    session.add(database)
    session.commit()
    session.refresh(database)

    query_create = QueryCreate(
        database_id=database.id,
        submitted_query="SELECT 1 AS col",
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post("/queries/", data=query_create.json())
    data = response.json()

    assert response.status_code == 200
    assert data["database_id"] == 1
    assert data["catalog"] is None
    assert data["schema_"] is None
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert data["results"]["columns"] == [{"name": "col", "type": "STRING"}]
    assert data["results"]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_async(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
) -> None:
    """
    Test ``POST /queries/``.
    """
    add_task = mocker.patch("fastapi.BackgroundTasks.add_task")

    database = Database(name="test", URI="sqlite://", async_=True)
    session.add(database)
    session.commit()
    session.refresh(database)

    query_create = QueryCreate(
        database_id=database.id,
        submitted_query="SELECT 1 AS col",
    )

    with freeze_time("2021-01-01T00:00:00Z", auto_tick_seconds=300):
        response = client.post("/queries/", data=query_create.json())
    data = response.json()

    assert response.status_code == 201
    assert data["database_id"] == 1
    assert data["catalog"] is None
    assert data["schema_"] is None
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] is None
    assert data["scheduled"] is None
    assert data["started"] is None
    assert data["finished"] is None
    assert data["state"] == "ACCEPTED"
    assert data["progress"] == 0.0
    assert data["results"]["columns"] == []
    assert data["results"]["rows"] == []
    assert data["errors"] == []

    # check that ``BackgroundTasks.add_task`` was called
    add_task.assert_called()
    arguments = add_task.mock_calls[0].args
    assert arguments[0] == process_query  # pylint: disable=comparison-with-callable
    assert arguments[1] == session
    assert isinstance(arguments[2], Query)


def test_submit_query_error(session: Session, client: TestClient) -> None:
    """
    Test submitting invalid query to ``POST /queries/``.
    """
    database = Database(name="test", URI="sqlite://")
    session.add(database)
    session.commit()
    session.refresh(database)

    query_create = QueryCreate(
        database_id=database.id,
        submitted_query="SELECT FROM",
    )

    response = client.post("/queries/", data=query_create.json())
    data = response.json()

    assert response.status_code == 200
    assert data["database_id"] == 1
    assert data["catalog"] is None
    assert data["schema_"] is None
    assert data["submitted_query"] == "SELECT FROM"
    assert data["executed_query"] == "SELECT FROM"
    assert data["state"] == "FAILED"
    assert data["progress"] == 0.0
    assert data["results"]["columns"] == []
    assert data["results"]["rows"] == []
    assert data["errors"] == [
        '(sqlite3.OperationalError) near "FROM": syntax error\n'
        "[SQL: SELECT FROM]\n"
        "(Background on this error at: https://sqlalche.me/e/14/e3q8)",
    ]


def test_read_query(session: Session, client: TestClient) -> None:
    """
    Test ``GET /queries/{query_id}``.
    """
    database = Database(name="test", URI="sqlite://")
    query = Query(
        database=database,
        submitted_query="SELECT 1",
        executed_query="SELECT 1",
        state=QueryState.RUNNING,
        progress=0.5,
    )
    session.add(query)
    session.commit()
    session.refresh(query)

    response = client.get(f"/queries/{query.id}")
    data = response.json()

    assert response.status_code == 200
    assert data["database_id"] == 1
    assert data["catalog"] is None
    assert data["schema_"] is None
    assert data["submitted_query"] == "SELECT 1"
    assert data["executed_query"] == "SELECT 1"
    assert data["state"] == "RUNNING"
    assert data["progress"] == 0.5
    assert data["results"]["columns"] == []
    assert data["results"]["rows"] == []
    assert data["errors"] == []

    response = client.get("/queries/27289db6-a75c-47fc-b451-da59a743a168")
    assert response.status_code == 404

    response = client.get("/queries/123")
    assert response.status_code == 422
