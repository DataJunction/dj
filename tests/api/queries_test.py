"""
Tests for the queries API.
"""

import json
from uuid import UUID, uuid4

import pytest
from fastapi.exceptions import HTTPException
from fastapi.testclient import TestClient
from freezegun import freeze_time
from pytest_mock import MockerFixture
from sqlmodel import Session

from datajunction.api.queries import dispatch_query
from datajunction.config import Settings
from datajunction.engine import process_query
from datajunction.models.query import (
    Database,
    Query,
    QueryCreate,
    QueryResults,
    QueryState,
    StatementResults,
)


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
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["columns"] == [{"name": "col", "type": "STRING"}]
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_multiple_statements(session: Session, client: TestClient) -> None:
    """
    Test ``POST /queries/``.
    """
    database = Database(name="test", URI="sqlite://")
    session.add(database)
    session.commit()
    session.refresh(database)

    query_create = QueryCreate(
        database_id=database.id,
        submitted_query="SELECT 1 AS col; SELECT 2 AS another_col",
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post("/queries/", data=query_create.json())
    data = response.json()

    assert response.status_code == 200
    assert data["database_id"] == 1
    assert data["catalog"] is None
    assert data["schema_"] is None
    assert data["submitted_query"] == "SELECT 1 AS col; SELECT 2 AS another_col"
    assert data["executed_query"] == "SELECT 1 AS col; SELECT 2 AS another_col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 2
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["columns"] == [{"name": "col", "type": "STRING"}]
    assert data["results"][0]["rows"] == [[1]]
    assert data["results"][1]["sql"] == "SELECT 2 AS another_col"
    assert data["results"][1]["columns"] == [{"name": "another_col", "type": "STRING"}]
    assert data["results"][1]["rows"] == [[2]]
    assert data["errors"] == []


def test_submit_query_results_backend(
    session: Session,
    settings: Settings,
    client: TestClient,
) -> None:
    """
    Test that ``POST /queries/`` stores results.
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

    cached = settings.results_backend.get(data["id"])
    assert json.loads(cached) == [
        {
            "sql": "SELECT 1 AS col",
            "columns": [{"name": "col", "type": "STRING"}],
            "rows": [[1]],
            "row_count": 1,
        },
    ]


def test_submit_query_async(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
) -> None:
    """
    Test ``POST /queries/`` on an async database.
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
    assert data["results"] == []
    assert data["errors"] == []

    # check that ``BackgroundTasks.add_task`` was called
    add_task.assert_called()
    arguments = add_task.mock_calls[0].args
    assert arguments[0] == process_query  # pylint: disable=comparison-with-callable
    assert arguments[1] == session
    assert isinstance(arguments[2], Settings)
    assert isinstance(arguments[3], Query)


def test_submit_query_async_celery(
    mocker: MockerFixture,
    session: Session,
    settings: Settings,
    client: TestClient,
) -> None:
    """
    Test ``POST /queries/`` on an async database via Celery.
    """
    settings.celery_broker = "redis://127.0.0.1:6379/0"

    dispatch_query = mocker.patch(  # pylint: disable=redefined-outer-name
        "datajunction.api.queries.dispatch_query",
    )

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

    dispatch_query.delay.assert_called_with(UUID(data["id"]))


def test_dispatch_query(
    mocker: MockerFixture,
    session: Session,
    settings: Settings,
) -> None:
    """
    Test ``dispatch_query``.
    """
    get_session = mocker.patch("datajunction.api.queries.get_session")
    get_session.return_value.__next__.return_value = (  # pylint: disable=redefined-outer-name
        session
    )

    mocker.patch("datajunction.api.queries.get_settings", return_value=settings)

    process_query = mocker.patch(  # pylint: disable=redefined-outer-name
        "datajunction.api.queries.process_query",
    )

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

    dispatch_query(query.id)

    process_query.assert_called()
    arguments = process_query.mock_calls[0].args
    assert arguments[0] == session
    assert arguments[1] == settings
    assert isinstance(arguments[2], Query)

    random_uuid = uuid4()
    with pytest.raises(HTTPException) as excinfo:
        dispatch_query(random_uuid)
    assert str(excinfo.value) == ""


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
    assert data["results"] == []
    assert data["errors"] == [
        '(sqlite3.OperationalError) near "FROM": syntax error\n'
        "[SQL: SELECT FROM]\n"
        "(Background on this error at: https://sqlalche.me/e/14/e3q8)",
    ]


def test_read_query(session: Session, settings: Settings, client: TestClient) -> None:
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

    results = QueryResults(
        __root__=[
            StatementResults(
                sql="SELECT 1",
                columns=[{"name": "col", "type": "STRING"}],
                rows=[[1]],
            ),
        ],
    )
    settings.results_backend.add(str(query.id), results.json())

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
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1"
    assert data["results"][0]["columns"] == [{"name": "col", "type": "STRING"}]
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []

    response = client.get("/queries/27289db6-a75c-47fc-b451-da59a743a168")
    assert response.status_code == 404

    response = client.get("/queries/123")
    assert response.status_code == 422


def test_read_query_no_results_backend(session: Session, client: TestClient) -> None:
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
    assert data["results"] == []
    assert data["errors"] == []

    response = client.get("/queries/27289db6-a75c-47fc-b451-da59a743a168")
    assert response.status_code == 404

    response = client.get("/queries/123")


def test_pagination(
    mocker: MockerFixture,
    session: Session,
    settings: Settings,
    client: TestClient,
) -> None:
    """
    Test paginated queries.

    The first call for paginated results should move results from the results backend to
    the cache. The second call should reuse the cache.
    """
    database = Database(name="test", URI="sqlite://")
    query = Query(
        database=database,
        submitted_query="SELECT some_col",
        executed_query="SELECT some_col",
        state=QueryState.RUNNING,
        progress=0.5,
    )
    session.add(query)
    session.commit()
    session.refresh(query)

    results = QueryResults(
        __root__=[
            StatementResults(
                sql="SELECT some_col",
                columns=[{"name": "some_col", "type": "NUMBER"}],
                rows=[[1], [2], [3], [4], [5], [6]],
                row_count=6,
            ),
        ],
    )
    settings.results_backend.add(str(query.id), results.json())

    cache = mocker.MagicMock()
    cache.has.side_effect = [False, True]
    cache.get.return_value = results.json()
    mocker.patch("datajunction.config.RedisCache", return_value=cache)
    settings.redis_cache = "dummy"

    # first request should load data into cache
    response = client.get(f"/queries/{query.id}?limit=2")
    data = response.json()
    assert data["next"] == f"http://testserver/queries/{query.id}?limit=2&offset=2"
    assert data["previous"] is None
    cache.add.assert_called()

    # second request should read from cache
    client.get(f"/queries/{query.id}?limit=2")
    cache.get.assert_called()


def test_pagination_last_page(
    mocker: MockerFixture,
    session: Session,
    settings: Settings,
    client: TestClient,
) -> None:
    """
    Test paginated queries.

    The last page should have a null `next` attribute.
    """
    database = Database(name="test", URI="sqlite://")
    query = Query(
        database=database,
        submitted_query="SELECT some_col",
        executed_query="SELECT some_Col",
        state=QueryState.RUNNING,
        progress=0.5,
    )
    session.add(query)
    session.commit()
    session.refresh(query)

    results = QueryResults(
        __root__=[
            StatementResults(
                sql="SELECT some_col",
                columns=[{"name": "some_col", "type": "NUMBER"}],
                rows=[[1], [2], [3], [4], [5], [6]],
                row_count=6,
            ),
        ],
    )
    settings.results_backend.add(str(query.id), results.json())

    cache = mocker.MagicMock()
    cache.has.side_effect = [False, True]
    cache.get.return_value = results.json()
    mocker.patch("datajunction.config.RedisCache", return_value=cache)
    settings.redis_cache = "dummy"

    response = client.get(f"/queries/{query.id}?offset=4&limit=2")
    data = response.json()
    assert data["next"] is None
    assert data["previous"] == f"http://testserver/queries/{query.id}?limit=2&offset=2"
