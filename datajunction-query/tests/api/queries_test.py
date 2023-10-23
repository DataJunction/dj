"""
Tests for the queries API.
"""

import datetime
import json
from http import HTTPStatus
from unittest import mock

import msgpack
from fastapi.testclient import TestClient
from freezegun import freeze_time
from pytest_mock import MockerFixture
from sqlmodel import Session

from djqs.config import Settings
from djqs.engine import process_query
from djqs.models.catalog import Catalog
from djqs.models.engine import Engine, EngineType
from djqs.models.query import (
    Query,
    QueryCreate,
    QueryState,
    Results,
    StatementResults,
    decode_results,
    encode_results,
)


def test_submit_query(session: Session, client: TestClient) -> None:
    """
    Test ``POST /queries/``.
    """
    engine = Engine(
        name="test_engine",
        type=EngineType.DUCKDB,
        version="1.0",
        uri="duckdb:///:memory:",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
    )
    payload = query_create.json(by_alias=True)
    assert payload == json.dumps(
        {
            "catalog_name": "test_catalog",
            "engine_name": "test_engine",
            "engine_version": "1.0",
            "submitted_query": "SELECT 1 AS col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["columns"] == []
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_msgpack(session: Session, client: TestClient) -> None:
    """
    Test ``POST /queries/`` using msgpack.
    """
    engine = Engine(
        name="test_engine",
        type=EngineType.DUCKDB,
        version="1.0",
        uri="duckdb:///:memory:",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
    )
    payload = query_create.dict(by_alias=True)
    data = msgpack.packb(payload, default=encode_results)

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=data,
            headers={
                "Content-Type": "application/msgpack",
                "Accept": "application/msgpack; q=1.0, application/json; q=0.5",
            },
        )
    data = msgpack.unpackb(response.content, ext_hook=decode_results)

    assert response.headers.get("content-type") == "application/msgpack"
    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == datetime.datetime(2021, 1, 1)
    assert data["started"] == datetime.datetime(2021, 1, 1)
    assert data["finished"] == datetime.datetime(2021, 1, 1)
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["columns"] == []
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_errors(
    session: Session,
    client_no_config_file: TestClient,
) -> None:
    """
    Test ``POST /queries/`` with missing/invalid content type.
    """
    client = client_no_config_file
    engine = Engine(
        name="test_engine",
        type=EngineType.DUCKDB,
        version="1.0",
        uri="duckdb:///:memory:",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
    )
    payload = query_create.json(by_alias=True)

    response = client.post(
        "/queries/",
        data=payload,
        headers={"Accept": "application/json"},
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Content type must be specified"}

    response = client.post(
        "/queries/",
        data=payload,
        headers={
            "Content-Type": "application/protobuf",
            "Accept": "application/json",
        },
    )
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": "Content type not accepted: application/protobuf",
    }

    response = client.post(
        "/queries/",
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/protobuf"},
    )
    assert response.status_code == 406
    assert response.json() == {
        "detail": "Client MUST accept: application/json, application/msgpack",
    }


def test_submit_query_multiple_statements(session: Session, client: TestClient) -> None:
    """
    Test ``POST /queries/``.
    """
    engine = Engine(
        name="test_engine",
        type=EngineType.SQLALCHEMY,
        version="1.0",
        uri="duckdb:///test.db",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col; SELECT 2 AS another_col",
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=query_create.json(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1 AS col; SELECT 2 AS another_col"
    assert data["executed_query"] == "SELECT 1 AS col; SELECT 2 AS another_col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 2
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["columns"] == [{"name": "col", "type": "STR"}]
    assert data["results"][0]["rows"] == [[2]]
    assert data["errors"] == []


def test_submit_query_results_backend(
    session: Session,
    settings: Settings,
    client: TestClient,
) -> None:
    """
    Test that ``POST /queries/`` stores results.
    """
    engine = Engine(
        name="test_engine",
        type=EngineType.DUCKDB,
        version="1.0",
        uri="duckdb:///:memory:",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=query_create.json(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()
    assert data == {
        "catalog_name": "test_catalog",
        "engine_name": "test_engine",
        "engine_version": "1.0",
        "id": mock.ANY,
        "submitted_query": "SELECT 1 AS col",
        "executed_query": "SELECT 1 AS col",
        "scheduled": "2021-01-01T00:00:00",
        "started": "2021-01-01T00:00:00",
        "finished": "2021-01-01T00:00:00",
        "state": "FINISHED",
        "progress": 1.0,
        "results": [
            {
                "sql": "SELECT 1 AS col",
                "columns": [],
                "rows": [[1]],
                "row_count": 1,
            },
        ],
        "next": None,
        "previous": None,
        "errors": [],
    }
    cached = settings.results_backend.get(data["id"])
    assert json.loads(cached) == [
        {
            "sql": "SELECT 1 AS col",
            "columns": [],
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

    engine = Engine(
        name="test_engine",
        type=EngineType.SQLALCHEMY,
        version="1.0",
        uri="sqlite://",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
        async_=True,
    )

    with freeze_time("2021-01-01T00:00:00Z", auto_tick_seconds=300):
        response = client.post(
            "/queries/",
            data=query_create.json(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 201
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
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


def test_submit_query_error(session: Session, client: TestClient) -> None:
    """
    Test submitting invalid query to ``POST /queries/``.
    """
    engine = Engine(
        name="test_engine",
        type=EngineType.SQLALCHEMY,
        version="1.0",
        uri="sqlite://",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT FROM",
        async_=False,
    )

    response = client.post(
        "/queries/",
        data=query_create.json(),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT FROM"
    assert data["executed_query"] == "SELECT FROM"
    assert data["state"] == "FAILED"
    assert data["progress"] == 0.0
    assert data["results"] == []
    assert "(sqlite3.OperationalError)" in data["errors"][0]


def test_read_query(session: Session, settings: Settings, client: TestClient) -> None:
    """
    Test ``GET /queries/{query_id}``.
    """
    engine = Engine(
        name="test_engine",
        type=EngineType.SQLALCHEMY,
        version="1.0",
        uri="sqlite://",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query = Query(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1",
        executed_query="SELECT 1",
        state=QueryState.RUNNING,
        progress=0.5,
        async_=False,
    )
    session.add(query)
    session.commit()
    session.refresh(query)

    results = Results(
        __root__=[
            StatementResults(
                sql="SELECT 1",
                columns=[{"name": "col", "type": "STR"}],
                rows=[[1]],
            ),
        ],
    )
    settings.results_backend.add(str(query.id), results.json())

    response = client.get(f"/queries/{query.id}")
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1"
    assert data["executed_query"] == "SELECT 1"
    assert data["state"] == "RUNNING"
    assert data["progress"] == 0.5
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1"
    assert data["results"][0]["columns"] == [{"name": "col", "type": "STR"}]
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []

    response = client.get("/queries/27289db6-a75c-47fc-b451-da59a743a168")
    assert response.status_code == 404

    response = client.get("/queries/123")
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


def test_read_query_no_results_backend(session: Session, client: TestClient) -> None:
    """
    Test ``GET /queries/{query_id}``.
    """
    engine = Engine(
        name="test_engine",
        type=EngineType.SQLALCHEMY,
        version="1.0",
        uri="sqlite://",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query = Query(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1",
        executed_query="SELECT 1",
        state=QueryState.RUNNING,
        progress=0.5,
        async_=False,
    )
    session.add(query)
    session.commit()
    session.refresh(query)

    response = client.get(f"/queries/{query.id}")
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1"
    assert data["executed_query"] == "SELECT 1"
    assert data["state"] == "RUNNING"
    assert data["progress"] == 0.5
    assert data["results"] == []
    assert data["errors"] == []

    response = client.get("/queries/27289db6-a75c-47fc-b451-da59a743a168")
    assert response.status_code == 404

    response = client.get("/queries/123")


@mock.patch("djqs.engine.duckdb.connect")
def test_submit_duckdb_query(
    mock_duckdb_connect,
    session: Session,
    client: TestClient,
    duckdb_conn,
) -> None:
    """
    Test submitting a Spark query
    """
    mock_duckdb_connect.return_value = duckdb_conn
    engine = Engine(
        name="test_duckdb_engine",
        type=EngineType.DUCKDB,
        version="0.7.1",
        uri="duckdb:///:memory:",
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS int_col, 'a' as str_col",
    )
    payload = query_create.json(by_alias=True)
    assert payload == json.dumps(
        {
            "catalog_name": "test_catalog",
            "engine_name": "test_duckdb_engine",
            "engine_version": "0.7.1",
            "submitted_query": "SELECT 1 AS int_col, 'a' as str_col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_duckdb_engine"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["executed_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["results"][0]["columns"] == []
    assert data["results"][0]["rows"] == [[1, "a"]]
    assert data["errors"] == []


@mock.patch("djqs.engine.snowflake.connector")
def test_submit_snowflake_query(
    mock_snowflake_connect,
    session: Session,
    client: TestClient,
) -> None:
    """
    Test submitting a Snowflake query
    """
    mock_exec = mock.MagicMock()
    mock_exec.fetchall.return_value = [[1, "a"]]
    mock_cur = mock.MagicMock()
    mock_cur.execute.return_value = mock_exec
    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_snowflake_connect.connect.return_value = mock_conn

    engine = Engine(
        name="test_snowflake_engine",
        type=EngineType.SNOWFLAKE,
        version="7.37",
        uri="snowflake:///:memory:",
        extra_params={"user": "foo", "account": "bar", "database": "foobar"},
    )
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS int_col, 'a' as str_col",
    )
    payload = query_create.json(by_alias=True)
    assert payload == json.dumps(
        {
            "catalog_name": "test_catalog",
            "engine_name": "test_snowflake_engine",
            "engine_version": "7.37",
            "submitted_query": "SELECT 1 AS int_col, 'a' as str_col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_snowflake_engine"
    assert data["engine_version"] == "7.37"
    assert data["submitted_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["executed_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["results"][0]["columns"] == []
    assert data["results"][0]["rows"] == [[1, "a"]]
    assert data["errors"] == []
